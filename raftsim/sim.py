"""Deterministic simulation of a Raft cluster under faults.

One seed fully determines a run: the cluster size, every message delay, drop
and duplicate, every partition, crash and restart, every client operation.
A failure therefore reproduces exactly from its seed number.

While the run executes, the simulator (which can see every node at once)
checks Raft's safety properties after each step, and at the end it checks that
the history clients observed is linearizable and that the cluster recovered
once the faults stopped.
"""
from __future__ import annotations

import hashlib
import heapq
import random
from collections import deque
from dataclasses import dataclass, field

from .kv import ClientReply, ClientRequest, KVServer, KVStateMachine
from .linearizability import Op, check
from .raft import LEADER, InstallSnapshot, MemoryStorage

CLIENT_BASE = 1000  # node ids are 0..n-1, client ids start here


class InvariantViolation(Exception):
    def __init__(self, invariant, detail):
        super().__init__(f"{invariant}: {detail}")
        self.invariant = invariant
        self.detail = detail


@dataclass
class Result:
    seed: int
    ok: bool
    invariant: str | None = None
    detail: str | None = None
    trace_hash: str = ""
    stats: dict = field(default_factory=dict)
    tail: list = field(default_factory=list)


class _Env:
    def __init__(self, sim, nid):
        self.sim, self.nid = sim, nid

    def now(self):
        return self.sim.time * self.sim.clock_rate[self.nid]

    def random(self):
        return self.sim.rng.random()

    def send(self, dst, msg):
        self.sim.send(self.nid, dst, msg)


class _Client:
    def __init__(self, cid):
        self.id = cid
        self.seq = 0
        self.op = None       # Op currently in flight
        self.attempt = 0
        self.target = None
        self.last_seen = {}  # key -> last value read, used to make CAS sometimes succeed


class Simulation:
    TICK = 10.0
    CLIENT_TIMEOUT = 300.0
    QUIET = 30000.0  # fault-free time at the end in which the cluster must recover (the run stops early once it has)
    MAX_PENDING = 5000  # more undelivered events than this means messages are multiplying

    def __init__(self, seed, bugs=(), duration=8000.0, keep_trace=0, pre_vote=None):
        self.seed = seed
        self.rng = random.Random(seed)
        self.bugs = frozenset(bugs)
        rng = self.rng

        self.n = rng.choice((3, 3, 5, 5, 5, 7))
        self.drop = rng.choice((0.0, 0.0, 0.02, 0.05, 0.15))
        self.dup = rng.choice((0.0, 0.0, 0.02, 0.1))
        self.slow = rng.choice((0.0, 0.02, 0.1))  # chance a message takes up to 300 ms
        self.max_down = rng.choice((self.n // 2, self.n // 2, self.n))
        self.clock_rate = [rng.uniform(0.9, 1.1) for _ in range(self.n)]
        self.keys = [f"k{i}" for i in range(rng.choice((1, 2, 3)))]
        n_clients = rng.choice((2, 3, 4, 5))
        # Small batches make followers acknowledge old-term entries on their own,
        # which is the situation the Figure 8 commit rule exists for.
        self.max_batch = rng.choice((1, 2, 64))
        # Swarm testing: each seed also draws how hostile the run is. A narrow
        # election timeout produces split votes; a fast nemesis lands faults
        # in the middle of elections and replication instead of between them.
        self.election_timeout = rng.choice(((150.0, 300.0), (150.0, 300.0), (150.0, 165.0)))
        self.nemesis_gap = rng.choice(((50.0, 1200.0), (50.0, 1200.0), (5.0, 150.0)))
        # Aggressive compaction, so lagging followers regularly need a snapshot.
        self.snapshot_every = rng.choice((0, 6, 25))
        # PreVote is drawn per seed too. With it always on, two of the planted
        # election bugs all but disappeared: an optional protocol feature can
        # hide a bug in the path it bypasses, so both paths have to be exercised.
        drawn = rng.choice((True, False))
        self.pre_vote = drawn if pre_vote is None else pre_vote

        self.time = 0.0
        self.fault_end = duration
        self.end = duration + self.QUIET
        self.queue = []
        self.counter = 0
        self.blocked = set()  # directed (src, dst) node links that drop everything
        self.healed = False

        self.storage = [MemoryStorage() for _ in range(self.n)]
        self.nodes = [None] * self.n
        for i in range(self.n):
            self._boot(i)
        self.clients = {CLIENT_BASE + i: _Client(CLIENT_BASE + i) for i in range(n_clients)}
        self.history = []

        # What the omniscient checker remembers.
        self.leaders = {}    # term -> node id
        self.committed = {}  # index -> (Entry, term in which it was committed)

        self.hash = hashlib.blake2b(digest_size=16)
        self.tail = deque(maxlen=keep_trace) if keep_trace else None
        self.stats = dict(events=0, delivered=0, dropped=0, elections=0, crashes=0,
                          partitions=0, ops=0, max_term=0, snapshots=0, installs=0)

        for i in range(self.n):
            self._at(rng.uniform(0, self.TICK), "tick", i)
        for cid in self.clients:
            self._at(rng.uniform(0, 100), "client_next", cid)
        self._at(rng.uniform(100, 600), "nemesis")
        self._at(self.fault_end, "heal_everything")

    # ------------------------------------------------------------ plumbing

    def _at(self, t, kind, *args):
        self.counter += 1
        heapq.heappush(self.queue, (t, self.counter, kind, args))

    def _boot(self, i):
        peers = [p for p in range(self.n) if p != i]
        self.nodes[i] = KVServer(i, peers, self.storage[i], _Env(self, i), bugs=self.bugs,
                                 on_leader=self._on_leader, on_apply=self._on_apply,
                                 on_snapshot=self._on_snapshot, snapshot_every=self.snapshot_every,
                                 max_batch=self.max_batch, election_timeout=self.election_timeout,
                                 pre_vote=self.pre_vote)

    def send(self, src, dst, msg):
        rng = self.rng
        if (src, dst) in self.blocked or rng.random() < self.drop:
            self.stats["dropped"] += 1
            return
        copies = 2 if rng.random() < self.dup else 1
        for _ in range(copies):
            delay = rng.uniform(1.0, 12.0)
            if rng.random() < self.slow:
                delay += rng.uniform(0.0, 300.0)
            self._at(self.time + delay, "deliver", src, dst, msg)

    # --------------------------------------------------- invariant checks

    def _on_leader(self, raft):
        self.stats["elections"] += 1
        self.stats["max_term"] = max(self.stats["max_term"], raft.term)
        other = self.leaders.setdefault(raft.term, raft.id)
        if other != raft.id:
            raise InvariantViolation("ElectionSafety",
                                     f"nodes {other} and {raft.id} both became leader in term {raft.term}")
        # Raft promises a committed entry appears in the log of every leader of
        # a LATER term. A candidate can still win an old term after the rest of
        # the cluster has moved on (its last vote arrives late); that leader is
        # harmless and owes nothing to entries committed after its term.
        st = raft.st
        for index, (entry, commit_term) in self.committed.items():
            if commit_term >= raft.term or index <= st.base:
                continue  # entries inside the snapshot are covered by the SnapshotIntegrity check
            if index > st.last_index or st.entry(index) != entry:
                raise InvariantViolation(
                    "LeaderCompleteness",
                    f"node {raft.id} won term {raft.term} without committed entry {index}")

    def _on_snapshot(self, nid, index, image):
        """A snapshot at `index` must equal the state reached by applying
        exactly the committed entries 1..index, sessions included."""
        self.stats["snapshots"] += 1
        ref = KVStateMachine(dedup="no_dedup" not in self.bugs)
        for i in range(1, index + 1):
            if i not in self.committed:
                raise InvariantViolation(
                    "SnapshotIntegrity", f"node {nid} snapshotted through index {index}, but {i} is not committed")
            cmd = self.committed[i][0].cmd
            if cmd is not None:
                ref.apply(cmd)
        if ref.dump() != image:
            raise InvariantViolation(
                "SnapshotIntegrity", f"node {nid}'s snapshot at index {index} differs from the committed history")

    def _on_apply(self, nid, index, entry):
        # The leader applies an entry the moment it commits it, so the first
        # node to apply is the committing leader and its term is the commit term.
        first, _ = self.committed.setdefault(index, (entry, self.storage[nid].term))
        if first != entry:
            raise InvariantViolation(
                "StateMachineSafety",
                f"node {nid} applied {entry} at index {index}, but {first} was applied there earlier")

    def _check_log_matching(self):
        for a in range(self.n):
            for b in range(a + 1, self.n):
                sa, sb = self.storage[a], self.storage[b]
                lo = max(sa.base, sb.base) + 1  # compare only what both still hold
                # Find the highest index where the terms agree; everything up to it must be identical.
                for i in range(min(sa.last_index, sb.last_index), lo - 1, -1):
                    if sa.term_at(i) == sb.term_at(i):
                        if sa.slice(lo, i + 1) != sb.slice(lo, i + 1):
                            raise InvariantViolation(
                                "LogMatching", f"nodes {a} and {b} agree on the term at index {i} but differ before it")
                        break

    def _final_checks(self):
        self._check_log_matching()
        stuck = [c.id for c in self.clients.values() if c.op is not None]
        if stuck:
            raise InvariantViolation(
                "Liveness", f"clients {stuck} still waiting {self.QUIET:.0f} ms after all faults were healed")
        applied = {n.raft.last_applied for n in self.nodes}
        states = {repr(sorted(n.sm.data.items())) for n in self.nodes}
        if len(applied) != 1 or len(states) != 1:
            raise InvariantViolation("Convergence", f"replicas did not converge: applied indexes {sorted(applied)}")
        ok, key = check(self.history)
        if not ok:
            raise InvariantViolation("Linearizability", f"history for key {key!r} has no valid linearization")

    # -------------------------------------------------------------- events

    def _ev_tick(self, i):
        node = self.nodes[i]
        if node is not None:
            node.tick()
        self._at(self.time + self.TICK, "tick", i)

    def _ev_deliver(self, src, dst, msg):
        if dst >= CLIENT_BASE:
            self._client_reply(self.clients[dst], msg)
            return
        node = self.nodes[dst]
        if node is None:
            self.stats["dropped"] += 1
            return
        self.stats["delivered"] += 1
        if isinstance(msg, InstallSnapshot):
            self.stats["installs"] += 1
        node.on_message(src, msg)

    def _ev_heal_everything(self):
        self.healed = True
        self.blocked.clear()
        self.drop = self.dup = self.slow = 0.0
        for i in range(self.n):
            if self.nodes[i] is None:
                self._boot(i)
        self._at(self.time + 100.0, "check_recovered")

    def _ev_check_recovered(self):
        if self._recovered():
            self.end = self.time  # nothing left to wait for
        else:
            self._at(self.time + 100.0, "check_recovered")

    def _recovered(self):
        if any(c.op is not None for c in self.clients.values()):
            return False
        top = max(s.last_index for s in self.storage)
        return all(n.raft.last_applied == top for n in self.nodes)

    def _ev_nemesis(self):
        if self.healed:
            return
        rng = self.rng
        up = [i for i in range(self.n) if self.nodes[i] is not None]
        down = [i for i in range(self.n) if self.nodes[i] is None]
        leaders = [i for i in up if self.nodes[i].raft.role == LEADER]
        action = rng.choice(("partition", "partition", "isolate_leader", "isolate_leader", "one_way",
                             "heal", "heal", "crash", "crash", "crash_leader", "restart", "restart", "restart",
                             "bounce", "bounce"))
        if action == "partition":
            order = list(range(self.n))
            rng.shuffle(order)
            cut = rng.randrange(1, self.n)
            a, b = order[:cut], order[cut:]
            self.blocked = {(x, y) for x in a for y in b} | {(y, x) for x in a for y in b}
            self.stats["partitions"] += 1
        elif action == "isolate_leader" and leaders:
            v = rng.choice(leaders)
            self.blocked = {(v, o) for o in range(self.n) if o != v} | {(o, v) for o in range(self.n) if o != v}
            self.stats["partitions"] += 1
        elif action == "one_way":
            a, b = rng.sample(range(self.n), 2)
            self.blocked.add((a, b))
            self.stats["partitions"] += 1
        elif action == "heal":
            self.blocked.clear()
        elif action in ("crash", "crash_leader") and up and len(down) < self.max_down:
            pool = leaders if action == "crash_leader" and leaders else up
            victim = rng.choice(pool)
            self.nodes[victim] = None    # volatile state is gone
            self.storage[victim].crash()  # and so is anything not yet synced to disk
            self.stats["crashes"] += 1
        elif action == "restart" and down:
            self._boot(rng.choice(down))
        elif action == "bounce" and up:
            # Kill and immediately restart: the node keeps its disk but forgets
            # everything else, possibly in the middle of an election.
            victim = rng.choice(up)
            self.storage[victim].crash()
            self._boot(victim)
            self.stats["crashes"] += 1
        self._at(self.time + rng.uniform(*self.nemesis_gap), "nemesis")

    # ------------------------------------------------------------- clients

    def _ev_client_next(self, cid):
        if self.time >= self.fault_end:
            return  # stop issuing new work; in-flight operations still finish
        c, rng = self.clients[cid], self.rng
        c.seq += 1
        key = rng.choice(self.keys)
        r = rng.random()
        if r < 0.35:
            op = Op(cid, "get", key)
        elif r < 0.55:
            op = Op(cid, "put", key, f"<{cid}.{c.seq}>")
        elif r < 0.90:
            op = Op(cid, "append", key, f"[{cid}.{c.seq}]")
        else:
            op = Op(cid, "cas", key, c.last_seen.get(key, ""), f"<{cid}.{c.seq}>")
        op.call = self.time
        c.op = op
        self.history.append(op)
        self._client_send(c)

    def _client_send(self, c):
        if c.target is None:
            c.target = self.rng.randrange(self.n)
        c.attempt += 1
        op = c.op
        payload = (op.kind, op.key) if op.kind == "get" else \
            (op.kind, op.key, op.a) if op.kind != "cas" else (op.kind, op.key, op.a, op.b)
        self.send(c.id, c.target, ClientRequest(c.id, c.seq, payload))
        self._at(self.time + self.CLIENT_TIMEOUT, "client_timeout", c.id, c.seq, c.attempt)

    def _ev_client_timeout(self, cid, seq, attempt):
        c = self.clients[cid]
        if c.op is None or c.seq != seq or c.attempt != attempt:
            return
        c.target = None  # no answer: try someone else, same sequence number
        self._client_send(c)

    def _ev_client_retry(self, cid, seq, attempt):
        c = self.clients[cid]
        if c.op is not None and c.seq == seq and c.attempt == attempt:
            self._client_send(c)

    def _client_reply(self, c, m):
        if not isinstance(m, ClientReply) or c.op is None or m.seq != c.seq:
            return  # late or duplicated reply to an operation that already finished
        if not m.ok:
            c.target = m.leader_hint
            self._at(self.time + 20.0, "client_retry", c.id, c.seq, c.attempt)
            return
        op = c.op
        op.ret, op.out = self.time, m.result
        if op.kind == "get":
            c.last_seen[op.key] = m.result
        c.op = None
        self.stats["ops"] += 1
        self._at(self.time + self.rng.uniform(0, 60), "client_next", c.id)

    # ----------------------------------------------------------------- run

    def run(self):
        res = Result(self.seed, True)
        try:
            while self.queue:
                t, _, kind, args = heapq.heappop(self.queue)
                if t > self.end:
                    break
                self.time = t
                line = f"{t:.3f} {kind} {args!r}"
                self.hash.update(line.encode())
                if self.tail is not None:
                    self.tail.append(line)
                self.stats["events"] += 1
                getattr(self, "_ev_" + kind)(*args)
                if len(self.queue) > self.MAX_PENDING:
                    raise InvariantViolation("MessageStorm", f"{len(self.queue)} events in flight")
            self._final_checks()
        except InvariantViolation as v:
            res.ok, res.invariant, res.detail = False, v.invariant, f"t={self.time:.1f}ms {v.detail}"
        except Exception as e:  # a replica blew up: with an injected bug that is a detection too
            res.ok, res.invariant, res.detail = False, "NodeCrash", f"t={self.time:.1f}ms {type(e).__name__}: {e}"
        res.trace_hash = self.hash.hexdigest()
        res.stats = dict(self.stats, nodes=self.n, clients=len(self.clients), term_sum=self.stats["max_term"])
        res.tail = list(self.tail) if self.tail is not None else []
        return res


def run_seed(seed, bugs=(), duration=8000.0, keep_trace=0, pre_vote=None):
    return Simulation(seed, bugs, duration, keep_trace, pre_vote).run()


def job(args):
    """Picklable worker for multiprocessing: (seed, bugs, duration) -> plain tuple."""
    import signal

    seed, bugs, duration, *rest = args
    pre_vote = rest[0] if rest else None

    def on_alarm(*_):
        raise TimeoutError

    signal.signal(signal.SIGALRM, on_alarm)
    signal.alarm(60)
    try:
        r = run_seed(seed, bugs, duration, pre_vote=pre_vote)
    except TimeoutError:
        return seed, False, "Timeout", "simulation did not finish in 60 s of wall time", {}
    finally:
        signal.alarm(0)
    return r.seed, r.ok, r.invariant, r.detail, r.stats
