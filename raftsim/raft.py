"""The Raft core: leader election, log replication, commitment.

This module does no I/O and reads no clock. Everything it needs from the
outside world comes through `env`:

    env.now()          -> current time in milliseconds
    env.random()       -> float in [0, 1)
    env.send(dst, msg) -> hand a message to the transport

That is what makes the same code runnable under the simulator (virtual time,
seeded randomness, an adversarial network) and on real sockets (`net.py`).

`bugs` switches on deliberately broken behaviour. Each flag reintroduces a
classic Raft implementation mistake so the test harness can prove that it
would have caught it. See BUGS below.
"""
from __future__ import annotations

from dataclasses import dataclass

FOLLOWER, PRECANDIDATE, CANDIDATE, LEADER = "follower", "precandidate", "candidate", "leader"

BUGS = {
    "commit_old_term": "leader commits entries from earlier terms by counting replicas (Raft paper, Figure 8)",
    "forget_vote": "votedFor is not persisted, so a restarted node can vote twice in one term",
    "skip_log_check": "votes are granted without checking the candidate's log is up to date",
    "no_truncate": "follower keeps a conflicting suffix instead of truncating it",
    "ack_whole_log": "follower acknowledges its whole log instead of only what the leader just sent",
    "no_dedup": "state machine re-applies a retried client command",
    "stale_read": "leader answers reads from local state without going through the log",
    "resend_on_dup": "leader resends on every reply, even a duplicate, so messages multiply (found by this simulator in an early version)",
    "skip_fsync": "votes and log entries are acknowledged before they are written to stable storage",
    "snapshot_wrong_index": "a snapshot is labelled with the last log index instead of the last applied index",
    "snapshot_drops_sessions": "snapshots leave out the client session table, so a retried command runs twice after a restore",
    "stale_snapshot": "a follower installs a snapshot older than what it has already committed, discarding its log",
}


@dataclass(frozen=True, slots=True)
class Entry:
    term: int
    cmd: tuple | None  # None is the no-op a new leader appends


@dataclass(frozen=True, slots=True)
class RequestVote:
    term: int
    candidate: int
    last_index: int
    last_term: int


@dataclass(frozen=True, slots=True)
class VoteReply:
    term: int
    granted: bool


@dataclass(frozen=True, slots=True)
class PreVote:
    term: int  # the term the sender WOULD start; nobody's term changes because of this message
    candidate: int
    last_index: int
    last_term: int


@dataclass(frozen=True, slots=True)
class PreVoteReply:
    term: int      # the replier's actual term
    for_term: int  # echoes PreVote.term so stale replies can be told apart
    granted: bool


@dataclass(frozen=True, slots=True)
class AppendEntries:
    term: int
    leader: int
    prev_index: int
    prev_term: int
    entries: tuple
    commit: int


@dataclass(frozen=True, slots=True)
class AppendReply:
    term: int
    success: bool
    index: int  # success: highest index known to match; failure: where the leader should retry from


@dataclass(frozen=True, slots=True)
class InstallSnapshot:
    term: int
    leader: int
    last_index: int  # the snapshot replaces every entry up to and including this one
    last_term: int
    data: tuple      # opaque state-machine image


class MemoryStorage:
    """Durable state: what survives a crash.

    The log is addressed by absolute index. Entries up to `base` have been
    folded into `snapshot` and discarded; `log[0]` is a sentinel standing for
    entry `base`, so entry i lives at `log[i - base]`.

    This class also models an honest disk for the simulator: anything written
    since the last sync() is lost by crash(). Without that, a node that forgot
    to fsync before replying would pass every test.
    """

    def __init__(self):
        self.term = 0
        self.voted_for = None
        self.base = 0
        self.snapshot = None
        self.log = [Entry(0, None)]
        self._durable_meta = (0, None)
        self._durable_len = 1
        self._shadow = None  # copy of the durable log prefix, made only if it gets overwritten

    # -- reads
    @property
    def last_index(self):
        return self.base + len(self.log) - 1

    def term_at(self, index):
        return self.log[index - self.base].term

    def entry(self, index):
        return self.log[index - self.base]

    def slice(self, lo, hi):
        return tuple(self.log[lo - self.base: hi - self.base])

    # -- writes
    def set_meta(self, term, voted_for):
        self.term = term
        self.voted_for = voted_for

    def append(self, entry):
        self.log.append(entry)

    def truncate(self, from_index):
        at = from_index - self.base
        if at < self._durable_len and self._shadow is None:
            self._shadow = self.log[:self._durable_len]
        del self.log[at:]

    def sync(self):
        self._durable_meta = (self.term, self.voted_for)
        self._durable_len = len(self.log)
        self._shadow = None

    def compact(self, index, snapshot):
        """Fold entries up to `index` into `snapshot`. Atomic and durable."""
        term = self.term_at(index)
        self.log = [Entry(term, None)] + self.log[index - self.base + 1:]
        self.base, self.snapshot = index, snapshot
        self.sync()

    def install(self, index, term, snapshot, keep_suffix):
        """Replace the log prefix with a snapshot received from the leader."""
        suffix = self.log[index - self.base + 1:] if keep_suffix else []
        self.log = [Entry(term, None)] + suffix
        self.base, self.snapshot = index, snapshot
        self.sync()

    def crash(self):
        """Power loss: everything not synced is gone."""
        if self._shadow is not None:
            self.log = self._shadow
        else:
            del self.log[self._durable_len:]
        self.term, self.voted_for = self._durable_meta
        self._shadow = None


class RaftNode:
    def __init__(self, node_id, peers, storage, env, apply_fn, *, restore_fn=None, bugs=frozenset(),
                 on_leader=None, election_timeout=(150.0, 300.0), heartbeat=50.0, max_batch=64,
                 pre_vote=True):
        self.id = node_id
        self.peers = list(peers)
        self.st = storage
        self.env = env
        self.apply_fn = apply_fn
        self.restore_fn = restore_fn
        self.bugs = bugs
        self.on_leader = on_leader
        self.election_timeout = election_timeout
        self.heartbeat = heartbeat
        self.max_batch = max_batch
        self.pre_vote = pre_vote
        self.leader_contact = -1e18  # when we last heard from a legitimate leader

        # Volatile state: rebuilt from scratch after a restart.
        self.role = FOLLOWER
        self.leader_id = None
        self.commit_index = storage.base  # everything inside the snapshot is committed and applied
        self.last_applied = storage.base
        self.votes = set()
        self.next_index = {}
        self.match_index = {}
        self.heartbeat_due = 0.0
        self.election_due = 0.0

        if "forget_vote" in bugs:
            self.st.voted_for = None
        self._reset_election_timer()

    # ----------------------------------------------------------- helpers

    @property
    def term(self):
        return self.st.term

    @property
    def last_index(self):
        return self.st.last_index

    def _sync(self):
        """Nothing may be acknowledged to another node until this returns."""
        if "skip_fsync" not in self.bugs:
            self.st.sync()

    @property
    def quorum(self):
        return (len(self.peers) + 1) // 2 + 1

    def _reset_election_timer(self):
        lo, hi = self.election_timeout
        self.election_due = self.env.now() + lo + self.env.random() * (hi - lo)

    def _step_down(self, term):
        if term > self.st.term:
            self.st.set_meta(term, None)
            self.leader_id = None
        self.role = FOLLOWER

    # ------------------------------------------------------------ driving

    def tick(self):
        now = self.env.now()
        if self.role == LEADER:
            if now >= self.heartbeat_due:
                self._broadcast()
        elif now >= self.election_due:
            if self.pre_vote:
                self._start_pre_vote()
            else:
                self._start_election()

    def propose(self, cmd):
        """Append a command to the log. Returns its index, or None if not leader."""
        if self.role != LEADER:
            return None
        self.st.append(Entry(self.st.term, cmd))
        self._sync()
        self._broadcast()
        self._advance_commit()
        return self.last_index

    def on_message(self, src, m):
        if isinstance(m, AppendEntries):
            self._on_append(src, m)
        elif isinstance(m, AppendReply):
            self._on_append_reply(src, m)
        elif isinstance(m, RequestVote):
            self._on_request_vote(src, m)
        elif isinstance(m, VoteReply):
            self._on_vote_reply(src, m)
        elif isinstance(m, InstallSnapshot):
            self._on_install_snapshot(src, m)
        elif isinstance(m, PreVote):
            self._on_pre_vote(src, m)
        elif isinstance(m, PreVoteReply):
            self._on_pre_vote_reply(src, m)

    def compact(self, image):
        """Called by the state machine owner: `image` is its state after last_applied."""
        index = self.last_index if "snapshot_wrong_index" in self.bugs else self.last_applied
        if index <= self.st.base:
            return
        self.st.compact(index, image)
        self.commit_index = max(self.commit_index, index)
        self.last_applied = max(self.last_applied, index)

    # ----------------------------------------------------------- election

    # PreVote (Ongaro's thesis, section 9.6). A node cut off from the cluster
    # times out again and again; with plain Raft each timeout bumps its term,
    # and when the partition heals that inflated term deposes a perfectly good
    # leader. With PreVote the node first asks, without changing any state,
    # whether it could win. It only starts a real election if a majority says yes.

    def _start_pre_vote(self):
        self.role = PRECANDIDATE
        self.leader_id = None
        self.votes = {self.id}
        self._reset_election_timer()
        req = PreVote(self.st.term + 1, self.id, self.last_index, self.st.term_at(self.last_index))
        for p in self.peers:
            self.env.send(p, req)
        if len(self.votes) >= self.quorum:
            self._start_election()

    def _on_pre_vote(self, src, m):
        up_to_date = (m.last_term, m.last_index) >= (self.st.term_at(self.last_index), self.last_index)
        # Refuse while we still believe there is a live leader.
        leader_is_quiet = self.env.now() - self.leader_contact >= self.election_timeout[0]
        granted = m.term > self.st.term and up_to_date and leader_is_quiet and self.role != LEADER
        self.env.send(src, PreVoteReply(self.st.term, m.term, granted))

    def _on_pre_vote_reply(self, src, m):
        if m.term > self.st.term:
            self._step_down(m.term)
            return
        if self.role != PRECANDIDATE or m.for_term != self.st.term + 1 or not m.granted:
            return
        self.votes.add(src)
        if len(self.votes) >= self.quorum:
            self._start_election()

    def _start_election(self):
        self.st.set_meta(self.st.term + 1, self.id)
        self._sync()
        self.role = CANDIDATE
        self.leader_id = None
        self.votes = {self.id}
        self._reset_election_timer()
        req = RequestVote(self.st.term, self.id, self.last_index, self.st.term_at(self.last_index))
        for p in self.peers:
            self.env.send(p, req)
        if len(self.votes) >= self.quorum:
            self._become_leader()

    def _on_request_vote(self, src, m):
        if m.term > self.st.term:
            self._step_down(m.term)
        granted = False
        if m.term == self.st.term and self.st.voted_for in (None, m.candidate):
            my_last_term = self.st.term_at(self.last_index)
            up_to_date = (m.last_term, m.last_index) >= (my_last_term, self.last_index)
            if up_to_date or "skip_log_check" in self.bugs:
                granted = True
                self.st.set_meta(self.st.term, m.candidate)
                self._sync()
                self._reset_election_timer()
        self.env.send(src, VoteReply(self.st.term, granted))

    def _on_vote_reply(self, src, m):
        if m.term > self.st.term:
            self._step_down(m.term)
            return
        if self.role != CANDIDATE or m.term != self.st.term or not m.granted:
            return
        self.votes.add(src)
        if len(self.votes) >= self.quorum:
            self._become_leader()

    def _become_leader(self):
        self.role = LEADER
        self.leader_id = self.id
        self.next_index = {p: self.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        if self.on_leader:
            self.on_leader(self)
        # A no-op in the new term lets the leader commit everything before it
        # without ever counting replicas for an old-term entry.
        self.st.append(Entry(self.st.term, None))
        self._sync()
        self._broadcast()
        self._advance_commit()

    # -------------------------------------------------------- replication

    def _broadcast(self):
        self.heartbeat_due = self.env.now() + self.heartbeat
        for p in self.peers:
            self._send_append(p)

    def _send_append(self, peer):
        prev = self.next_index[peer] - 1
        if prev < self.st.base:
            # The entries this follower needs have been compacted away.
            self.env.send(peer, InstallSnapshot(self.st.term, self.id, self.st.base,
                                                self.st.term_at(self.st.base), self.st.snapshot))
            return
        entries = self.st.slice(prev + 1, prev + 1 + self.max_batch)
        self.env.send(peer, AppendEntries(self.st.term, self.id, prev, self.st.term_at(prev),
                                          entries, self.commit_index))

    def _accept_leader(self, src, m):
        """Common preamble for messages only a leader sends. False if the sender is stale."""
        if m.term > self.st.term:
            self._step_down(m.term)
        if m.term < self.st.term:
            self.env.send(src, AppendReply(self.st.term, False, 0))
            return False
        # m.term == our term: there is a legitimate leader for this term.
        self.role = FOLLOWER
        self.leader_id = m.leader
        self.leader_contact = self.env.now()
        self._reset_election_timer()
        return True

    def _on_append(self, src, m):
        if not self._accept_leader(src, m):
            return
        st = self.st
        prev, entries = m.prev_index, m.entries
        if prev > self.last_index:
            self.env.send(src, AppendReply(st.term, False, self.last_index + 1))
            return
        if prev < st.base:
            # The start of this batch is already inside our snapshot. Those
            # entries are committed, hence identical; skip past them.
            entries = entries[st.base - prev:]
            prev = st.base
        elif st.term_at(prev) != m.prev_term:
            # Skip back over the whole conflicting term in one round trip.
            bad_term = st.term_at(prev)
            i = prev
            while i > st.base + 1 and st.term_at(i - 1) == bad_term:
                i -= 1
            self.env.send(src, AppendReply(st.term, False, i))
            return

        for k, entry in enumerate(entries):
            idx = prev + 1 + k
            if idx <= self.last_index:
                if st.term_at(idx) == entry.term:
                    continue
                if "no_truncate" in self.bugs:
                    continue
                st.truncate(idx)
            st.append(entry)
        self._sync()

        # Only entries the leader actually sent are known to match. Anything
        # beyond them may be a stale suffix from an older leader.
        matched = m.prev_index + len(m.entries)
        if "ack_whole_log" in self.bugs:
            matched = self.last_index
        new_commit = min(m.commit, matched)
        if new_commit > self.commit_index:
            self.commit_index = new_commit
            self._apply_committed()
        self.env.send(src, AppendReply(st.term, True, matched))

    def _on_install_snapshot(self, src, m):
        if not self._accept_leader(src, m):
            return
        st = self.st
        stale = m.last_index <= self.commit_index
        if stale and "stale_snapshot" not in self.bugs:
            # A delayed or duplicated snapshot: we are already past it.
            self.env.send(src, AppendReply(st.term, True, m.last_index))
            return
        # Keep whatever follows the snapshot if our log agrees with it there.
        keep = (not stale and st.base <= m.last_index <= self.last_index
                and st.term_at(m.last_index) == m.last_term)
        st.install(m.last_index, m.last_term, m.data, keep)
        self.commit_index = self.last_applied = m.last_index
        if self.restore_fn:
            self.restore_fn(m.data)
        self.env.send(src, AppendReply(st.term, True, m.last_index))

    def _on_append_reply(self, src, m):
        if m.term > self.st.term:
            self._step_down(m.term)
            return
        if self.role != LEADER or m.term != self.st.term:
            return
        amplify = "resend_on_dup" in self.bugs
        if m.success:
            progressed = m.index > self.match_index[src]
            if progressed:
                self.match_index[src] = m.index
                self.next_index[src] = max(self.next_index[src], m.index + 1)
                self._advance_commit()
            # Keep streaming to a follower that is behind, but only in response
            # to a reply that moved it forward. The network may duplicate
            # replies; answering each copy with a new request would make the
            # number of messages in flight grow geometrically.
            if (progressed or amplify) and self.next_index[src] <= self.last_index:
                self._send_append(src)
        else:
            # Never move next_index below what the follower has already confirmed.
            retry = max(1, min(m.index, self.next_index[src] - 1))
            retry = max(retry, self.match_index[src] + 1)
            moved = retry != self.next_index[src]
            self.next_index[src] = retry
            if moved or amplify:
                self._send_append(src)

    def _advance_commit(self):
        for n in range(self.last_index, self.commit_index, -1):
            if self.st.term_at(n) != self.st.term and "commit_old_term" not in self.bugs:
                break  # older-term entries commit only indirectly, via a current-term entry
            replicas = 1 + sum(1 for p in self.peers if self.match_index[p] >= n)
            if replicas >= self.quorum:
                self.commit_index = n
                self._apply_committed()
                break

    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.apply_fn(self.last_applied, self.st.entry(self.last_applied))
