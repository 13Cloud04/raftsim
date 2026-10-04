"""A replicated key-value store on top of the Raft core.

Clients tag every command with (client_id, seq). The state machine remembers
the last sequence number it executed for each client together with its result,
so a command that is retried after a timeout or a leader change takes effect
exactly once.
"""
from __future__ import annotations

from dataclasses import dataclass

from .raft import LEADER, RaftNode


@dataclass(frozen=True, slots=True)
class ClientRequest:
    client: int
    seq: int
    op: tuple  # ("get", k) | ("put", k, v) | ("append", k, v) | ("cas", k, expected, new)


@dataclass(frozen=True, slots=True)
class ClientReply:
    client: int
    seq: int
    ok: bool  # False: not the leader, try `leader_hint`
    result: object
    leader_hint: int | None


class KVStateMachine:
    def __init__(self, dedup=True):
        self.data = {}
        self.sessions = {}  # client -> (last seq applied, its result)
        self.dedup = dedup

    def apply(self, cmd):
        client, seq, op = cmd
        if self.dedup:
            last = self.sessions.get(client)
            if last is not None and seq <= last[0]:
                return last[1] if seq == last[0] else None
        result = self._execute(op)
        self.sessions[client] = (seq, result)
        return result

    def dump(self, with_sessions=True):
        """An immutable image of the whole state, sessions included: a restored
        replica must still recognise a retry of a command it has already run."""
        sessions = tuple(sorted(self.sessions.items())) if with_sessions else ()
        return (tuple(sorted(self.data.items())), sessions)

    def load(self, image):
        data, sessions = image
        self.data = {k: v for k, v in data}
        self.sessions = {c: (s[0], s[1]) for c, s in sessions}

    def _execute(self, op):
        kind, key = op[0], op[1]
        if kind == "get":
            return self.data.get(key, "")
        if kind == "put":
            self.data[key] = op[2]
            return "ok"
        if kind == "append":
            self.data[key] = self.data.get(key, "") + op[2]
            return "ok"
        if kind == "cas":
            if self.data.get(key, "") == op[2]:
                self.data[key] = op[3]
                return True
            return False
        raise ValueError(f"unknown op {kind}")


class KVServer:
    """One replica: a Raft node plus the state machine it feeds."""

    def __init__(self, node_id, peers, storage, env, *, bugs=frozenset(), on_leader=None,
                 on_apply=None, on_snapshot=None, snapshot_every=0, **raft_opts):
        self.id = node_id
        self.env = env
        self.bugs = bugs
        self.on_apply = on_apply
        self.on_snapshot = on_snapshot
        self.snapshot_every = snapshot_every  # compact once this many entries have been applied; 0 = never
        self.sm = KVStateMachine(dedup="no_dedup" not in bugs)
        if storage.snapshot is not None:
            self.sm.load(storage.snapshot)  # restart: resume from the snapshot, then replay the log
        self.raft = RaftNode(node_id, peers, storage, env, self._apply, restore_fn=self.sm.load,
                             bugs=bugs, on_leader=on_leader, **raft_opts)

    def tick(self):
        self.raft.tick()

    def on_message(self, src, m):
        if isinstance(m, ClientRequest):
            self._on_client(src, m)
        else:
            self.raft.on_message(src, m)

    def _on_client(self, src, req):
        if self.raft.role != LEADER:
            self.env.send(src, ClientReply(req.client, req.seq, False, None, self.raft.leader_id))
            return
        if "stale_read" in self.bugs and req.op[0] == "get":
            self.env.send(src, ClientReply(req.client, req.seq, True,
                                           self.sm.data.get(req.op[1], ""), self.id))
            return
        last = self.sm.sessions.get(req.client)
        if self.sm.dedup and last is not None and req.seq <= last[0]:
            if req.seq == last[0]:  # already executed: answer from the session table
                self.env.send(src, ClientReply(req.client, req.seq, True, last[1], self.id))
            return
        # Reads go through the log too. That costs a round of replication but
        # makes them linearizable without relying on clocks.
        self.raft.propose((req.client, req.seq, req.op))

    def _apply(self, index, entry):
        if self.on_apply:
            self.on_apply(self.id, index, entry)
        if entry.cmd is not None:
            result = self.sm.apply(entry.cmd)
            # Only the leader that proposed the entry answers the client.
            if self.raft.role == LEADER and entry.term == self.raft.term:
                client, seq, _ = entry.cmd
                self.env.send(client, ClientReply(client, seq, True, result, self.id))
        if self.snapshot_every and index - self.raft.st.base >= self.snapshot_every \
                and index == self.raft.commit_index:
            image = self.sm.dump(with_sessions="snapshot_drops_sessions" not in self.bugs)
            self.raft.compact(image)
            if self.on_snapshot:
                self.on_snapshot(self.id, self.raft.st.base, image)
