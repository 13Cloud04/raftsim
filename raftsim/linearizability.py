"""Linearizability checker for key-value histories.

A history is linearizable if every operation can be assigned a single instant
between its invocation and its response such that, replayed in that order on
one machine, every response is what the client actually saw.

Algorithm: Wing & Gong's search with Lowe's memoisation (the approach used by
Porcupine and Knossos). Operations on different keys are independent, so each
key is checked separately. An operation whose response was never observed
(`ret is None`) may have taken effect at any time after it was invoked, or
never; it is modelled as returning at infinity with any result allowed.
"""
from __future__ import annotations

from dataclasses import dataclass

UNKNOWN = object()


@dataclass(slots=True)
class Op:
    client: int
    kind: str  # get | put | append | cas
    key: str
    a: object = None
    b: object = None
    call: float = 0.0
    ret: float | None = None
    out: object = UNKNOWN


def _step(state, op):
    """Sequential specification for one key. Returns (legal, new_state)."""
    if op.kind == "get":
        return (op.out is UNKNOWN or op.out == state), state
    if op.kind == "put":
        return True, op.a
    if op.kind == "append":
        return True, state + op.a
    if op.kind == "cas":
        hit = state == op.a
        if op.out is not UNKNOWN and op.out != hit:
            return False, state
        return True, (op.b if hit else state)
    raise ValueError(op.kind)


class _Node:
    __slots__ = ("op", "id", "is_call", "match", "prev", "next")

    def __init__(self, op, op_id, is_call):
        self.op, self.id, self.is_call = op, op_id, is_call
        self.match = self.prev = self.next = None


def _check_key(ops):
    inf = float("inf")
    events = []
    for i, op in enumerate(ops):
        events.append((op.call, 0, i))
        events.append((inf if op.ret is None else op.ret, 1, i))
    # At equal timestamps a call sorts before a return, which treats the two
    # operations as concurrent: the permissive, sound choice.
    events.sort()

    head = _Node(None, -1, False)
    tail = head
    calls = {}
    for _, kind, i in events:
        node = _Node(ops[i], i, kind == 0)
        if kind == 0:
            calls[i] = node
        else:
            calls[i].match = node
        node.prev, tail.next = tail, node
        tail = node

    def lift(call):
        ret = call.match
        call.prev.next = call.next
        call.next.prev = call.prev
        ret.prev.next = ret.next
        if ret.next:
            ret.next.prev = ret.prev

    def unlift(call):
        ret = call.match
        ret.prev.next = ret
        if ret.next:
            ret.next.prev = ret
        call.prev.next = call
        call.next.prev = call

    state, done = "", 0
    seen = set()
    stack = []
    entry = head.next
    while head.next is not None:
        if entry.is_call:
            ok, new_state = _step(state, entry.op)
            if ok:
                key = (done | (1 << entry.id), new_state)
                if key not in seen:
                    seen.add(key)
                    stack.append((entry, state))
                    state, done = new_state, key[0]
                    lift(entry)
                    entry = head.next
                    continue
            entry = entry.next
        else:
            # Reached a response whose operation is not yet linearized: every
            # candidate before it has been tried, so undo the last choice.
            if not stack:
                return False
            entry, state = stack.pop()
            done &= ~(1 << entry.id)
            unlift(entry)
            entry = entry.next
    return True


def check(history):
    """Returns (True, None) or (False, key) naming a key whose history is not linearizable."""
    by_key = {}
    for op in history:
        by_key.setdefault(op.key, []).append(op)
    for key in sorted(by_key):
        if not _check_key(by_key[key]):
            return False, key
    return True, None
