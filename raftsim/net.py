"""Run the same Raft core on real TCP sockets with a write-ahead log on disk.

Nothing in raft.py or kv.py changes: this file only supplies a different
`env` (wall clock, real randomness, sockets) and a durable Storage.

Wire format: one JSON array per line, [sender_id, type_name, *fields].
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import time

from . import raft as R
from .kv import ClientReply, ClientRequest, KVServer
from .sim import CLIENT_BASE

# ------------------------------------------------------------------ codec


def _tup(x):
    return tuple(_tup(i) for i in x) if isinstance(x, list) else x


def encode(sender, m):
    name = type(m).__name__
    if name == "AppendEntries":
        body = [m.term, m.leader, m.prev_index, m.prev_term, [[e.term, e.cmd] for e in m.entries], m.commit]
    elif name == "ClientRequest":
        body = [m.client, m.seq, m.op]
    else:
        body = [getattr(m, f) for f in m.__slots__]
    return (json.dumps([sender, name, *body], separators=(",", ":")) + "\n").encode()


def decode(line):
    sender, name, *body = json.loads(line)
    if name == "AppendEntries":
        body[4] = tuple(R.Entry(t, _tup(c)) for t, c in body[4])
        return sender, R.AppendEntries(*body)
    if name == "ClientRequest":
        return sender, ClientRequest(body[0], body[1], _tup(body[2]))
    if name == "ClientReply":
        return sender, ClientReply(*body)
    if name == "InstallSnapshot":
        body[4] = _tup(body[4])
    return sender, getattr(R, name)(*body)


# ---------------------------------------------------------------- storage


def _write_atomically(path, text):
    """Readers see the old file or the new one, never a half-written one."""
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class DiskStorage(R.MemoryStorage):
    """Three files in one directory:

        meta.json      [term, voted_for]                      rewritten atomically
        snapshot.json  {index, term, image}                   rewritten atomically
        log.jsonl      {"base": n} then one entry per line    appended, fsynced on sync()

    Compaction writes the snapshot first and the shortened log second. If the
    process dies in between, the log on disk still starts before the snapshot;
    load() notices and skips the entries the snapshot already covers.

    (On macOS, os.fsync reaches the drive's cache, not the platter; F_FULLFSYNC
    would be needed for power-loss safety.)
    """

    def __init__(self, path):
        super().__init__()
        os.makedirs(path, exist_ok=True)
        self.meta_path = os.path.join(path, "meta.json")
        self.snap_path = os.path.join(path, "snapshot.json")
        self.log_path = os.path.join(path, "log.jsonl")
        self.f = None
        if os.path.exists(self.meta_path):
            with open(self.meta_path) as f:
                self.term, self.voted_for = json.load(f)
        if os.path.exists(self.snap_path):
            with open(self.snap_path) as f:
                snap = json.load(f)
            self.base, self.snapshot = snap["index"], _tup(snap["image"])
            self.log = [R.Entry(snap["term"], None)]
        if os.path.exists(self.log_path):
            entries, log_base = [], 0
            with open(self.log_path, "rb") as f:
                for n, line in enumerate(f):
                    if not line.endswith(b"\n"):
                        break  # torn final write from a crash: discard it
                    if n == 0:
                        log_base = json.loads(line)["base"]
                        continue
                    t, c = json.loads(line)
                    entries.append(R.Entry(t, _tup(c)))
            if log_base > self.base:
                raise RuntimeError(f"{path}: log starts at {log_base} but the snapshot ends at {self.base}")
            self.log += entries[self.base - log_base:]
        self._rewrite_log()

    def _line(self, entry):
        return (json.dumps([entry.term, entry.cmd], separators=(",", ":")) + "\n").encode()

    def _rewrite_log(self):
        if self.f:
            self.f.close()
        tmp = self.log_path + ".tmp"
        header = (json.dumps({"base": self.base}) + "\n").encode()
        self.offsets = [0]  # offsets[k] = byte offset of the k-th entry after the snapshot (k >= 1)
        self.end = len(header)
        with open(tmp, "wb") as f:
            f.write(header)
            for entry in self.log[1:]:
                data = self._line(entry)
                self.offsets.append(self.end)
                self.end += len(data)
                f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.log_path)
        self.f = open(self.log_path, "ab")
        self.dirty = False

    def _write_snapshot(self, index, term, image):
        _write_atomically(self.snap_path, json.dumps({"index": index, "term": term, "image": image}))

    def set_meta(self, term, voted_for):
        super().set_meta(term, voted_for)
        _write_atomically(self.meta_path, json.dumps([term, voted_for]))

    def append(self, entry):
        super().append(entry)
        data = self._line(entry)
        self.offsets.append(self.end)
        self.end += len(data)
        self.f.write(data)
        self.dirty = True

    def truncate(self, from_index):
        at = from_index - self.base
        super().truncate(from_index)
        self.end = self.offsets[at]
        del self.offsets[at:]
        self.f.flush()
        self.f.truncate(self.end)
        self.dirty = True

    def sync(self):
        if self.dirty:
            self.f.flush()
            os.fsync(self.f.fileno())
            self.dirty = False

    def compact(self, index, snapshot):
        self._write_snapshot(index, self.term_at(index), snapshot)
        super().compact(index, snapshot)
        self._rewrite_log()

    def install(self, index, term, snapshot, keep_suffix):
        self._write_snapshot(index, term, snapshot)
        super().install(index, term, snapshot, keep_suffix)
        self._rewrite_log()


# ----------------------------------------------------------------- server


class _NetEnv:
    def __init__(self, server):
        self.server = server

    def now(self):
        return time.monotonic() * 1000.0

    def random(self):
        return random.random()

    def send(self, dst, msg):
        self.server.send(dst, msg)


class Server:
    def __init__(self, node_id, addrs, data_dir, snapshot_every=1000):
        self.id = node_id
        self.addrs = addrs
        self.peer_writers = {}  # node id -> StreamWriter (outgoing)
        self.connecting = set()
        self.client_writers = {}  # client id -> StreamWriter (the connection it came in on)
        peers = [i for i in range(len(addrs)) if i != node_id]
        self.node = KVServer(node_id, peers, DiskStorage(data_dir), _NetEnv(self), snapshot_every=snapshot_every)

    def send(self, dst, msg):
        if dst >= CLIENT_BASE:
            w = self.client_writers.get(dst)
        else:
            w = self.peer_writers.get(dst)
            if w is None and dst not in self.connecting:
                self.connecting.add(dst)
                asyncio.ensure_future(self._connect(dst))
        if w is None or w.is_closing():
            return  # not connected: the message is lost, which Raft tolerates
        w.write(encode(self.id, msg))

    async def _connect(self, dst):
        try:
            host, port = self.addrs[dst]
            _, w = await asyncio.open_connection(host, port)
            self.peer_writers[dst] = w
        except OSError:
            await asyncio.sleep(0.05)
        finally:
            self.connecting.discard(dst)

    async def _serve(self, reader, writer):
        try:
            while line := await reader.readline():
                src, msg = decode(line)
                if src >= CLIENT_BASE:
                    self.client_writers[src] = writer
                self.node.on_message(src, msg)
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async def run(self):
        host, port = self.addrs[self.id]
        srv = await asyncio.start_server(self._serve, host, port)
        st = self.node.raft.st
        print(f"node {self.id} listening on {host}:{port}: snapshot through index {st.base}, "
              f"{st.last_index - st.base} log entries after it", flush=True)
        async with srv:
            role = None
            while True:
                self.node.tick()
                for dst, w in list(self.peer_writers.items()):
                    if w.is_closing():
                        del self.peer_writers[dst]
                if self.node.raft.role != role:
                    role = self.node.raft.role
                    print(f"node {self.id}: {role} in term {self.node.raft.term}", flush=True)
                await asyncio.sleep(0.01)


# ----------------------------------------------------------------- client


class Client:
    def __init__(self, addrs):
        self.addrs = addrs
        self.id = CLIENT_BASE + random.getrandbits(40)
        self.seq = 0
        self.target = 0
        self.conn = None

    async def _open(self):
        if self.conn is None:
            host, port = self.addrs[self.target]
            self.conn = await asyncio.wait_for(asyncio.open_connection(host, port), 1.0)
        return self.conn

    def _drop(self, hint=None):
        if self.conn:
            self.conn[1].close()
        self.conn = None
        self.target = hint if hint is not None else (self.target + 1) % len(self.addrs)

    async def call(self, *op):
        """Retries with the same sequence number until a leader answers."""
        self.seq += 1
        req = encode(self.id, ClientRequest(self.id, self.seq, tuple(op)))
        while True:
            try:
                reader, writer = await self._open()
                writer.write(req)
                while True:
                    _, reply = decode(await asyncio.wait_for(reader.readline(), 1.0))
                    if reply.seq == self.seq:
                        break
                if reply.ok:
                    return reply.result
                self._drop(reply.leader_hint)
                await asyncio.sleep(0.02)
            except (OSError, asyncio.TimeoutError, ValueError):
                self._drop()
                await asyncio.sleep(0.05)


async def _bench(addrs, total, concurrency):
    lat = []

    async def worker(n):
        c = Client(addrs)
        for i in range(n):
            t = time.perf_counter()
            await c.call("put", f"key{i % 100}", "x" * 64)
            lat.append((time.perf_counter() - t) * 1000)

    await Client(addrs).call("put", "warmup", "1")  # wait for a leader
    t0 = time.perf_counter()
    await asyncio.gather(*(worker(total // concurrency) for _ in range(concurrency)))
    dt = time.perf_counter() - t0
    lat.sort()
    print(f"{len(lat)} linearizable writes, {concurrency} clients: {len(lat) / dt:,.0f} ops/sec, "
          f"p50 {lat[len(lat) // 2]:.2f} ms, p99 {lat[int(len(lat) * .99)]:.2f} ms")


# -------------------------------------------------------------------- cli


def _addrs(s):
    out = []
    for part in s.split(","):
        host, port = part.rsplit(":", 1)
        out.append((host, int(port)))
    return out


def _cmd_serve(a):
    try:
        asyncio.run(Server(a.id, _addrs(a.cluster), a.data, a.snapshot_every).run())
    except KeyboardInterrupt:
        pass
    return 0


def _cmd_client(a):
    addrs = _addrs(a.cluster)
    if a.op[0] == "bench":
        total = int(a.op[1]) if len(a.op) > 1 else 2000
        conc = int(a.op[2]) if len(a.op) > 2 else 16
        asyncio.run(_bench(addrs, total, conc))
    else:
        print(asyncio.run(Client(addrs).call(*a.op)))
    return 0


def add_parsers(sub):
    s = sub.add_parser("serve", help="run one real node")
    s.add_argument("--id", type=int, required=True)
    s.add_argument("--cluster", required=True, help="comma-separated host:port, index = node id")
    s.add_argument("--data", required=True, help="directory for this node's log")
    s.add_argument("--snapshot-every", type=int, default=1000, help="compact the log every N applied entries")
    s.set_defaults(fn=_cmd_serve)

    c = sub.add_parser("client", help="get K | put K V | append K V | cas K OLD NEW | bench [N] [CLIENTS]")
    c.add_argument("--cluster", required=True)
    c.add_argument("op", nargs="+")
    c.set_defaults(fn=_cmd_client)
