"""Run with `python -m unittest -v` (no dependencies) or `pytest`."""
import os
import subprocess
import sys
import tempfile
import unittest

from raftsim import raft as R
from raftsim.kv import ClientReply, ClientRequest
from raftsim.linearizability import Op, check
from raftsim.net import DiskStorage, decode, encode
from raftsim.sim import run_seed

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def op(client, kind, call, ret, a=None, b=None, out=None, key="k"):
    o = Op(client, kind, key, a, b, call, ret)
    if out is not None:
        o.out = out
    return o


class Linearizability(unittest.TestCase):
    def test_sequential_history(self):
        h = [op(1, "put", 0, 1, a="x"), op(1, "get", 2, 3, out="x"), op(1, "append", 4, 5, a="y"),
             op(1, "get", 6, 7, out="xy")]
        self.assertTrue(check(h)[0])

    def test_stale_read_is_rejected(self):
        # The write finished before the read began, so the read must see it.
        h = [op(1, "put", 0, 1, a="x"), op(2, "get", 2, 3, out="")]
        self.assertEqual(check(h), (False, "k"))

    def test_concurrent_read_may_see_either_value(self):
        for seen in ("", "x"):
            h = [op(1, "put", 0, 10, a="x"), op(2, "get", 1, 2, out=seen)]
            self.assertTrue(check(h)[0])

    def test_reads_cannot_go_back_in_time(self):
        # Two reads during one write: once the new value is seen, the old one cannot reappear.
        h = [op(1, "put", 0, 10, a="x"), op(2, "get", 1, 2, out="x"), op(2, "get", 3, 4, out="")]
        self.assertFalse(check(h)[0])

    def test_double_apply_is_rejected(self):
        h = [op(1, "append", 0, 1, a="a"), op(2, "get", 2, 3, out="aa")]
        self.assertFalse(check(h)[0])

    def test_unfinished_write_may_or_may_not_have_happened(self):
        for seen in ("", "x"):
            h = [op(1, "put", 0, None, a="x"), op(2, "get", 5, 6, out=seen)]
            self.assertTrue(check(h)[0])

    def test_cas(self):
        ok = [op(1, "put", 0, 1, a="a"), op(2, "cas", 2, 3, a="a", b="b", out=True), op(1, "get", 4, 5, out="b")]
        self.assertTrue(check(ok)[0])
        bad = [op(1, "put", 0, 1, a="a"), op(2, "cas", 2, 3, a="z", b="b", out=True)]
        self.assertFalse(check(bad)[0])

    def test_keys_are_independent(self):
        h = [op(1, "put", 0, 1, a="x", key="a"), op(2, "get", 2, 3, out="", key="b")]
        self.assertTrue(check(h)[0])


class Simulator(unittest.TestCase):
    def test_correct_implementation_survives_faults(self):
        for seed in range(60):
            r = run_seed(seed)
            self.assertTrue(r.ok, f"seed {seed}: {r.invariant}: {r.detail}")

    def test_same_seed_same_trace(self):
        self.assertEqual(run_seed(7).trace_hash, run_seed(7).trace_hash)
        self.assertNotEqual(run_seed(7).trace_hash, run_seed(8).trace_hash)

    def test_trace_is_identical_across_processes(self):
        # Different hash randomisation in each process must not change the run.
        hashes = set()
        for hashseed in ("1", "2"):
            out = subprocess.run(
                [sys.executable, "-c", "from raftsim.sim import run_seed; print(run_seed(11).trace_hash)"],
                cwd=ROOT, env={**os.environ, "PYTHONHASHSEED": hashseed}, capture_output=True, text=True, check=True)
            hashes.add(out.stdout.strip())
        self.assertEqual(len(hashes), 1)

    # Each injected bug, at the first seed where `python -m raftsim hunt` found it.
    CAUGHT = {
        "commit_old_term": (1387, "LeaderCompleteness"),
        "forget_vote": (1356, "ElectionSafety"),
        "skip_log_check": (4, "LeaderCompleteness"),
        "no_truncate": (2, "StateMachineSafety"),
        "ack_whole_log": (4, "NodeCrash"),
        "no_dedup": (0, "Linearizability"),
        "stale_read": (0, "Linearizability"),
        "resend_on_dup": (137, "MessageStorm"),
        "skip_fsync": (2, "LeaderCompleteness"),
        "snapshot_wrong_index": (0, "SnapshotIntegrity"),
        "snapshot_drops_sessions": (0, "SnapshotIntegrity"),
        "stale_snapshot": (12, "Convergence"),
    }

    def test_every_injected_bug_is_caught(self):
        self.assertEqual(set(self.CAUGHT), set(R.BUGS))
        for bug, (seed, invariant) in self.CAUGHT.items():
            r = run_seed(seed, (bug,))
            self.assertFalse(r.ok, f"{bug} not caught at seed {seed}")
            self.assertEqual(r.invariant, invariant, f"{bug}: {r.detail}")
            self.assertTrue(run_seed(seed).ok, f"seed {seed} fails without the bug")


    def test_snapshots_and_both_election_modes_are_exercised(self):
        totals = {"snapshots": 0, "installs": 0}
        for seed in range(40):
            for pre_vote in (True, False):
                r = run_seed(seed, pre_vote=pre_vote)
                self.assertTrue(r.ok, f"seed {seed} pre_vote={pre_vote}: {r.invariant}: {r.detail}")
            for k in totals:
                totals[k] += r.stats[k]
        self.assertGreater(totals["snapshots"], 100)
        self.assertGreater(totals["installs"], 20)  # lagging followers really were sent snapshots


class Storage(unittest.TestCase):
    def test_crash_loses_exactly_what_was_not_synced(self):
        st = R.MemoryStorage()
        st.set_meta(3, 1)
        st.append(R.Entry(1, "a"))
        st.append(R.Entry(1, "b"))
        st.sync()
        st.set_meta(4, 2)             # not synced
        st.append(R.Entry(2, "c"))    # not synced
        st.crash()
        self.assertEqual((st.term, st.voted_for, st.last_index), (3, 1, 2))

        st.truncate(2)                # overwrite a synced entry...
        st.append(R.Entry(5, "x"))
        st.crash()                    # ...and lose power before syncing: the old entry is still there
        self.assertEqual([e.cmd for e in st.log[1:]], ["a", "b"])

        st.truncate(2)
        st.append(R.Entry(5, "x"))
        st.sync()
        st.crash()
        self.assertEqual([e.cmd for e in st.log[1:]], ["a", "x"])

    def test_compaction_keeps_absolute_indexes(self):
        st = R.MemoryStorage()
        for i in range(1, 11):
            st.append(R.Entry(i, f"cmd{i}"))
        st.compact(6, ("image",))
        self.assertEqual((st.base, st.last_index, st.term_at(6), st.entry(7).cmd), (6, 10, 6, "cmd7"))
        self.assertEqual([e.cmd for e in st.slice(8, 11)], ["cmd8", "cmd9", "cmd10"])
        st.truncate(9)
        self.assertEqual(st.last_index, 8)
        st.install(20, 9, ("newer",), keep_suffix=False)
        self.assertEqual((st.base, st.last_index, st.term_at(20), st.snapshot), (20, 20, 9, ("newer",)))

    def test_state_machine_image_round_trip(self):
        from raftsim.kv import KVStateMachine
        a = KVStateMachine()
        a.apply((1001, 1, ("put", "k", "v")))
        a.apply((1002, 1, ("append", "k", "+w")))
        b = KVStateMachine()
        b.load(a.dump())
        self.assertEqual((b.data, b.sessions), (a.data, a.sessions))
        self.assertEqual(b.apply((1002, 1, ("append", "k", "+w"))), "ok")  # a retry after restore...
        self.assertEqual(b.data["k"], "v+w")                              # ...does not run twice


class Network(unittest.TestCase):
    def test_codec_round_trip(self):
        msgs = [
            R.RequestVote(3, 1, 10, 2),
            R.VoteReply(3, True),
            R.AppendEntries(3, 1, 9, 2, (R.Entry(3, None), R.Entry(3, (1001, 4, ("cas", "k", "a", "b")))), 8),
            R.AppendReply(3, False, 7),
            R.PreVote(4, 2, 10, 3),
            R.PreVoteReply(3, 4, True),
            R.InstallSnapshot(3, 1, 40, 2, ((("k", "v"),), ((1001, (4, "ok")),))),
            ClientRequest(1001, 4, ("put", "k", "v")),
            ClientReply(1001, 4, True, "ok", 2),
        ]
        for m in msgs:
            self.assertEqual(decode(encode(5, m)), (5, m))

    def opened(self, path):
        s = DiskStorage(path)
        self.addCleanup(s.f.close)
        return s

    def test_disk_storage_recovers(self):
        with tempfile.TemporaryDirectory() as d:
            s = DiskStorage(d)
            s.set_meta(4, 2)
            for i in range(1, 6):
                s.append(R.Entry(i, (1000, i, ("put", "k", str(i)))))
            s.truncate(4)  # drop entries 4 and 5
            s.append(R.Entry(9, None))
            s.sync()
            s.f.close()
            # Simulate a crash in the middle of a write: a partial last line.
            with open(os.path.join(d, "log.jsonl"), "ab") as f:
                f.write(b'[10,[1000,99,["put"')

            again = DiskStorage(d)
            self.assertEqual((again.term, again.voted_for), (4, 2))
            self.assertEqual([e.term for e in again.log], [0, 1, 2, 3, 9])
            self.assertEqual(again.log[3].cmd, (1000, 3, ("put", "k", "3")))
            again.append(R.Entry(11, None))
            again.sync()
            again.f.close()
            last = self.opened(d)
            self.assertEqual([e.term for e in last.log], [0, 1, 2, 3, 9, 11])

    def test_disk_snapshot_and_interrupted_compaction(self):
        image = ((("k", "v"),), ((1001, (4, "ok")),))
        with tempfile.TemporaryDirectory() as d:
            s = DiskStorage(d)
            for i in range(1, 9):
                s.append(R.Entry(1, (1000, i, ("put", "k", str(i)))))
            s.sync()
            with open(os.path.join(d, "log.jsonl"), "rb") as f:
                uncompacted = f.read()
            s.compact(5, image)
            s.append(R.Entry(2, None))
            s.sync()
            s.f.close()

            back = DiskStorage(d)
            self.assertEqual((back.base, back.last_index, back.snapshot), (5, 9, image))
            self.assertEqual((back.entry(6).cmd[1], back.term_at(5), back.term_at(9)), (6, 1, 2))
            back.truncate(8)
            back.append(R.Entry(3, None))
            back.sync()
            back.f.close()
            self.assertEqual([e.term for e in self.opened(d).slice(6, 9)], [1, 1, 3])

        # Crash after the snapshot was written but before the log was shortened:
        # the old, longer log is still on disk and must be reconciled on load.
        with tempfile.TemporaryDirectory() as d:
            s = DiskStorage(d)
            s._write_snapshot(5, 1, image)
            s.f.close()
            with open(os.path.join(d, "log.jsonl"), "wb") as f:
                f.write(uncompacted)
            back = self.opened(d)
            self.assertEqual((back.base, back.last_index), (5, 8))
            self.assertEqual(back.entry(6).cmd[1], 6)


if __name__ == "__main__":
    unittest.main()
