# raftsim

A Raft-replicated key-value store, and the test harness that tries to break it.

The consensus code covers what a production Raft needs: leader election with
PreVote, log replication, log compaction with snapshots, and exactly-once
client sessions. The interesting part is how it is tested: the whole cluster
runs inside a **deterministic simulator** that controls time, randomness, the
network and the disk, injects crashes and partitions, and checks Raft's safety
properties after every step. One integer seed reproduces any failure exactly.
This is the testing style used by FoundationDB and TigerBeetle, applied to a
from-scratch Raft.

Python 3.10+, standard library only.

## Results

All numbers from an Apple M5 (10 cores). Reproduce with the commands shown.

**The correct implementation survives.** `python -m raftsim run --seeds 20000`

| | |
|---|---|
| Seeds | 20,000 (3-, 5- and 7-node clusters) |
| Simulated events | 135.5 million |
| Messages | 39.5 M delivered, 10.6 M dropped by the simulated network |
| Faults injected | 233,359 crashes (each loses unsynced disk writes), 240,820 partitions |
| Leaders elected | 131,208 |
| Snapshots | 766,384 taken; 230,231 shipped to followers that had fallen behind the log |
| Client operations checked for linearizability | 3,047,952 |
| Invariant violations | **0** |
| Wall time | 76 s |

**The harness catches real bugs.** A test suite that never fails proves little,
so `raft.py` carries twelve switchable bugs, each a classic Raft implementation
mistake. `python -m raftsim hunt --max-seeds 3000` runs each one:

| Injected bug | First failing seed | Seeds failing (of 3,000) | Caught by |
|---|---|---|---|
| Commits old-term entries by counting replicas (Raft paper, Fig. 8) | 1387 | 3 | Leader Completeness |
| `votedFor` not persisted across restart | 1356 | 2 | Election Safety (two leaders, one term) |
| Vote granted without the log up-to-date check | 4 | 1,110 | Leader Completeness |
| Follower does not truncate a conflicting suffix | 2 | 1,681 | State Machine Safety |
| Follower acknowledges its whole log, not what was sent | 4 | 650 | State Machine Safety |
| Retried client command applied twice | 0 | 2,309 | Linearizability |
| Leader serves reads without going through the log | 0 | 504 | Linearizability |
| Leader resends on every duplicated reply | 137 | 15 | Message Storm |
| Replies sent before the write reaches stable storage (no fsync) | 2 | 1,514 | Leader Completeness, Election Safety |
| Snapshot labelled with the last log index, not the last applied | 0 | 1,894 | Snapshot Integrity |
| Snapshot omits the client session table | 0 | 1,958 | Snapshot Integrity |
| Follower installs a snapshot older than its commit point | 12 | 135 | Convergence, Leader Completeness |

12 of 12 caught. The two rare ones matter most: the Figure 8 bug needs a
specific sequence of leader crashes and shows up in 1 seed out of 1,000.
Hand-written tests would not find it; random fault injection over thousands of
seeds does.

**Four things the simulator taught me that I had not planned:**

1. *A message-amplification bug in my own code.* The first version of the
   leader sent a follow-up AppendEntries in response to every reply. When the
   simulated network duplicated a reply, the leader sent two follow-ups, each
   of which could be duplicated again. One seed had 21,000 messages in flight.
   The fix (only react to a reply that moves the follower forward) is in
   `_on_append_reply`; the old behaviour is kept as the `resend_on_dup` bug
   and a Message Storm invariant now guards it.
2. *A wrong invariant in my checker.* My first Leader Completeness check
   required every new leader to hold every committed entry. One run in tens of
   thousands failed it on correct code: a candidate won an *old* term when its
   last vote arrived late, after a newer leader had already committed more. The
   paper's wording is precise (leaders of *higher-numbered* terms), and the
   check now matches it.
3. *A perfect disk hides missing fsyncs.* In the first version, deleting
   every `sync()` call changed nothing, because the simulated disk never lost
   data. The storage model now discards whatever was written since the last
   sync when a node crashes, and "no fsync" became a planted bug that fails
   half of all seeds.
4. *An optional feature can hide a bug.* When I added PreVote and left it
   always on, two planted election bugs nearly vanished: "vote without the log
   check" fell from 2,250 failing seeds to 2, and "votedFor not persisted" was
   not caught at all in 3,000 seeds. PreVote does its own log check and
   suppresses the competing candidacies those bugs need. The simulator now
   draws PreVote on or off per seed, so both election paths stay under test.

**PreVote does what it is for.** 20,000 seeds each way, identical fault
schedules (`--pre-vote on` / `--pre-vote off`):

| | PreVote off | PreVote on |
|---|---|---|
| Highest term reached, mean per run | 22.0 | **6.8** |
| Leaders elected | 148,724 | 113,136 (24% fewer) |
| Client operations completed | 2,997,899 | 3,100,491 (3.4% more) |

A node cut off by a partition can no longer inflate its term and depose a
healthy leader when it reconnects.

**The same core runs on real sockets.** `./demo.sh` starts three OS processes
on localhost with fsynced logs on disk, writes through them, kills the leader
with `kill -9`, keeps writing until the survivors have compacted their logs
past where the dead node stopped, and restarts it:

```
2992 linearizable writes, 16 clients: 5,148 ops/sec, p50 2.84 ms, p99 4.79 ms
killing leader (node 2)
get city           -> Shillong, IN
writing 3000 more entries while node 2 is down (the survivors compact their logs)
restarting node 2: its log is now behind the survivors' snapshots
  before: node 2 listening on 127.0.0.1:7102: snapshot through index 2000, 997 log entries after it
  after:  node 2 listening on 127.0.0.1:7102: snapshot through index 5000, 993 log entries after it
get city           -> Shillong, Meghalaya
```

The restarted node could not be caught up from the log (those entries no longer
exist), so the leader sent it a snapshot. `raft.py` and `kv.py` are
byte-for-byte the same code in both modes.

## How it works

```
raftsim/raft.py             Raft core: election, PreVote, replication, commit, snapshots. No I/O, no clock.
raftsim/kv.py               Key-value state machine with exactly-once client sessions.
raftsim/sim.py              Discrete-event simulator, fault injection, invariant checks.
raftsim/linearizability.py  Wing-Gong-Lowe linearizability checker.
raftsim/net.py              asyncio TCP transport, on-disk write-ahead log and snapshot files.
```

**Sans-I/O core.** `RaftNode` never reads a clock, opens a socket or calls
`random`. It is handed an `env` with `now()`, `random()` and `send()`. The
simulator supplies virtual time and a seeded generator; `net.py` supplies the
wall clock and TCP. That separation is what makes determinism possible.

**The simulator** is a priority queue of timestamped events. Each seed draws
its own configuration (cluster size, message loss up to 15%, duplication up to
10%, delays up to 300 ms, clock drift of +/-10% per node, election timeout
range, batch size, compaction interval, PreVote on or off, fault frequency)
and then a "nemesis" repeatedly partitions the network, isolates the leader,
cuts one-way links, and crashes or bounces nodes. A crashed node loses all
memory and every disk write it had not synced.

**Checked after every step**, by an observer that sees all nodes:

- *Election Safety*: at most one leader per term.
- *Leader Completeness*: a new leader holds every entry committed in earlier terms.
- *State Machine Safety*: no two nodes apply different commands at the same index.
- *Snapshot Integrity*: a snapshot at index i equals the state produced by
  applying exactly the committed entries 1..i, client sessions included.

**Checked at the end of every run:**

- *Log Matching*: logs that agree at an index agree on everything before it.
- *Linearizability* of the full client history (get / put / append / cas).
- *Liveness and convergence*: once faults stop, every pending client operation
  completes and all replicas reach the same state.

**Determinism is itself tested**: the same seed produces the same trace hash in
separate processes with different `PYTHONHASHSEED` values.

## Usage

```
python -m unittest                                  # 19 tests, ~4 s
python -m raftsim run --seeds 2000                  # clean run
python -m raftsim hunt --max-seeds 1000             # every injected bug must be caught
python -m raftsim replay --seed 1356 --bug forget_vote --tail 30   # watch a failure happen
./demo.sh                                           # real 3-node cluster: kill the leader, catch up by snapshot
```

## Limits

- No membership changes; the cluster is fixed at start.
- Reads go through the log (one replication round each) rather than using
  ReadIndex or leases.
- A snapshot is sent as one message. A large state machine would need it
  chunked.
- The simulated disk loses unsynced writes but never corrupts or tears a
  write. (`net.py` does handle a torn final log line and an interrupted
  compaction; those paths are covered by unit tests, not by the simulator.)
- Python: 5,000 writes/sec on localhost is enough to show the system works and
  is not a performance claim. `os.fsync` on macOS does not force the drive to
  flush its cache.
- Passing 20,000 seeds is evidence, not proof. The Raft safety argument is in
  the paper (Ongaro & Ousterhout, 2014); this project tests an implementation
  of it.
