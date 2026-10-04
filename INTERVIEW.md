# Defending raftsim in an interview

Read `raft.py` (about 500 lines) until you can redraw it on a whiteboard, then
`sim.py`. Run `python -m raftsim replay --seed 1356 --bug forget_vote --tail 60`
and follow the trace to the double leader; that one exercise will prepare you
for most questions.

## The 30-second pitch

"I implemented Raft, with PreVote and snapshots, and then built a
deterministic simulator to attack it: seeded network faults, crashes that lose
unsynced disk writes, partitions, with safety invariants checked on every step
and linearizability checked on the client history. The correct version passes
20,000 seeds. I also planted twelve classic Raft bugs and the harness catches
all twelve, and it found a bug in my own code I had not planted."

## Questions you will get

**What is Raft for?**
Keeping several machines' copies of a log identical even when some crash or
the network misbehaves, so that a service on top looks like one reliable
machine. A majority must agree before anything is considered committed.

**Walk me through an election.**
A follower that hears nothing for a random 150-300 ms increments its term,
votes for itself and asks the others. A node grants one vote per term, and
only to a candidate whose log is at least as up to date as its own (compare
last term, then last index). A majority of votes makes a leader. The random
timeout keeps candidates from colliding forever.

**Why must `votedFor` be on disk?**
Otherwise a node can vote, crash, restart and vote again in the same term for
someone else: two leaders in one term. Seed 1356 with `--bug forget_vote`
shows exactly this.

**Explain the Figure 8 problem.**
A leader must not commit an entry from an older term just because a majority
holds it; a later leader that never saw it can still win and overwrite it.
Raft only counts replicas for entries of the leader's *current* term; older
entries become committed indirectly. That is the `break` in `_advance_commit`,
and it is why a new leader appends a no-op immediately.

**What is PreVote and what did you measure?**
Before starting a real election a node asks, without changing anyone's state,
"would you vote for me?" Peers say no if they have heard from a leader
recently or the asker's log is behind. Only with a majority of yeses does it
increment its term. Without it, a node stuck behind a partition keeps bumping
its term, and when it reconnects that higher term forces the healthy leader to
step down. Over 20,000 seeds the mean highest term fell from 22.0 to 6.8 and
24% fewer leaders were elected.

**Why did PreVote hide bugs, and what did you do about it?**
PreVote performs its own log-up-to-date check and stops most competing
candidacies. Two of my planted bugs live in the real-vote path and need
exactly those: with PreVote always on, one fell from 2,250 failing seeds to 2
and the other was never caught. A feature that is always on leaves the code it
bypasses untested, so the simulator now picks PreVote on or off per seed.

**How do snapshots work?**
Once enough entries have been applied, a node saves an image of its state
machine and deletes the log up to that point. The log keeps absolute indexes,
so entry i lives at position i minus the snapshot index. If a follower needs
entries the leader has already deleted, the leader sends the snapshot instead
(InstallSnapshot); the follower replaces its state, keeps any log suffix that
still agrees, and carries on from there.

**What must a snapshot contain besides the data?**
The client session table. Without it, a replica restored from a snapshot no
longer recognises a retry of a command it already ran, and runs it twice. That
is one of the planted bugs, and the Snapshot Integrity check catches it.

**What can go wrong when installing a snapshot?**
Snapshots can arrive late or twice. Installing one that is older than what the
follower has already committed rolls its state back and throws away log
entries it had acknowledged; a later election can then pick a leader that is
missing committed data. The guard is one line: ignore a snapshot at or below
the commit index.

**Why does fsync matter, and how do you test it?**
A node that says "I voted for you" or "I stored that entry" and then loses it
in a crash has lied, and Raft's safety argument assumes nodes do not lie. My
simulated disk keeps a durable copy as of the last sync and reverts to it on
crash. With the sync calls removed, half of all seeds fail.

**What is linearizability and how do you check it?**
Each operation must appear to take effect at one instant between its call and
its return, and replaying them in that order on one machine must give the
results clients actually saw. The checker searches for such an order
(Wing-Gong), remembering (set of operations done, state) pairs it has already
tried so it does not repeat work (Lowe). Keys are checked independently.

**What about an operation that timed out?**
The client does not know whether it happened. The checker treats it as
possibly taking effect at any time after the call, or never.

**How do you get exactly-once semantics with retries?**
Every command carries (client id, sequence number). The state machine stores
each client's last sequence number and result. A retry of an already-applied
command returns the stored result instead of running again. Because this table
is part of the replicated state, it survives leader changes.

**Why are stale reads possible if a leader answers reads locally?**
A leader cut off from the majority still believes it is leader until it hears
a higher term. Meanwhile the majority elects someone else and accepts writes.
The old leader serves old data. Sending reads through the log forces a
majority round trip, which a deposed leader cannot complete.

**What makes the simulation deterministic?**
One seeded generator supplies every random choice; time is a number the
simulator advances; events are ordered by (time, insertion counter) so ties
never depend on anything else; and the Raft core has no I/O of its own. I
avoid iterating over sets of strings because Python randomises string hashes
per process, and a test checks the trace hash across processes.

**Why is that better than running real processes and killing them (Jepsen style)?**
Speed and reproducibility. A simulated run takes about 30 ms of CPU for 8+
seconds of cluster time, so 20,000 runs take a minute or two, and any failure
replays identically from its seed. Real-process testing finds different bugs
(real disks, real kernels) and is complementary.

**What does the simulator not cover?**
Disk corruption and torn writes (it models lost writes only), bugs in
`net.py` itself, and anything about performance. Say so before they ask.

**Tell me about a bug you found.**
The message storm (README, "Four things the simulator taught me"). Explain the
mechanism: duplicated reply, duplicated follow-up, geometric growth. Then the
fix: only react to a reply that makes progress.

**What would you add next?**
ReadIndex for cheap linearizable reads, membership changes, chunked snapshot
transfer, disk corruption in the simulator, and automatic shrinking of a
failing schedule to a minimal one.

## Things to try yourself

1. Run `python -m raftsim run --seeds 2000 --bug skip_fsync` and read how the
   first failure unfolds with `replay`.
2. Change the quorum to `len(peers) // 2` and run `python -m raftsim run`.
3. Remove the stale-snapshot guard in `_on_install_snapshot` by hand and see
   how many seeds it takes.
4. Add a thirteenth bug of your own.
