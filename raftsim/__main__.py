"""Command line.

    python -m raftsim run   --seeds 10000            # correct implementation must survive every seed
    python -m raftsim hunt  --max-seeds 2000         # each injected bug must be caught
    python -m raftsim replay --seed 42 --bug forget_vote
    python -m raftsim serve ... / client ...         # real TCP cluster, see README
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import sys
import time

from .raft import BUGS
from .sim import job as _job
from .sim import run_seed


def _pool(jobs):
    return mp.get_context("spawn").Pool(jobs)


def cmd_run(a):
    bugs = tuple(a.bug or ())
    seeds = range(a.start, a.start + a.seeds)
    totals = {}
    failures = []
    t0 = time.time()
    with _pool(a.jobs) as pool:
        for seed, ok, inv, detail, stats in pool.imap_unordered(
                _job, ((s, bugs, a.duration, {'mixed': None, 'on': True, 'off': False}[a.pre_vote]) for s in seeds), chunksize=8):
            for k, v in stats.items():
                totals[k] = max(totals.get(k, 0), v) if k == "max_term" else totals.get(k, 0) + v
            if not ok:
                failures.append((seed, inv, detail))
    dt = time.time() - t0
    print(f"seeds {a.start}..{a.start + a.seeds - 1}  bugs={list(bugs) or 'none'}  pre-vote={a.pre_vote}  "
          f"{dt:.1f}s on {a.jobs} processes")
    print(f"  events           {totals['events']:,}")
    print(f"  messages         {totals['delivered']:,} delivered, {totals['dropped']:,} dropped")
    print(f"  faults           {totals['crashes']:,} crashes, {totals['partitions']:,} partitions")
    print(f"  leaders elected  {totals['elections']:,} (highest term reached, mean per run: {totals['term_sum'] / a.seeds:.1f})")
    print(f"  snapshots        {totals['snapshots']:,} taken, {totals['installs']:,} sent to lagging followers")
    print(f"  client ops       {totals['ops']:,} (all checked for linearizability)")
    if failures:
        failures.sort()
        print(f"  FAILED {len(failures)} seeds; first: seed {failures[0][0]}: {failures[0][1]}: {failures[0][2]}")
        return 1
    print("  violations       0")
    return 0


def cmd_hunt(a):
    """For every injected bug, count how many seeds fail out of the first N."""
    print(f"{'bug':<16} {'first seed':>10} {'seeds failing':>14}  caught by")
    missed = 0
    with _pool(a.jobs) as pool:
        for bug in BUGS:
            results = pool.map(_job, [(s, (bug,), a.duration) for s in range(a.max_seeds)], chunksize=8)
            bad = sorted((s, inv) for s, ok, inv, _, _ in results if not ok)
            if not bad:
                missed += 1
                print(f"{bug:<16} {'-':>10} {'0':>14}  NOT CAUGHT in {a.max_seeds} seeds")
                continue
            kinds = {}
            for _, inv in bad:
                kinds[inv] = kinds.get(inv, 0) + 1
            by = ", ".join(f"{k} ({v})" for k, v in sorted(kinds.items(), key=lambda kv: -kv[1]))
            print(f"{bug:<16} {bad[0][0]:>10} {f'{len(bad)}/{a.max_seeds}':>14}  {by}")
    return 1 if missed else 0


def cmd_replay(a):
    r = run_seed(a.seed, tuple(a.bug or ()), a.duration, keep_trace=a.tail)
    for line in r.tail:
        print(line)
    print(f"\nseed {a.seed}: {'ok' if r.ok else 'VIOLATION ' + r.invariant + ': ' + r.detail}")
    print(f"trace hash {r.trace_hash}  {r.stats}")
    return 0 if r.ok else 1


def main(argv=None):
    p = argparse.ArgumentParser(prog="raftsim")
    sub = p.add_subparsers(dest="cmd", required=True)
    jobs = os.cpu_count() or 4

    r = sub.add_parser("run", help="run many seeds")
    r.add_argument("--seeds", type=int, default=1000)
    r.add_argument("--start", type=int, default=0)
    r.add_argument("--bug", action="append", choices=sorted(BUGS))
    r.add_argument("--duration", type=float, default=8000.0, help="ms of simulated time with faults")
    r.add_argument("--jobs", type=int, default=jobs)
    r.add_argument("--pre-vote", choices=("mixed", "on", "off"), default="mixed",
                   help="mixed (default) draws it per seed; on/off force it, to compare")
    r.set_defaults(fn=cmd_run)

    h = sub.add_parser("hunt", help="check that every injected bug is detected")
    h.add_argument("--max-seeds", type=int, default=1000)
    h.add_argument("--duration", type=float, default=8000.0)
    h.add_argument("--jobs", type=int, default=jobs)
    h.set_defaults(fn=cmd_hunt)

    y = sub.add_parser("replay", help="re-run one seed and print the end of its trace")
    y.add_argument("--seed", type=int, required=True)
    y.add_argument("--bug", action="append", choices=sorted(BUGS))
    y.add_argument("--duration", type=float, default=8000.0)
    y.add_argument("--tail", type=int, default=40)
    y.set_defaults(fn=cmd_replay)

    from . import net
    net.add_parsers(sub)

    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
