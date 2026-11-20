#!/usr/bin/env python3
"""Summarise pgfleet's JSON logs (stderr) into per-check latency stats.

    pgfleet verify -driver mock -config fleet.json 2> run.log
    python3 tools/summarize_logs.py run.log          # or: ... | python3 tools/summarize_logs.py -
"""
import json
import statistics
import sys
from collections import defaultdict


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100))]


def main(path):
    fh = sys.stdin if path == "-" else open(path)
    lat, fails, commands = defaultdict(list), defaultdict(int), []
    for line in fh:
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue  # tolerate interleaved non-JSON output
        if ev.get("msg") == "check":
            lat[ev["check"]].append(ev["latency_ms"])
            if not ev["ok"]:
                fails[ev["check"]] += 1
        elif ev.get("msg") == "command_done":
            commands.append(ev)
    print(f"{'check':<20}{'n':>6}{'fail':>6}{'p50 ms':>10}{'p95 ms':>10}{'max ms':>10}")
    for name, xs in sorted(lat.items()):
        print(f"{name:<20}{len(xs):>6}{fails[name]:>6}{statistics.median(xs):>10.2f}{pct(xs, 95):>10.2f}{max(xs):>10.2f}")
    for c in commands:
        print(f"\ncommand {c['command']}: ok={c['ok']} elapsed={c['elapsed_ms']:.1f}ms checks={c['checks']}")
    return 1 if any(fails.values()) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "-"))
