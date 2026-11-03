"""Load test for the Kinesis -> consumer -> Braze mock pipeline.

Resets the players, produces N events with the Go producer, then reads the
Braze mock's receive log and reports latency percentiles and throughput.

Latency per event = received_at_ms (mock) - produced_at (producer). This needs
the consumer to forward `event_id` and `produced_at` in each Braze event's or
purchase's `properties`.
"""

import argparse
import datetime
import json
import math
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLAYERS = 250
LABEL = "mock - pipeline latency, Braze API excluded"


def parse_args():
    p = argparse.ArgumentParser(description="Kinesis -> Braze mock load test")
    p.add_argument("--events", type=int, default=500, help="number of events to produce")
    p.add_argument("--rate", type=int, default=20, help="events per second")
    p.add_argument("--burst", action="store_true", help="produce at 10x the rate")
    p.add_argument("--braze-url", default="http://localhost:8081")
    p.add_argument("--receive-log", default=os.path.join(ROOT, "tmp", "brazemock", "received.jsonl"))
    p.add_argument("--timeout", type=float, default=15, help="seconds without new events before stopping")
    p.add_argument("--results", default=os.path.join(ROOT, "results", "latency.md"))
    return p.parse_args()


def aws_env():
    env = dict(os.environ)
    env.setdefault("AWS_ENDPOINT_URL", "http://localhost:4566")
    env.setdefault("AWS_REGION", "us-east-1")
    env.setdefault("AWS_ACCESS_KEY_ID", "test")
    env.setdefault("AWS_SECRET_ACCESS_KEY", "test")
    return env


def run_producer(*flags):
    cmd = ["go", "run", "./cmd/producer"] + [str(f) for f in flags]
    subprocess.run(cmd, cwd=ROOT, env=aws_env(), check=True)


def delete_users(braze_url):
    ids = ["p-%d" % i for i in range(1, PLAYERS + 1)]
    for i in range(0, len(ids), 50):
        body = json.dumps({"external_ids": ids[i:i + 50]}).encode()
        req = urllib.request.Request(braze_url + "/users/delete", data=body,
                                     headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=10).read()


def reset(braze_url):
    print("reset: deleting %d users from %s" % (PLAYERS, braze_url))
    delete_users(braze_url)
    print("reset: re-seeding player_state (clears first_purchase_at)")
    run_producer("-seed-only")


def count_lines(path):
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return sum(1 for _ in f)


def read_received(path, skip):
    received = []
    if not os.path.exists(path):
        return received
    with open(path) as f:
        for n, line in enumerate(f):
            if n < skip or not line.strip():
                continue
            entry = json.loads(line)
            payload = entry.get("payload") or {}
            for obj in (payload.get("events") or []) + (payload.get("purchases") or []):
                props = obj.get("properties") or {}
                if "event_id" in props and "produced_at" in props:
                    received.append((props["event_id"], props["produced_at"], entry["received_at_ms"]))
    return received


def wait_for_events(path, skip, timeout):
    last_count, last_change = -1, time.time()
    while True:
        received = read_received(path, skip)
        if len(received) != last_count:
            last_count, last_change = len(received), time.time()
            print("waiting: %d events received" % len(received))
        if time.time() - last_change > timeout:
            return received
        time.sleep(1)


def percentile(values, p):
    ordered = sorted(values)
    return ordered[max(1, math.ceil(p / 100 * len(ordered))) - 1]


def summarize(sent, received):
    first_seen = {}
    duplicates = 0
    for event_id, produced_at, received_at in received:
        if event_id in first_seen:
            duplicates += 1
            continue
        first_seen[event_id] = (produced_at, received_at)

    stats = {"sent": sent, "received": len(first_seen), "duplicates": duplicates}
    if first_seen:
        latencies = [r - p for p, r in first_seen.values()]
        span = (max(r for _, r in first_seen.values()) - min(p for p, _ in first_seen.values())) / 1000
        stats.update({
            "p50": percentile(latencies, 50),
            "p95": percentile(latencies, 95),
            "p99": percentile(latencies, 99),
            "max": max(latencies),
            "throughput": len(first_seen) / span if span > 0 else 0,
        })
    return stats


def git_commit():
    out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True)
    return out.stdout.strip() or "unknown"


def write_results(args, stats):
    rate = args.rate * 10 if args.burst else args.rate
    lines = [
        "# Latency results",
        "",
        "**Target:** %s" % LABEL,
        "",
        "| Run | Value |",
        "|---|---|",
        "| Date | %s |" % datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "| Commit | `%s` |" % git_commit(),
        "| Events | %d at %d/s%s |" % (args.events, rate, " (burst)" if args.burst else ""),
        "",
        "| Metric | Value |",
        "|---|---|",
        "| Received | %d / %d sent |" % (stats["received"], stats["sent"]),
        "| Duplicates | %d |" % stats["duplicates"],
    ]
    if "p50" in stats:
        lines += [
            "| p50 | %d ms |" % stats["p50"],
            "| p95 | %d ms |" % stats["p95"],
            "| p99 | %d ms |" % stats["p99"],
            "| max | %d ms |" % stats["max"],
            "| Throughput | %.1f events/s |" % stats["throughput"],
        ]
    lines += [
        "",
        "Latency = mock receive time - `produced_at`. Events for deleted players (and any the",
        "consumer filters for minors) are not forwarded, so received < sent is expected.",
    ]
    os.makedirs(os.path.dirname(args.results), exist_ok=True)
    with open(args.results, "w") as f:
        f.write("\n".join(lines) + "\n")


def main():
    args = parse_args()
    if "localhost" not in args.braze_url and "127.0.0.1" not in args.braze_url:
        sys.exit("only the local mock writes receive timestamps; "
                 "end-to-end latency against the Braze trial needs a different measurement")

    reset(args.braze_url)

    skip = count_lines(args.receive_log)
    flags = ["-count", args.events, "-rate", args.rate]
    if args.burst:
        flags.append("-burst")
    print("produce: %d events at %d/s%s" % (args.events, args.rate, " x10 (burst)" if args.burst else ""))
    run_producer(*flags)

    received = wait_for_events(args.receive_log, skip, args.timeout)
    stats = summarize(args.events, received)
    write_results(args, stats)

    print()
    print("target:     %s" % LABEL)
    print("received:   %d / %d sent (duplicates %d)" % (stats["received"], stats["sent"], stats["duplicates"]))
    if "p50" in stats:
        print("latency:    p50 %d ms | p95 %d ms | p99 %d ms | max %d ms"
              % (stats["p50"], stats["p95"], stats["p99"], stats["max"]))
        print("throughput: %.1f events/s" % stats["throughput"])
    print("written:    %s" % os.path.relpath(args.results, ROOT))

    if stats["received"] == 0:
        sys.exit("no events reached the mock; is the consumer (PER-96) running?")


if __name__ == "__main__":
    main()
