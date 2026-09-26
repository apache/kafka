#!/usr/bin/env python3
"""Produce deterministic-count JSON commit-log events into the primary Kafka container."""

import argparse
import datetime as dt
import json
import subprocess
import sys
import uuid


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce JSON events to the MM2 challenge commit-log topic")
    parser.add_argument("--count", type=int, required=True, help="exact number of events to produce")
    parser.add_argument("--topic", default="commit-log", help="source topic (default: commit-log)")
    parser.add_argument("--container", default="mm2-primary", help="Docker container running the primary broker")
    parser.add_argument("--bootstrap-server", default="primary:19092", help="broker address visible inside the container")
    args = parser.parse_args()
    if args.count < 0:
        parser.error("--count must be zero or greater")
    return args


def make_event(index: int) -> str:
    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "op_type": "UPSERT",
        "key": f"key-{index}",
        "value": f"value-{index}",
    }
    return json.dumps(event, separators=(",", ":"))


def main() -> int:
    args = parse_args()
    command = [
        "docker", "exec", "-i", args.container,
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server", args.bootstrap_server,
        "--topic", args.topic,
    ]

    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, text=True)
    except OSError as exc:
        print(f"Unable to start Kafka producer: {exc}", file=sys.stderr)
        return 2

    assert process.stdin is not None
    try:
        for index in range(args.count):
            process.stdin.write(make_event(index) + "\n")
        process.stdin.close()
    except (BrokenPipeError, OSError) as exc:
        process.kill()
        process.wait()
        print(f"Production failed while writing events: {exc}", file=sys.stderr)
        return 3

    return_code = process.wait()
    if return_code != 0:
        print(f"Kafka console producer exited with status {return_code}", file=sys.stderr)
        return return_code

    print(f"Produced exactly {args.count} JSON event(s) to {args.topic}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
