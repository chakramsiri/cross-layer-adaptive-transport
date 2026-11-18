"""Check sender-echoed CE totals and queue delays against router telemetry."""

import argparse
import csv
import re
import sys
from pathlib import Path

QUEUE_DELAY_TOLERANCE_US = 1.0


def read_rows(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def detail_value(details, key):
    match = re.search(rf"{key}=(\d+)", details)
    return int(match.group(1)) if match else None


def feedback_samples(sender_rows):
    samples = []
    for row in sender_rows:
        if row["event"] != "FEEDBACK":
            continue

        ce_total = detail_value(row["details"], "ce_total")
        q_delay_us = detail_value(row["details"], "q_delay_us")
        if ce_total is not None and q_delay_us is not None:
            samples.append((ce_total, q_delay_us))

    return samples


FLOW_KINDS = ("", "data")


def is_flow_row(row):
    return row.get("kind", "") in FLOW_KINDS


def forward_sojourns_us(telemetry_rows):
    return [
        float(row["sojourn_ms"]) * 1000.0
        for row in telemetry_rows
        if row["event"] == "forward" and row["sojourn_ms"]
        and is_flow_row(row)
    ]


def verify(sender_rows, telemetry_rows):
    samples = feedback_samples(sender_rows)
    sojourns = forward_sojourns_us(telemetry_rows)
    marks = sum(
        1 for row in telemetry_rows
        if row["event"] == "mark" and is_flow_row(row)
    )

    sender_total = max((total for total, _ in samples), default=0)
    unmatched = sum(
        1
        for _, q_delay_us in samples
        if not any(
            abs(q_delay_us - sojourn) <= QUEUE_DELAY_TOLERANCE_US
            for sojourn in sojourns
        )
    )

    ce_match = sender_total == marks
    return {
        "feedback_samples": len(samples),
        "sender_ce_total": sender_total,
        "router_marks": marks,
        "unmatched_queue_delays": unmatched,
        "ce_match": ce_match,
        "passed": bool(samples) and ce_match and unmatched == 0,
    }


def build_parser():
    parser = argparse.ArgumentParser(
        description="Compare sender feedback echoes with router telemetry."
    )
    parser.add_argument("--sender-log", type=Path, required=True,
                        help="sender_log.csv or manual_sender_log.csv")
    parser.add_argument("--telemetry", type=Path, required=True,
                        help="router_telemetry.csv of the same run")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    result = verify(read_rows(args.sender_log), read_rows(args.telemetry))

    print(f"feedback samples:        {result['feedback_samples']}")
    print(f"sender CE total:         {result['sender_ce_total']}")
    print(f"router mark events:      {result['router_marks']}")
    print(f"CE totals agree:         {result['ce_match']}")
    print(f"unmatched queue delays:  {result['unmatched_queue_delays']}")
    print("PASS" if result["passed"] else "FAIL")
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
