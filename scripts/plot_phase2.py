#!/usr/bin/env python3
"""Phase 2 plot generator.

Usage:
    python scripts/plot_phase2.py rtt <sender_log.csv> <out.png> [title]
    python scripts/plot_phase2.py throughput <out.png> <label:csv> ...

'rtt' plots RTT samples, SRTT and RTO over time (from RTT_SAMPLE log
rows) and marks RTO backoff events.  'throughput' plots cumulative
acknowledged bytes over time for one or more sender logs.
"""

import argparse
import csv
import datetime as dt
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

CHUNK_SIZE = 1000


def load_rows(path):
    with open(path) as file:
        return list(csv.DictReader(file))


def parse_details(text):
    values = {}

    for token in text.split():
        key, _, value = token.partition("=")
        try:
            values[key] = float(value)
        except ValueError:
            values[key] = value

    return values


def elapsed_seconds(rows):
    base = dt.datetime.fromisoformat(rows[0]["timestamp"])
    times = []

    for row in rows:
        times.append(
            (dt.datetime.fromisoformat(row["timestamp"]) - base).total_seconds()
        )

    return times


def plot_rtt(csv_path, png_path, title):
    rows = load_rows(csv_path)
    times = elapsed_seconds(rows)

    samples = [
        (t, parse_details(row["details"]))
        for t, row in zip(times, rows)
        if row["event"] == "RTT_SAMPLE"
    ]
    backoffs = [
        (t, parse_details(row["details"]))
        for t, row in zip(times, rows)
        if row["event"] == "RTO_BACKOFF"
    ]

    figure, axis = plt.subplots(figsize=(10, 5))

    if samples:
        axis.scatter(
            [t for t, _ in samples],
            [d["rtt"] for _, d in samples],
            s=6, alpha=0.4, label="RTT sample", color="tab:blue",
        )
        axis.plot(
            [t for t, _ in samples],
            [d["srtt"] for _, d in samples],
            label="SRTT", color="tab:orange", linewidth=1.5,
        )
        axis.plot(
            [t for t, _ in samples],
            [d["rto"] for _, d in samples],
            label="RTO", color="tab:red", linewidth=1.5,
        )

    if backoffs:
        axis.scatter(
            [t for t, _ in backoffs],
            [d["rto"] for _, d in backoffs],
            marker="x", s=40, color="darkred", label="RTO backoff",
        )

    axis.set_xlabel("time since transfer start (s)")
    axis.set_ylabel("seconds")
    axis.set_title(title)
    axis.grid(alpha=0.3)
    axis.legend()
    figure.tight_layout()
    figure.savefig(png_path, dpi=120)
    plt.close(figure)


def cumulative_bytes(rows):
    """Cumulative acknowledged bytes over time (approximate at chunk size)."""

    times = elapsed_seconds(rows)
    points = []
    highest = 0

    for t, row in zip(times, rows):
        if row["event"] == "ACK_RECEIVED" and row["ack"]:
            highest = max(highest, int(row["ack"]))
            points.append((t, highest * CHUNK_SIZE))

    final = None
    for row in rows:
        if row["event"] == "TRANSFER_COMPLETED" and row["bytes"]:
            final = int(row["bytes"])

    if final is not None and points:
        points = [(t, min(value, final)) for t, value in points]
        points.append((times[-1], final))

    return points


def plot_throughput(output_path, series):
    figure, axis = plt.subplots(figsize=(10, 5))

    for label, csv_path in series:
        rows = load_rows(csv_path)
        points = cumulative_bytes(rows)

        if not points:
            continue

        final_time = points[-1][0]
        final_bytes = points[-1][1]
        goodput = final_bytes / final_time / 1000.0 if final_time else 0.0

        axis.plot(
            [t for t, _ in points],
            [value / (1 << 20) for _, value in points],
            label=f"{label} ({goodput:.0f} kB/s)",
        )

    axis.set_xlabel("time (s)")
    axis.set_ylabel("cumulative acknowledged data (MiB)")
    axis.set_title("Phase 2 throughput comparison")
    axis.grid(alpha=0.3)
    axis.legend()
    figure.tight_layout()
    figure.savefig(output_path, dpi=120)
    plt.close(figure)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    rtt = sub.add_parser("rtt")
    rtt.add_argument("csv")
    rtt.add_argument("png")
    rtt.add_argument("--title", default="RTT / SRTT / RTO")

    throughput = sub.add_parser("throughput")
    throughput.add_argument("png")
    throughput.add_argument(
        "series", nargs="+", help="label:csv entries"
    )

    args = parser.parse_args()

    if args.command == "rtt":
        plot_rtt(args.csv, args.png, args.title)
    else:
        series = []
        for entry in args.series:
            label, _, csv_path = entry.partition(":")
            series.append((label, csv_path))
        plot_throughput(args.png, series)

    print(f"wrote {args.png}")


if __name__ == "__main__":
    main()
