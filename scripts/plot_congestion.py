#!/usr/bin/env python3
"""Congestion-window plot generator.

Usage:
    python scripts/plot_congestion.py cwnd <sender_log.csv> <out.png> [title]

'cwnd' plots the congestion window and ssthresh over time from
CWND_UPDATE log rows and marks fast-recovery and timeout reductions.
"""

import argparse
import csv
import datetime as dt

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REDUCTION_REASONS = ("fast_recovery", "timeout")


def load_cwnd_rows(path):
    rows = []

    with open(path) as file:
        for row in csv.DictReader(file):
            if row["event"] != "CWND_UPDATE":
                continue

            values = {"timestamp": row["timestamp"]}

            for token in row["details"].split():
                key, _, value = token.partition("=")
                try:
                    values[key] = float(value)
                except ValueError:
                    values[key] = value

            rows.append(values)

    return rows


def elapsed_seconds(rows):
    base = dt.datetime.fromisoformat(rows[0]["timestamp"])

    return [
        (dt.datetime.fromisoformat(row["timestamp"]) - base).total_seconds()
        for row in rows
    ]


def plot_cwnd(csv_path, png_path, title):
    rows = load_cwnd_rows(csv_path)

    if not rows:
        raise SystemExit(f"no CWND_UPDATE rows in {csv_path}")

    times = elapsed_seconds(rows)
    cwnd = [row["cwnd"] for row in rows]

    figure, axis = plt.subplots(figsize=(10, 5))
    axis.plot(
        times, cwnd, label="cwnd", color="tab:blue", linewidth=1.2
    )

    finite = [(t, row["ssthresh"]) for t, row in zip(times, rows)
              if row.get("ssthresh", float("inf")) != float("inf")]
    if finite:
        axis.plot(
            [t for t, _ in finite], [s for _, s in finite],
            label="ssthresh", color="tab:orange",
            linewidth=1.0, linestyle="--",
        )

    reductions = [
        (t, row["cwnd"]) for t, row in zip(times, rows)
        if row.get("reason") in REDUCTION_REASONS
    ]
    if reductions:
        axis.scatter(
            [t for t, _ in reductions], [c for _, c in reductions],
            marker="x", s=40, color="darkred", label="reduction",
        )

    axis.set_xlabel("time (s)")
    axis.set_ylabel("cwnd (packets)")
    axis.set_title(title or "Congestion window over time")
    axis.grid(alpha=0.3)
    axis.legend(loc="lower right")
    figure.tight_layout()
    figure.savefig(png_path, dpi=120)
    print(f"wrote {png_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["cwnd"])
    parser.add_argument("sender_log")
    parser.add_argument("out_png")
    parser.add_argument("title", nargs="?", default=None)
    args = parser.parse_args()

    plot_cwnd(args.sender_log, args.out_png, args.title)


if __name__ == "__main__":
    main()
