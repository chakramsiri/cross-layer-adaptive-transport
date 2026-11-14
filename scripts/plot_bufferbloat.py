"""Bufferbloat comparison plot generator for a Phase 4 run directory."""

import argparse
import csv
import datetime as dt
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.plot_congestion import load_cwnd_rows

RUN_COLORS = {"large_buffer": "tab:red", "small_buffer": "tab:blue"}
RUN_LABELS = {
    "large_buffer": "large buffer",
    "small_buffer": "small buffer",
}


def telemetry_rows(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def sender_rows(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def elapsed_from_timestamps(rows):
    base = dt.datetime.fromisoformat(rows[0]["timestamp"])
    return [
        (dt.datetime.fromisoformat(row["timestamp"]) - base).total_seconds()
        for row in rows
    ]


def load_base_rtt_ms(run_dir):
    sender_csv = run_dir / "base_rtt" / "sender_log.csv"
    if not sender_csv.exists():
        return None

    samples = []
    for row in sender_rows(sender_csv):
        if row["event"] != "RTT_SAMPLE":
            continue
        for token in row["details"].split():
            key, _, value = token.partition("=")
            if key == "rtt":
                try:
                    samples.append(float(value) * 1000.0)
                except ValueError:
                    pass
    return samples


def add_queue_panel(axis, run_dir, name):
    path = run_dir / name / "router_telemetry.csv"
    rows = telemetry_rows(path)
    times = [float(row["elapsed_s"]) for row in rows]
    lengths = [int(row["queue_length_packets"]) for row in rows]

    axis.step(times, lengths, where="post",
              color=RUN_COLORS[name], label=RUN_LABELS[name])
    axis.set_xlabel("time since router start (s)")
    axis.set_ylabel("queue length (packets)")
    axis.set_title("Forward-path queue occupancy")
    axis.grid(True, alpha=0.3)
    axis.legend()


def add_sojourn_panel(axis, run_dir, name):
    path = run_dir / name / "router_telemetry.csv"
    rows = [
        row
        for row in telemetry_rows(path)
        if row["event"] == "forward" and row["sojourn_ms"]
    ]
    times = [float(row["elapsed_s"]) for row in rows]
    sojourns = [float(row["sojourn_ms"]) for row in rows]

    axis.scatter(times, sojourns, s=4, alpha=0.5,
                 color=RUN_COLORS[name], label=RUN_LABELS[name])
    axis.set_xlabel("time since router start (s)")
    axis.set_ylabel("queue sojourn (ms)")
    axis.set_title("Queueing delay inside the router")
    axis.grid(True, alpha=0.3)
    axis.legend()


def add_rtt_panel(axis, run_dir, name, base_samples):
    path = run_dir / name / "sender_log.csv"
    rows = [
        row for row in sender_rows(path) if row["event"] == "RTT_SAMPLE"
    ]
    times = elapsed_from_timestamps(rows)
    rtts = []

    for row in rows:
        for token in row["details"].split():
            if token.startswith("rtt="):
                rtts.append(float(token.partition("=")[2]) * 1000.0)
                break

    axis.scatter(times, rtts, s=4, alpha=0.5,
                 color=RUN_COLORS[name], label=f"{RUN_LABELS[name]} (loaded)")

    if base_samples:
        base_ms = sorted(base_samples)[len(base_samples) // 2]
        label = (
            f"base RTT ~{base_ms:.1f} ms (unloaded)"
            if name == "large_buffer"
            else None
        )
        axis.axhline(
            base_ms,
            color="gray",
            linestyle="--",
            linewidth=1.5,
            label=label,
        )

    axis.set_xlabel("time since transfer start (s)")
    axis.set_ylabel("measured RTT (ms)")
    axis.set_title("Sender RTT (Karn-filtered samples)")
    axis.grid(True, alpha=0.3)
    axis.legend()


def add_cwnd_panel(axis, run_dir, name):
    path = run_dir / name / "sender_log.csv"
    rows = load_cwnd_rows(path)
    times = elapsed_from_timestamps(rows)
    cwnds = [row["cwnd"] for row in rows]

    axis.plot(times, cwnds, color=RUN_COLORS[name],
              label=RUN_LABELS[name])
    axis.set_xlabel("time since transfer start (s)")
    axis.set_ylabel("cwnd (packets)")
    axis.set_title("Sender congestion window")
    axis.grid(True, alpha=0.3)
    axis.legend()


def plot_run(run_dir, output_path):
    base_samples = load_base_rtt_ms(run_dir)

    figure, axes = plt.subplots(2, 2, figsize=(13, 8))

    add_queue_panel(axes[0][0], run_dir, "large_buffer")
    add_queue_panel(axes[0][0], run_dir, "small_buffer")
    add_sojourn_panel(axes[0][1], run_dir, "large_buffer")
    add_sojourn_panel(axes[0][1], run_dir, "small_buffer")
    add_rtt_panel(
        axes[1][0], run_dir, "large_buffer", base_samples
    )
    add_rtt_panel(
        axes[1][0], run_dir, "small_buffer", base_samples
    )
    add_cwnd_panel(axes[1][1], run_dir, "large_buffer")
    add_cwnd_panel(axes[1][1], run_dir, "small_buffer")

    figure.suptitle(
        "Bufferbloat: large vs small DropTail buffer "
        "(real transport through the user-space router)",
        fontsize=12,
    )
    figure.tight_layout(rect=(0, 0, 1, 0.96))
    figure.savefig(output_path, dpi=120)
    plt.close(figure)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", help="phase4 run directory")
    parser.add_argument("--output", default=None,
                        help="output PNG (default <run-dir>/comparison/)")
    args = parser.parse_args(argv)

    run_dir = Path(args.run_dir)
    if not run_dir.is_dir():
        parser.error(f"run directory does not exist: {run_dir}")

    for required in ("base_rtt", "large_buffer", "small_buffer"):
        if not (run_dir / required).is_dir():
            parser.error(
                f"missing experiment data: {run_dir / required}"
            )

    output_path = (
        Path(args.output)
        if args.output
        else run_dir / "comparison" / "bufferbloat.png"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plot_run(run_dir, output_path)
    print(f"plot written to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
