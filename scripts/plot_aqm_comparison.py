"""AQM comparison plot generator and metric summary for a Phase 5 run."""

import argparse
import csv
import datetime as dt
import hashlib
import json
import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

AQMS = ("droptail", "red", "codel")

RUN_COLORS = {
    "droptail": "tab:red",
    "red": "tab:orange",
    "codel": "tab:blue",
}
RUN_LABELS = {"droptail": "DropTail", "red": "RED", "codel": "CoDel"}

SENDER_LOG_NAMES = ("sender_log.csv", "manual_sender_log.csv")
RECEIVER_LOG_NAMES = ("receiver_log.csv", "manual_receiver_log.csv")


def telemetry_rows(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def sender_rows(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def load_json(path):
    path = Path(path)
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def percentile(values, pct):
    ordered = sorted(values)
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def find_log(run_dir, names):
    for name in names:
        candidate = Path(run_dir) / name
        if candidate.exists():
            return candidate
    return Path(run_dir) / names[0]


def add_queue_panel(axis, run_dir, aqm):
    rows = telemetry_rows(Path(run_dir) / "router_telemetry.csv")
    times = [float(row["elapsed_s"]) for row in rows]
    lengths = [int(row["queue_length_packets"]) for row in rows]

    axis.step(times, lengths, where="post", linewidth=1.0,
              color=RUN_COLORS[aqm], label=RUN_LABELS[aqm])
    axis.set_xlabel("time since router start (s)")
    axis.set_ylabel("queue length (packets)")
    axis.set_title("Forward-path queue occupancy")
    axis.grid(True, alpha=0.3)
    axis.legend()


def add_average_queue_panel(axis, red_dir):
    rows = [
        row
        for row in telemetry_rows(Path(red_dir) / "router_telemetry.csv")
        if row.get("avg_queue_packets")
    ]
    times = [float(row["elapsed_s"]) for row in rows]
    averages = [float(row["avg_queue_packets"]) for row in rows]
    parameters = load_json(
        Path(red_dir) / "router_metadata.json"
    ).get("parameters", {})
    min_th = parameters.get("min_th", 20.0)
    max_th = parameters.get("max_th", 80.0)

    axis.plot(times, averages, linewidth=1.0, color=RUN_COLORS["red"],
              label="RED average (sampled at enqueue)")
    axis.axhline(min_th, color="gray", linestyle="--", linewidth=1.0,
                 label=f"min_th={min_th:g}p")
    axis.axhline(max_th, color="gray", linestyle=":", linewidth=1.0,
                 label=f"max_th={max_th:g}p")
    axis.set_xlabel("time since router start (s)")
    axis.set_ylabel("average queue length (packets)")
    axis.set_title("RED average queue length vs thresholds")
    axis.grid(True, alpha=0.3)
    axis.legend()


def add_sojourn_panel(axis, run_dir, aqm):
    rows = [
        row
        for row in telemetry_rows(Path(run_dir) / "router_telemetry.csv")
        if row["event"] == "forward" and row["sojourn_ms"]
    ]
    times = [float(row["elapsed_s"]) for row in rows]
    sojourns = [float(row["sojourn_ms"]) for row in rows]

    axis.scatter(times, sojourns, s=1.5, alpha=0.4, color=RUN_COLORS[aqm],
                 label=RUN_LABELS[aqm])

    if aqm == "codel":
        target_ms = load_json(
            Path(run_dir) / "router_metadata.json"
        ).get("parameters", {}).get("target_ms", 5.0)
        axis.axhline(target_ms, color="black", linestyle="--", linewidth=1.0,
                     label=f"CoDel target={target_ms:g}ms")

    axis.set_xlabel("time since router start (s)")
    axis.set_ylabel("queue sojourn at dequeue (ms)")
    axis.set_title("Queueing delay inside the router")
    axis.grid(True, alpha=0.3)
    axis.legend()


def add_distribution_panel(axis, run_dir, aqm):
    rows = [
        row
        for row in telemetry_rows(Path(run_dir) / "router_telemetry.csv")
        if row["event"] == "forward" and row["sojourn_ms"]
    ]
    sojourns = sorted(float(row["sojourn_ms"]) for row in rows)
    if not sojourns:
        return

    fractions = [(index + 1) / len(sojourns) for index in range(len(sojourns))]
    axis.step(sojourns, fractions, where="post", linewidth=1.2,
              color=RUN_COLORS[aqm], label=RUN_LABELS[aqm])
    axis.set_xlabel("queue sojourn at dequeue (ms)")
    axis.set_ylabel("cumulative fraction of forwarded packets")
    axis.set_title("Queue-delay distribution (CDF)")
    axis.grid(True, alpha=0.3)
    axis.legend()


def plot_run(layout, output_path):
    figure, axes = plt.subplots(2, 2, figsize=(13, 8))

    for aqm in AQMS:
        add_queue_panel(axes[0][0], layout[aqm], aqm)
    add_average_queue_panel(axes[0][1], layout["red"])
    for aqm in AQMS:
        add_sojourn_panel(axes[1][0], layout[aqm], aqm)
    for aqm in AQMS:
        add_distribution_panel(axes[1][1], layout[aqm], aqm)

    figure.suptitle(
        "AQM comparison through the user-space router "
        "(DropTail vs RED vs CoDel, same bulk workload)",
        fontsize=12,
    )
    figure.tight_layout(rect=(0, 0, 1, 0.96))
    figure.savefig(output_path, dpi=120)
    plt.close(figure)


def run_metrics(run_dir, aqm):
    run_dir = Path(run_dir)
    metadata = load_json(run_dir / "run_metadata.json")
    result = metadata.get("result", {})
    network = metadata.get("network", {})
    telemetry = telemetry_rows(run_dir / "router_telemetry.csv")
    sender_log = find_log(run_dir, SENDER_LOG_NAMES)
    senders = sender_rows(sender_log) if sender_log.exists() else []

    metrics = {
        "success": result.get("success"),
        "duration_s": result.get("duration_s"),
        "goodput_mbps": None,
        "sha256_match": result.get("sha256_match"),
        "sojourn_samples": 0,
        "mean_sojourn_ms": None,
        "median_sojourn_ms": None,
        "p95_sojourn_ms": None,
        "p99_sojourn_ms": None,
        "max_sojourn_ms": None,
        "max_queue": 0,
        "mean_queue_enqueue_sampled": None,
        "avg_queue_mean": None,
        "avg_queue_max": None,
        "drops": 0,
        "drops_full": 0,
        "drops_red": 0,
        "drops_codel": 0,
        "marks": 0,
        "marks_red": 0,
        "marks_codel": 0,
        "retransmissions": 0,
        "timeouts": 0,
    }

    file_bytes = network.get("file_bytes")
    output_path = run_dir / "out.bin"
    if file_bytes is None and output_path.exists():
        file_bytes = output_path.stat().st_size

    duration = metrics["duration_s"]
    if duration is None and senders:
        first = dt.datetime.fromisoformat(senders[0]["timestamp"])
        last = dt.datetime.fromisoformat(senders[-1]["timestamp"])
        duration = (last - first).total_seconds()
    if duration and file_bytes:
        metrics["goodput_mbps"] = file_bytes * 8 / duration / 1_000_000

    if metrics["sha256_match"] is None:
        metrics["sha256_match"] = integrity_check(run_dir, network)

    sojourns = []
    queue_samples = []
    averages = []
    for row in telemetry:
        length = int(row["queue_length_packets"])
        metrics["max_queue"] = max(metrics["max_queue"], length)
        if row["event"] == "enqueue":
            queue_samples.append(length)
        if row.get("avg_queue_packets"):
            averages.append(float(row["avg_queue_packets"]))
        reason = row.get("reason") or ""
        if row["event"] == "drop":
            metrics["drops"] += 1
            if reason == "full":
                metrics["drops_full"] += 1
            elif reason == "red":
                metrics["drops_red"] += 1
            elif reason == "codel":
                metrics["drops_codel"] += 1
        elif row["event"] == "mark":
            metrics["marks"] += 1
            if reason == "red":
                metrics["marks_red"] += 1
            elif reason == "codel":
                metrics["marks_codel"] += 1
        if row["event"] == "forward" and row["sojourn_ms"]:
            sojourns.append(float(row["sojourn_ms"]))

    if sojourns:
        metrics["sojourn_samples"] = len(sojourns)
        metrics["mean_sojourn_ms"] = statistics.fmean(sojourns)
        metrics["median_sojourn_ms"] = statistics.median(sojourns)
        metrics["p95_sojourn_ms"] = percentile(sojourns, 95)
        metrics["p99_sojourn_ms"] = percentile(sojourns, 99)
        metrics["max_sojourn_ms"] = max(sojourns)
    if queue_samples:
        metrics["mean_queue_enqueue_sampled"] = statistics.fmean(
            queue_samples
        )
    if averages:
        metrics["avg_queue_mean"] = statistics.fmean(averages)
        metrics["avg_queue_max"] = max(averages)

    for row in senders:
        if row["event"] == "RETRANSMISSION":
            metrics["retransmissions"] += 1
        elif (
            row["event"] == "CWND_UPDATE"
            and "reason=timeout" in row["details"].split()
        ):
            metrics["timeouts"] += 1

    if metrics["success"] is None:
        metrics["success"] = derive_success(run_dir, network)

    return metrics


def integrity_check(run_dir, network):
    output_path = Path(run_dir) / "out.bin"
    if not output_path.exists():
        return None

    expected = network.get("input_sha256")
    if expected is None:
        input_path = Path(run_dir) / "input.bin"
        if not input_path.exists():
            return None
        expected = sha256_file(input_path)

    return sha256_file(output_path) == expected


def log_completed(path):
    if not Path(path).exists():
        return False

    return any(
        row.get("event") == "TRANSFER_COMPLETED" for row in sender_rows(path)
    )


def expected_size(run_dir, network):
    size = network.get("file_bytes")
    if size is None:
        input_path = Path(run_dir) / "input.bin"
        if input_path.exists():
            size = input_path.stat().st_size
    return size


def output_evidence(run_dir, network):
    output_path = Path(run_dir) / "out.bin"
    if not output_path.exists():
        return False, False

    expected = expected_size(run_dir, network)
    size_matches = (
        expected is not None and output_path.stat().st_size == expected
    )
    return True, size_matches


def derive_success(run_dir, network):
    """True only when sender, receiver, output size and hash all verify."""
    output_exists, size_matches = output_evidence(run_dir, network)

    return (
        log_completed(find_log(run_dir, SENDER_LOG_NAMES))
        and log_completed(find_log(run_dir, RECEIVER_LOG_NAMES))
        and output_exists
        and size_matches
        and integrity_check(run_dir, network) is True
    )


def format_number(value, digits=3):
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def write_metrics(metrics, output_path):
    lines = ["Phase 5 AQM numerical summary", ""]

    for aqm in AQMS:
        if aqm not in metrics:
            continue
        entry = metrics[aqm]
        lines.append(f"{RUN_LABELS[aqm]} ({aqm}):")
        lines.append(f"  success: {entry['success']}")
        lines.append(f"  goodput: {format_number(entry['goodput_mbps'])} Mbps")
        lines.append(
            f"  sha256 integrity: {entry['sha256_match']}"
        )
        lines.append(
            f"  router sojourn at dequeue (ms): "
            f"n={entry['sojourn_samples']} "
            f"mean={format_number(entry['mean_sojourn_ms'])} "
            f"median={format_number(entry['median_sojourn_ms'])} "
            f"p95={format_number(entry['p95_sojourn_ms'])} "
            f"p99={format_number(entry['p99_sojourn_ms'])} "
            f"max={format_number(entry['max_sojourn_ms'])}"
        )
        lines.append(
            f"  queue occupancy: max={entry['max_queue']}p "
            f"mean at enqueue samples="
            f"{format_number(entry['mean_queue_enqueue_sampled'])}p"
        )
        lines.append(
            f"  RED average queue: mean="
            f"{format_number(entry['avg_queue_mean'])}p max="
            f"{format_number(entry['avg_queue_max'])}p"
        )
        lines.append(
            f"  drops: total={entry['drops']} full={entry['drops_full']} "
            f"red={entry['drops_red']} codel={entry['drops_codel']}"
        )
        lines.append(
            f"  marks: total={entry['marks']} red={entry['marks_red']} "
            f"codel={entry['marks_codel']}"
        )
        lines.append(
            f"  retransmissions={entry['retransmissions']} "
            f"timeouts={entry['timeouts']}"
        )
        lines.append("")

    lines.append(
        "Definitions: router sojourn is the per-datagram delay inside the "
        "bottleneck queue, measured when the router dequeues the packet, "
        "and excludes any token-bucket wait after dequeue. Queue occupancy "
        "is sampled at enqueue events, not time-weighted. The RED average "
        "is the EWMA sampled at enqueue. Goodput uses the transfer "
        "duration and the application file size."
    )

    output_path.write_text("\n".join(lines) + "\n")
    return lines


def resolve_layout(args, parser):
    explicit = {
        "droptail": args.droptail_dir,
        "red": args.red_dir,
        "codel": args.codel_dir,
    }
    given = {aqm: value for aqm, value in explicit.items() if value}

    if args.run_dir:
        run_dir = Path(args.run_dir)
        if not run_dir.is_dir():
            parser.error(f"run directory does not exist: {run_dir}")
        if given:
            parser.error(
                "pass either a run directory or explicit --*-dir "
                "options, not both"
            )
        layout = {aqm: run_dir / aqm for aqm in AQMS}
        return layout, run_dir

    if len(given) != len(AQMS):
        parser.error(
            "pass a phase5 run directory containing droptail/red/codel, "
            "or all three of --droptail-dir/--red-dir/--codel-dir"
        )

    layout = {aqm: Path(value) for aqm, value in explicit.items()}
    for aqm, directory in layout.items():
        if not directory.is_dir():
            parser.error(f"{aqm} directory does not exist: {directory}")
    return layout, None


def build_arg_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", nargs="?",
                        help="phase5 run directory with droptail/red/codel")
    parser.add_argument("--droptail-dir", default=None,
                        help="explicit DropTail run directory")
    parser.add_argument("--red-dir", default=None,
                        help="explicit RED run directory")
    parser.add_argument("--codel-dir", default=None,
                        help="explicit CoDel run directory")
    parser.add_argument("--output", default=None,
                        help="output PNG (default <run-dir>/comparison/)")
    return parser


def main(argv=None):
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    layout, run_dir = resolve_layout(args, parser)
    for aqm, directory in layout.items():
        if not (directory / "router_telemetry.csv").exists():
            parser.error(f"missing experiment data: {directory}")

    if args.output:
        output_path = Path(args.output)
    elif run_dir is not None:
        output_path = run_dir / "comparison" / "aqm_comparison.png"
    else:
        output_path = Path("aqm_comparison.png")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plot_run(layout, output_path)
    print(f"plot written to {output_path}")

    metrics = {
        aqm: run_metrics(directory, aqm)
        for aqm, directory in layout.items()
    }
    metrics_path = output_path.parent / "metrics.txt"
    lines = write_metrics(metrics, metrics_path)
    for line in lines:
        print(line)
    print(f"metrics written to {metrics_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
