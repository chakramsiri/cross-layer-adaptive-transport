"""Phase 8 matrix summary: one row per run, then per-cell statistics."""

import argparse
import csv
import json
import re
import statistics
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.summarize_cc_modes import percentile

FLOW_KINDS = ("", "data")
RUN_METRICS = (
    "goodput_mbps", "duration_s",
    "rtt_mean_ms", "rtt_p50_ms", "rtt_p95_ms", "rtt_p99_ms",
    "queue_mean_ms", "queue_p50_ms", "queue_p95_ms", "queue_p99_ms",
    "max_queue_packets", "flow_drops", "flow_marks", "cross_forwarded",
    "retransmissions", "timeouts", "fast_retransmits",
    "cwnd_mean", "cwnd_max",
)
CELL_KEYS = ("scenario", "buffer", "aqm", "aqm_mode", "controller")


def read_rows(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def stats_or_none(values, function):
    return function(values) if values else None


def summarise_run(run_dir):
    metadata = json.loads((run_dir / "run_metadata.json").read_text())
    cell = metadata["cell"]
    result = metadata.get("result", {})
    sender = read_rows(run_dir / "sender_log.csv") if (
        run_dir / "sender_log.csv").exists() else []
    telemetry = read_rows(run_dir / "router_telemetry.csv") if (
        run_dir / "router_telemetry.csv").exists() else []

    rtts = [
        float(match.group(1)) * 1000.0
        for row in sender if row["event"] == "RTT_SAMPLE"
        for match in [re.search(r"rtt=([\d.]+)", row["details"])] if match
    ]
    windows = [
        float(match.group(1))
        for row in sender
        if row["event"] == "CWND_UPDATE"
        and "reason=init" not in row["details"]
        for match in [re.search(r"cwnd=([\d.]+)", row["details"])] if match
    ]
    flow_sojourns = [
        float(row["sojourn_ms"]) for row in telemetry
        if row["event"] == "forward" and row["sojourn_ms"]
        and row.get("kind", "") in FLOW_KINDS
    ]

    summary = {
        "scenario": cell["scenario"],
        "buffer": cell["buffer"],
        "aqm": cell["aqm"],
        "aqm_mode": cell["aqm_mode"],
        "controller": cell["controller"],
        "seed": cell["seed"],
        "success": result.get("success"),
        "failure_reasons": ";".join(result.get("failure_reasons", [])),
        "goodput_mbps": result.get("goodput_mbps"),
        "duration_s": result.get("duration_s"),
        "rtt_mean_ms": stats_or_none(rtts, statistics.fmean),
        "rtt_p50_ms": stats_or_none(rtts, lambda v: percentile(v, 50)),
        "rtt_p95_ms": stats_or_none(rtts, lambda v: percentile(v, 95)),
        "rtt_p99_ms": stats_or_none(rtts, lambda v: percentile(v, 99)),
        "queue_mean_ms": stats_or_none(flow_sojourns, statistics.fmean),
        "queue_p50_ms": stats_or_none(flow_sojourns,
                                      lambda v: percentile(v, 50)),
        "queue_p95_ms": stats_or_none(flow_sojourns,
                                      lambda v: percentile(v, 95)),
        "queue_p99_ms": stats_or_none(flow_sojourns,
                                      lambda v: percentile(v, 99)),
        "max_queue_packets": max(
            (int(row["queue_length_packets"]) for row in telemetry),
            default=0,
        ),
        "flow_drops": sum(
            1 for row in telemetry
            if row["event"] == "drop" and row.get("kind", "") in FLOW_KINDS
        ),
        "flow_marks": sum(
            1 for row in telemetry
            if row["event"] == "mark" and row.get("kind", "") in FLOW_KINDS
        ),
        "cross_forwarded": sum(
            1 for row in telemetry
            if row["event"] == "forward" and row.get("kind") == "cross"
        ),
        "retransmissions": sum(
            1 for row in sender if row["event"] == "RETRANSMISSION"
        ),
        "timeouts": sum(
            1 for row in sender
            if row["event"] == "RETRANSMISSION" and row["details"] == "Timeout"
        ),
        "fast_retransmits": sum(
            1 for row in sender
            if row["event"] == "RETRANSMISSION"
            and row["details"].startswith("Fast retransmit")
        ),
        "cwnd_mean": stats_or_none(windows, statistics.fmean),
        "cwnd_max": max(windows) if windows else None,
    }
    return summary


def find_runs(root):
    return sorted(
        path.parent for path in Path(root).rglob("run_metadata.json")
    )


def aggregate_cells(runs):
    groups = {}
    for run in runs:
        key = tuple(run[name] for name in CELL_KEYS)
        groups.setdefault(key, []).append(run)

    cells = []
    for key in sorted(groups, key=lambda item: (item[0], item[1], item[2],
                                                item[3], item[4])):
        members = groups[key]
        successful = [run for run in members if run["success"]]
        cell = dict(zip(CELL_KEYS, key))
        cell["runs"] = len(members)
        cell["successes"] = len(successful)
        for metric in RUN_METRICS:
            values = [run[metric] for run in successful
                      if run[metric] is not None]
            cell[f"{metric}_median"] = (
                statistics.median(values) if values else None
            )
            cell[f"{metric}_min"] = min(values) if values else None
            cell[f"{metric}_max"] = max(values) if values else None
        cells.append(cell)
    return cells


def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: ("" if value is None else value)
                for key, value in row.items()
            })


def build_parser():
    parser = argparse.ArgumentParser(
        description="Summarise Phase 8 runs and per-cell repetitions."
    )
    parser.add_argument("root", type=Path,
                        help="results/phase8 (searched recursively)")
    parser.add_argument("--runs-csv", type=Path, default=None)
    parser.add_argument("--cells-csv", type=Path, default=None)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    run_dirs = find_runs(args.root)
    if not run_dirs:
        print(f"no runs found under {args.root}", file=sys.stderr)
        return 1

    runs = []
    for run_dir in run_dirs:
        row = summarise_run(run_dir)
        row["run"] = str(run_dir.relative_to(args.root))
        runs.append(row)
    cells = aggregate_cells(runs)

    successes = sum(1 for run in runs if run["success"])
    print(f"runs: {len(runs)}  successful: {successes}  "
          f"failed: {len(runs) - successes}")
    print(f"cells: {len(cells)}")

    if args.runs_csv is not None:
        write_csv(args.runs_csv, runs)
    if args.cells_csv is not None:
        write_csv(args.cells_csv, cells)
    return 0


if __name__ == "__main__":
    sys.exit(main())
