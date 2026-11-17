"""Summarise Phase 7 runs: queue delay, goodput and window behaviour."""

import argparse
import csv
import json
import re
import statistics
import sys
from pathlib import Path

PERCENTILE_LEVELS = (50, 95, 99)


def read_rows(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def percentile(values, level):
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, round(level / 100 * len(ordered)) - 1))
    return ordered[rank]


def summarise_run(run_dir, root):
    metadata = json.loads((run_dir / "run_metadata.json").read_text())
    telemetry = read_rows(run_dir / "router_telemetry.csv")
    sender = read_rows(run_dir / "sender_log.csv")

    sojourns = [
        float(row["sojourn_ms"])
        for row in telemetry
        if row["event"] == "forward" and row["sojourn_ms"]
    ]
    windows = [
        float(match.group(1))
        for row in sender
        if row["event"] == "CWND_UPDATE"
        and "reason=init" not in row["details"]
        for match in [re.search(r"cwnd=([\d.]+)", row["details"])]
        if match
    ]
    modes = {
        row["details"].split("=", 1)[1]
        for row in sender
        if row["event"] == "CC_MODE"
    }

    result = metadata.get("result", {})
    duration = result.get("duration_s")
    file_bytes = metadata["network"]["file_bytes"]

    summary = {
        "run": str(run_dir.relative_to(root)),
        "aqm": metadata["algorithm"],
        "aqm_mode": metadata["mode"],
        "controller": metadata["congestion_control"]["mode"],
        "controller_parameters": metadata["congestion_control"]["parameters"],
        "logged_controller": ",".join(sorted(modes)),
        "success": result.get("success"),
        "duration_s": duration,
        "goodput_mbps": (
            file_bytes * 8 / duration / 1_000_000 if duration else None
        ),
        "delay_samples": len(sojourns),
        "mean_delay_ms": statistics.fmean(sojourns) if sojourns else None,
        "max_queue_packets": max(
            (int(row["queue_length_packets"]) for row in telemetry),
            default=0,
        ),
        "router_marks": sum(1 for row in telemetry if row["event"] == "mark"),
        "router_drops": sum(1 for row in telemetry if row["event"] == "drop"),
        "cwnd_mean": statistics.fmean(windows) if windows else None,
        "cwnd_min": min(windows) if windows else None,
        "cwnd_max": max(windows) if windows else None,
    }
    for level in PERCENTILE_LEVELS:
        key = f"p{level}_delay_ms"
        summary[key] = percentile(sojourns, level) if sojourns else None
    return summary


def find_runs(root):
    return sorted(
        path.parent for path in Path(root).rglob("run_metadata.json")
    )


def format_table(rows):
    columns = [
        "run", "aqm_mode", "controller", "success", "duration_s",
        "goodput_mbps", "mean_delay_ms", "p50_delay_ms", "p95_delay_ms",
        "p99_delay_ms", "max_queue_packets", "router_marks", "router_drops",
        "cwnd_mean", "cwnd_min", "cwnd_max",
    ]

    def cell(value):
        if value is None:
            return "-"
        if isinstance(value, float):
            return f"{value:.3f}"
        return str(value)

    widths = {
        column: max(len(column), *(len(cell(row[column])) for row in rows))
        for column in columns
    }
    lines = [
        "  ".join(column.ljust(widths[column]) for column in columns),
        "  ".join("-" * widths[column] for column in columns),
    ]
    for row in rows:
        lines.append("  ".join(
            cell(row[column]).ljust(widths[column]) for column in columns
        ))
    return "\n".join(lines)


def build_parser():
    parser = argparse.ArgumentParser(
        description="Summarise Phase 7 controller runs under a results root."
    )
    parser.add_argument("root", type=Path,
                        help="directory searched recursively for runs")
    parser.add_argument("--csv", type=Path, default=None,
                        help="optional CSV output path")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    runs = find_runs(args.root)
    if not runs:
        print(f"no runs found under {args.root}", file=sys.stderr)
        return 1

    rows = [summarise_run(run, args.root) for run in runs]
    print(format_table(rows))

    if args.csv is not None:
        with open(args.csv, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0]))
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    key: (json.dumps(value) if isinstance(value, dict)
                          else value)
                    for key, value in row.items()
                })
    return 0


if __name__ == "__main__":
    sys.exit(main())
