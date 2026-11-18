"""Phase 9 analysis: figures, summary tables and findings from Phase 8 runs."""

import argparse
import csv
import datetime as dt
import json
import math
import re
import statistics
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from scripts.summarize_cc_modes import percentile
from scripts.summarize_matrix import summarise_run

DEFAULT_RESULTS = REPO_ROOT / "results" / "phase8"
DEFAULT_OUTPUT = REPO_ROOT / "results" / "phase9"
FLOW_KINDS = ("", "data")
CONTROLLER_LABELS = {"reno": "A reno", "dctcp": "B dctcp", "delay": "C delay"}
AQM_ORDER = {"droptail": 0, "red": 1, "codel": 2}
MODE_ORDER = {"drop": 0, "mark": 1}
CONTROLLER_ORDER = {"reno": 0, "dctcp": 1, "delay": 2}

COMBOS = (
    ("droptail", "drop", "reno"),
    ("droptail", "drop", "dctcp"),
    ("droptail", "drop", "delay"),
    ("red", "mark", "reno"),
    ("red", "mark", "dctcp"),
    ("red", "mark", "delay"),
    ("codel", "mark", "reno"),
    ("codel", "mark", "dctcp"),
    ("codel", "mark", "delay"),
    ("red", "drop", "reno"),
    ("codel", "drop", "reno"),
)

FAMILY_COMBOS = {
    "DropTail": (("droptail", "drop", "reno"), ("droptail", "drop", "dctcp"),
                 ("droptail", "drop", "delay")),
    "RED": (("red", "drop", "reno"), ("red", "mark", "reno"),
            ("red", "mark", "dctcp"), ("red", "mark", "delay")),
    "CoDel": (("codel", "drop", "reno"), ("codel", "mark", "reno"),
              ("codel", "mark", "dctcp"), ("codel", "mark", "delay")),
}

SUMMARY_METRICS = (
    "goodput_mbps", "rtt_mean_ms", "rtt_p50_ms", "rtt_p95_ms", "rtt_p99_ms",
    "queue_p95_ms", "retransmissions", "timeouts", "flow_drops", "flow_marks",
    "drop_rate", "mark_rate", "cwnd_mean",
)
SUMMARY_KEYS = ("scenario", "buffer", "aqm", "aqm_mode", "controller", "runs",
                "successes") + SUMMARY_METRICS
MARKDOWN_COLUMNS = ("aqm", "aqm_mode", "controller", "goodput_mbps",
                    "rtt_mean_ms", "rtt_p95_ms", "rtt_p99_ms", "queue_p95_ms",
                    "retransmissions", "flow_drops", "flow_marks")
PERCENTILE_LEVELS = (50, 95, 99)
DT_RENO = ("droptail", "drop", "reno")
DT_DCTCP = ("droptail", "drop", "dctcp")
DT_DELAY = ("droptail", "drop", "delay")
RED_DROP_RENO = ("red", "drop", "reno")
CODEL_DROP_RENO = ("codel", "drop", "reno")
CODEL_MARK_RENO = ("codel", "mark", "reno")
CODEL_MARK_DCTCP = ("codel", "mark", "dctcp")
CODEL_MARK_DELAY = ("codel", "mark", "delay")
CWND_PANELS = (
    ("A reno, RED drop", ("red", "drop", "reno")),
    ("B dctcp, RED mark", ("red", "mark", "dctcp")),
    ("C delay, RED mark", ("red", "mark", "delay")),
    ("A reno, RED mark", ("red", "mark", "reno")),
    ("B dctcp, CoDel mark", ("codel", "mark", "dctcp")),
    ("C delay, CoDel mark", ("codel", "mark", "delay")),
)


def read_rows(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def combo_label(aqm, mode, controller):
    return f"{aqm}-{mode} {CONTROLLER_LABELS[controller]}"


def cell_directories(root, scenario, buffer, aqm, mode, controller):
    base = (Path(root) / scenario / f"buf{buffer}"
            / f"{aqm}_{mode}_{controller}")
    return sorted(
        path for path in base.glob("rep*")
        if (path / "run_metadata.json").exists()
    )


def cdf_points(values):
    ordered = sorted(values)
    count = len(ordered)
    return ordered, [(index + 1) / count for index in range(count)]


def error_range(values):
    if not values:
        return None, None, None
    return statistics.median(values), min(values), max(values)


def queue_timeline(rows):
    timed = [row for row in rows if row.get("queue_length_packets", "") != ""]
    if not timed:
        return [], []
    origin = float(timed[0]["elapsed_s"])
    times = [float(row["elapsed_s"]) - origin for row in timed]
    lengths = [int(row["queue_length_packets"]) for row in timed]
    return times, lengths


def cwnd_timeline(rows):
    updates = [row for row in rows
               if row["event"] == "CWND_UPDATE" and "cwnd=" in row["details"]]
    if not updates:
        return [], []
    origin = dt.datetime.fromisoformat(updates[0]["timestamp"])
    times = [
        (dt.datetime.fromisoformat(row["timestamp"]) - origin).total_seconds()
        for row in updates
    ]
    windows = [float(re.search(r"cwnd=([\d.]+)", row["details"]).group(1))
               for row in updates]
    return times, windows


def cwnd_cuts(windows):
    cuts = [(before - after) / before
            for before, after in zip(windows, windows[1:]) if after < before]
    return len(cuts), (statistics.fmean(cuts) if cuts else None)


def rtt_samples(rows):
    samples = []
    for row in rows:
        if row["event"] != "RTT_SAMPLE":
            continue
        match = re.search(r"rtt=([\d.]+)", row["details"])
        if match:
            samples.append(float(match.group(1)) * 1000.0)
    return samples


def flow_queue_delays(rows):
    return [
        float(row["sojourn_ms"]) for row in rows
        if row["event"] == "forward" and row["sojourn_ms"]
        and row.get("kind", "") in FLOW_KINDS
    ]


def run_statistics(run_dir):
    stats = summarise_run(Path(run_dir))
    telemetry = read_rows(Path(run_dir) / "router_telemetry.csv")
    flow_packets = sum(
        1 for row in telemetry
        if row["event"] == "forward" and row.get("kind", "") in FLOW_KINDS
    )
    stats["flow_packets"] = flow_packets
    stats["drop_rate"] = (
        1000.0 * stats["flow_drops"] / flow_packets if flow_packets else None
    )
    stats["mark_rate"] = (
        1000.0 * stats["flow_marks"] / flow_packets if flow_packets else None
    )
    return stats


def median_or_none(values):
    present = [value for value in values if value is not None]
    return statistics.median(present) if present else None


def summary_rows(root):
    groups = {}
    for metadata in Path(root).rglob("run_metadata.json"):
        cell = _cell_of(metadata)
        groups.setdefault(cell, []).append(metadata.parent)

    rows = []
    for cell in sorted(groups, key=_sort_key):
        runs = [run_statistics(path) for path in sorted(groups[cell])]
        successful = [run for run in runs if run["success"]]
        row = dict(zip(("scenario", "buffer", "aqm", "aqm_mode", "controller"),
                       cell))
        row["runs"] = len(runs)
        row["successes"] = len(successful)
        for metric in SUMMARY_METRICS:
            row[metric] = median_or_none([run[metric] for run in successful])
        rows.append(row)
    return rows


def _cell_of(metadata_path):
    cell = _load_json(metadata_path)["cell"]
    return (cell["scenario"], cell["buffer"], cell["aqm"], cell["aqm_mode"],
            cell["controller"])


def _sort_key(cell):
    scenario, buffer, aqm, mode, controller = cell
    return (scenario, buffer, AQM_ORDER[aqm], MODE_ORDER[mode],
            CONTROLLER_ORDER[controller])


def _load_json(path):
    return json.loads(Path(path).read_text())


def write_table(rows, csv_path, markdown_path=None):
    with open(csv_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(SUMMARY_KEYS))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _cell(row.get(key)) for key in SUMMARY_KEYS})
    if markdown_path is None:
        return
    lines = ["| " + " | ".join(MARKDOWN_COLUMNS) + " |",
             "|" + "---|" * len(MARKDOWN_COLUMNS)]
    for row in rows:
        lines.append("| " + " | ".join(
            _markdown_cell(row.get(column)) for column in MARKDOWN_COLUMNS
        ) + " |")
    Path(markdown_path).write_text("\n".join(lines) + "\n")


def _cell(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6g}"
    return value


def _markdown_cell(value):
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def pooled_samples(root, scenario, buffer, combo, kind):
    values = []
    aqm, mode, controller = combo
    for run_dir in cell_directories(root, scenario, buffer, aqm, mode,
                                    controller):
        rows = read_rows(run_dir / "sender_log.csv" if kind == "rtt"
                         else run_dir / "router_telemetry.csv")
        if kind == "rtt":
            values += rtt_samples(rows)
        else:
            values += flow_queue_delays(rows)
    return values


def first_run(root, scenario, buffer, combo):
    aqm, mode, controller = combo
    dirs = cell_directories(root, scenario, buffer, aqm, mode, controller)
    return dirs[0] if dirs else None


def timelines_for(root, families, scenario="S1", buffer=200):
    panels = []
    for family, combos in families.items():
        series = {}
        for combo in combos:
            run_dir = first_run(root, scenario, buffer, combo)
            if run_dir is None:
                continue
            times, lengths = queue_timeline(
                read_rows(run_dir / "router_telemetry.csv"))
            series[combo_label(*combo)] = (times, lengths)
        panels.append((family, series))
    return panels


def cdf_groups(root, scenario, buffer=200):
    groups = []
    for kind, xlabel in (("rtt", "RTT (ms)"),
                         ("queue", "flow queue delay (ms)")):
        panels = []
        for family, combos in FAMILY_COMBOS.items():
            data = {}
            for combo in combos:
                values = pooled_samples(root, scenario, buffer, combo, kind)
                if values:
                    data[combo_label(*combo)] = values
            panels.append((family, data))
        groups.append((xlabel, panels))
    return groups


def render_timelines(path, panels, ylabel, xlabel="time (s)", columns=None):
    columns = columns or len(panels)
    rows = math.ceil(len(panels) / columns)
    fig, axes = plt.subplots(rows, columns,
                             figsize=(4.4 * columns, 3.2 * rows),
                             squeeze=False)
    for index, (title, series) in enumerate(panels):
        axis = axes[index // columns][index % columns]
        for label, (xs, ys) in series.items():
            if xs:
                axis.step(xs, ys, where="post", linewidth=1, label=label)
        axis.set_title(title, fontsize=10)
        axis.set_xlabel(xlabel, fontsize=8)
        axis.set_ylabel(ylabel, fontsize=8)
        if series:
            axis.legend(fontsize=6)
    for index in range(len(panels), rows * columns):
        axes[index // columns][index % columns].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def render_cdfs(path, groups):
    width = max(len(panels) for _, panels in groups)
    fig, axes = plt.subplots(len(groups), width,
                             figsize=(4.4 * width, 3.4 * len(groups)),
                             squeeze=False)
    for row, (xlabel, panels) in enumerate(groups):
        for column in range(width):
            axis = axes[row][column]
            if column >= len(panels):
                axis.set_visible(False)
                continue
            title, data = panels[column]
            for label, values in data.items():
                xs, ys = cdf_points(values)
                line, = axis.plot(xs, ys, linewidth=1, label=label)
                for level in PERCENTILE_LEVELS:
                    axis.plot([percentile(values, level)], [level / 100],
                              marker="o", markersize=3,
                              color=line.get_color())
            axis.set_title(title, fontsize=10)
            axis.set_xlabel(xlabel, fontsize=8)
            axis.set_ylabel("CDF", fontsize=8)
            axis.set_ylim(0, 1.02)
            if data:
                axis.legend(fontsize=6)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def render_bars(path, labels, series):
    fig, axes = plt.subplots(1, len(series), figsize=(4.6 * len(series), 4.4),
                             squeeze=False)
    for axis, (name, values, lows, highs) in zip(axes[0], series):
        xs = [index for index, value in enumerate(values) if value is not None]
        heights = [values[index] for index in xs]
        lower = [height - (lows[index] if lows[index] is not None else height)
                 for index, height in zip(xs, heights)]
        upper = [
            (highs[index] if highs[index] is not None else height) - height
            for index, height in zip(xs, heights)
        ]
        axis.bar(xs, heights, yerr=[lower, upper], capsize=3, color="tab:blue")
        axis.set_xticks(range(len(labels)))
        axis.set_xticklabels(labels, rotation=60, ha="right", fontsize=6)
        axis.set_title(name, fontsize=10)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def render_lines(path, panels, xlabel, xlog=True):
    fig, axes = plt.subplots(1, len(panels), figsize=(4.8 * len(panels), 3.6),
                             squeeze=False)
    for axis, (title, series) in zip(axes[0], panels):
        xticks = set()
        for label, (xs, ys) in series.items():
            axis.plot(xs, ys, marker="o", markersize=4, label=label)
            xticks.update(xs)
        if xlog:
            axis.set_xscale("log", base=2)
        axis.set_xticks(sorted(xticks))
        axis.set_xticklabels([str(int(x)) for x in sorted(xticks)],
                             fontsize=7)
        axis.set_title(title, fontsize=10)
        axis.set_xlabel(xlabel, fontsize=8)
        if series:
            axis.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(path, dpi=120)
    plt.close(fig)


def compare_runs(reference_root, verification_root):
    rows = []
    for metadata in sorted(Path(verification_root).rglob("run_metadata.json")):
        relative = metadata.parent.relative_to(verification_root)
        reference = Path(reference_root) / relative
        if not (reference / "run_metadata.json").exists():
            continue
        new = run_statistics(metadata.parent)
        old = run_statistics(reference)
        rows.append({
            "run": str(relative),
            "reference_goodput_mbps": old["goodput_mbps"],
            "verification_goodput_mbps": new["goodput_mbps"],
            "goodput_ratio": _ratio(new["goodput_mbps"], old["goodput_mbps"]),
            "reference_rtt_p95_ms": old["rtt_p95_ms"],
            "verification_rtt_p95_ms": new["rtt_p95_ms"],
            "rtt_p95_ratio": _ratio(new["rtt_p95_ms"], old["rtt_p95_ms"]),
            "verification_success": new["success"],
        })
    return rows


def _ratio(new, old):
    if new is None or old is None or old == 0:
        return None
    return new / old


def write_comparison(rows, path):
    if not rows:
        return
    with open(path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _cell(value) for key, value in row.items()})


def _row(rows, scenario, buffer, combo):
    aqm, mode, controller = combo
    for row in rows:
        key = (row["scenario"], row["buffer"], row["aqm"], row["aqm_mode"],
               row["controller"])
        if key == (scenario, buffer, aqm, mode, controller):
            return row
    return None


def _value(row, key, fmt="{:.1f}"):
    if row is None or row.get(key) is None:
        return "n/a"
    return fmt.format(row[key])


def seed_spread(root, scenario, buffer, combo):
    aqm, mode, controller = combo
    spread = []
    for run_dir in cell_directories(root, scenario, buffer, aqm, mode,
                                    controller):
        stats = run_statistics(run_dir)
        spread.append((stats["goodput_mbps"], stats["timeouts"]))
    return spread


def verification_lines(verification):
    if not verification:
        return []
    goodput = [abs(row["goodput_ratio"] - 1.0) for row in verification
               if row["goodput_ratio"] is not None]
    rtt = [abs(row["rtt_p95_ratio"] - 1.0) for row in verification
           if row["rtt_p95_ratio"] is not None]
    within = sum(1 for value in goodput if value <= 0.05)
    worst = max(verification, key=lambda row: abs(row["goodput_ratio"] - 1.0))
    return [
        "Verification: "
        f"{len(verification)} runs (S1 and S3, seed 1) were re-run on Mininet "
        "with run_experiment.py and compared with the matrix run at the same "
        f"seed. Goodput is within 5% in {within} of {len(goodput)} runs; the "
        "median absolute deviation is "
        f"{100 * statistics.median(goodput):.1f}% for goodput and "
        f"{100 * statistics.median(rtt):.1f}% for p95 RTT. "
        "The largest goodput "
        f"change is {worst['run']}: {worst['reference_goodput_mbps']:.2f} to "
        f"{worst['verification_goodput_mbps']:.2f} Mbps, the timeout effect "
        "described below.",
        "",
    ]


def make_cell(rows):
    def cell(scenario, buffer, combo, key, fmt="{:.1f}"):
        return _value(_row(rows, scenario, buffer, combo), key, fmt)

    return cell


def surprises_lines(rows, spread):
    cell = make_cell(rows)
    per_seed = ", ".join(
        f"{goodput:.2f} Mbps with {timeouts} timeouts"
        for goodput, timeouts in spread
        if goodput is not None and timeouts is not None
    ) or "n/a"
    dctcp_dt = cell("S1", 200, DT_DCTCP, "goodput_mbps", "{:.2f}")
    reno_dt = cell("S1", 200, DT_RENO, "goodput_mbps", "{:.2f}")
    codel_mark_queue = cell("S1", 200, CODEL_MARK_RENO, "queue_p95_ms")
    delay_s3 = cell("S3", 200, CODEL_MARK_DELAY, "goodput_mbps", "{:.2f}")
    delay_dt_s3 = cell("S3", 200, DT_DELAY, "goodput_mbps", "{:.2f}")
    red_timeouts = cell("S4", 200, RED_DROP_RENO, "timeouts", "{:.0f}")
    return [
        "Surprising results, explained:",
        "",
        f"- Mode B on DropTail equals Reno ({dctcp_dt} vs {reno_dt} Mbps): "
        "DropTail never marks, so mode B has no signal to act on.",
        "- Reno with CoDel in mark mode keeps the DropTail queue "
        f"({codel_mark_queue} ms p95): marks are not drops, and Reno does not "
        "react to marks.",
        "- RED-drop Reno under bursts has a wide seed spread (per seed: "
        f"{per_seed}). Whether a 1 s timeout fires decides the goodput, so "
        "the "
        "cell's range is wide. Medians hide this; read the cells table range.",
        "- Mode C has the lowest goodput under bursts ("
        f"{delay_s3} Mbps on CoDel marks, {delay_dt_s3} on DropTail): it "
        "backs off to its 5 ms queue target, so the burst traffic keeps the "
        "link busy during the window it holds back.",
        "- With 1% loss the timeouts, not the queue, set the goodput "
        f"(RED-drop Reno: {red_timeouts} timeouts per run), so no queue-side "
        "policy lifts it.",
        "",
    ]


def build_findings(rows, cwnd_data, reno_peak, spread=None, verification=None):
    cell = make_cell(rows)
    cwnd = {title: stats for title, _, _, stats in cwnd_data}
    steady = {title: _steady_range(times, windows)
              for title, times, windows, _ in cwnd_data}
    loss = [row for row in rows if row["scenario"] == "S4"]
    goodputs = [row["goodput_mbps"] for row in loss
                if row["goodput_mbps"] is not None]
    timeouts = [row["timeouts"] for row in loss if row["timeouts"] is not None]

    dt_q = cell("S1", 200, DT_RENO, "queue_p95_ms")
    dt_g = cell("S1", 200, DT_RENO, "goodput_mbps", "{:.2f}")
    codel_q = cell("S1", 200, CODEL_DROP_RENO, "queue_p95_ms")
    codel_g = cell("S1", 200, CODEL_DROP_RENO, "goodput_mbps", "{:.2f}")
    codel_mark_q = cell("S1", 200, CODEL_MARK_RENO, "queue_p95_ms")
    dt_delay_rtt = cell("S1", 200, DT_DELAY, "rtt_p95_ms")
    dt_delay_g = cell("S1", 200, DT_DELAY, "goodput_mbps", "{:.2f}")
    dt_rtt = cell("S1", 200, DT_RENO, "rtt_p95_ms")
    mark_dctcp_rtt = cell("S1", 200, CODEL_MARK_DCTCP, "rtt_p95_ms")
    mark_dctcp_g = cell("S1", 200, CODEL_MARK_DCTCP, "goodput_mbps", "{:.2f}")
    s3_dt_rtt = cell("S3", 200, DT_RENO, "rtt_p95_ms")
    s3_dt_g = cell("S3", 200, DT_RENO, "goodput_mbps", "{:.2f}")
    s3_dctcp_g = cell("S3", 200, CODEL_MARK_DCTCP, "goodput_mbps", "{:.2f}")
    s3_dctcp_rtt = cell("S3", 200, CODEL_MARK_DCTCP, "rtt_p95_ms")
    s3_delay_q = cell("S3", 200, CODEL_MARK_DELAY, "queue_p95_ms")
    s3_delay_g = cell("S3", 200, CODEL_MARK_DELAY, "goodput_mbps", "{:.2f}")
    s3_dt_delay_q = cell("S3", 200, DT_DELAY, "queue_p95_ms")
    s3_dt_delay_g = cell("S3", 200, DT_DELAY, "goodput_mbps", "{:.2f}")
    s2_20 = cell("S2", 20, DT_RENO, "queue_p95_ms")
    s2_500 = cell("S2", 500, DT_RENO, "queue_p95_ms")

    red_drop = _cuts_text(cwnd, "A reno, RED drop")
    red_mark_reno = _cuts_text(cwnd, "A reno, RED mark")
    dctcp_cuts = _cuts_text(cwnd, "B dctcp, RED mark")
    dctcp_cut = _percent_of(cwnd, "B dctcp, RED mark")
    delay_cuts = _cuts_text(cwnd, "C delay, RED mark")
    delay_range = steady.get("C delay, RED mark") or "n/a"

    lines = ["# Phase 9 findings", "",
             "Generated from results/phase8 by "
             "scripts/phase9_analysis.py.", ""]
    lines += [
        "Finding 1: buffer occupancy sets queueing delay "
        "(fig_queue_timeline.png, summary_table_s1.md). With Reno on DropTail "
        f"the flow queue p95 is {dt_q} ms. Reno with CoDel drops cuts it to "
        f"{codel_q} ms, at {codel_g} Mbps against {dt_g} Mbps. CoDel in mark "
        f"mode does not help Reno (queue p95 {codel_mark_q} ms): Reno ignores "
        "marks, and mark mode never drops.",
        "",
        "Finding 2: delay distributions (fig_delay_cdfs.png). Delay-aware "
        f"mode on DropTail lowers RTT p95 to {dt_delay_rtt} ms, against "
        f"{dt_rtt} ms "
        f"for Reno, with goodput {dt_delay_g} Mbps. DCTCP-style mode on CoDel "
        f"marks gives RTT p95 {mark_dctcp_rtt} ms at {mark_dctcp_g} Mbps.",
        "",
        "Finding 3: the research question under bursty cross traffic "
        "(fig_bursty_delay_cdfs.png). Reno on DropTail reaches RTT p95 "
        f"{s3_dt_rtt} ms at {s3_dt_g} Mbps. DCTCP-style feedback on CoDel "
        "marks "
        f"keeps {s3_dctcp_g} Mbps with RTT p95 {s3_dctcp_rtt} ms. Delay-aware "
        f"mode keeps queue p95 at {s3_delay_q} ms on CoDel marks and "
        f"{s3_dt_delay_q} ms on DropTail, but reaches only {s3_delay_g} and "
        f"{s3_dt_delay_g} Mbps.",
        "",
        "Finding 4: window dynamics (fig_cwnd_modes.png, repetition 1). Reno "
        f"cut its window {red_drop} on RED drops, but {red_mark_reno} on RED "
        "marks, because it ignores marks. DCTCP-style mode cut "
        f"{dctcp_cuts} on RED marks, by {dctcp_cut} on average, following the "
        "marked fraction. Delay-aware mode cut "
        f"{delay_cuts} and saw-tooths between {delay_range} packets after the "
        "start.",
        "",
        "Finding 5: random loss and buffer size (fig_loss_goodput.png, "
        "fig_buffer_sweep.png). At 1% loss the goodput of all "
        f"{len(loss)} S4 variants lies between "
        f"{_bound(min(goodputs, default=None))} and "
        f"{_bound(max(goodputs, default=None))} Mbps, with "
        f"{_bound(min(timeouts, default=None), '{:.0f}')} to "
        f"{_bound(max(timeouts, default=None), '{:.0f}')} timeouts per run. "
        "Each timeout waits at least the 1 s minimum RTO on a 40 ms path, and "
        "the feedback modes did not raise goodput here. In the buffer sweep, "
        f"Reno on DropTail reaches {s2_20} ms queue p95 at 20 packets, {dt_q} "
        f"ms at 200 and {s2_500} ms at 500. Its window peaks at "
        f"{_bound(reno_peak, '{:.0f}')} packets, so buffers beyond that size "
        "do "
        "not change the queue.",
        "",
        "Stretch goal (Jain's fairness index) is not run: every experiment "
        "has "
        "one transport flow.",
        "",
    ]
    lines += verification_lines(verification)
    lines += surprises_lines(rows, spread or [])
    return "\n".join(lines)


def _cuts_text(cwnd, title):
    stats = cwnd.get(title)
    if stats is None or stats[0] is None:
        return "n/a"
    return f"{stats[0]} times"


def _percent_of(cwnd, title):
    stats = cwnd.get(title)
    if stats is None or stats[1] is None:
        return "n/a"
    return f"{100 * stats[1]:.0f}%"


def _steady_range(times, windows):
    steady = [window for time, window in zip(times, windows) if time >= 2.0]
    if not steady:
        return None
    return f"{min(steady):.0f} and {max(steady):.0f}"


def _bound(value, fmt="{:.2f}"):
    return "n/a" if value is None else fmt.format(value)


def reno_window_peak(root, scenario="S1", buffer=200):
    run_dir = first_run(root, scenario, buffer, ("droptail", "drop", "reno"))
    if run_dir is None:
        return None
    _, windows = cwnd_timeline(read_rows(run_dir / "sender_log.csv"))
    return max(windows) if windows else None


def cwnd_panel_data(root, scenario="S1", buffer=200):
    data = []
    for title, combo in CWND_PANELS:
        run_dir = first_run(root, scenario, buffer, combo)
        if run_dir is None:
            data.append((title, [], [], None))
            continue
        times, windows = cwnd_timeline(read_rows(run_dir / "sender_log.csv"))
        cuts, mean_cut = cwnd_cuts(windows)
        data.append((title, times, windows, (cuts, mean_cut)))
    return data


def run_pipeline(root, out, verification_root=None):
    root = Path(root)
    out = Path(out)
    figures = out / "figures"
    figures.mkdir(parents=True, exist_ok=True)

    rows = summary_rows(root)
    write_table(rows, out / "summary_table_all.csv")
    s1 = [row for row in rows if row["scenario"] == "S1"]
    write_table(s1, out / "summary_table_s1.csv", out / "summary_table_s1.md")

    render_timelines(
        figures / "fig_queue_timeline.png",
        timelines_for(root, FAMILY_COMBOS),
        ylabel="queue (packets)",
        columns=3,
    )
    render_cdfs(figures / "fig_delay_cdfs.png", cdf_groups(root, "S1"))
    render_cdfs(figures / "fig_bursty_delay_cdfs.png", cdf_groups(root, "S3"))

    cwnd_data = cwnd_panel_data(root)
    reno_peak = reno_window_peak(root)
    render_timelines(
        figures / "fig_cwnd_modes.png",
        [(title, {title: (times, windows)} if times else {})
         for title, times, windows, _ in cwnd_data],
        ylabel="cwnd (packets)",
        columns=3,
    )

    bar_labels = [combo_label(*combo) for combo in COMBOS]
    goodput = bar_series(root, rows, "S1", 200, "goodput_mbps")
    drops = bar_series(root, rows, "S1", 200, "drop_rate")
    marks = bar_series(root, rows, "S1", 200, "mark_rate")
    render_bars(figures / "fig_throughput_bars.png", bar_labels,
                [("goodput (Mbps)",) + goodput,
                 ("drops per 1000 flow packets",) + drops,
                 ("marks per 1000 flow packets",) + marks])

    s4_goodput = bar_series(root, rows, "S4", 200, "goodput_mbps")
    s4_timeouts = bar_series(root, rows, "S4", 200, "timeouts")
    render_bars(figures / "fig_loss_goodput.png", bar_labels,
                [("goodput at 1% loss (Mbps)",) + s4_goodput,
                 ("timeouts per run",) + s4_timeouts])

    render_lines(figures / "fig_buffer_sweep.png", buffer_panels(rows),
                 xlabel="buffer (packets)")

    spread = seed_spread(root, "S3", 200, ("red", "drop", "reno"))
    verification = None
    if verification_root is not None:
        verification = compare_runs(root, verification_root)
        write_comparison(verification, out / "verification.csv")
    (out / "findings.md").write_text(
        build_findings(rows, cwnd_data, reno_peak, spread, verification))
    return rows


def bar_series(root, rows, scenario, buffer, key):
    values, lows, highs = [], [], []
    for combo in COMBOS:
        aqm, mode, controller = combo
        row = _row(rows, scenario, buffer, combo)
        samples = []
        for run_dir in cell_directories(root, scenario, buffer, aqm, mode,
                                        controller):
            stats = run_statistics(run_dir)
            if stats["success"] and stats.get(key) is not None:
                samples.append(stats[key])
        median, low, high = error_range(samples)
        values.append(median if row is not None else None)
        lows.append(low)
        highs.append(high)
    return values, lows, highs


def buffer_panels(rows):
    lines = (
        ("droptail", "drop", "reno"),
        ("red", "drop", "reno"),
        ("codel", "drop", "reno"),
        ("droptail", "drop", "delay"),
        ("codel", "mark", "dctcp"),
    )
    buffers = (20, 50, 100, 200, 500)
    queue, goodput = {}, {}
    for combo in lines:
        label = combo_label(*combo)
        xs, qs, gs = [], [], []
        for buffer in buffers:
            scenario = "S1" if buffer == 200 else "S2"
            row = _row(rows, scenario, buffer, combo)
            if row is None:
                continue
            xs.append(buffer)
            qs.append(row["queue_p95_ms"])
            gs.append(row["goodput_mbps"])
        queue[label] = (xs, qs)
        goodput[label] = (xs, gs)
    return [("flow queue p95 (ms)", queue), ("goodput (Mbps)", goodput)]


def build_parser():
    parser = argparse.ArgumentParser(description="Phase 9 analysis.")
    parser.add_argument("--results", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--verification", type=Path, default=None,
                        help="re-run results to compare against --results")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    run_pipeline(args.results, args.output, args.verification)
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
