#!/usr/bin/env python3
"""
Run a Reno congestion-control experiment over a rate-limited Mininet link.
"""
import argparse
import csv
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from phase2_experiments import (  # noqa: E402
    NODE,
    apply_netem,
    build_network,
    clear_netem,
    make_input,
    qdisc_state,
    sha256_file,
)
from transport.cc.reno import INITIAL_CWND, INITIAL_SSTHRESH  # noqa: E402

RESULTS_ROOT = REPO_ROOT / "results" / "phase3"
PLOT_SCRIPT = REPO_ROOT / "scripts" / "plot_congestion.py"


def reno_sender_flags(ssthresh_override):
    """Explicit Reno configuration for the sender subprocess."""

    ssthresh = (
        INITIAL_SSTHRESH if ssthresh_override is None else ssthresh_override
    )

    return [
        "--initial-cwnd", str(INITIAL_CWND),
        "--initial-ssthresh", str(ssthresh),
    ]

PYTHON = "python3"
PORT = 5001
DATA_NETEM = "delay 10ms rate 2mbit limit 20"
ACK_NETEM = "delay 10ms"
RATE_KBYTES_PER_S = 250.0  # 2 mbit
TRANSFER_BYTES = 2 << 20


def sender_transfer_completed(sender_log):
    if not Path(sender_log).exists():
        return False

    with open(sender_log) as file:
        events = [row["event"] for row in csv.DictReader(file)]

    return "TRANSFER_COMPLETED" in events and "TRANSFER_FAILED" not in events


def cwnd_events(sender_log):
    rows = []

    if not Path(sender_log).exists():
        return rows

    with open(sender_log) as file:
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


def summarize_cwnd(events):
    if not events:
        return {
            "cwnd_events": 0,
            "fast_recoveries": 0,
            "timeouts": 0,
            "max_cwnd": 0.0,
            "final_cwnd": 0.0,
            "slow_start_observed": False,
            "first_reduction": None,
        }

    reductions = [e for e in events
                  if e.get("reason") in ("fast_recovery", "timeout")]
    slow_start = any(e.get("phase") == "slow_start" for e in events)

    return {
        "cwnd_events": len(events),
        "fast_recoveries": sum(
            1 for e in reductions if e["reason"] == "fast_recovery"
        ),
        "timeouts": sum(1 for e in reductions if e["reason"] == "timeout"),
        "max_cwnd": max(e["cwnd"] for e in events),
        "final_cwnd": events[-1]["cwnd"],
        "slow_start_observed": slow_start,
        "first_reduction": reductions[0] if reductions else None,
    }


def resolve_results_dir(root, results_name):
    """Create and return a fresh, non-existent directory strictly
    inside `root`."""

    resolved_root = root.resolve()
    candidate = (root / results_name).resolve()

    if not candidate.is_relative_to(resolved_root):
        raise ValueError(
            f"results name {results_name!r} escapes {resolved_root}"
        )

    candidate.mkdir(parents=True)
    return candidate


def run_experiment(h1, h2, run_dir, size_bytes, ssthresh=None):
    input_path = run_dir / "input.bin"
    output_path = run_dir / "received.bin"

    if not input_path.exists() or input_path.stat().st_size != size_bytes:
        make_input(input_path, size_bytes)

    apply_netem(h1, "h1-eth0", DATA_NETEM)
    apply_netem(h2, "h2-eth0", ACK_NETEM)
    qdisc_data = qdisc_state(h1, "h1-eth0")
    qdisc_ack = qdisc_state(h2, "h2-eth0")

    receiver_cmd = [
        PYTHON, str(NODE), "receiver", str(output_path), "0.0.0.0",
        str(PORT), str(run_dir / "receiver_log.csv"),
        "--fin-linger", "1.0",
    ]

    with open(run_dir / "receiver.out", "w") as receiver_output:
        receiver = h2.popen(
            receiver_cmd,
            stdout=receiver_output,
            stderr=subprocess.STDOUT,
        )

    time.sleep(0.7)

    sender_cmd = [
        PYTHON, str(NODE), "sender", str(input_path), "10.0.0.2",
        str(PORT), str(run_dir / "sender_log.csv"),
    ] + reno_sender_flags(ssthresh)

    try:
        with open(run_dir / "sender.out", "w") as sender_output:
            sender = h1.popen(
                sender_cmd,
                stdout=sender_output,
                stderr=subprocess.STDOUT,
            )
    except Exception:
        receiver.kill()
        receiver.wait()
        raise

    start = time.monotonic()
    sender_exit = sender.wait()
    duration = time.monotonic() - start
    try:
        receiver.wait(timeout=30)
    except subprocess.TimeoutExpired:
        receiver.kill()
        receiver.wait()
    receiver_exit = receiver.returncode

    source_hash = sha256_file(input_path)
    if output_path.exists():
        destination_hash = sha256_file(output_path)
    else:
        destination_hash = "missing"
    match = source_hash == destination_hash

    (run_dir / "sha256_verification.txt").write_text(
        f"source      {source_hash}\n"
        f"destination {destination_hash}\n"
        f"{'PASS' if match else 'FAIL'}: SHA-256 hashes "
        f"{'match' if match else 'differ'}\n"
    )

    events = cwnd_events(run_dir / "sender_log.csv")
    stats = summarize_cwnd(events)
    goodput_kbytes_per_s = size_bytes / duration / 1000.0

    (run_dir / "cwnd_stats.txt").write_text(
        f"cwnd_events={stats['cwnd_events']} "
        f"fast_recoveries={stats['fast_recoveries']} "
        f"timeouts={stats['timeouts']} "
        f"max_cwnd={stats['max_cwnd']:.1f} "
        f"final_cwnd={stats['final_cwnd']:.1f} "
        f"slow_start_observed={stats['slow_start_observed']}\n"
    )

    plot_python = REPO_ROOT / ".venv" / "bin" / "python"
    if not plot_python.exists():
        plot_python = Path(sys.executable)

    plot_cmd = [
        str(plot_python), str(PLOT_SCRIPT), "cwnd",
        str(run_dir / "sender_log.csv"), str(run_dir / "cwnd.png"),
        "cwnd under Reno, 2 mbit bottleneck, 20-packet queue",
    ]

    (run_dir / "run_metadata.txt").write_text(
        "\n".join(
            [
                "Phase 3 congestion-control experiment (Mininet)",
                f"Topology: h1 (10.0.0.1) -- s1 -- h2 (10.0.0.2), port {PORT}",
                f"DATA netem (h1-eth0): {DATA_NETEM}",
                f"ACK netem  (h2-eth0): {ACK_NETEM}",
                f"Configured bottleneck rate: {RATE_KBYTES_PER_S:.0f} kB/s (2 mbit)",
                "Queue limit: 20 packets, controller: Reno",
                f"Initial cwnd: {INITIAL_CWND}",
                f"Initial ssthresh: {INITIAL_SSTHRESH if ssthresh is None else ssthresh}",
                f"File size: {size_bytes} bytes",
                f"Sender exit: {sender_exit}, receiver exit: {receiver_exit}",
                f"Duration: {duration:.2f} s",
                f"Goodput: {goodput_kbytes_per_s:.1f} kB/s "
                f"({100.0 * goodput_kbytes_per_s / RATE_KBYTES_PER_S:.0f}% of link rate)",
                f"cwnd events: {stats['cwnd_events']} "
                f"(fast_recovery={stats['fast_recoveries']}, "
                f"timeout={stats['timeouts']})",
                f"Max cwnd: {stats['max_cwnd']:.1f} packets",
                f"Slow start observed: {stats['slow_start_observed']}",
                f"SHA-256 match: {match}",
                f"qdisc data: {qdisc_data}",
                f"qdisc ack:  {qdisc_ack}",
                "",
                "Exact commands used:",
                f"  receiver: {' '.join(receiver_cmd)}",
                f"  sender:   {' '.join(sender_cmd)}",
                f"  tc (h1-eth0): netem {DATA_NETEM}",
                f"  tc (h2-eth0): netem {ACK_NETEM}",
                f"  plot: {' '.join(plot_cmd)}",
            ]
        )
        + "\n"
    )

    plot = subprocess.run(plot_cmd, check=False)
    if plot.returncode != 0:
        print(
            f"plot generation failed with exit {plot.returncode}",
            file=sys.stderr,
        )

    clear_netem(h1, "h1-eth0")
    clear_netem(h2, "h2-eth0")

    completed = sender_transfer_completed(run_dir / "sender_log.csv")

    ok = (
        sender_exit == 0
        and receiver_exit == 0
        and completed
        and match
        and plot.returncode == 0
    )

    return {
        "duration_s": duration,
        "goodput_kbytes_per_s": goodput_kbytes_per_s,
        "pct_of_rate": 100.0 * goodput_kbytes_per_s / RATE_KBYTES_PER_S,
        "ok": ok,
        "sender_exit": sender_exit,
        "receiver_exit": receiver_exit,
        "sha256_match": match,
        "cwnd": stats,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "size_bytes", nargs="?", type=int, default=TRANSFER_BYTES
    )
    parser.add_argument(
        "--results-name",
        default="mininet_cwnd_sawtooth",
        help="fresh subdirectory under results/phase3/ (never overwritten)",
    )
    parser.add_argument(
        "--initial-ssthresh",
        type=float,
        default=None,
        help="override the controller's initial ssthresh (packets)",
    )
    args = parser.parse_args()

    os.chdir(REPO_ROOT)

    net, h1, h2 = build_network()

    try:
        run_dir = resolve_results_dir(RESULTS_ROOT, args.results_name)
        info = run_experiment(
            h1, h2, run_dir, args.size_bytes, args.initial_ssthresh
        )
    finally:
        net.stop()

    print(info)
    sys.exit(0 if info["ok"] else 1)


if __name__ == "__main__":
    main()
