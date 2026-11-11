#!/usr/bin/env python3
"""Phase 2 experiments over the user-space lossy channel (no root needed).

Runs the Phase 2 acceptance experiments without Mininet/netem via
tests/mininet/phase2_node.py: rtt_baseline (~100 ms RTT), rtt_delay_change
(mid-run delay change), rwnd_small (small window limits throughput), and
lossy_reorder (10 MiB through 10% loss + reordering, SHA-256).

Outputs land in results/phase2/<experiment>/ (CSV logs, relay stats,
SHA-256 verification, run metadata).

Usage: python tests/mininet/phase2_uspace_experiments.py [experiment ...]
(default: all except lossy_reorder, which takes ~10 minutes).  Not
collected by pytest (no test_* functions).
"""

import argparse
import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE = REPO_ROOT / "tests" / "mininet" / "phase2_node.py"
DEFAULT_RESULTS_DIR = REPO_ROOT / "results" / "phase2"

# Lossy acceptance runs can take ~10 minutes; allow a comfortable margin.
LOSSY_TIMEOUT_S = 2700

PYTHON = sys.executable


def prepare_directory(run_dir):
    run_dir.mkdir(parents=True, exist_ok=True)

    for child in run_dir.iterdir():
        if child.is_file():
            child.unlink()

    return run_dir


def make_input(path, size_bytes):
    with open(path, "wb") as file:
        remaining = size_bytes
        while remaining > 0:
            chunk = min(remaining, 1 << 20)
            file.write(os.urandom(chunk))
            remaining -= chunk


def sha256_file(path):
    digest = hashlib.sha256()

    with open(path, "rb") as file:
        while True:
            chunk = file.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)

    return digest.hexdigest()


def spawn(args, stdout_path):
    return subprocess.Popen(
        [PYTHON, *args],
        cwd=REPO_ROOT,
        stdout=open(stdout_path, "w"),
        stderr=subprocess.STDOUT,
    )


def write_metadata(run_dir, lines):
    (run_dir / "run_metadata.txt").write_text("\n".join(lines) + "\n")


def run_transfer(run_dir, size_bytes, relay_options, rwnd_bytes=None,
                 fin_linger=None, port=24001, timeout_s=1800):
    """Run one sender -> relay -> receiver transfer.

    Returns a dict with duration, exit codes, SHA-256 result and goodput.
    """

    prepare_directory(run_dir)

    input_path = run_dir / "input.bin"
    output_path = run_dir / "out.bin"
    make_input(input_path, size_bytes)

    receiver_args = [
        str(NODE), "receiver", str(output_path), "127.0.0.2", str(port),
        str(run_dir / "receiver_log.csv"),
    ]
    if rwnd_bytes is not None:
        receiver_args += ["--rwnd-bytes", str(rwnd_bytes)]
    if fin_linger is not None:
        receiver_args += ["--fin-linger", str(fin_linger)]

    receiver = spawn(receiver_args, run_dir / "receiver.out")

    relay_args = [
        str(NODE), "relay",
        f"127.0.0.1:{port}", f"127.0.0.2:{port}",
        str(run_dir / "relay_stats.txt"),
        *relay_options,
    ]
    relay = spawn(relay_args, run_dir / "relay.out")

    time.sleep(0.7)

    sender = spawn(
        [
            str(NODE), "sender", str(input_path), "127.0.0.1", str(port),
            str(run_dir / "sender_log.csv"),
        ],
        run_dir / "sender.out",
    )

    start = time.monotonic()
    timed_out = False

    try:
        sender_exit = sender.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        sender.kill()
        sender.wait()
        sender_exit = -1

    duration = time.monotonic() - start

    try:
        receiver_exit = receiver.wait(timeout=60)
    except subprocess.TimeoutExpired:
        receiver.kill()
        receiver.wait()
        receiver_exit = -1

    relay.terminate()
    try:
        relay.wait(timeout=10)
    except subprocess.TimeoutExpired:
        relay.kill()
        relay.wait()

    source_hash = sha256_file(input_path)
    destination_hash = sha256_file(output_path)
    match = source_hash == destination_hash

    (run_dir / "sha256_verification.txt").write_text(
        f"source      {source_hash}\n"
        f"destination {destination_hash}\n"
        f"{'PASS' if match else 'FAIL'}: SHA-256 hashes "
        f"{'match' if match else 'differ'}\n"
    )

    return {
        "size_bytes": size_bytes,
        "duration_s": duration,
        "sender_exit": sender_exit,
        "receiver_exit": receiver_exit,
        "timed_out": timed_out,
        "sha256_match": match,
        "goodput_kbytes_per_s": size_bytes / duration / 1000.0 if duration else 0.0,
        "relay_options": " ".join(relay_options),
        "rwnd_bytes": rwnd_bytes,
        "port": port,
    }


def _summary_lines(title, info):
    return [
        f"Phase 2 experiment: {title}",
        "Channel: user-space UDP relay (tests/mininet/phase2_node.py relay)",
        f"Relay options: {info['relay_options']}",
        f"Port: {info['port']}",
        f"Receiver rwnd override: {info['rwnd_bytes']}",
        f"File size: {info['size_bytes']} bytes",
        f"Sender exit: {info['sender_exit']}, "
        f"receiver exit: {info['receiver_exit']}, "
        f"timed out: {info['timed_out']}",
        f"Duration: {info['duration_s']:.2f} s",
        f"Goodput: {info['goodput_kbytes_per_s']:.1f} kB/s",
        f"SHA-256 match: {info['sha256_match']}",
    ]


#  experiments


def experiment_rtt_baseline(results_root):
    run_dir = results_root / "rtt_baseline"
    info = run_transfer(
        run_dir,
        2 << 20,
        ["--delay-ms", "50", "--ack-delay-ms", "50", "--seed", "1"],
        fin_linger=1.0,
        port=24001,
    )
    write_metadata(
        run_dir,
        _summary_lines(
            "RTT estimation under a known 100 ms RTT (50 ms each way)", info
        ),
    )
    print("rtt_baseline:", info)


def experiment_rtt_delay_change(results_root):
    run_dir = results_root / "rtt_delay_change"
    info = run_transfer(
        run_dir,
        2 << 20,
        [
            "--delay-ms", "50", "--ack-delay-ms", "50",
            "--delay-change-ms", "400",
            "--delay-change-after-s", "3",
            "--seed", "1",
        ],
        fin_linger=1.0,
        port=24002,
    )
    write_metadata(
        run_dir,
        _summary_lines(
            "RTO adaptation when the one-way delay changes from 50 ms to "
            "400 ms at t=3 s (RTT 100 ms -> 800 ms)",
            info,
        ),
    )
    print("rtt_delay_change:", info)


def experiment_rwnd_small(results_root):
    base_dir = results_root / "rwnd_small"
    relay_options = ["--delay-ms", "30", "--ack-delay-ms", "30", "--seed", "1"]

    default = run_transfer(
        base_dir / "default",
        1 << 20,
        relay_options,
        fin_linger=1.0,
        port=24003,
    )
    small = run_transfer(
        base_dir / "small",
        1 << 20,
        relay_options,
        rwnd_bytes=4000,
        fin_linger=1.0,
        port=24004,
    )

    write_metadata(
        base_dir / "default",
        _summary_lines(
            "Baseline: default receiver window (64,000 bytes)", default
        ),
    )
    write_metadata(
        base_dir / "small",
        _summary_lines(
            "Small receiver window (4,000 bytes) advertisement", small
        ),
    )
    write_metadata(
        base_dir,
        [
            "Phase 2 experiment: small receiver window limiting throughput",
            f"Default rwnd run: {default['duration_s']:.2f} s, "
            f"{default['goodput_kbytes_per_s']:.1f} kB/s",
            f"Small rwnd run:   {small['duration_s']:.2f} s, "
            f"{small['goodput_kbytes_per_s']:.1f} kB/s",
            f"Throughput ratio (default / small): "
            f"{default['goodput_kbytes_per_s'] / small['goodput_kbytes_per_s']:.2f}x",
            f"SHA-256 match: default={default['sha256_match']}, "
            f"small={small['sha256_match']}",
        ],
    )
    print("rwnd_small:", {"default": default, "small": small})


def experiment_lossy_reorder(results_root):
    run_dir = results_root / "lossy_reorder"
    info = run_transfer(
        run_dir,
        10 << 20,
        [
            "--loss", "0.10", "--ack-loss", "0.10",
            "--delay-ms", "50", "--ack-delay-ms", "50",
            "--reorder-p", "0.15",
            "--seed", "1",
        ],
        port=24005,
        timeout_s=LOSSY_TIMEOUT_S,
    )
    write_metadata(
        run_dir,
        _summary_lines(
            "10 MiB through 10% data loss, 10% ACK loss and reordering "
            "(Phase 1 parity configuration)",
            info,
        ),
    )
    print("lossy_reorder:", info)


EXPERIMENTS = {
    "rtt_baseline": experiment_rtt_baseline,
    "rtt_delay_change": experiment_rtt_delay_change,
    "rwnd_small": experiment_rwnd_small,
    "lossy_reorder": experiment_lossy_reorder,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "experiments",
        nargs="*",
        help="experiment names; default: all except lossy_reorder",
    )
    parser.add_argument(
        "--results-dir",
        default=str(DEFAULT_RESULTS_DIR),
        help="output root (default: results/phase2); use a separate "
        "directory to preserve earlier evidence",
    )
    args = parser.parse_args()

    names = args.experiments
    results_root = Path(args.results_dir).resolve()

    if not names:
        names = ["rtt_baseline", "rtt_delay_change", "rwnd_small"]
    elif names == ["all"]:
        names = list(EXPERIMENTS)

    for name in names:
        if name not in EXPERIMENTS:
            parser.error(f"unknown experiment: {name}")
        EXPERIMENTS[name](results_root)


if __name__ == "__main__":
    main()
