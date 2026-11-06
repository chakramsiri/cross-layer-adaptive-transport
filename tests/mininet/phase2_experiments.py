#!/usr/bin/env python3
"""Phase 2 experiments under Mininet + tc netem (course VM, root required).

Topology: h1 (10.0.0.1) -- s1 -- h2 (10.0.0.2); h1-eth0 carries DATA
(delay, loss, reordering), h2-eth0 carries ACKs (delay, loss); the
delay-change experiment rewrites the qdisc mid-run.

Experiments: rtt_baseline (~100 ms RTT), rtt_delay_change (mid-run delay
change), rwnd_small (window limits throughput), lossy_reorder (10% loss).
Outputs land in results/phase2/mininet_<experiment>/.

Usage (in the Mininet VM, as root): sudo -E python
tests/mininet/phase2_experiments.py [experiment ...|all].  Not collected
by pytest; sandbox equivalents: tests/mininet/phase2_uspace_experiments.py.
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
RESULTS = REPO_ROOT / "results" / "phase2"

PYTHON = "python3"  # system python of the VM


def wrap_python(code):
    """Run a python one-liner inside a Mininet host."""

    return [PYTHON, "-c", code]


def build_network():
    from mininet.net import Mininet
    from mininet.node import OVSBridge
    from mininet.link import TCLink
    from mininet.log import setLogLevel

    setLogLevel("warning")

    net = Mininet(controller=None, switch=OVSBridge, link=TCLink)
    h1 = net.addHost("h1", ip="10.0.0.1")
    h2 = net.addHost("h2", ip="10.0.0.2")
    s1 = net.addSwitch("s1")

    net.addLink(h1, s1)
    net.addLink(h2, s1)

    net.start()
    return net, h1, h2


def clear_netem(host, interface):
    host.cmd(f"tc qdisc del dev {interface} root 2>/dev/null")


def apply_netem(host, interface, options):
    clear_netem(host, interface)
    host.cmd(f"tc qdisc add dev {interface} root netem {options}")


def change_netem(host, interface, options):
    host.cmd(f"tc qdisc change dev {interface} root netem {options}")


def qdisc_state(host, interface):
    return host.cmd(f"tc qdisc show dev {interface}").strip()


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


def run_transfer(h1, h2, run_dir, size_bytes, data_netem, ack_netem,
                 rwnd_bytes=None, fin_linger=None, port=5001,
                 delay_change=None):
    """One h1 -> h2 transfer; delay_change is (after_s, data, ack)."""

    run_dir.mkdir(parents=True, exist_ok=True)

    input_path = run_dir / "input.bin"
    output_path = run_dir / "received.bin"

    if not input_path.exists() or input_path.stat().st_size != size_bytes:
        make_input(input_path, size_bytes)

    apply_netem(h1, "h1-eth0", data_netem)
    apply_netem(h2, "h2-eth0", ack_netem)

    qdisc_data_before = qdisc_state(h1, "h1-eth0")
    qdisc_ack_before = qdisc_state(h2, "h2-eth0")

    receiver_cmd = [
        PYTHON, str(NODE), "receiver", str(output_path), "0.0.0.0",
        str(port), str(run_dir / "receiver_log.csv"),
    ]
    if rwnd_bytes is not None:
        receiver_cmd += ["--rwnd-bytes", str(rwnd_bytes)]
    if fin_linger is not None:
        receiver_cmd += ["--fin-linger", str(fin_linger)]

    receiver = h2.popen(
        receiver_cmd,
        stdout=open(run_dir / "receiver.out", "w"),
        stderr=subprocess.STDOUT,
    )

    time.sleep(0.7)

    sender = h1.popen(
        [
            PYTHON, str(NODE), "sender", str(input_path), "10.0.0.2",
            str(port), str(run_dir / "sender_log.csv"),
        ],
        stdout=open(run_dir / "sender.out", "w"),
        stderr=subprocess.STDOUT,
    )

    start = time.monotonic()
    change_note = "none"

    if delay_change is not None:
        after_s, data_options, ack_options = delay_change

        while time.monotonic() - start < after_s and sender.poll() is None:
            time.sleep(0.1)

        if sender.poll() is None:
            change_netem(h1, "h1-eth0", data_options)
            change_netem(h2, "h2-eth0", ack_options)
            change_note = (
                f"at t={time.monotonic() - start:.1f}s: "
                f"h1 <- {data_options}; h2 <- {ack_options}"
            )

    sender_exit = sender.wait()
    duration = time.monotonic() - start
    receiver_exit = receiver.wait()

    qdisc_data_after = qdisc_state(h1, "h1-eth0")
    qdisc_ack_after = qdisc_state(h2, "h2-eth0")

    source_hash = sha256_file(input_path)
    destination_hash = sha256_file(output_path)
    match = source_hash == destination_hash

    (run_dir / "sha256_verification.txt").write_text(
        f"source      {source_hash}\n"
        f"destination {destination_hash}\n"
        f"{'PASS' if match else 'FAIL'}: SHA-256 hashes "
        f"{'match' if match else 'differ'}\n"
    )

    (run_dir / "run_metadata.txt").write_text(
        "\n".join(
            [
                "Phase 2 Mininet/netem experiment",
                f"Topology: h1 (10.0.0.1) -- s1 -- h2 (10.0.0.2), port {port}",
                f"DATA netem (h1-eth0): {data_netem}",
                f"ACK netem  (h2-eth0): {ack_netem}",
                f"Delay change: {change_note}",
                f"Receiver rwnd override: {rwnd_bytes}",
                f"File size: {size_bytes} bytes",
                f"Sender exit: {sender_exit}, receiver exit: {receiver_exit}",
                f"Duration: {duration:.2f} s",
                f"Goodput: {size_bytes / duration / 1000.0:.1f} kB/s",
                f"SHA-256 match: {match}",
                f"qdisc data before: {qdisc_data_before}",
                f"qdisc data after:  {qdisc_data_after}",
                f"qdisc ack before: {qdisc_ack_before}",
                f"qdisc ack after:  {qdisc_ack_after}",
                "",
                "Exact commands used:",
                f"  h2: {PYTHON} {NODE} receiver <out> 0.0.0.0 {port} "
                f"<log>{' --rwnd-bytes ' + str(rwnd_bytes) if rwnd_bytes else ''}",
                f"  h1: {PYTHON} {NODE} sender <file> 10.0.0.2 {port} <log>",
                f"  tc (h1-eth0): netem {data_netem}",
                f"  tc (h2-eth0): netem {ack_netem}",
            ]
        )
        + "\n"
    )

    clear_netem(h1, "h1-eth0")
    clear_netem(h2, "h2-eth0")

    return {
        "duration_s": duration,
        "sender_exit": sender_exit,
        "receiver_exit": receiver_exit,
        "sha256_match": match,
    }


# experiments


def experiment_rtt_baseline(h1, h2):
    print("== rtt_baseline ==")
    info = run_transfer(
        h1, h2, RESULTS / "mininet_rtt_baseline", 2 << 20,
        "delay 50ms", "delay 50ms", fin_linger=1.0,
    )
    print(info)


def experiment_rtt_delay_change(h1, h2):
    print("== rtt_delay_change ==")
    info = run_transfer(
        h1, h2, RESULTS / "mininet_rtt_delay_change", 2 << 20,
        "delay 50ms", "delay 50ms", fin_linger=1.0,
        delay_change=(3.0, "delay 400ms", "delay 400ms"),
    )
    print(info)


def experiment_rwnd_small(h1, h2):
    print("== rwnd_small ==")
    default = run_transfer(
        h1, h2, RESULTS / "mininet_rwnd_small" / "default", 1 << 20,
        "delay 30ms", "delay 30ms", fin_linger=1.0,
    )
    small = run_transfer(
        h1, h2, RESULTS / "mininet_rwnd_small" / "small", 1 << 20,
        "delay 30ms", "delay 30ms", rwnd_bytes=4000, fin_linger=1.0,
    )
    print({"default": default, "small": small})


def experiment_lossy_reorder(h1, h2):
    print("== lossy_reorder ==")
    info = run_transfer(
        h1, h2, RESULTS / "mininet_lossy_reorder", 10 << 20,
        "delay 50ms loss 10% reorder 25% 50%", "delay 50ms loss 10%",
    )
    print(info)


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
    args = parser.parse_args()

    names = args.experiments
    if not names:
        names = ["rtt_baseline", "rtt_delay_change", "rwnd_small"]
    elif names == ["all"]:
        names = list(EXPERIMENTS)

    for name in names:
        if name not in EXPERIMENTS:
            parser.error(f"unknown experiment: {name}")

    os.chdir(REPO_ROOT)

    net, h1, h2 = build_network()

    try:
        for name in names:
            EXPERIMENTS[name](h1, h2)
    finally:
        net.stop()


if __name__ == "__main__":
    main()
