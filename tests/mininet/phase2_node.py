#!/usr/bin/env python3
"""Run transport endpoints and a UDP relay for network experiments.

Provides receiver, sender and relay command-line roles.
"""

import argparse
import heapq
import os
import random
import selectors
import signal
import socket
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _isolate_cwd():
    """Move to a temporary directory to isolate relative log paths."""

    os.chdir(tempfile.mkdtemp(prefix="phase2_node_"))



def run_receiver(args):
    _isolate_cwd()

    import transport.receiver as receiver_module
    from transport.csv_logger import CSVLogger

    receiver_module.HOST = args.host
    receiver_module.PORT = args.port

    if args.rwnd_bytes is not None:
        receiver_module.RECV_BUFFER_BYTES = args.rwnd_bytes

    if args.fin_linger is not None:
        receiver_module.FIN_LINGER = args.fin_linger

    receiver_module.logger = CSVLogger(args.log)
    receiver_module.receive_file(args.output)


def run_sender(args):
    _isolate_cwd()

    import transport.sender as sender_module
    from transport.cc.reno import INITIAL_CWND, INITIAL_SSTHRESH, RenoController
    from transport.csv_logger import CSVLogger

    sender_module.PORT = args.port
    sender_module.logger = CSVLogger(args.log)

    controller = None
    if args.initial_cwnd is not None or args.initial_ssthresh is not None:
        controller = RenoController(
            initial_cwnd=(
                INITIAL_CWND if args.initial_cwnd is None
                else args.initial_cwnd
            ),
            ssthresh=(
                INITIAL_SSTHRESH if args.initial_ssthresh is None
                else args.initial_ssthresh
            ),
        )

    if not sender_module.send_file(args.file, args.host, controller):
        sys.exit(1)


def _parse_address(text):
    host, _, port = text.rpartition(":")
    return host, int(port)


def run_relay(args):
    """Single-socket UDP relay with seeded delay/loss/reordering.

    Data path: sender -> relay -> forward address (the receiver).
    Reverse path: receiver -> relay -> last sender address.
    A mid-run delay change is applied once after --delay-change-after-s.
    """

    listen_addr = _parse_address(args.listen)
    forward_addr = _parse_address(args.forward)

    stop = {"flag": False}

    def _handle_stop(signum, frame):  # noqa: ARG001
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    rng = random.Random(args.seed)
    selector = selectors.DefaultSelector()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(listen_addr)
    sock.setblocking(False)
    selector.register(sock, selectors.EVENT_READ)

    sender_addr = None
    heap = []
    push_seq = 0

    counters = {
        "forward_in": 0,
        "forward_out": 0,
        "forward_dropped": 0,
        "reverse_in": 0,
        "reverse_out": 0,
        "reverse_dropped": 0,
        "reorder_scheduled": 0,
        "delay_changes": 0,
    }

    delay_ms = args.delay_ms
    ack_delay_ms = args.ack_delay_ms
    delay_changed = False
    start = time.monotonic()

    def schedule(delay_seconds, destination, payload):
        nonlocal push_seq
        heapq.heappush(
            heap,
            (time.monotonic() + delay_seconds, push_seq, destination, payload),
        )
        push_seq += 1

    while not stop["flag"]:
        now = time.monotonic()

        if now - start > args.max_seconds:
            print("relay: max-seconds reached, exiting")
            break

        if (
            args.delay_change_ms is not None
            and not delay_changed
            and now - start >= args.delay_change_after_s
        ):
            delay_ms = args.delay_change_ms
            if args.ack_delay_change_ms is not None:
                ack_delay_ms = args.ack_delay_change_ms
            else:
                ack_delay_ms = args.delay_change_ms
            delay_changed = True
            counters["delay_changes"] += 1
            print(f"relay: delay changed at t={now - start:.2f}s")

        while heap and heap[0][0] <= now:
            _, _, destination, payload = heapq.heappop(heap)
            try:
                sock.sendto(payload, destination)
            except OSError:
                continue
            if destination == forward_addr:
                counters["forward_out"] += 1
            else:
                counters["reverse_out"] += 1

        timeout = 0.01
        if heap:
            timeout = max(0.0, min(timeout, heap[0][0] - time.monotonic()))

        for _ in selector.select(timeout):
            try:
                data, addr = sock.recvfrom(65535)
            except OSError:
                continue

            if addr == forward_addr:
                # From the receiver; forward back to the sender.
                counters["reverse_in"] += 1
                if sender_addr is None:
                    continue
                if rng.random() < args.ack_loss:
                    counters["reverse_dropped"] += 1
                    continue
                schedule(ack_delay_ms / 1000.0, sender_addr, data)
            else:
                # From the sender; forward to the receiver with impairments.
                sender_addr = addr
                counters["forward_in"] += 1
                delay = delay_ms / 1000.0
                if rng.random() < args.reorder_p:
                    delay += args.reorder_extra_ms / 1000.0
                    counters["reorder_scheduled"] += 1
                if rng.random() < args.loss:
                    counters["forward_dropped"] += 1
                    continue
                schedule(delay, forward_addr, data)

    with open(args.stats_file, "w") as stats_file:
        stats_file.write(
            f"seed={args.seed} loss={args.loss} ack_loss={args.ack_loss} "
            f"delay_ms={args.delay_ms} ack_delay_ms={args.ack_delay_ms} "
            f"reorder_p={args.reorder_p} "
            f"reorder_extra_ms={args.reorder_extra_ms} "
            f"delay_change_ms={args.delay_change_ms} "
            f"delay_change_after_s={args.delay_change_after_s}\n"
        )
        for key, value in counters.items():
            stats_file.write(f"{key}={value}\n")

    print("relay: stats written to", args.stats_file)



def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="role", required=True)

    receiver = sub.add_parser("receiver")
    receiver.add_argument("output")
    receiver.add_argument("host")
    receiver.add_argument("port", type=int)
    receiver.add_argument("log")
    receiver.add_argument("--rwnd-bytes", type=int, default=None)
    receiver.add_argument("--fin-linger", type=float, default=None)
    receiver.set_defaults(func=run_receiver)

    sender = sub.add_parser("sender")
    sender.add_argument("file")
    sender.add_argument("host")
    sender.add_argument("port", type=int)
    sender.add_argument("log")
    sender.add_argument("--initial-cwnd", type=float, default=None)
    sender.add_argument("--initial-ssthresh", type=float, default=None)
    sender.set_defaults(func=run_sender)

    relay = sub.add_parser("relay")
    relay.add_argument("listen")
    relay.add_argument("forward")
    relay.add_argument("stats_file")
    relay.add_argument("--loss", type=float, default=0.0)
    relay.add_argument("--ack-loss", type=float, default=0.0)
    relay.add_argument("--delay-ms", type=float, default=0.0)
    relay.add_argument("--ack-delay-ms", type=float, default=0.0)
    relay.add_argument("--reorder-p", type=float, default=0.0)
    relay.add_argument("--reorder-extra-ms", type=float, default=100.0)
    relay.add_argument("--delay-change-ms", type=float, default=None)
    relay.add_argument("--ack-delay-change-ms", type=float, default=None)
    relay.add_argument("--delay-change-after-s", type=float, default=0.0)
    relay.add_argument("--seed", type=int, default=1)
    relay.add_argument("--max-seconds", type=float, default=1800.0)
    relay.set_defaults(func=run_relay)

    return parser


if __name__ == "__main__":
    parsed = build_parser().parse_args()
    parsed.func(parsed)
