"""Rate-limited user-space UDP relay: transparent two-face forwarding,
with a queued forward path and unqueued reverse-path ACK relaying."""

import argparse
import json
import math
import os
import selectors
import signal
import socket
import sys
import threading
import time
from pathlib import Path

from router.queues.base import (
    DequeueResult,
    EnqueueResult,
    Queue,
    QueuedPacket,
)
from router.queues.codel import CoDel
from router.queues.droptail import DropTail
from router.queues.red import DEFAULT_TYPICAL_PACKET_BYTES, RED
from router.telemetry import TelemetryLogger
from transport.feedback import stamp_forwarded_datagram
from transport.packet import CROSS, DATA, FIN, SYN, TYPE_MASK

MAX_DATAGRAM = 65507

MIN_BURST_BYTES = 4096
BURST_TIME_SECONDS = 0.01

IDLE_POLL_SECONDS = 0.5


def parse_address(text):
    host, separator, port = text.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError(
            f"expected ip:port, got {text!r}"
        )

    try:
        port_number = int(port)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"port must be an integer, got {port!r}"
        ) from None

    if not 0 <= port_number <= 65535:
        raise argparse.ArgumentTypeError(
            f"port out of range: {port_number}"
        )

    return host, port_number


def format_address(address):
    return f"{address[0]}:{address[1]}"


def is_cross_datagram(data):
    return bool(data) and (data[0] & TYPE_MASK) == CROSS


def datagram_kind(payload):
    if not payload:
        return "other"
    return {
        DATA: "data",
        CROSS: "cross",
        SYN: "control",
        FIN: "control",
    }.get(payload[0] & TYPE_MASK, "other")


def build_queue(args):
    """Instantiate the queue discipline selected by the CLI options."""
    if args.aqm == "droptail":
        if args.aqm_mode != "drop":
            raise ValueError("droptail supports only --aqm-mode drop")
        return DropTail(args.queue_capacity)

    if args.aqm == "red":
        return RED(
            args.queue_capacity,
            w_q=args.red_w_q,
            min_th=args.red_min_th,
            max_th=args.red_max_th,
            max_p=args.red_max_p,
            gentle=args.red_gentle,
            mode=args.aqm_mode,
            seed=args.red_seed,
            typical_packet_bytes=args.red_typical_packet_bytes,
            rate_bps=args.rate_mbps * 1_000_000 / 8,
        )

    if args.aqm == "codel":
        return CoDel(
            args.queue_capacity,
            target_seconds=args.codel_target_ms / 1000.0,
            interval_seconds=args.codel_interval_ms / 1000.0,
            mode=args.aqm_mode,
        )

    raise ValueError(f"unknown aqm: {args.aqm!r}")


def build_router_metadata(args, queue):
    return {
        "algorithm": args.aqm,
        "mode": args.aqm_mode,
        "queue_capacity": args.queue_capacity,
        "rate_mbps": args.rate_mbps,
        "rate_bytes_per_second": args.rate_mbps * 1_000_000 / 8,
        "parameters": queue.parameters,
        "sender_face": format_address(args.sender_face),
        "receiver_face": format_address(args.receiver_face),
        "receiver": format_address(args.receiver),
        "telemetry": str(args.telemetry),
        "pid": os.getpid(),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def write_router_metadata(args, queue):
    path = Path(args.telemetry).parent / "router_metadata.json"
    with open(path, "w") as file:
        json.dump(build_router_metadata(args, queue), file, indent=2)
        file.write("\n")
    return path


class Forwarder:
    def __init__(
        self,
        sender_face,
        receiver_face,
        receiver_target,
        rate_bps,
        queue,
        telemetry=None,
        ready_file=None,
        max_seconds=None,
    ):
        self._validate_face(sender_face, "sender_face")
        self._validate_face(receiver_face, "receiver_face")
        self._validate_face(receiver_target, "receiver_target")

        if isinstance(rate_bps, bool) or not isinstance(rate_bps, (int, float)):
            raise ValueError(
                f"rate must be a number of bytes per second, got {rate_bps!r}"
            )
        if not math.isfinite(rate_bps) or rate_bps <= 0:
            raise ValueError(
                f"rate must be positive and finite, got {rate_bps!r}"
            )

        if not isinstance(queue, Queue):
            raise TypeError(
                f"queue must implement router.queues.base.Queue, "
                f"got {type(queue).__name__}"
            )

        self._sender_face = tuple(sender_face)
        self._receiver_face = tuple(receiver_face)
        self._receiver_target = tuple(receiver_target)
        self._rate_bps = float(rate_bps)
        self._queue = queue
        self._telemetry = telemetry
        self._ready_file = Path(ready_file) if ready_file else None
        self._max_seconds = max_seconds

        self._burst_bytes = max(
            MIN_BURST_BYTES, self._rate_bps * BURST_TIME_SECONDS
        )
        self._tokens = self._burst_bytes

        self._stop_event = threading.Event()
        self._ready_event = threading.Event()

        self.sender_socket = None
        self.receiver_socket = None
        self._wake_reader = None
        self._wake_writer = None

        self.sender_addr = None
        self.counters = {
            "accepted": 0,
            "forwarded": 0,
            "dropped": 0,
            "marks": 0,
            "ce_marked": 0,
            "forwarded_bytes": 0,
            "acks_forwarded": 0,
            "send_errors": 0,
        }

    @staticmethod
    def _validate_face(address, name):
        if (
            not isinstance(address, tuple)
            or len(address) != 2
            or not isinstance(address[0], str)
            or not address[0]
            or isinstance(address[1], bool)
            or not isinstance(address[1], int)
            or not 0 <= address[1] <= 65535
        ):
            raise ValueError(
                f"{name} must be a (host, port) tuple, got {address!r}"
            )

    @property
    def rate_bps(self):
        return self._rate_bps

    @property
    def sender_face(self):
        """Actual (ip, port) of the sender face after setup."""
        return self.sender_socket.getsockname() if self.sender_socket else None

    @property
    def receiver_face(self):
        """Actual (ip, port) of the receiver face after setup."""
        return (
            self.receiver_socket.getsockname() if self.receiver_socket else None
        )

    def wait_ready(self, timeout=None):
        return self._ready_event.wait(timeout)

    def stop(self):
        """Request shutdown from any thread; wakes the service loop."""
        self._stop_event.set()
        if self._wake_writer is not None:
            try:
                self._wake_writer.send(b"\x00")
            except OSError:
                pass

    def run(self):
        """Bind faces, run the service loop, and clean up.  Blocking."""
        try:
            self._setup()
            self._loop()
        finally:
            self._teardown()

    def _setup(self):
        self.sender_socket = self._bind(self._sender_face)
        self.receiver_socket = self._bind(self._receiver_face)
        self._wake_reader, self._wake_writer = socket.socketpair()
        self._wake_reader.setblocking(False)
        self._wake_writer.setblocking(False)

        if self._ready_file is not None:
            self._ready_file.parent.mkdir(parents=True, exist_ok=True)
            self._ready_file.write_text(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "sender_face": format_address(self.sender_face),
                        "receiver_face": format_address(self.receiver_face),
                    }
                )
                + "\n"
            )

        self._ready_event.set()

    @staticmethod
    def _bind(address):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.bind(address)
            sock.setblocking(False)
        except OSError:
            sock.close()
            raise
        return sock

    def _loop(self):
        selector = selectors.DefaultSelector()
        selector.register(self.sender_socket, selectors.EVENT_READ)
        selector.register(self.receiver_socket, selectors.EVENT_READ)
        selector.register(self._wake_reader, selectors.EVENT_READ)

        pending = None
        last_refill = time.monotonic()
        started = last_refill

        try:
            while not self._stop_event.is_set():
                now = time.monotonic()
                if self._max_seconds is not None and now - started >= self._max_seconds:
                    print(
                        f"router: max-seconds ({self._max_seconds}s) reached"
                    )
                    break

                pending = self._service(now, last_refill, pending)
                last_refill = now

                timeout = self._wait_timeout(pending, started)
                for key, _ in selector.select(timeout):
                    if key.fileobj is self._wake_reader:
                        self._drain(self._wake_reader)
                    elif key.fileobj is self.sender_socket:
                        self._handle_sender_datagrams()
                    else:
                        self._handle_receiver_datagrams()
        finally:
            selector.close()

    def _service(self, now, last_refill, pending):
        """Refill tokens and forward what the bucket allows; returns unsent pending."""
        if pending is None and self._queue.length:
            pending = self._apply_dequeue_result(self._queue.dequeue())

        size = len(pending[0].payload) if pending else 0

        fill_limit = max(self._burst_bytes, size)
        self._tokens = min(
            fill_limit,
            self._tokens + self._rate_bps * (now - last_refill),
        )

        if pending is None or self._tokens < size:
            return pending

        packet, sojourn, _result = pending
        datagram, ce_applied = stamp_forwarded_datagram(
            packet.payload, sojourn, packet.marked
        )
        if self._send(datagram, self._receiver_target, "forward"):
            self._tokens -= size
            self.counters["forwarded"] += 1
            self.counters["forwarded_bytes"] += size
            if ce_applied:
                self.counters["ce_marked"] += 1
            self._record("forward", sojourn, size,
                         kind=datagram_kind(packet.payload))
            return None

        return pending

    def _apply_dequeue_result(self, pending):
        if pending is None:
            return None

        packet, sojourn, result = pending

        if result is DequeueResult.DROP:
            self.counters["dropped"] += 1
            self._record("drop", sojourn, len(packet.payload), reason="codel",
                         kind=datagram_kind(packet.payload))
            return None

        if result is DequeueResult.MARK:
            packet.marked = True
            self.counters["marks"] += 1
            self._record("mark", sojourn, len(packet.payload), reason="codel",
                         kind=datagram_kind(packet.payload))

        return pending

    def _record(self, event, sojourn, datagram_bytes, reason=None, kind=None):
        if self._telemetry is None:
            return

        self._telemetry.record(
            event,
            self._queue.length,
            datagram_bytes,
            self.counters["dropped"],
            self.counters["marks"],
            self.counters["forwarded_bytes"],
            sojourn_seconds=sojourn,
            avg_queue=self._queue.average_queue,
            reason=reason,
            kind=kind,
        )

    def _wait_timeout(self, pending, started):
        """Next service wait: token deficit, idle cap, or the max-seconds deadline."""
        if pending is not None:
            deficit = len(pending[0].payload) - self._tokens
            timeout = max(0.0, deficit / self._rate_bps)
        elif self._queue.length:
            timeout = 0.0
        else:
            timeout = IDLE_POLL_SECONDS

        if self._max_seconds is not None:
            remaining = max(
                0.0,
                self._max_seconds - (time.monotonic() - started),
            )
            timeout = min(timeout, remaining)

        return timeout

    @staticmethod
    def _drain(sock):
        while True:
            try:
                sock.recv(4096)
            except (BlockingIOError, OSError):
                return

    def _handle_sender_datagrams(self):
        while True:
            try:
                data, addr = self.sender_socket.recvfrom(MAX_DATAGRAM)
            except BlockingIOError:
                return
            except OSError as error:
                self._warn(f"sender face recv failed: {error}")
                return

            if not is_cross_datagram(data):
                self.sender_addr = addr
            packet = QueuedPacket(payload=data, source=addr)
            was_full = self._queue.length >= self._queue.capacity
            result = self._queue.enqueue(packet)

            if result is EnqueueResult.DROPPED:
                self.counters["dropped"] += 1
                reason = "full" if was_full else "red"
                self._record("drop", None, len(data), reason=reason,
                             kind=datagram_kind(data))
            elif result is EnqueueResult.MARKED:
                packet.marked = True
                self.counters["marks"] += 1
                self.counters["accepted"] += 1
                self._record("mark", None, len(data), reason="red",
                             kind=datagram_kind(data))
            else:
                self.counters["accepted"] += 1
                self._record("enqueue", None, len(data),
                             kind=datagram_kind(data))

    def _handle_receiver_datagrams(self):
        while True:
            try:
                data, _addr = self.receiver_socket.recvfrom(MAX_DATAGRAM)
            except BlockingIOError:
                return
            except OSError as error:
                self._warn(f"receiver face recv failed: {error}")
                return

            if self.sender_addr is None:
                self._warn("reverse datagram before any sender seen; dropped")
                continue

            if self._send(data, self.sender_addr, "reverse"):
                self.counters["acks_forwarded"] += 1

    def _send(self, payload, destination, direction):
        try:
            sock = (
                self.receiver_socket
                if direction == "forward"
                else self.sender_socket
            )
            sock.sendto(payload, destination)
            return True
        except BlockingIOError:
            self.counters["send_errors"] += 1
            self._warn(f"{direction} send would block; datagram dropped")
            return False
        except OSError as error:
            self.counters["send_errors"] += 1
            self._warn(f"{direction} send to {destination} failed: {error}")
            return False

    def _warn(self, message):
        print(f"router: {message}", file=sys.stderr, flush=True)

    def _teardown(self):
        self._ready_event.clear()
        for sock in (self.sender_socket, self.receiver_socket):
            if sock is not None:
                sock.close()
        self.sender_socket = None
        self.receiver_socket = None
        for sock in (self._wake_reader, self._wake_writer):
            if sock is not None:
                sock.close()
        self._wake_reader = None
        self._wake_writer = None
        if self._telemetry is not None:
            self._telemetry.close()


def build_parser():
    parser = argparse.ArgumentParser(
        description="Rate-limited UDP forwarder with a DropTail queue."
    )
    parser.add_argument(
        "--sender-face",
        type=parse_address,
        required=True,
        help="ip:port the transport sender sends datagrams to",
    )
    parser.add_argument(
        "--receiver-face",
        type=parse_address,
        required=True,
        help="ip:port datagrams are forwarded from; ACKs arrive here",
    )
    parser.add_argument(
        "--receiver",
        type=parse_address,
        required=True,
        help="ip:port of the transport receiver",
    )
    parser.add_argument(
        "--rate-mbps",
        type=float,
        required=True,
        help="forward-path service rate in megabits per second (2-10)",
    )
    parser.add_argument(
        "--queue-capacity",
        type=int,
        required=True,
        help="queue capacity in packets for every discipline",
    )
    parser.add_argument(
        "--aqm",
        choices=("droptail", "red", "codel"),
        default="droptail",
        help="queue discipline for the forward path (default droptail)",
    )
    parser.add_argument(
        "--aqm-mode",
        choices=("drop", "mark"),
        default="drop",
        help="congestion signalling mode for red and codel",
    )
    parser.add_argument(
        "--red-w-q",
        type=float,
        default=0.002,
        help="RED EWMA weight on the instantaneous queue length",
    )
    parser.add_argument(
        "--red-min-th",
        type=float,
        default=20.0,
        help="RED minimum average queue threshold in packets",
    )
    parser.add_argument(
        "--red-max-th",
        type=float,
        default=80.0,
        help="RED maximum average queue threshold in packets",
    )
    parser.add_argument(
        "--red-max-p",
        type=float,
        default=0.1,
        help="RED maximum signalling probability",
    )
    parser.add_argument(
        "--red-gentle",
        action="store_true",
        help="ramp RED signalling to certainty at twice max_th",
    )
    parser.add_argument(
        "--red-seed",
        type=int,
        default=None,
        help="seed for reproducible RED signalling decisions",
    )
    parser.add_argument(
        "--red-typical-packet-bytes",
        type=float,
        default=DEFAULT_TYPICAL_PACKET_BYTES,
        help="typical RED packet size on the bottleneck in bytes",
    )
    parser.add_argument(
        "--codel-target-ms",
        type=float,
        default=5.0,
        help="CoDel target sojourn in milliseconds (default 5)",
    )
    parser.add_argument(
        "--codel-interval-ms",
        type=float,
        default=100.0,
        help="CoDel above-target window in milliseconds (default 100)",
    )
    parser.add_argument(
        "--telemetry",
        required=True,
        help="CSV path for router telemetry",
    )
    parser.add_argument(
        "--ready-file",
        default=None,
        help="file written once both faces are bound",
    )
    parser.add_argument(
        "--max-seconds",
        type=float,
        default=1800.0,
        help="safety stop after this many seconds (default 1800)",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)

    try:
        queue = build_queue(args)
        telemetry = TelemetryLogger(args.telemetry)
        forwarder = Forwarder(
            sender_face=args.sender_face,
            receiver_face=args.receiver_face,
            receiver_target=args.receiver,
            rate_bps=args.rate_mbps * 1_000_000 / 8,
            queue=queue,
            telemetry=telemetry,
            ready_file=args.ready_file,
            max_seconds=args.max_seconds,
        )
        write_router_metadata(args, queue)
    except (TypeError, ValueError, OSError) as error:
        print(f"router: startup failed: {error}", file=sys.stderr)
        return 1

    def _handle_signal(signum, _frame):
        forwarder.stop()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    print(
        f"router: sender face {format_address(args.sender_face)}, "
        f"receiver face {format_address(args.receiver_face)}, "
        f"receiver {format_address(args.receiver)}, "
        f"rate {args.rate_mbps} Mbps, aqm {args.aqm} ({args.aqm_mode}), "
        f"queue {args.queue_capacity} packets"
    )

    try:
        forwarder.run()
    except OSError as error:
        print(f"router: failed: {error}", file=sys.stderr)
        return 1

    counters = forwarder.counters
    print(
        f"router: accepted={counters['accepted']} "
        f"forwarded={counters['forwarded']} "
        f"dropped={counters['dropped']} "
        f"marks={counters['marks']} "
        f"ce_marked={counters['ce_marked']} "
        f"acks_forwarded={counters['acks_forwarded']} "
        f"send_errors={counters['send_errors']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
