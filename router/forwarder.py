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

from router.queues.base import EnqueueResult, Queue, QueuedPacket
from router.queues.droptail import DropTail
from router.telemetry import TelemetryLogger

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
            pending = self._queue.dequeue()

        size = len(pending[0].payload) if pending else 0

        fill_limit = max(self._burst_bytes, size)
        self._tokens = min(
            fill_limit,
            self._tokens + self._rate_bps * (now - last_refill),
        )

        if pending is None or self._tokens < size:
            return pending

        packet, sojourn = pending
        if self._send(packet.payload, self._receiver_target, "forward"):
            self._tokens -= size
            self.counters["forwarded"] += 1
            self.counters["forwarded_bytes"] += size
            if self._telemetry is not None:
                self._telemetry.record(
                    "forward",
                    self._queue.length,
                    size,
                    self.counters["dropped"],
                    self.counters["marks"],
                    self.counters["forwarded_bytes"],
                    sojourn_seconds=sojourn,
                )
            return None

        return pending

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

            self.sender_addr = addr
            packet = QueuedPacket(payload=data, source=addr)
            result = self._queue.enqueue(packet)

            if result is EnqueueResult.DROPPED:
                self.counters["dropped"] += 1
                event = "drop"
            elif result is EnqueueResult.MARKED:
                self.counters["marks"] += 1
                self.counters["accepted"] += 1
                event = "enqueue"
            else:
                self.counters["accepted"] += 1
                event = "enqueue"

            if self._telemetry is not None:
                self._telemetry.record(
                    event,
                    self._queue.length,
                    len(data),
                    self.counters["dropped"],
                    self.counters["marks"],
                    self.counters["forwarded_bytes"],
                )

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
        help="DropTail capacity in packets",
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
        queue = DropTail(args.queue_capacity)
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
        f"rate {args.rate_mbps} Mbps, queue {args.queue_capacity} packets"
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
        f"acks_forwarded={counters['acks_forwarded']} "
        f"send_errors={counters['send_errors']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
