"""Forwarder integration tests over real loopback UDP sockets."""

import csv
import socket
import threading
import tempfile
import time
import unittest
from pathlib import Path

from router.forwarder import Forwarder, build_parser, main
from router.queues.droptail import DropTail
from router.telemetry import TelemetryLogger


def free_udp_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def recv_all(sock, count, timeout=5.0):
    deadline = time.monotonic() + timeout
    datagrams = []

    while len(datagrams) < count:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"received {len(datagrams)} of {count} datagrams"
            )
        sock.settimeout(remaining)
        data, addr = sock.recvfrom(65535)
        datagrams.append((data, addr))

    return datagrams


class ForwarderFixture:
    """Starts a forwarder thread and guarantees teardown."""

    def __init__(self, test, rate_bps=5_000_000, capacity=64,
                 with_receiver=True, telemetry=True):
        self.test = test
        self.tmp = Path(tempfile.mkdtemp(prefix="fwd_test_"))
        self.receiver = None

        if with_receiver:
            self.receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.receiver.bind(("127.0.0.1", 0))
            target = self.receiver.getsockname()
        else:
            target = ("127.0.0.1", free_udp_port())

        telemetry_logger = (
            TelemetryLogger(self.tmp / "router_telemetry.csv")
            if telemetry
            else None
        )
        self.forwarder = Forwarder(
            sender_face=("127.0.0.1", 0),
            receiver_face=("127.0.0.1", 0),
            receiver_target=target,
            rate_bps=rate_bps,
            queue=DropTail(capacity),
            telemetry=telemetry_logger,
        )
        self.thread = threading.Thread(
            target=self.forwarder.run, daemon=True
        )
        self.thread.start()

        if not self.forwarder.wait_ready(5.0):
            self.stop()
            raise AssertionError("forwarder did not become ready")

        self.sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sender.bind(("127.0.0.1", 0))
        test.addCleanup(self.stop)

    @property
    def sender_face(self):
        return self.forwarder.sender_face

    @property
    def receiver_face(self):
        return self.forwarder.receiver_face

    def stop(self):
        self.forwarder.stop()
        self.thread.join(timeout=5.0)
        self.sender.close()
        if self.receiver is not None:
            self.receiver.close()


class TestForwardPath(unittest.TestCase):
    def test_datagrams_forwarded_preserved_and_in_order(self):
        fixture = ForwarderFixture(self)
        sent = [bytes([index % 256]) * 256 for index in range(20)]

        for datagram in sent:
            fixture.sender.sendto(datagram, fixture.sender_face)

        received = recv_all(fixture.receiver, len(sent))
        self.assertEqual([data for data, _addr in received], sent)

    def test_control_sized_datagrams_pass_through(self):
        """SYN/FIN sized control datagrams traverse the same path."""
        fixture = ForwarderFixture(self)
        syn = b"\x01" + b"\x00" * 28

        fin = b"\x04" + b"\x00" * 28

        fixture.sender.sendto(syn, fixture.sender_face)
        fixture.sender.sendto(fin, fixture.sender_face)

        received = recv_all(fixture.receiver, 2)
        self.assertEqual([data for data, _addr in received], [syn, fin])

    def test_source_address_preserved_for_receiver(self):
        fixture = ForwarderFixture(self)

        fixture.sender.sendto(b"hello", fixture.sender_face)
        _data, addr = recv_all(fixture.receiver, 1)[0]

        self.assertEqual(addr, fixture.receiver_face)


class TestReversePath(unittest.TestCase):
    def test_acks_relayed_from_sender_face(self):
        fixture = ForwarderFixture(self)

        fixture.sender.sendto(b"data", fixture.sender_face)
        recv_all(fixture.receiver, 1)

        fixture.receiver.sendto(b"ACK-1", fixture.receiver_face)
        data, addr = recv_all(fixture.sender, 1)[0]

        self.assertEqual(data, b"ACK-1")
        self.assertEqual(addr, fixture.sender_face)

    def test_reverse_never_queued_behind_full_forward_queue(self):
        """ACKs are relayed while the forward queue is saturated."""
        fixture = ForwarderFixture(self, rate_bps=20_000, capacity=2)

        for index in range(30):
            fixture.sender.sendto(bytes([index]) * 2000, fixture.sender_face)

        fixture.receiver.sendto(b"ACK-now", fixture.receiver_face)
        data, _addr = recv_all(fixture.sender, 1, timeout=2.0)[0]

        self.assertEqual(data, b"ACK-now")

    def test_reverse_before_any_sender_is_dropped_not_fatal(self):
        fixture = ForwarderFixture(self)

        fixture.receiver.sendto(b"stray-ack", fixture.receiver_face)
        time.sleep(0.2)

        self.assertTrue(fixture.thread.is_alive())
        self.assertEqual(fixture.forwarder.counters["acks_forwarded"], 0)


class TestRateLimiting(unittest.TestCase):
    def test_service_rate_enforced(self):
        rate_bps = 50_000
        packet_size = 512
        count = 30
        total_bytes = packet_size * count
        fixture = ForwarderFixture(self, rate_bps=rate_bps, capacity=128)

        started = time.monotonic()
        for index in range(count):
            fixture.sender.sendto(bytes([index]) * packet_size,
                                  fixture.sender_face)
        recv_all(fixture.receiver, count, timeout=10.0)
        elapsed = time.monotonic() - started

        burst = max(4096, rate_bps * 0.01)
        minimum = max(0.0, (total_bytes - burst) / rate_bps)
        self.assertGreaterEqual(elapsed, minimum * 0.85)
        self.assertLess(elapsed, 10.0)

    def test_long_term_rate_not_exceeded(self):
        rate_bps = 100_000
        fixture = ForwarderFixture(self, rate_bps=rate_bps, capacity=256)

        count, packet_size = 100, 1000
        started = time.monotonic()
        for index in range(count):
            fixture.sender.sendto(bytes([index]) * packet_size,
                                  fixture.sender_face)
        recv_all(fixture.receiver, count, timeout=15.0)
        elapsed = time.monotonic() - started

        effective_bps = count * packet_size / elapsed
        self.assertLessEqual(effective_bps, rate_bps * 1.15)


class TestQueueOverflow(unittest.TestCase):
    def test_overflow_drops_and_preserves_fifo_prefix(self):
        fixture = ForwarderFixture(self, rate_bps=20_000, capacity=3)
        count, packet_size = 30, 2000

        for index in range(count):
            fixture.sender.sendto(bytes([index]) * packet_size,
                                  fixture.sender_face)

        counters = fixture.forwarder.counters
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if counters["forwarded"] >= counters["accepted"]:
                break
            time.sleep(0.05)

        accepted = counters["accepted"]
        dropped = counters["dropped"]

        self.assertLessEqual(accepted, 8)
        self.assertGreaterEqual(dropped, 20)
        self.assertEqual(accepted + dropped, count)

        received = recv_all(fixture.receiver, accepted, timeout=5.0)
        payloads = [data[0] for data in received]
        indices = [payload[0] for payload in payloads]
        self.assertEqual(indices, sorted(indices))

    def test_dropped_datagrams_never_reach_receiver(self):
        fixture = ForwarderFixture(self, rate_bps=20_000, capacity=2)
        count = 25

        for index in range(count):
            fixture.sender.sendto(bytes([index]) * 2000, fixture.sender_face)

        counters = fixture.forwarder.counters
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if counters["forwarded"] >= counters["accepted"]:
                break
            time.sleep(0.05)

        received = recv_all(fixture.receiver, counters["accepted"],
                            timeout=5.0)
        self.assertEqual(len(received), counters["accepted"])
        self.assertLess(len(received), count)


class TestTelemetryIntegration(unittest.TestCase):
    def test_rows_match_counters(self):
        fixture = ForwarderFixture(self, rate_bps=20_000, capacity=2)

        for index in range(20):
            fixture.sender.sendto(bytes([index]) * 2000, fixture.sender_face)
        fixture.receiver.sendto(b"ACK", fixture.receiver_face)

        counters = fixture.forwarder.counters
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if counters["forwarded"] >= counters["accepted"]:
                break
            time.sleep(0.05)

        fixture.forwarder.stop()
        fixture.thread.join(timeout=5.0)

        path = fixture.tmp / "router_telemetry.csv"
        with open(path, newline="") as file:
            rows = list(csv.DictReader(file))

        events = [row["event"] for row in rows]
        self.assertEqual(events.count("enqueue"), counters["accepted"])
        self.assertEqual(events.count("forward"), counters["forwarded"])
        self.assertEqual(events.count("drop"), counters["dropped"])
        self.assertEqual(counters["marks"], 0)
        self.assertTrue(all(int(row["marks"]) == 0 for row in rows))
        self.assertEqual(
            max(int(row["queue_length_packets"]) for row in rows),
            max(0, min(2, counters["accepted"])),
        )
        for row in rows:
            if row["event"] != "forward":
                self.assertEqual(row["sojourn_ms"], "")
            else:
                self.assertGreaterEqual(float(row["sojourn_ms"]), 0.0)


class TestLifecycle(unittest.TestCase):
    def test_ready_file_written_after_bind(self):
        tmp = Path(tempfile.mkdtemp(prefix="fwd_ready_"))
        ready_file = tmp / "router.ready"
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind(("127.0.0.1", 0))

        forwarder = Forwarder(
            sender_face=("127.0.0.1", 0),
            receiver_face=("127.0.0.1", 0),
            receiver_target=receiver.getsockname(),
            rate_bps=1_000_000,
            queue=DropTail(4),
            ready_file=ready_file,
        )
        thread = threading.Thread(target=forwarder.run, daemon=True)
        thread.start()
        self.addCleanup(receiver.close)
        self.addCleanup(lambda: (forwarder.stop(), thread.join(5)))

        self.assertTrue(forwarder.wait_ready(5.0))
        self.assertTrue(ready_file.exists())

        content = ready_file.read_text()
        self.assertIn(str(forwarder.sender_face[1]), content)
        self.assertIn(str(forwarder.receiver_face[1]), content)

    def test_stop_is_idempotent_and_closes_sockets(self):
        fixture = ForwarderFixture(self)

        fixture.forwarder.stop()
        fixture.thread.join(timeout=5.0)
        self.assertFalse(fixture.thread.is_alive())
        self.assertIsNone(fixture.forwarder.sender_socket)
        self.assertIsNone(fixture.forwarder.receiver_socket)

        fixture.forwarder.stop()

    def test_max_seconds_stops_the_loop(self):
        tmp = Path(tempfile.mkdtemp(prefix="fwd_max_"))
        forwarder = Forwarder(
            sender_face=("127.0.0.1", 0),
            receiver_face=("127.0.0.1", 0),
            receiver_target=("127.0.0.1", free_udp_port()),
            rate_bps=1_000_000,
            queue=DropTail(4),
            max_seconds=0.2,
        )
        thread = threading.Thread(target=forwarder.run, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)

        thread.join(timeout=5.0)
        self.assertFalse(thread.is_alive())

    def test_receiver_absent_does_not_crash_forwarder(self):
        fixture = ForwarderFixture(self, with_receiver=False)

        for index in range(3):
            fixture.sender.sendto(bytes([index]) * 100, fixture.sender_face)

        time.sleep(0.3)
        self.assertTrue(fixture.thread.is_alive())
        self.assertEqual(fixture.forwarder.counters["accepted"], 3)


class TestConfiguration(unittest.TestCase):
    def _make(self, **overrides):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind(("127.0.0.1", 0))
        self.addCleanup(receiver.close)

        options = dict(
            sender_face=("127.0.0.1", 0),
            receiver_face=("127.0.0.1", 0),
            receiver_target=receiver.getsockname(),
            rate_bps=1_000_000,
            queue=DropTail(4),
        )
        options.update(overrides)
        return Forwarder(**options)

    def test_invalid_rate_rejected(self):
        for bad_rate in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                self._make(rate_bps=bad_rate)

    def test_invalid_faces_rejected(self):
        for bad_face in ("127.0.0.1:0", ("127.0.0.1",), ("127.0.0.1", -1),
                         ("127.0.0.1", 70000), ("127.0.0.1", "x"),
                         (None, 5000)):
            with self.assertRaises(ValueError):
                self._make(sender_face=bad_face)

    def test_non_queue_object_rejected(self):
        with self.assertRaises(TypeError):
            self._make(queue=object())

    def test_mbcd_conversion_in_cli(self):
        args = build_parser().parse_args(
            [
                "--sender-face", "127.0.0.1:5001",
                "--receiver-face", "127.0.0.1:5003",
                "--receiver", "127.0.0.1:5002",
                "--rate-mbps", "5",
                "--queue-capacity", "20",
                "--telemetry", "t.csv",
            ]
        )
        self.assertEqual(args.rate_mbps * 1_000_000 / 8, 625_000)

    def test_cli_rejects_bad_address(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                [
                    "--sender-face", "not-an-address",
                    "--receiver-face", "127.0.0.1:5003",
                    "--receiver", "127.0.0.1:5002",
                    "--rate-mbps", "5",
                    "--queue-capacity", "20",
                    "--telemetry", "t.csv",
                ]
            )

    def test_cli_rejects_missing_arguments(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(["--rate-mbps", "5"])

    def test_main_returns_nonzero_on_invalid_rate(self):
        tmp = Path(tempfile.mkdtemp(prefix="fwd_cli_"))
        exit_code = main(
            [
                "--sender-face", "127.0.0.1:0",
                "--receiver-face", "127.0.0.1:0",
                "--receiver", "127.0.0.1:0",
                "--rate-mbps", "-1",
                "--queue-capacity", "4",
                "--telemetry", str(tmp / "t.csv"),
            ]
        )
        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
