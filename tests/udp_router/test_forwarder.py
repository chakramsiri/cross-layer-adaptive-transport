"""Forwarder integration tests over real loopback UDP sockets."""

import csv
import json
import socket
import threading
import tempfile
import time
import unittest
from pathlib import Path

from router.forwarder import Forwarder, build_parser, build_queue, main
from router.queues.codel import CoDel
from router.queues.droptail import DropTail
from router.queues.red import RED
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
                 with_receiver=True, telemetry=True, queue=None):
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
            queue=queue if queue is not None else DropTail(capacity),
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
        syn = b"\x03" + b"\x00" * 28

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


def read_events(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


def wait_until(condition, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


class TestAqmSelection(unittest.TestCase):
    def _argv(self, telemetry, *extra):
        return [
            "--sender-face", "127.0.0.1:0",
            "--receiver-face", "127.0.0.1:0",
            "--receiver", "127.0.0.1:5999",
            "--rate-mbps", "5",
            "--queue-capacity", "200",
            "--telemetry", str(telemetry),
            *extra,
        ]

    def test_default_aqm_is_droptail(self):
        args = build_parser().parse_args(self._argv("t.csv"))

        self.assertEqual(args.aqm, "droptail")
        self.assertEqual(args.aqm_mode, "drop")

        queue = build_queue(args)
        self.assertIsInstance(queue, DropTail)
        self.assertEqual(queue.capacity, 200)

    def test_red_selection_plumbs_parameters(self):
        args = build_parser().parse_args(
            self._argv(
                "t.csv",
                "--aqm", "red",
                "--red-w-q", "0.002",
                "--red-min-th", "20",
                "--red-max-th", "80",
                "--red-max-p", "0.1",
                "--red-seed", "42",
            )
        )

        queue = build_queue(args)
        self.assertIsInstance(queue, RED)
        self.assertEqual(
            queue.parameters,
            {
                "w_q": 0.002,
                "min_th": 20.0,
                "max_th": 80.0,
                "max_p": 0.1,
                "gentle": False,
                "mode": "drop",
                "seed": 42,
                "typical_packet_bytes": 1029.0,
                "rate_bps": 625_000.0,
                "typical_tx_ms": 1029.0 / 625_000.0 * 1000.0,
            },
        )

    def test_red_packet_size_and_rate_propagated(self):
        args = build_parser().parse_args(
            self._argv(
                "t.csv",
                "--aqm", "red",
                "--rate-mbps", "2",
                "--red-typical-packet-bytes", "1200",
            )
        )

        parameters = build_queue(args).parameters

        self.assertEqual(parameters["rate_bps"], 250_000.0)
        self.assertEqual(parameters["typical_packet_bytes"], 1200.0)
        self.assertAlmostEqual(
            parameters["typical_tx_ms"], 1200.0 / 250_000.0 * 1000.0
        )

    def test_red_mark_and_gentle_plumbed(self):
        args = build_parser().parse_args(
            self._argv(
                "t.csv",
                "--aqm", "red",
                "--aqm-mode", "mark",
                "--red-gentle",
            )
        )

        queue = build_queue(args)
        self.assertEqual(queue.parameters["mode"], "mark")
        self.assertTrue(queue.parameters["gentle"])

    def test_codel_selection_converts_milliseconds(self):
        args = build_parser().parse_args(
            self._argv("t.csv", "--aqm", "codel")
        )

        queue = build_queue(args)
        self.assertIsInstance(queue, CoDel)
        self.assertEqual(queue.parameters["target_ms"], 5.0)
        self.assertEqual(queue.parameters["interval_ms"], 100.0)
        self.assertEqual(queue.parameters["mode"], "drop")

    def test_droptail_rejects_mark_mode(self):
        args = build_parser().parse_args(
            self._argv("t.csv", "--aqm", "droptail", "--aqm-mode", "mark")
        )

        with self.assertRaises(ValueError):
            build_queue(args)

    def test_main_rejects_droptail_mark_mode(self):
        tmp = Path(tempfile.mkdtemp(prefix="fwd_aqm_"))
        exit_code = main(
            self._argv(
                tmp / "t.csv",
                "--aqm", "droptail",
                "--aqm-mode", "mark",
                "--max-seconds", "0.2",
            )
        )

        self.assertEqual(exit_code, 1)

    def test_main_rejects_invalid_red_parameters_before_forwarding(self):
        tmp = Path(tempfile.mkdtemp(prefix="fwd_aqm_"))
        exit_code = main(
            self._argv(
                tmp / "t.csv",
                "--aqm", "red",
                "--red-w-q", "0",
                "--max-seconds", "0.2",
            )
        )

        self.assertEqual(exit_code, 1)
        self.assertFalse((tmp / "router_telemetry.csv").exists())

    def test_main_rejects_red_thresholds_above_capacity(self):
        tmp = Path(tempfile.mkdtemp(prefix="fwd_aqm_"))
        exit_code = main(
            [
                "--sender-face", "127.0.0.1:0",
                "--receiver-face", "127.0.0.1:0",
                "--receiver", "127.0.0.1:5999",
                "--rate-mbps", "5",
                "--queue-capacity", "50",
                "--aqm", "red",
                "--telemetry", str(tmp / "t.csv"),
                "--max-seconds", "0.2",
            ]
        )

        self.assertEqual(exit_code, 1)

    def test_main_rejects_invalid_codel_interval(self):
        tmp = Path(tempfile.mkdtemp(prefix="fwd_aqm_"))
        exit_code = main(
            self._argv(
                tmp / "t.csv",
                "--aqm", "codel",
                "--codel-target-ms", "5",
                "--codel-interval-ms", "5",
                "--max-seconds", "0.2",
            )
        )

        self.assertEqual(exit_code, 1)

    def test_router_metadata_written_beside_telemetry(self):
        tmp = Path(tempfile.mkdtemp(prefix="fwd_aqm_"))
        exit_code = main(
            [
                "--sender-face", "127.0.0.1:0",
                "--receiver-face", "127.0.0.1:0",
                "--receiver", "127.0.0.1:5999",
                "--rate-mbps", "1",
                "--queue-capacity", "100",
                "--aqm", "red",
                "--red-min-th", "10",
                "--red-max-th", "40",
                "--red-seed", "7",
                "--telemetry", str(tmp / "router_telemetry.csv"),
                "--max-seconds", "0.3",
            ]
        )

        self.assertEqual(exit_code, 0)
        metadata = json.loads((tmp / "router_metadata.json").read_text())
        self.assertEqual(metadata["algorithm"], "red")
        self.assertEqual(metadata["mode"], "drop")
        self.assertEqual(metadata["queue_capacity"], 100)
        self.assertEqual(metadata["rate_mbps"], 1.0)
        self.assertEqual(metadata["parameters"]["min_th"], 10.0)
        self.assertEqual(metadata["parameters"]["max_th"], 40.0)
        self.assertEqual(metadata["parameters"]["seed"], 7)
        self.assertEqual(metadata["parameters"]["rate_bps"], 125_000.0)
        self.assertEqual(
            metadata["parameters"]["typical_packet_bytes"], 1029.0
        )
        self.assertEqual(metadata["telemetry"], str(tmp / "router_telemetry.csv"))
        self.assertIn("pid", metadata)
        self.assertIn("started_at", metadata)


class TestRedIntegration(unittest.TestCase):
    def test_mark_mode_marks_without_dropping(self):
        queue = RED(
            capacity=64, w_q=1.0, min_th=1.0, max_th=2.0, max_p=1.0,
            mode="mark",
        )
        fixture = ForwarderFixture(
            self, rate_bps=50_000, capacity=64, queue=queue
        )
        count = 20

        for index in range(count):
            fixture.sender.sendto(bytes([index]) * 1000,
                                  fixture.sender_face)

        counters = fixture.forwarder.counters
        self.assertTrue(
            wait_until(lambda: counters["forwarded"] >= count)
        )

        self.assertEqual(counters["dropped"], 0)
        self.assertGreaterEqual(counters["marks"], 5)
        self.assertEqual(counters["forwarded"], count)
        received = recv_all(fixture.receiver, count, timeout=10.0)
        self.assertEqual(len(received), count)

        fixture.forwarder.stop()
        fixture.thread.join(timeout=5.0)
        rows = read_events(fixture.tmp / "router_telemetry.csv")
        marks = [row for row in rows if row["event"] == "mark"]
        self.assertEqual(len(marks), counters["marks"])
        self.assertTrue(all(row["reason"] == "red" for row in marks))
        self.assertFalse(any(row["event"] == "drop" for row in rows))

    def test_drop_mode_drops_are_counted_exactly_once(self):
        queue = RED(
            capacity=64, w_q=1.0, min_th=1.0, max_th=2.0, max_p=1.0,
            mode="drop",
        )
        fixture = ForwarderFixture(
            self, rate_bps=50_000, capacity=64, queue=queue
        )
        count = 20

        for index in range(count):
            fixture.sender.sendto(bytes([index]) * 1000,
                                  fixture.sender_face)

        counters = fixture.forwarder.counters
        self.assertTrue(
            wait_until(
                lambda: counters["forwarded"] + counters["dropped"]
                >= count
                and counters["forwarded"] >= counters["accepted"]
            )
        )

        self.assertGreaterEqual(counters["dropped"], 5)
        self.assertEqual(
            counters["accepted"] + counters["dropped"], count
        )
        self.assertEqual(counters["forwarded"], counters["accepted"])
        received = recv_all(fixture.receiver, counters["accepted"],
                            timeout=10.0)
        self.assertEqual(len(received), counters["accepted"])

        fixture.forwarder.stop()
        fixture.thread.join(timeout=5.0)
        rows = read_events(fixture.tmp / "router_telemetry.csv")
        drops = [row for row in rows if row["event"] == "drop"]
        self.assertEqual(len(drops), counters["dropped"])
        self.assertTrue(all(row["reason"] == "red" for row in drops))
        self.assertTrue(all(row["sojourn_ms"] == "" for row in drops))
        self.assertEqual(counters["marks"], 0)


class TestCoDelIntegration(unittest.TestCase):
    def _run_flood(self, mode):
        queue = CoDel(
            capacity=64,
            target_seconds=0.005,
            interval_seconds=0.02,
            mode=mode,
        )
        fixture = ForwarderFixture(
            self, rate_bps=100_000, capacity=64, queue=queue
        )
        count = 30

        for index in range(count):
            fixture.sender.sendto(bytes([index]) * 1000,
                                  fixture.sender_face)

        counters = fixture.forwarder.counters
        self.assertTrue(
            wait_until(
                lambda: counters["forwarded"] + counters["dropped"]
                >= count,
                timeout=20.0,
            )
        )
        fixture.forwarder.stop()
        fixture.thread.join(timeout=5.0)
        rows = read_events(fixture.tmp / "router_telemetry.csv")
        return fixture, counters, rows, count

    def test_drop_mode_dequeue_drops_reach_telemetry(self):
        fixture, counters, rows, count = self._run_flood("drop")

        self.assertEqual(counters["accepted"], count)
        self.assertEqual(
            counters["forwarded"] + counters["dropped"], count
        )
        self.assertGreaterEqual(counters["dropped"], 1)
        self.assertEqual(counters["marks"], 0)

        drops = [row for row in rows if row["event"] == "drop"]
        forwards = [row for row in rows if row["event"] == "forward"]
        self.assertEqual(len(drops), counters["dropped"])
        self.assertEqual(len(forwards), counters["forwarded"])
        self.assertTrue(all(row["reason"] == "codel" for row in drops))
        self.assertTrue(all(row["sojourn_ms"] != "" for row in drops))

        received = recv_all(fixture.receiver, counters["forwarded"],
                            timeout=10.0)
        self.assertEqual(len(received), counters["forwarded"])

    def test_mark_mode_records_signals_and_forwards_everything(self):
        fixture, counters, rows, count = self._run_flood("mark")

        self.assertEqual(counters["accepted"], count)
        self.assertEqual(counters["forwarded"], count)
        self.assertEqual(counters["dropped"], 0)
        self.assertGreaterEqual(counters["marks"], 1)

        marks = [row for row in rows if row["event"] == "mark"]
        self.assertEqual(len(marks), counters["marks"])
        self.assertTrue(all(row["reason"] == "codel" for row in marks))
        self.assertFalse(any(row["event"] == "drop" for row in rows))

        received = recv_all(fixture.receiver, count, timeout=10.0)
        self.assertEqual(len(received), count)


class TestAqmReversePath(unittest.TestCase):
    def test_acks_relayed_while_red_queue_saturated(self):
        queue = RED(
            capacity=2, w_q=1.0, min_th=1.0, max_th=2.0, max_p=1.0,
            mode="drop",
        )
        fixture = ForwarderFixture(
            self, rate_bps=20_000, capacity=2, queue=queue
        )

        for index in range(30):
            fixture.sender.sendto(bytes([index]) * 2000,
                                  fixture.sender_face)

        fixture.receiver.sendto(b"ACK-now", fixture.receiver_face)
        data, _addr = recv_all(fixture.sender, 1, timeout=2.0)[0]

        self.assertEqual(data, b"ACK-now")


if __name__ == "__main__":
    unittest.main()
