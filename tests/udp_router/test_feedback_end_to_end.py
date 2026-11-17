"""Loopback feedback loop with reverse-path ACK loss: echoed CE counts and
queue delays must agree with the router's counters and telemetry."""

import contextlib
import hashlib
import io
import math
import re
import shutil
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from router.forwarder import Forwarder
from router.queues.red import RED
from router.telemetry import TelemetryLogger
from transport import receiver as receiver_module
from transport import sender as sender_module
from transport.csv_logger import CSVLogger
from transport.packet import ACK, Packet

from tests.helpers import pick_test_port
from tests.udp_router.test_forwarder import read_events

CHUNK = 1000


class AckDroppingRelay:
    """Carries sender datagrams to the router and returns ACKs, dropping
    every Nth data-phase ACK (0 < ack < last)."""

    def __init__(self, router_face, last_ack, drop_every):
        self.router_face = router_face
        self.last_ack = last_ack
        self.drop_every = drop_every
        self.sender_addr = None
        self.data_acks = 0
        self.dropped = 0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.settimeout(0.05)
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    @property
    def port(self):
        return self.sock.getsockname()[1]

    def start(self):
        self.thread.start()

    def stop(self):
        self._stop.set()
        self.thread.join(timeout=5.0)
        self.sock.close()

    def _run(self):
        while not self._stop.is_set():
            try:
                raw, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return

            if addr == self.router_face:
                self._return_ack(raw)
            else:
                self.sender_addr = addr
                self.sock.sendto(raw, self.router_face)

    def _return_ack(self, raw):
        if self.sender_addr is None:
            return

        try:
            packet = Packet.unpack(raw)
        except ValueError:
            return

        if packet.packet_type == ACK and 0 < packet.ack < self.last_ack:
            self.data_acks += 1
            if self.data_acks % self.drop_every == 0:
                self.dropped += 1
                return

        self.sock.sendto(raw, self.sender_addr)


class FeedbackLoopEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="feedback_e2e_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

        self.payload = bytes(range(256)) * 160
        self.input_path = self.tmp / "input.bin"
        self.output_path = self.tmp / "output.bin"
        self.input_path.write_bytes(self.payload)
        self.total_packets = math.ceil(len(self.payload) / CHUNK)

        self.saved = (
            receiver_module.HOST,
            receiver_module.PORT,
            receiver_module.FIN_LINGER,
            receiver_module.logger,
            sender_module.PORT,
            sender_module.logger,
        )
        self.addCleanup(self._restore_modules)

    def _restore_modules(self):
        (
            receiver_module.HOST,
            receiver_module.PORT,
            receiver_module.FIN_LINGER,
            receiver_module.logger,
            sender_module.PORT,
            sender_module.logger,
        ) = self.saved

    def _start_router(self, receiver_port):
        queue = RED(capacity=64, w_q=1.0, min_th=1.0, max_th=2.0,
                    max_p=1.0, mode="mark")
        self.router = Forwarder(
            sender_face=("127.0.0.1", 0),
            receiver_face=("127.0.0.1", 0),
            receiver_target=("127.0.0.1", receiver_port),
            rate_bps=625_000,
            queue=queue,
            telemetry=TelemetryLogger(self.tmp / "router_telemetry.csv"),
        )
        self.router_thread = threading.Thread(target=self.router.run,
                                              daemon=True)
        self.router_thread.start()
        self.addCleanup(self._stop_router)
        self.assertTrue(self.router.wait_ready(5.0))

    def _stop_router(self):
        self.router.stop()
        self.router_thread.join(timeout=5.0)

    def test_echoed_feedback_matches_router_marks_and_queue_delays(self):
        receiver_port = pick_test_port()
        receiver_module.HOST = "127.0.0.1"
        receiver_module.PORT = receiver_port
        receiver_module.FIN_LINGER = 0.5
        receiver_module.logger = CSVLogger(str(self.tmp / "receiver_log.csv"))
        sender_module.logger = CSVLogger(str(self.tmp / "sender_log.csv"))

        self._start_router(receiver_port)
        relay = AckDroppingRelay(self.router.sender_face,
                                 last_ack=self.total_packets, drop_every=3)
        relay.start()
        self.addCleanup(relay.stop)
        sender_module.PORT = relay.port

        receiver_errors = []

        def run_receiver():
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    receiver_module.receive_file(str(self.output_path))
            except Exception as error:
                receiver_errors.append(error)

        receiver_thread = threading.Thread(target=run_receiver, daemon=True)
        receiver_thread.start()
        time.sleep(0.2)

        sender_result = []

        def run_sender():
            with contextlib.redirect_stdout(io.StringIO()):
                sender_result.append(sender_module.send_file(
                    str(self.input_path), "127.0.0.1"
                ))

        sender_thread = threading.Thread(target=run_sender, daemon=True)
        sender_thread.start()
        sender_thread.join(timeout=30.0)
        receiver_thread.join(timeout=10.0)

        self.assertFalse(sender_thread.is_alive())
        self.assertFalse(receiver_thread.is_alive())
        self.assertEqual(receiver_errors, [])
        self.assertEqual(sender_result, [True])
        self.assertEqual(
            hashlib.sha256(self.output_path.read_bytes()).hexdigest(),
            hashlib.sha256(self.payload).hexdigest(),
        )
        self.assertGreater(relay.dropped, 0)

        self._stop_router()
        counters = self.router.counters

        feedback = read_events(self.tmp / "sender_log.csv")
        feedback = [row for row in feedback if row["event"] == "FEEDBACK"]
        self.assertTrue(feedback)
        sender_total = int(
            re.search(r"ce_total=(\d+)", feedback[-1]["details"]).group(1)
        )

        receiver_acks = [
            row for row in read_events(self.tmp / "receiver_log.csv")
            if row["event"] == "ACK_SENT"
        ]
        receiver_total = int(
            re.search(r"ce_count=(\d+)", receiver_acks[-1]["details"]).group(1)
        )

        telemetry = read_events(self.tmp / "router_telemetry.csv")
        mark_rows = [row for row in telemetry if row["event"] == "mark"]
        forwards = [row for row in telemetry if row["event"] == "forward"]

        self.assertGreater(counters["ce_marked"], 0)
        self.assertEqual(counters["ce_marked"], len(mark_rows))
        self.assertEqual(receiver_total, counters["ce_marked"])
        self.assertEqual(sender_total, counters["ce_marked"])

        for row in feedback:
            echoed = int(
                re.search(r"q_delay_us=(\d+)", row["details"]).group(1)
            )
            self.assertTrue(
                any(
                    abs(echoed - round(float(forward["sojourn_ms"]) * 1000))
                    <= 1
                    for forward in forwards
                ),
                f"echoed q_delay_us={echoed} matches no forwarded sojourn",
            )


if __name__ == "__main__":
    unittest.main()
