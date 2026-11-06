"""FIN teardown tests: sender retry policy, exhaustion and the receiver's
duplicate-FIN linger behavior.  The sender retries FIN at the fixed 1 s
interval; the receiver lingers so every retry can be acknowledged.
"""

import contextlib
import io
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from transport import receiver as receiver_module
from transport import sender as sender_module
from transport.csv_logger import CSVLogger
from transport.packet import Packet, ACK, FIN, SYN

from tests.helpers import SenderTestBase, pick_test_port


class FinRetryTests(SenderTestBase):
    def test_lost_fin_ack_is_retried_at_one_second_and_completes(self):
        self.open_window()
        self.peer.send_ack(len(self.CHUNKS))

        # First FIN or its ACK is lost; the sender must retry after its
        # fixed 1 s interval and complete on the retry.
        packet, _ = self.peer.recv_any(timeout=2.0)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, sender_module.FIN)
        first = time.monotonic()

        packet, _ = self.peer.recv_any(timeout=2.0)
        self.assertIsNotNone(packet)
        self.assertEqual(packet.packet_type, sender_module.FIN)
        gap = time.monotonic() - first
        self.assertGreaterEqual(gap, 0.8)
        self.assertLess(gap, 1.5)

        self.peer.send_ack(packet.seq)
        self.thread.join(timeout=5.0)
        self.assertFalse(self.thread.is_alive())
        self.assertIsNone(self.sender_error)
        self.assertEqual(len(self.events("TRANSFER_COMPLETED")), 1)

    def test_fin_retry_exhaustion_reports_failure(self):
        self.open_window()
        original = sender_module.MAX_RETRIES
        sender_module.MAX_RETRIES = 2  # keep the exhaustion test quick
        self.addCleanup(setattr, sender_module, "MAX_RETRIES", original)

        self.peer.send_ack(len(self.CHUNKS))

        # Never acknowledge the FIN: the sender must give up and log it.
        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline:
            packet, _ = self.peer.recv_any(timeout=0.3)
            if packet is None and not self.thread.is_alive():
                break

        self.thread.join(timeout=3.0)
        self.assertFalse(self.thread.is_alive(), "sender did not give up")
        self.assertEqual(len(self.events("TRANSFER_FAILED")), 1)
        self.assertEqual(len(self.events("TRANSFER_COMPLETED")), 0)


class ReceiverDuplicateFinTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.output_path = Path(self.tmp.name) / "out.bin"
        self.log_path = Path(self.tmp.name) / "receiver.csv"

        self.port = pick_test_port()
        self.orig_settings = (
            receiver_module.HOST,
            receiver_module.PORT,
            receiver_module.FIN_LINGER,
            receiver_module.logger,
        )
        receiver_module.HOST = "127.0.0.1"
        receiver_module.PORT = self.port
        receiver_module.FIN_LINGER = 1.0
        receiver_module.logger = CSVLogger(str(self.log_path))
        self.addCleanup(self._restore_receiver_module)

        self.error = None
        self.thread = threading.Thread(target=self._run_receiver, daemon=True)
        self.thread.start()

        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client.settimeout(5.0)
        self.addCleanup(self.client.close)

    def _restore_receiver_module(self):
        (
            receiver_module.HOST,
            receiver_module.PORT,
            receiver_module.FIN_LINGER,
            receiver_module.logger,
        ) = self.orig_settings

    def _run_receiver(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                receiver_module.receive_file(str(self.output_path))
        except Exception as error:  # pragma: no cover - failure path
            self.error = error

    def _send(self, packet):
        self.client.sendto(packet.pack(), ("127.0.0.1", self.port))

    def _recv_packet(self):
        raw, _ = self.client.recvfrom(65535)
        return Packet.unpack(raw)

    def _sync_receiver(self):
        deadline = time.monotonic() + 5.0
        self.client.settimeout(0.2)

        try:
            while time.monotonic() < deadline:
                self._send(Packet(packet_type=SYN))
                try:
                    packet = self._recv_packet()
                except socket.timeout:
                    continue
                if packet.packet_type == ACK and packet.ack == 0:
                    return
        finally:
            self.client.settimeout(5.0)

        raise AssertionError("receiver did not answer SYN")

    def test_duplicate_fin_is_reacknowledged_during_linger(self):
        self._sync_receiver()

        self._send(Packet(packet_type=FIN, seq=0))
        ack = self._recv_packet()
        self.assertEqual(ack.packet_type, ACK)
        self.assertEqual(ack.ack, 0)

        # A duplicate FIN (the sender's retry) must be acknowledged again
        # while the receiver lingers.
        time.sleep(0.2)
        self._send(Packet(packet_type=FIN, seq=0))
        ack = self._recv_packet()
        self.assertEqual(ack.packet_type, ACK)
        self.assertEqual(ack.ack, 0)

        self.thread.join(timeout=5.0)
        self.assertFalse(self.thread.is_alive())
        self.assertIsNone(self.error)
        self.assertIn("DUPLICATE_FIN_RECEIVED", self.log_path.read_text())


if __name__ == "__main__":
    unittest.main()
