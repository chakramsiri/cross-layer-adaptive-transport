"""Deterministic receiver tests: cumulative ACKs, duplicate ACKs for
out-of-order data, and in-order flushing of the buffered payload.

Phases the sender relies on: every DATA packet (in-order, out-of-order or
duplicate) must produce a cumulative ACK so that the sender's duplicate-ACK
detection can fast retransmit.
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
from transport.csv_logger import CSVLogger
from transport.packet import Packet, ACK, DATA, SYN, FIN

from tests.helpers import pick_test_port


class ReceiverDuplicateAckTests(unittest.TestCase):
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
        receiver_module.FIN_LINGER = 0.5  # keep the test fast
        receiver_module.logger = CSVLogger(str(self.log_path))
        self.addCleanup(self._restore_receiver_module)

        self.error = None
        self.thread = threading.Thread(target=self._run_receiver, daemon=True)
        self.thread.start()

        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client.settimeout(5.0)
        self.addCleanup(self.client.close)

    # -- setup / teardown --------------------------------------------------

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

    # -- helpers -----------------------------------------------------------

    def _send(self, packet):
        self.client.sendto(packet.pack(), ("127.0.0.1", self.port))

    def _recv_packet(self):
        raw, _ = self.client.recvfrom(65535)
        return Packet.unpack(raw)

    def _expect_ack(self, expected):
        packet = self._recv_packet()
        self.assertEqual(packet.packet_type, ACK)
        self.assertEqual(packet.ack, expected)
        return packet

    def _sync_receiver(self):
        """Send SYN until the receiver answers (thread startup race)."""

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
                    self._drain_acks()
                    return
        finally:
            self.client.settimeout(5.0)

        raise AssertionError(
            "receiver did not answer SYN "
            f"(error={self.error!r}, thread_alive={self.thread.is_alive()})"
        )

    def _drain_acks(self):
        """Discard ACKs left over from SYN retries during startup sync."""

        self.client.settimeout(0.05)
        try:
            while True:
                self.client.recvfrom(65535)
        except socket.timeout:
            pass
        finally:
            self.client.settimeout(5.0)

    def _join_receiver(self):
        self.thread.join(timeout=5.0)
        self.assertFalse(self.thread.is_alive(), "receiver thread did not exit")
        self.assertIsNone(
            self.error, f"receiver raised: {self.error!r}"
        )

    # -- tests -------------------------------------------------------------

    def test_out_of_order_packets_produce_duplicate_acks(self):
        self._sync_receiver()

        # seq 1 and 2 arrive before seq 0: each must produce a duplicate
        # cumulative ACK for the same missing sequence (ack stays 0).
        self._send(Packet(packet_type=DATA, seq=1, payload=b"B"))
        self._expect_ack(0)
        self._send(Packet(packet_type=DATA, seq=2, payload=b"C"))
        self._expect_ack(0)

        # seq 0 fills the gap: all three are written, ack jumps to 3.
        self._send(Packet(packet_type=DATA, seq=0, payload=b"A"))
        self._expect_ack(3)

        self._send(Packet(packet_type=DATA, seq=3, payload=b"D"))
        self._expect_ack(4)

        self._send(Packet(packet_type=FIN, seq=4))
        self._expect_ack(4)

        self._join_receiver()
        self.assertEqual(self.output_path.read_bytes(), b"ABCD")

        log_text = self.log_path.read_text()
        self.assertIn("OUT_OF_ORDER_BUFFERED", log_text)
        self.assertIn("BUFFERED_PACKET_WRITTEN", log_text)

    def test_duplicate_data_packet_is_acknowledged_again(self):
        self._sync_receiver()

        self._send(Packet(packet_type=DATA, seq=0, payload=b"A"))
        self._expect_ack(1)

        # A retransmitted/duplicated packet must re-trigger the cumulative
        # ACK so a sender that lost the first ACK can recover.
        self._send(Packet(packet_type=DATA, seq=0, payload=b"A"))
        self._expect_ack(1)

        self._send(Packet(packet_type=FIN, seq=1))
        self._expect_ack(1)

        self._join_receiver()
        self.assertEqual(self.output_path.read_bytes(), b"A")


if __name__ == "__main__":
    unittest.main()
