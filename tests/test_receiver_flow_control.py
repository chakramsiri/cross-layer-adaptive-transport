"""Deterministic receiver tests for Phase 2 flow control and timestamp echo.

Covers the SYN-ACK advertised capacity, rwnd shrinking by buffered
out-of-order bytes and restoring after the flush, defensive drops beyond
capacity, and the timestamp echo on every ACK.
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

CAPACITY = 3000  # bytes: exactly three 1000-byte packets


class ReceiverFlowControlTests(unittest.TestCase):
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
            receiver_module.RECV_BUFFER_BYTES,
            receiver_module.logger,
        )
        receiver_module.HOST = "127.0.0.1"
        receiver_module.PORT = self.port
        receiver_module.FIN_LINGER = 0.5  # keep the tests fast
        receiver_module.RECV_BUFFER_BYTES = CAPACITY
        receiver_module.logger = CSVLogger(str(self.log_path))
        self.addCleanup(self._restore_receiver_module)

        self.error = None
        self.thread = threading.Thread(target=self._run_receiver, daemon=True)
        self.thread.start()

        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client.settimeout(5.0)
        self.addCleanup(self.client.close)

    # setup

    def _restore_receiver_module(self):
        (
            receiver_module.HOST,
            receiver_module.PORT,
            receiver_module.FIN_LINGER,
            receiver_module.RECV_BUFFER_BYTES,
            receiver_module.logger,
        ) = self.orig_settings

    def _run_receiver(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                receiver_module.receive_file(str(self.output_path))
        except Exception as error:  # pragma: no cover - failure path
            self.error = error

    #  helpers

    def _send(self, packet):
        self.client.sendto(packet.pack(), ("127.0.0.1", self.port))

    def _recv_packet(self):
        raw, _ = self.client.recvfrom(65535)
        return Packet.unpack(raw)

    def _expect_ack(self, expected_ack, expected_rwnd=None, expected_echo=None):
        packet = self._recv_packet()
        self.assertEqual(packet.packet_type, ACK)
        self.assertEqual(packet.ack, expected_ack)

        if expected_rwnd is not None:
            self.assertEqual(packet.rwnd, expected_rwnd)

        if expected_echo is not None:
            self.assertEqual(packet.ts_echo, expected_echo)

        return packet

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
                    self.client.settimeout(5.0)
                    return packet
        finally:
            self.client.settimeout(5.0)

        raise AssertionError(
            "receiver did not answer SYN "
            f"(error={self.error!r}, thread_alive={self.thread.is_alive()})"
        )

    def _finish(self, fin_seq):
        self._send(Packet(packet_type=FIN, seq=fin_seq))
        self._expect_ack(fin_seq)

        self.thread.join(timeout=5.0)
        self.assertFalse(self.thread.is_alive(), "receiver thread did not exit")
        self.assertIsNone(self.error, f"receiver raised: {self.error!r}")

    @staticmethod
    def _payload(marker):
        return bytes([marker]) * 1000

    # tests

    def test_syn_ack_advertises_full_capacity(self):
        syn_ack = self._sync_receiver()
        self.assertEqual(syn_ack.rwnd, CAPACITY)

        self._finish(0)

    def test_ooo_buffering_reduces_rwnd_by_buffered_bytes(self):
        self._sync_receiver()

        self._send(Packet(packet_type=DATA, seq=1, payload=self._payload(51)))
        self._expect_ack(0, expected_rwnd=CAPACITY - 1000)

        self._send(Packet(packet_type=DATA, seq=2, payload=self._payload(50)))
        self._expect_ack(0, expected_rwnd=CAPACITY - 2000)

        self._send(Packet(packet_type=DATA, seq=0, payload=self._payload(48)))
        self._expect_ack(3, expected_rwnd=CAPACITY)

        self._finish(3)

    def test_flush_restores_full_rwnd(self):
        self._sync_receiver()

        self._send(Packet(packet_type=DATA, seq=1, payload=self._payload(49)))
        self._expect_ack(0)
        self._send(Packet(packet_type=DATA, seq=2, payload=self._payload(50)))
        self._expect_ack(0)

        self._send(Packet(packet_type=DATA, seq=0, payload=self._payload(48)))
        self._expect_ack(3, expected_rwnd=CAPACITY)

        self._finish(3)
        self.assertEqual(
            self.output_path.read_bytes(),
            self._payload(48) + self._payload(49) + self._payload(50),
        )

    def test_packet_exceeding_capacity_is_dropped(self):
        self._sync_receiver()

        self._send(Packet(packet_type=DATA, seq=1, payload=self._payload(49)))
        self._expect_ack(0, expected_rwnd=2000)
        self._send(Packet(packet_type=DATA, seq=2, payload=self._payload(50)))
        self._expect_ack(0, expected_rwnd=1000)
        self._send(Packet(packet_type=DATA, seq=3, payload=self._payload(51)))
        self._expect_ack(0, expected_rwnd=0)

        # The buffer is full: this packet must be dropped, not buffered.
        self._send(Packet(packet_type=DATA, seq=4, payload=self._payload(52)))
        self._expect_ack(0, expected_rwnd=0)

        # Filling the gap flushes 1..3 and frees the whole buffer.
        self._send(Packet(packet_type=DATA, seq=0, payload=self._payload(48)))
        self._expect_ack(4, expected_rwnd=CAPACITY)

        # The dropped packet 4 is retransmitted and now in order: it is
        # written directly and rwnd stays at the full capacity.
        self._send(Packet(packet_type=DATA, seq=4, payload=self._payload(52)))
        self._expect_ack(5, expected_rwnd=CAPACITY)

        self._finish(5)

        self.assertEqual(
            self.output_path.read_bytes(),
            b"".join(self._payload(marker) for marker in (48, 49, 50, 51, 52)),
        )
        self.assertIn("BUFFER_OVERFLOW_DROPPED", self.log_path.read_text())

    def test_every_ack_echoes_trigger_timestamp(self):
        self._sync_receiver()

        self._send(
            Packet(packet_type=DATA, seq=0, payload=self._payload(48),
                   ts_send=11111)
        )
        self._expect_ack(1, expected_echo=11111)

        # Out-of-order trigger: the ACK does not advance but still echoes
        # the timestamp of the packet that caused it.
        self._send(
            Packet(packet_type=DATA, seq=2, payload=self._payload(50),
                   ts_send=33333)
        )
        self._expect_ack(1, expected_echo=33333)

        # Filling the gap echoes the gap-filling packet's timestamp.
        self._send(
            Packet(packet_type=DATA, seq=1, payload=self._payload(49),
                   ts_send=22222)
        )
        self._expect_ack(3, expected_echo=22222)

        self._finish(3)


if __name__ == "__main__":
    unittest.main()
