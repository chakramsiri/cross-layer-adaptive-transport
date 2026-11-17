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
from transport.packet import ACK, CE, DATA, ECE, FIN, SYN, Packet

from tests.helpers import pick_test_port


class ReceiverFeedbackTests(unittest.TestCase):
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
        receiver_module.FIN_LINGER = 0.5
        receiver_module.logger = CSVLogger(str(self.log_path))
        self.addCleanup(self._restore_receiver_module)

        self.error = None
        self.thread = threading.Thread(target=self._run_receiver, daemon=True)
        self.thread.start()

        self.client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.client.settimeout(5.0)
        self.addCleanup(self.client.close)
        self._sync_receiver()

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
        except Exception as error:
            self.error = error

    def _send(self, packet):
        self.client.sendto(packet.pack(), ("127.0.0.1", self.port))

    def _recv_ack(self):
        raw, _ = self.client.recvfrom(65535)
        packet = Packet.unpack(raw)
        self.assertEqual(packet.packet_type, ACK)
        return packet

    def _sync_receiver(self):
        deadline = time.monotonic() + 5.0
        self.client.settimeout(0.2)

        try:
            while time.monotonic() < deadline:
                self._send(Packet(packet_type=SYN))
                try:
                    packet = self._recv_ack()
                except socket.timeout:
                    continue
                if packet.ack == 0:
                    self._drain_acks()
                    return
        finally:
            self.client.settimeout(5.0)

        raise AssertionError("receiver did not answer SYN")

    def _drain_acks(self):
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
        self.assertFalse(
            self.thread.is_alive(), "receiver thread did not exit"
        )
        self.assertIsNone(self.error, f"receiver raised: {self.error!r}")

    def test_ack_echoes_cumulative_ce_count_and_latest_queue_delay(self):
        self._send(Packet(packet_type=DATA, seq=0, payload=b"A",
                          q_delay_us=500, flags=CE))
        first = self._recv_ack()
        self.assertEqual((first.ack, first.ce_count, first.q_delay_us),
                         (1, 1, 500))
        self.assertTrue(first.flags & ECE)

        self._send(Packet(packet_type=DATA, seq=1, payload=b"B",
                          q_delay_us=700))
        second = self._recv_ack()
        self.assertEqual((second.ack, second.ce_count, second.q_delay_us),
                         (2, 1, 700))

        self._send(Packet(packet_type=DATA, seq=2, payload=b"C",
                          q_delay_us=900, flags=CE))
        third = self._recv_ack()
        self.assertEqual((third.ack, third.ce_count, third.q_delay_us),
                         (3, 2, 900))

        self._send(Packet(packet_type=FIN, seq=3))
        self._recv_ack()
        self._join_receiver()

    def test_out_of_order_and_duplicate_marked_packets_are_still_counted(self):
        self._send(Packet(packet_type=DATA, seq=2, payload=b"C", flags=CE))
        out_of_order = self._recv_ack()
        self.assertEqual((out_of_order.ack, out_of_order.ce_count), (0, 1))

        self._send(Packet(packet_type=DATA, seq=0, payload=b"A"))
        self._recv_ack()

        self._send(Packet(packet_type=DATA, seq=0, payload=b"A", flags=CE))
        duplicate = self._recv_ack()
        self.assertEqual((duplicate.ack, duplicate.ce_count), (1, 2))

        self._send(Packet(packet_type=DATA, seq=1, payload=b"B"))
        self._recv_ack()

        self._send(Packet(packet_type=FIN, seq=3))
        self._recv_ack()
        self._join_receiver()

    def test_unmarked_transfer_echoes_zero_and_no_ece(self):
        self._send(Packet(packet_type=DATA, seq=0, payload=b"A",
                          q_delay_us=250))
        ack = self._recv_ack()

        self.assertEqual(ack.ce_count, 0)
        self.assertEqual(ack.q_delay_us, 250)
        self.assertFalse(ack.flags & ECE)

        self._send(Packet(packet_type=FIN, seq=1))
        self._recv_ack()
        self._join_receiver()

    def test_marked_packet_discarded_by_the_receiver_is_still_counted(self):
        original = receiver_module.RECV_BUFFER_BYTES
        receiver_module.RECV_BUFFER_BYTES = 1500
        self.addCleanup(
            setattr, receiver_module, "RECV_BUFFER_BYTES", original
        )

        self._send(Packet(packet_type=DATA, seq=2, payload=b"C" * 1000,
                          flags=CE))
        buffered = self._recv_ack()
        self.assertEqual((buffered.ack, buffered.ce_count), (0, 1))

        self._send(Packet(packet_type=DATA, seq=3, payload=b"D" * 1000,
                          flags=CE))
        discarded = self._recv_ack()
        self.assertEqual((discarded.ack, discarded.ce_count), (0, 2))

        self._send(Packet(packet_type=DATA, seq=0, payload=b"A" * 1000))
        self._recv_ack()
        self._send(Packet(packet_type=DATA, seq=1, payload=b"B" * 1000))
        self._recv_ack()

        self._send(Packet(packet_type=FIN, seq=3))
        fin_ack = self._recv_ack()
        self.assertEqual((fin_ack.ack, fin_ack.ce_count), (3, 2))
        self._join_receiver()

    def test_fin_acknowledgement_carries_the_final_feedback(self):
        self._send(Packet(packet_type=DATA, seq=0, payload=b"A",
                          q_delay_us=640, flags=CE))
        self._recv_ack()

        self._send(Packet(packet_type=FIN, seq=1))
        fin_ack = self._recv_ack()

        self.assertEqual(fin_ack.ack, 1)
        self.assertEqual(fin_ack.ce_count, 1)
        self.assertEqual(fin_ack.q_delay_us, 640)
        self._join_receiver()


if __name__ == "__main__":
    unittest.main()
