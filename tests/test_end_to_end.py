"""Deterministic end-to-end test: real sender + receiver through a scripted
impairing relay (fixed script, no RNG):
- seq 3: first transmission dropped       -> fast retransmit recovery
- seq 10: first two transmissions dropped -> timeout path
- the ACK with value 8: dropped once      -> healed by cumulative ACKs
- seq 20: held back until seq 21 arrives  -> reordering

Verifies clean completion, SHA-256 integrity, both retransmission kinds,
out-of-order buffering and flushing, and no transfer failure.
"""

import hashlib
import io
import contextlib
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from transport import receiver as receiver_module
from transport import sender as sender_module
from transport.csv_logger import CSVLogger
from transport.packet import Packet, ACK, DATA, SYN

from tests.helpers import pick_test_port

CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(30)]
LISTEN_HOST = "127.0.0.1"


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        while True:
            chunk = file.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


class ScriptedRelay(threading.Thread):
    """Single-socket relay with a deterministic impairment script."""

    def __init__(self, listen_port, receiver_addr, drop_data,
                 drop_ack_once, hold_until):
        super().__init__(daemon=True)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((LISTEN_HOST, listen_port))
        self.sock.settimeout(0.05)
        self.receiver_addr = receiver_addr
        self.drop_data = dict(drop_data)     # seq -> transmissions to drop
        self.drop_ack_once = set(drop_ack_once)
        self.hold_until = dict(hold_until)   # seq -> must-forward-first seq
        self.sender_addr = None
        self.seen = {}
        self.held = {}
        self.forwarded = set()
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            try:
                data, addr = self.sock.recvfrom(65535)
            except (socket.timeout, OSError):
                continue

            try:
                packet = Packet.unpack(data)
            except ValueError:
                continue

            if addr == self.receiver_addr:
                # From the receiver: ACK path.
                if packet.packet_type == ACK and (
                    packet.ack in self.drop_ack_once
                ):
                    self.drop_ack_once.discard(packet.ack)
                    continue
                if self.sender_addr is not None:
                    self.sock.sendto(data, self.sender_addr)
                continue

            # From the sender.
            self.sender_addr = addr

            if packet.packet_type != DATA:
                self.sock.sendto(data, self.receiver_addr)
                continue

            seq = packet.seq
            self.seen[seq] = self.seen.get(seq, 0) + 1
            if self.seen[seq] <= self.drop_data.get(seq, 0):
                continue

            if seq in self.hold_until and seq not in self.forwarded:
                self.held[seq] = data
                continue

            self.sock.sendto(data, self.receiver_addr)
            self.forwarded.add(seq)

            # Release any held packet whose trigger just went out.
            for held_seq, trigger in list(self.hold_until.items()):
                if (
                    held_seq in self.held
                    and trigger in self.forwarded
                ):
                    self.sock.sendto(
                        self.held.pop(held_seq), self.receiver_addr
                    )
                    self.forwarded.add(held_seq)

    def close(self):
        """Close the relay socket and stop the thread."""
        self.stop_event.set()
        try:
            self.sock.close()
        except OSError:
            pass


class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.input_path = Path(self.tmp.name) / "input.bin"
        self.input_path.write_bytes(b"".join(CHUNKS))
        self.output_path = Path(self.tmp.name) / "out.bin"
        self.sender_log = Path(self.tmp.name) / "sender.csv"
        self.receiver_log = Path(self.tmp.name) / "receiver.csv"

        self.listen_port = pick_test_port()
        self.receiver_port = pick_test_port()

        self.orig_receiver = (
            receiver_module.HOST,
            receiver_module.PORT,
            receiver_module.FIN_LINGER,
            receiver_module.logger,
        )
        receiver_module.HOST = LISTEN_HOST
        receiver_module.PORT = self.receiver_port
        receiver_module.FIN_LINGER = 1.0
        receiver_module.logger = CSVLogger(str(self.receiver_log))

        self.orig_sender = (sender_module.PORT, sender_module.logger)
        sender_module.PORT = self.listen_port
        sender_module.logger = CSVLogger(str(self.sender_log))

        self.errors = {}
        self.receiver_thread = threading.Thread(
            target=self._run_receiver, daemon=True
        )
        self.sender_thread = threading.Thread(
            target=self._run_sender, daemon=True
        )
        self.relay = ScriptedRelay(
            self.listen_port,
            (LISTEN_HOST, self.receiver_port),
            drop_data={3: 1, 10: 2},
            drop_ack_once={8},
            hold_until={20: 21},
        )

        # Register cleanup before starting any resource so it runs even if
        # an assertion fails mid-test.
        self.addCleanup(self._restore_modules)
        self.addCleanup(self._cleanup_resources)

        self.receiver_thread.start()
        self.relay.start()
        time.sleep(0.2)
        self.sender_thread.start()
        

    def _cleanup_resources(self):
        """Stop relay, close its socket, join threads (best effort)."""
        self.relay.close()
        self.relay.join(timeout=2.0)
        self.sender_thread.join(timeout=5.0)
        self.receiver_thread.join(timeout=5.0)

    def _restore_modules(self):
        receiver_module.HOST, receiver_module.PORT, \
            receiver_module.FIN_LINGER, receiver_module.logger = (
                self.orig_receiver
            )
        sender_module.PORT, sender_module.logger = self.orig_sender

    def _run_receiver(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                receiver_module.receive_file(str(self.output_path))
        except Exception as error:  # pragma: no cover - failure path
            self.errors["receiver"] = error

    def _run_sender(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                sender_module.send_file(
                    str(self.input_path), LISTEN_HOST
                )
        except Exception as error:  # pragma: no cover - failure path
            self.errors["sender"] = error

    def test_lossy_reordered_transfer_recovers_with_integrity(self):
        # Wait for the transfer to complete.
        self.sender_thread.join(timeout=30.0)
        self.receiver_thread.join(timeout=10.0)

        # Both threads must have exited cleanly.
        self.assertFalse(
            self.sender_thread.is_alive(), "sender thread did not exit"
        )
        self.assertFalse(
            self.receiver_thread.is_alive(), "receiver thread did not exit"
        )
        self.assertEqual(self.errors, {})

        # File integrity through loss, an ACK drop and reordering.
        self.assertEqual(
            sha256_file(self.input_path), sha256_file(self.output_path)
        )

        import csv

        with open(self.sender_log) as file:
            sender_rows = list(csv.DictReader(file))

        events = [row["event"] for row in sender_rows]
        self.assertIn("TRANSFER_COMPLETED", events)
        self.assertNotIn("TRANSFER_FAILED", events)

        kinds = [
            row["details"]
            for row in sender_rows
            if row["event"] == "RETRANSMISSION"
            and row["packet_type"] == "DATA"
        ]
        self.assertTrue(
            any("Fast retransmit" in kind for kind in kinds), kinds
        )
        self.assertTrue(any(kind == "Timeout" for kind in kinds), kinds)

        receiver_text = self.receiver_log.read_text()
        self.assertIn("OUT_OF_ORDER_BUFFERED", receiver_text)
        self.assertIn("BUFFERED_PACKET_WRITTEN", receiver_text)
        self.assertIn("TRANSFER_COMPLETED", receiver_text)


if __name__ == "__main__":
    unittest.main()
