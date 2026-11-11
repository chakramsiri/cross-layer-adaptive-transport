"""Tests fast retransmission and timeout recovery."""

import contextlib
import csv
import io
import tempfile
import threading
import time
import unittest
from pathlib import Path

from transport import sender as sender_module
from transport.cc.reno import RenoController
from transport.csv_logger import CSVLogger

from tests.helpers import ScriptedPeer, pick_test_port


class FastRetransmitTests(unittest.TestCase):
    CHUNKS = [bytes([65 + index]) * 1000 for index in range(5)]

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.input_path = Path(self.tmp.name) / "input.bin"
        self.input_path.write_bytes(b"".join(self.CHUNKS))
        self.log_path = Path(self.tmp.name) / "sender.csv"

        self.port = pick_test_port()
        self.orig_port = sender_module.PORT
        self.orig_logger = sender_module.logger
        sender_module.PORT = self.port
        sender_module.logger = CSVLogger(str(self.log_path))
        self.addCleanup(self._restore_sender_module)

        # Bind the peer before the sender starts so the SYN cannot be lost.
        self.peer = ScriptedPeer(self.port)
        self.addCleanup(self.peer.close)

        self.sender_error = None
        self.thread = threading.Thread(target=self._run_sender, daemon=True)
        self.addCleanup(self._stop_sender_quietly)
        self.thread.start()

    def _restore_sender_module(self):
        # Keep the test logger installed until the sender thread exits.
        if self.thread.is_alive():
            return

        sender_module.PORT = self.orig_port
        sender_module.logger = self.orig_logger

    def _run_sender(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                sender_module.send_file(
                    str(self.input_path),
                    "127.0.0.1",
                    RenoController(initial_cwnd=sender_module.WINDOW_SIZE),
                )
        except Exception as error:  # pragma: no cover - failure path
            self.sender_error = error

    def _stop_sender_quietly(self):
        """Best-effort completion so a failed test cannot leak a sender thread."""

        if not self.thread.is_alive():
            return

        try:
            if self.peer.sender_addr is None:
                self.peer.accept_syn(timeout=2.6)
        except Exception:
            return

        try:
            self.peer.send_ack(0)
            self.peer.send_ack(len(self.CHUNKS))
            self.peer.wait_for_fin(timeout=1.0)
        except Exception:
            pass

        self.thread.join(timeout=1.0)

    def _open_window(self):
        """Complete the SYN handshake and collect the initial 5 DATA packets."""

        self.peer.accept_syn()
        seqs = self.peer.collect_data(len(self.CHUNKS))
        self.assertEqual(seqs, list(range(len(self.CHUNKS))))

    def _finish_transfer(self):
        self.peer.send_ack(len(self.CHUNKS))
        self.assertTrue(
            self.peer.wait_for_fin(), "FIN handshake did not complete"
        )
        self.thread.join(timeout=5.0)
        self.assertFalse(self.thread.is_alive(), "sender thread did not exit")
        self.assertIsNone(
            self.sender_error, f"sender raised: {self.sender_error!r}"
        )

    def _log_rows(self):
        with open(self.log_path) as file:
            return list(csv.DictReader(file))

    def _retransmissions(self, kind=None):
        rows = [r for r in self._log_rows() if r["event"] == "RETRANSMISSION"]

        if kind == "timeout":
            rows = [r for r in rows if r["details"] == "Timeout"]
        elif kind == "fast":
            rows = [
                r for r in rows if r["details"].startswith("Fast retransmit")
            ]

        return rows

    def _wait_for_retransmissions(self, kind=None, count=1, timeout=1.0):
        """Wait until at least `count` matching log rows exist.

        The sender logs just after sendto(), so the test thread can observe
        the packet first; exact-count assertions still catch extra retx.
        """

        deadline = time.monotonic() + timeout
        rows = self._retransmissions(kind)

        while len(rows) < count and time.monotonic() < deadline:
            time.sleep(0.02)
            rows = self._retransmissions(kind)

        return rows

    def _fast_deadline(self):
        """Deadline for an immediate retransmission.

        Half the RTO rules out the timeout path; the fast path answers
        within one poll interval (~50 ms).
        """

        return min(0.5, sender_module.TIMEOUT / 2)


    def test_three_duplicate_acks_trigger_fast_retransmit_before_rto(self):
        self._open_window()

        self.peer.send_ack(0)
        self.peer.send_ack(0)
        sent_at = time.monotonic()
        self.peer.send_ack(0)

        retransmission = self.peer.recv_data(timeout=self._fast_deadline())
        elapsed = time.monotonic() - sent_at

        self.assertIsNotNone(
            retransmission,
            "no retransmission arrived after 3 duplicate ACKs within "
            f"{self._fast_deadline():.2f}s",
        )
        self.assertEqual(retransmission.seq, 0)
        self.assertEqual(retransmission.payload, self.CHUNKS[0])
        self.assertLess(
            elapsed,
            sender_module.TIMEOUT,
            f"retransmission took {elapsed:.3f}s, "
            f"RTO={sender_module.TIMEOUT}s",
        )

        fast_rows = self._wait_for_retransmissions("fast")
        self.assertEqual(len(fast_rows), 1, fast_rows)

        self._finish_transfer()

    def test_one_or_two_duplicate_acks_do_not_trigger_retransmit(self):
        self._open_window()

        self.peer.send_ack(0)
        self.peer.send_ack(0)

        self.assertIsNone(
            self.peer.expect_no_data(0.5),
            "retransmission triggered without 3 duplicate ACKs",
        )
        self.assertEqual(self._retransmissions(), [])

        self._finish_transfer()

    def test_new_cumulative_ack_resets_duplicate_ack_count(self):
        self._open_window()

        self.peer.send_ack(0)
        self.peer.send_ack(0)
        self.peer.send_ack(1)  # new data acknowledged -> reset
        self.peer.send_ack(1)
        self.peer.send_ack(1)

        self.assertIsNone(
            self.peer.expect_no_data(0.2),
            "duplicate-ACK count leaked across a new cumulative ACK",
        )

        self.peer.send_ack(1)  # third duplicate for the new base
        retransmission = self.peer.recv_data(timeout=self._fast_deadline())

        self.assertIsNotNone(
            retransmission, "no fast retransmit after a counter reset"
        )
        self.assertEqual(retransmission.seq, 1)

        self._finish_transfer()

    def test_duplicate_acks_do_not_repeat_retransmission_for_same_event(self):
        self._open_window()

        for _ in range(3):
            self.peer.send_ack(0)

        first = self.peer.recv_data(timeout=self._fast_deadline())
        self.assertIsNotNone(first, "fast retransmit did not arrive")
        self.assertEqual(first.seq, 0)

        for _ in range(8):
            self.peer.send_ack(0)

        self.assertIsNone(
            self.peer.expect_no_data(0.5),
            "duplicate ACKs retriggered the same recovery event",
        )
        self.assertEqual(len(self._wait_for_retransmissions()), 1)

        self._finish_transfer()

    def test_timeout_retransmits_only_the_earliest_unacked_packet(self):
        self._open_window()

        self.peer.send_ack(0)
        self.peer.send_ack(0)  # two duplicates: no fast retransmit

        retransmission = self.peer.recv_data(
            timeout=sender_module.TIMEOUT + 0.6
        )
        self.assertIsNotNone(
            retransmission, "timeout retransmission did not arrive"
        )
        self.assertEqual(retransmission.seq, 0)

        self.assertIsNone(
            self.peer.expect_no_data(0.4),
            "timeout retransmitted more than the earliest unacked packet",
        )

        timeout_rows = self._wait_for_retransmissions("timeout")
        self.assertEqual(len(timeout_rows), 1, timeout_rows)

        self._finish_transfer()

    def test_retransmission_after_simulated_loss_delivers_correct_bytes(self):
        # Packet 1 is genuinely lost (discarded, no ACK): the out-of-order
        # arrivals 2-4 produce duplicate ACK(1)s, triggering fast retransmit.
        self.peer.accept_syn()

        first = self.peer.recv_data(timeout=1.0)
        self.assertIsNotNone(first)
        self.assertEqual(first.seq, 0)
        self.peer.send_ack(1)

        lost = self.peer.recv_data(timeout=1.0)
        self.assertIsNotNone(lost)
        self.assertEqual(lost.seq, 1)
        # Drop it: no record, no ACK -- indistinguishable from network loss.
        self.peer.payloads.pop(1, None)
        self.peer.timestamps.pop(1, None)

        for _ in range(3):
            arrival = self.peer.recv_data(timeout=1.0)
            self.assertIsNotNone(arrival)
            self.peer.send_ack(1)

        retransmission = self.peer.recv_data(timeout=self._fast_deadline())
        self.assertIsNotNone(
            retransmission, "fast retransmit of the lost packet did not arrive"
        )
        self.assertEqual(retransmission.seq, 1)
        self.assertEqual(retransmission.payload, self.CHUNKS[1])
        # payloads[1] was cleared, so this entry can only come from the
        # retransmission: the lost bytes were genuinely recovered.
        self.assertEqual(self.peer.payloads[1], self.CHUNKS[1])

        self._finish_transfer()

    def test_reordering_spurious_fast_retransmit_does_not_corrupt(self):
        # Reordering delays a packet's ACK, producing duplicate ACKs even
        # though the data arrived; the spurious retransmit must not corrupt.
        self._open_window()

        for _ in range(3):
            self.peer.send_ack(0)

        spurious = self.peer.recv_data(timeout=self._fast_deadline())
        self.assertIsNotNone(
            spurious, "spurious fast retransmit did not arrive"
        )
        self.assertEqual(spurious.payload, self.CHUNKS[spurious.seq])

        self._finish_transfer()

        for seq, chunk in enumerate(self.CHUNKS):
            self.assertEqual(self.peer.payloads[seq], chunk)


if __name__ == "__main__":
    unittest.main()
