"""Deterministic regression tests for three-duplicate-ACK fast retransmit and
the single-timer (earliest-unacked-packet) timeout retransmission.

Covers the Phase 1 remediation acceptance list:

1. three duplicate ACKs trigger an immediate retransmission
2. one or two duplicate ACKs do not
3. a new cumulative ACK resets duplicate-ACK counting
4. duplicate ACKs do not retrigger the same recovery event
5. timeout retransmits only the earliest unacknowledged packet
6. timeout and fast retransmission do not conflict
7. retransmission after (simulated) loss delivers the correct bytes
8. reordering-induced spurious fast retransmit does not corrupt delivery
9. fast retransmit happens before the configured RTO expires
"""

import contextlib
import csv
import io
import tempfile
import threading
import time
import unittest
from pathlib import Path

from transport import sender as sender_module
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

    # -- setup / teardown --------------------------------------------------

    def _restore_sender_module(self):
        # Keep the temporary logger/port in place while the sender thread is
        # still alive so a stray retransmission can never be appended to the
        # repository's results/ logs.
        if self.thread.is_alive():
            return

        sender_module.PORT = self.orig_port
        sender_module.logger = self.orig_logger

    def _run_sender(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                sender_module.send_file(str(self.input_path), "127.0.0.1")
        except Exception as error:  # pragma: no cover - failure path
            self.sender_error = error

    def _stop_sender_quietly(self):
        """Best-effort completion so a failed test cannot leak a sender thread."""

        if not self.thread.is_alive():
            return

        try:
            if self.peer.sender_addr is None:
                self.peer.accept_syn(timeout=1.2)
        except Exception:
            return

        try:
            self.peer.send_ack(0)
            self.peer.send_ack(len(self.CHUNKS))
            self.peer.wait_for_fin(timeout=1.0)
        except Exception:
            pass

        self.thread.join(timeout=1.0)

    # -- helpers -----------------------------------------------------------

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

    def _fast_deadline(self):
        """Deadline for an immediate retransmission.

        Half the RTO is far below the timeout interval, so a retransmission
        driven by the timeout timer cannot satisfy it; the fast path answers
        within one poll interval (~50 ms).
        """

        return min(0.5, sender_module.TIMEOUT / 2)

    # -- tests -------------------------------------------------------------

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

        fast_rows = self._retransmissions("fast")
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
        self.assertEqual(len(self._retransmissions()), 1)

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

        timeout_rows = self._retransmissions("timeout")
        self.assertEqual(len(timeout_rows), 1, timeout_rows)

        self._finish_transfer()

    def test_fast_retransmit_restarts_the_timeout_timer(self):
        self._open_window()

        for _ in range(3):
            self.peer.send_ack(0)

        fast = self.peer.recv_data(timeout=self._fast_deadline())
        self.assertIsNotNone(fast, "fast retransmit did not arrive")
        fast_arrival = time.monotonic()

        # Do not ACK. With the RTO restarted by the fast retransmit, the
        # timeout retransmission must take about one RTO from that moment,
        # not fire immediately and not storm the whole window.
        timeout_retx = self.peer.recv_data(
            timeout=sender_module.TIMEOUT + 0.6
        )
        self.assertIsNotNone(
            timeout_retx, "timeout did not follow the fast retransmit"
        )
        delta = time.monotonic() - fast_arrival

        self.assertGreater(delta, sender_module.TIMEOUT * 0.6)
        self.assertLess(delta, sender_module.TIMEOUT + 0.6)

        self.assertIsNone(self.peer.expect_no_data(0.4))
        self.assertEqual(len(self._retransmissions("fast")), 1)
        self.assertEqual(len(self._retransmissions("timeout")), 1)

        self._finish_transfer()

    def test_retransmission_after_simulated_loss_delivers_correct_bytes(self):
        # Packet 3 is treated as lost: only 0..2 are acknowledged and the
        # duplicate ACKs that later arrivals would produce are sent. The
        # sender must fast-retransmit the missing bytes of seq 3.
        self._open_window()

        self.peer.send_ack(3)
        for _ in range(3):
            self.peer.send_ack(3)

        retransmission = self.peer.recv_data(timeout=self._fast_deadline())
        self.assertIsNotNone(
            retransmission, "fast retransmit of the gap did not arrive"
        )
        self.assertEqual(retransmission.seq, 3)
        self.assertEqual(retransmission.payload, self.CHUNKS[3])
        self.assertEqual(self.peer.payloads[3], self.CHUNKS[3])

        self._finish_transfer()

    def test_reordering_spurious_fast_retransmit_does_not_corrupt(self):
        # Reordering can delay a packet's ACK and trigger duplicate ACKs even
        # though the data was delivered. The spurious retransmit must not
        # corrupt delivery.
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
