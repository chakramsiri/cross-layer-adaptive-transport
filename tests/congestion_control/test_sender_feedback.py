import re
import time
import unittest

from transport.cc.reno import RenoController
from transport.packet import FIN

from tests.helpers import SenderTestBase


class RecordingController(RenoController):
    def __init__(self, initial_cwnd):
        super().__init__(initial_cwnd=initial_cwnd)
        self.feedback = []

    def on_feedback(self, ce_count, q_delay):
        self.feedback.append((ce_count, q_delay))
        super().on_feedback(ce_count, q_delay)


class SenderFeedbackTests(SenderTestBase):
    def make_controller(self):
        return RecordingController(initial_cwnd=float(self.INITIAL_CWND))

    def feedback_rows(self):
        return self.events("FEEDBACK")

    def ce_totals(self):
        return [
            int(re.search(r"ce_total=(\d+)", row["details"]).group(1))
            for row in self.feedback_rows()
        ]

    def finish_with_feedback(self, fin_ce, fin_q, ack_ce, ack_q):
        """Complete the transfer; the FIN is answered with its own echo."""

        self.peer.send_ack(len(self.CHUNKS), ce_count=ack_ce,
                           q_delay_us=ack_q)

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            packet, _ = self.peer._recv_raw(deadline - time.monotonic())
            if packet is not None and packet.packet_type == FIN:
                self.peer.send_ack(packet.seq, ce_count=fin_ce,
                                   q_delay_us=fin_q)
                break

        self.thread.join(timeout=5.0)
        self.assertFalse(self.thread.is_alive(), "sender thread did not exit")
        self.assertIsNone(self.sender_error,
                          f"sender raised: {self.sender_error!r}")
        self.assertTrue(self.sender_result)

    def test_each_echo_reaches_the_controller_as_an_increment(self):
        self.open_window()

        self.peer.send_ack(1, ce_count=2, q_delay_us=1500)
        self.wait_for_event("FEEDBACK", 1)
        self.peer.send_ack(2, ce_count=2, q_delay_us=2500)
        self.wait_for_event("FEEDBACK", 2)
        self.peer.send_ack(3, ce_count=5, q_delay_us=3000)
        self.wait_for_event("FEEDBACK", 3)

        self.finish_with_feedback(fin_ce=5, fin_q=3000, ack_ce=5, ack_q=3000)

        self.assertEqual(
            self.controller.feedback,
            [(2, 0.0015), (0, 0.0025), (3, 0.003),
             (0, 0.003), (0, 0.003)],
        )
        self.assertEqual(self.ce_totals(), [2, 2, 5, 5, 5])

    def test_echo_lost_in_transit_is_recovered_by_the_next_ack(self):
        self.open_window()

        self.peer.send_ack(1, ce_count=1, q_delay_us=100)
        self.wait_for_event("FEEDBACK", 1)
        self.peer.send_ack(3, ce_count=4, q_delay_us=400)
        rows = self.wait_for_event("FEEDBACK", 2)

        self.assertIn("ce_new=3", rows[1]["details"])
        self.assertIn("ce_total=4", rows[1]["details"])
        self.assertIn("q_delay_us=400", rows[1]["details"])

        self.finish_with_feedback(fin_ce=4, fin_q=400, ack_ce=4, ack_q=400)

    def test_reordered_older_echo_is_not_reported(self):
        self.open_window()

        self.peer.send_ack(2, ce_count=4, q_delay_us=4000)
        self.wait_for_event("FEEDBACK", 1)
        self.peer.send_ack(2, ce_count=1, q_delay_us=9999)
        self.peer.send_ack(3, ce_count=4, q_delay_us=4100)
        self.wait_for_event("FEEDBACK", 2)

        self.assertEqual(self.ce_totals(), [4, 4])
        self.assertEqual(self.controller.feedback, [(4, 0.004), (0, 0.0041)])

        self.finish_with_feedback(fin_ce=4, fin_q=4100, ack_ce=4, ack_q=4100)

    def test_loss_only_controller_ignores_marks_for_its_window(self):
        self.open_window()

        self.peer.send_ack(1, ce_count=3, q_delay_us=800)
        self.wait_for_event("FEEDBACK", 1)

        deadline = time.monotonic() + 2.0
        cwnd = None
        while time.monotonic() < deadline and cwnd is None:
            for row in self.events("CWND_UPDATE"):
                if "reason=ack" in row["details"]:
                    cwnd = float(
                        re.search(r"cwnd=([\d.]+)", row["details"]).group(1)
                    )
            time.sleep(0.02)

        self.assertIsNotNone(cwnd)
        self.assertAlmostEqual(cwnd, 32 + 1 / 32, places=3)

        self.finish_with_feedback(fin_ce=3, fin_q=800, ack_ce=3, ack_q=800)

    def test_fin_acknowledgement_feedback_is_logged(self):
        self.open_window()

        self.finish_with_feedback(fin_ce=6, fin_q=1234, ack_ce=2, ack_q=500)

        rows = self.feedback_rows()
        self.assertEqual(len(rows), 2)
        self.assertIn("ce_new=4", rows[-1]["details"])
        self.assertIn("ce_total=6", rows[-1]["details"])
        self.assertIn("q_delay_us=1234", rows[-1]["details"])
        self.assertEqual(self.controller.feedback[-1], (4, 0.001234))


if __name__ == "__main__":
    unittest.main()
