import math
import unittest

from transport.cc.dctcp_style import DEFAULT_G, DctcpController
from transport.cc.reno import RenoController


def finish_window(controller):
    """ACK one packet at a time until the controller closes its window."""

    acks = 0
    while True:
        controller.on_ack(1.0)
        acks += 1
        if controller.window_acked == 0.0:
            return acks


class DctcpControllerTests(unittest.TestCase):
    def test_name_identifies_the_mode(self):
        self.assertEqual(DctcpController().name, "dctcp")

    def test_without_marks_growth_and_window_match_reno(self):
        dctcp = DctcpController(initial_cwnd=10.0, ssthresh=5.0)
        reno = RenoController(initial_cwnd=10.0, ssthresh=5.0)

        for _ in range(60):
            dctcp.on_ack(1.0)
            reno.on_ack(1.0)
            self.assertAlmostEqual(dctcp.cwnd, reno.cwnd)

        self.assertEqual(dctcp.alpha, 0.0)

    def test_fully_marked_window_reduces_cwnd_by_alpha_over_two(self):
        dctcp = DctcpController(initial_cwnd=10.0, ssthresh=5.0)
        reno = RenoController(initial_cwnd=10.0, ssthresh=5.0)

        dctcp.on_feedback(10, 0.0)
        acks = finish_window(dctcp)
        for _ in range(acks):
            reno.on_ack(1.0)

        self.assertAlmostEqual(dctcp.alpha, DEFAULT_G)
        self.assertAlmostEqual(
            dctcp.cwnd, reno.cwnd * (1 - DEFAULT_G / 2)
        )

    def test_reduction_happens_once_per_window_however_marks_arrive(self):
        split = DctcpController(initial_cwnd=10.0, ssthresh=5.0)
        whole = DctcpController(initial_cwnd=10.0, ssthresh=5.0)

        split.on_feedback(3, 0.0)
        split.on_ack(1.0)
        split.on_feedback(7, 0.0)
        finish_window(split)

        whole.on_feedback(10, 0.0)
        finish_window(whole)

        self.assertAlmostEqual(split.cwnd, whole.cwnd)
        self.assertAlmostEqual(split.alpha, whole.alpha)

    def test_partially_marked_window_uses_the_marked_fraction(self):
        dctcp = DctcpController(initial_cwnd=10.0, ssthresh=5.0, g=0.5)

        dctcp.on_feedback(5, 0.0)
        finish_window(dctcp)

        self.assertAlmostEqual(dctcp.alpha, 0.5 * 0.5)

    def test_alpha_follows_the_ewma_across_windows(self):
        g = DEFAULT_G
        dctcp = DctcpController(initial_cwnd=10.0, ssthresh=5.0, g=g)

        dctcp.on_feedback(dctcp.cwnd, 0.0)
        finish_window(dctcp)
        self.assertAlmostEqual(dctcp.alpha, g)

        finish_window(dctcp)
        self.assertAlmostEqual(dctcp.alpha, (1 - g) * g)

        dctcp.on_feedback(math.ceil(dctcp.window_target), 0.0)
        finish_window(dctcp)
        self.assertAlmostEqual(dctcp.alpha, (1 - g) * (1 - g) * g + g)

    def test_marked_window_sets_ssthresh_to_the_reduced_window(self):
        dctcp = DctcpController(initial_cwnd=10.0, ssthresh=5.0)

        dctcp.on_feedback(10, 0.0)
        finish_window(dctcp)

        self.assertAlmostEqual(dctcp.ssthresh, dctcp.cwnd)
        self.assertEqual(dctcp.phase, "congestion_avoidance")

    def test_cwnd_never_drops_below_one_packet(self):
        dctcp = DctcpController(initial_cwnd=1.0, ssthresh=1.0, g=1.0)

        dctcp.on_feedback(1, 0.0)
        finish_window(dctcp)

        self.assertGreaterEqual(dctcp.cwnd, 1.0)

    def test_timeout_and_loss_responses_are_unchanged(self):
        dctcp = DctcpController(initial_cwnd=10.0, ssthresh=20.0)

        dctcp.on_loss(10.0)
        self.assertEqual(dctcp.cwnd, 5.0)
        self.assertEqual(dctcp.ssthresh, 5.0)
        self.assertEqual(dctcp.phase, "fast_recovery")

        dctcp.on_timeout(6.0, 0)
        self.assertEqual(dctcp.cwnd, 1.0)
        self.assertEqual(dctcp.phase, "slow_start")

    def test_invalid_gain_is_rejected(self):
        for value in (0.0, -0.1, 1.5, float("nan"), True):
            with self.subTest(g=value):
                with self.assertRaises(ValueError):
                    DctcpController(g=value)

    def test_gain_of_one_is_accepted(self):
        dctcp = DctcpController(initial_cwnd=10.0, ssthresh=5.0, g=1.0)

        dctcp.on_feedback(10, 0.0)
        finish_window(dctcp)

        self.assertAlmostEqual(dctcp.alpha, 1.0)


if __name__ == "__main__":
    unittest.main()
