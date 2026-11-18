import unittest

from transport.cc.delay_aware import (
    DEFAULT_INCREASE_STEP,
    DEFAULT_TARGET_SECONDS,
    DelayAwareController,
)
from transport.cc.reno import RenoController


def finish_window(controller):
    acks = 0
    while True:
        controller.on_ack(1.0)
        acks += 1
        if controller.window_acked == 0.0:
            return acks


class DelayAwareControllerTests(unittest.TestCase):
    def test_name_identifies_the_mode(self):
        self.assertEqual(DelayAwareController().name, "delay")

    def test_defaults_follow_the_guide_range(self):
        self.assertEqual(DEFAULT_TARGET_SECONDS, 0.005)
        self.assertEqual(DEFAULT_INCREASE_STEP, 1.0)

    def test_below_target_growth_matches_reno_with_default_step(self):
        delay = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0)
        reno = RenoController(initial_cwnd=10.0, ssthresh=5.0)

        for _ in range(60):
            delay.on_feedback(0, 0.001)
            delay.on_ack(1.0)
            reno.on_ack(1.0)
            self.assertAlmostEqual(delay.cwnd, reno.cwnd)

    def test_below_target_increase_step_scales_additive_growth(self):
        delay = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0,
                                     increase_step=2.0)

        delay.on_feedback(0, 0.001)
        delay.on_ack(1.0)

        self.assertAlmostEqual(delay.cwnd, 10.0 + 2.0 / 10.0)

    def test_slow_start_below_target_is_unchanged(self):
        delay = DelayAwareController(initial_cwnd=2.0, ssthresh=8.0,
                                     increase_step=3.0)

        delay.on_feedback(0, 0.001)
        delay.on_ack(1.0)

        self.assertEqual(delay.cwnd, 3.0)
        self.assertEqual(delay.phase, "slow_start")

    def test_above_target_reduces_proportionally_once_per_window(self):
        delay = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0,
                                     target_seconds=0.010)
        reno = RenoController(initial_cwnd=10.0, ssthresh=5.0)

        delay.on_feedback(0, 0.020)
        acks = finish_window(delay)
        for _ in range(acks):
            reno.on_ack(1.0)

        factor = 1 - 0.5 * (0.020 - 0.010) / 0.020
        self.assertAlmostEqual(delay.cwnd, reno.cwnd * factor)
        self.assertAlmostEqual(delay.ssthresh, delay.cwnd)

    def test_larger_excess_gives_a_larger_reduction(self):
        mild = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0,
                                    target_seconds=0.010)
        severe = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0,
                                      target_seconds=0.010)

        mild.on_feedback(0, 0.012)
        finish_window(mild)
        severe.on_feedback(0, 0.100)
        finish_window(severe)

        self.assertLess(severe.cwnd, mild.cwnd)

    def test_growth_is_held_while_congested(self):
        delay = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0,
                                     target_seconds=0.010)
        delay.on_feedback(0, 0.050)
        finish_window(delay)
        held = delay.cwnd

        delay.on_feedback(0, 0.050)
        for _ in range(int(held) - 1):
            delay.on_ack(1.0)

        self.assertEqual(delay.cwnd, held)

    def test_window_minimum_decides_the_verdict(self):
        delay = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0,
                                     target_seconds=0.010)
        reno = RenoController(initial_cwnd=10.0, ssthresh=5.0)

        delay.on_feedback(0, 0.050)
        delay.on_feedback(0, 0.002)
        acks = finish_window(delay)
        for _ in range(acks):
            reno.on_ack(1.0)

        self.assertAlmostEqual(delay.cwnd, reno.cwnd)

    def test_window_without_delay_samples_leaves_state_alone(self):
        delay = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0)
        reno = RenoController(initial_cwnd=10.0, ssthresh=5.0)

        acks = finish_window(delay)
        for _ in range(acks):
            reno.on_ack(1.0)

        self.assertAlmostEqual(delay.cwnd, reno.cwnd)

    def test_cwnd_never_drops_below_one_packet(self):
        delay = DelayAwareController(initial_cwnd=1.0, ssthresh=1.0,
                                     target_seconds=0.001)

        delay.on_feedback(0, 10.0)
        finish_window(delay)

        self.assertGreaterEqual(delay.cwnd, 1.0)

    def test_loss_and_timeout_responses_are_unchanged(self):
        delay = DelayAwareController(initial_cwnd=10.0, ssthresh=20.0)

        delay.on_loss(10.0)
        self.assertEqual(delay.cwnd, 5.0)
        self.assertEqual(delay.phase, "fast_recovery")

        delay.on_timeout(6.0, 0)
        self.assertEqual(delay.cwnd, 1.0)
        self.assertEqual(delay.phase, "slow_start")

    def test_invalid_parameters_are_rejected(self):
        bad = (
            {"target_seconds": 0.0},
            {"target_seconds": -0.001},
            {"target_seconds": float("inf")},
            {"target_seconds": True},
            {"increase_step": 0.0},
            {"increase_step": -1.0},
            {"increase_step": float("nan")},
        )
        for kwargs in bad:
            with self.subTest(**kwargs):
                with self.assertRaises(ValueError):
                    DelayAwareController(**kwargs)


if __name__ == "__main__":
    unittest.main()


class DelayAwareLossResetTests(unittest.TestCase):
    def test_timeout_clears_a_congested_verdict_so_growth_resumes(self):
        delay = DelayAwareController(initial_cwnd=4.0, ssthresh=4.0,
                                     target_seconds=0.005)
        delay.on_feedback(0, 0.050)
        for _ in range(4):
            delay.on_ack(1.0)
        self.assertTrue(delay.congested)

        delay.on_timeout(3.0, 0)
        delay.on_feedback(0, 0.001)
        delay.on_ack(1.0)

        self.assertFalse(delay.congested)
        self.assertEqual(delay.cwnd, 2.0)

    def test_delay_samples_before_a_timeout_do_not_decide_later_windows(self):
        delay = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0,
                                     target_seconds=0.005)
        delay.on_feedback(0, 0.001)
        delay.on_ack(1.0)

        delay.on_timeout(6.0, 0)
        delay.on_feedback(0, 0.050)
        finish_window(delay)

        self.assertTrue(delay.congested)

    def test_loss_reopens_the_window_at_the_reduced_window(self):
        delay = DelayAwareController(initial_cwnd=10.0, ssthresh=5.0)
        delay.on_ack(1.0)

        delay.on_loss(8.0)

        self.assertEqual(delay.window_target, delay.cwnd)
        self.assertEqual(delay.window_acked, 0.0)
