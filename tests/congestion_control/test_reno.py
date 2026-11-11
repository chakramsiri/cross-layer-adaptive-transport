"""Deterministic Reno controller tests (slow start, AIMD, recovery, timeout).

FlightSize is reported by the sender in packets (the same units as cwnd,
fractional for partial-sized packets).
"""

import unittest

from transport.cc.reno import (
    INITIAL_CWND,
    INITIAL_SSTHRESH,
    MIN_SSTHRESH,
    RenoController,
)


class InitialStateTests(unittest.TestCase):
    def test_initial_window_is_one_packet(self):
        controller = RenoController()

        self.assertEqual(controller.cwnd, INITIAL_CWND)
        self.assertEqual(controller.cwnd, 1.0)
        self.assertEqual(controller.ssthresh, INITIAL_SSTHRESH)
        self.assertFalse(controller.in_fast_recovery)

    def test_initial_window_and_ssthresh_are_configurable(self):
        controller = RenoController(initial_cwnd=4, ssthresh=6)

        self.assertEqual(controller.cwnd, 4.0)
        self.assertEqual(controller.ssthresh, 6.0)

    def test_initial_window_below_one_is_rejected(self):
        with self.assertRaises(ValueError):
            RenoController(initial_cwnd=0.5)

    def test_non_finite_initial_window_is_rejected(self):
        for bad in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(ValueError):
                RenoController(initial_cwnd=bad)

    def test_nan_and_negative_infinite_ssthresh_are_rejected(self):
        for bad in (float("nan"), float("-inf")):
            with self.assertRaises(ValueError):
                RenoController(ssthresh=bad)

    def test_positive_infinite_ssthresh_is_supported(self):
        controller = RenoController(ssthresh=float("inf"))

        self.assertEqual(controller.ssthresh, float("inf"))
        controller.on_ack(1)  # slow start with an unbounded threshold
        self.assertEqual(controller.cwnd, 2.0)

    def test_floor_constants(self):
        self.assertEqual(INITIAL_CWND, 1.0)
        self.assertEqual(INITIAL_SSTHRESH, 8.0)
        self.assertEqual(MIN_SSTHRESH, 2.0)


class SlowStartTests(unittest.TestCase):
    def test_window_grows_one_smss_per_acknowledging_ack(self):
        controller = RenoController()

        controller.on_ack(1)
        self.assertEqual(controller.cwnd, 2.0)
        controller.on_ack(2)  # one ACK covering two segments: +1 SMSS
        self.assertEqual(controller.cwnd, 3.0)
        controller.on_ack(4)  # one ACK covering four segments: +1 SMSS
        self.assertEqual(controller.cwnd, 4.0)

    def test_cumulative_ack_in_slow_start_grows_less_than_individual_acks(self):
        cumulative = RenoController()
        cumulative.on_ack(6)  # one ACK covering six segments: +1 SMSS

        individual = RenoController()
        for _ in range(6):
            individual.on_ack(1)  # six separate ACKs: +1 SMSS each

        self.assertAlmostEqual(cumulative.cwnd, 2.0)
        self.assertAlmostEqual(individual.cwnd, 7.0)
        self.assertLess(cumulative.cwnd, individual.cwnd)

    def test_partial_sized_acknowledged_data_counts_fractionally(self):
        controller = RenoController()  # slow start, cwnd 1

        # 400 B of a 1000 B segment: the min(N, SMSS) increment is 0.4.
        controller.on_ack(0.4)
        self.assertAlmostEqual(controller.cwnd, 1.4)
        controller.on_ack(0.6)
        self.assertAlmostEqual(controller.cwnd, 2.0)

    def test_transition_into_congestion_avoidance_at_ssthresh(self):
        controller = RenoController(initial_cwnd=4, ssthresh=6)

        controller.on_ack(1)  # 5 < 6: still slow start
        self.assertEqual(controller.cwnd, 5.0)
        controller.on_ack(1)  # 6 == ssthresh
        self.assertEqual(controller.cwnd, 6.0)
        controller.on_ack(1)  # congestion avoidance: + 1/6
        self.assertAlmostEqual(controller.cwnd, 6.0 + 1.0 / 6.0)

    def test_cumulative_ack_never_crosses_ssthresh_in_one_step(self):
        controller = RenoController(initial_cwnd=4, ssthresh=5)

        controller.on_ack(3)  # one ACK covering three segments: +1 SMSS
        self.assertAlmostEqual(controller.cwnd, 5.0)
        controller.on_ack(1)  # cwnd == ssthresh: congestion avoidance
        self.assertAlmostEqual(controller.cwnd, 5.0 + 1.0 / 5.0)


class CongestionAvoidanceTests(unittest.TestCase):
    def test_additive_increase_per_acked_packet(self):
        controller = RenoController(initial_cwnd=2, ssthresh=2)

        controller.on_ack(1)
        self.assertAlmostEqual(controller.cwnd, 2.5)
        controller.on_ack(1)
        self.assertAlmostEqual(controller.cwnd, 2.9)

    def test_cumulative_ack_of_multiple_packets_applies_one_update(self):
        controller = RenoController(initial_cwnd=2, ssthresh=2)

        controller.on_ack(2)  # one ACK covering two segments: one step

        self.assertAlmostEqual(controller.cwnd, 2.5)

    def test_cumulative_ack_grows_less_than_individual_acks(self):
        cumulative = RenoController(initial_cwnd=6, ssthresh=6)
        cumulative.on_ack(6)  # one ACK covering six segments: one step

        individual = RenoController(initial_cwnd=6, ssthresh=6)
        for _ in range(6):
            individual.on_ack(1)  # six separate ACKs: six steps

        self.assertAlmostEqual(cumulative.cwnd, 6.0 + 1.0 / 6.0)
        self.assertGreater(individual.cwnd, cumulative.cwnd)

    def test_growth_stays_fractional_but_never_shrinks_on_acks(self):
        controller = RenoController(initial_cwnd=6, ssthresh=6)
        before = controller.cwnd

        for _ in range(10):
            controller.on_ack(1)
            self.assertGreaterEqual(controller.cwnd, before)
            before = controller.cwnd

        self.assertGreater(controller.cwnd, 6.0)
        self.assertLess(controller.cwnd, 8.0)


class FastRecoveryTests(unittest.TestCase):
    def test_ssthresh_is_calculated_from_flight_size(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_loss(16)

        self.assertEqual(controller.ssthresh, 8.0)
        self.assertEqual(controller.cwnd, 8.0)
        self.assertTrue(controller.in_fast_recovery)

    def test_flight_size_below_cwnd_is_respected(self):
        controller = RenoController(initial_cwnd=4)
        controller.on_ack(16)  # cwnd grows to 20

        controller.on_loss(6)

        self.assertEqual(controller.ssthresh, 3.0)
        self.assertEqual(controller.cwnd, 3.0)

    def test_flight_size_above_cwnd_is_respected(self):
        controller = RenoController(initial_cwnd=4)

        controller.on_loss(30)

        self.assertEqual(controller.ssthresh, 15.0)
        self.assertEqual(controller.cwnd, 15.0)

    def test_ssthresh_has_a_floor_of_two_segments(self):
        controller = RenoController(initial_cwnd=4)

        controller.on_loss(1)

        self.assertEqual(controller.ssthresh, 2.0)
        self.assertEqual(controller.cwnd, 2.0)

    def test_partial_sized_flight_size_is_fractional(self):
        controller = RenoController(initial_cwnd=8)

        # 2 full packets plus a 0.4-sized partial final packet.
        controller.on_loss(4.4)

        self.assertAlmostEqual(controller.ssthresh, 2.2)
        self.assertAlmostEqual(controller.cwnd, 2.2)

    def test_repeated_losses_in_one_event_do_not_reduce_again(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_loss(16)
        controller.on_loss(16)
        controller.on_loss(16)

        self.assertEqual(controller.cwnd, 8.0)
        self.assertEqual(controller.ssthresh, 8.0)

    def test_recovery_exits_on_a_new_ack_and_increase_resumes(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_loss(16)
        controller.on_ack(1)

        self.assertFalse(controller.in_fast_recovery)
        self.assertAlmostEqual(controller.cwnd, 8.0 + 1.0 / 8.0)

    def test_a_new_loss_event_after_recovery_reduces_again(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_loss(16)
        controller.on_ack(1)  # cwnd 8.125, recovery over
        controller.on_loss(8.125)

        self.assertAlmostEqual(controller.ssthresh, 8.125 / 2.0)
        self.assertAlmostEqual(controller.cwnd, 8.125 / 2.0)


class TimeoutTests(unittest.TestCase):
    def test_timeout_resets_cwnd_and_sets_ssthresh_from_flight_size(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_timeout(16, 0)

        self.assertEqual(controller.ssthresh, 8.0)
        self.assertEqual(controller.cwnd, 1.0)
        self.assertFalse(controller.in_fast_recovery)

    def test_timeout_after_fast_recovery_recalcs_from_flight_size(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_loss(16)  # ssthresh 8, cwnd 8
        controller.on_timeout(4, 7)  # a different segment: recalculated

        self.assertEqual(controller.ssthresh, 2.0)
        self.assertEqual(controller.cwnd, 1.0)

    def test_timeout_ssthresh_floor(self):
        controller = RenoController(initial_cwnd=2)

        controller.on_timeout(1, 0)

        self.assertEqual(controller.ssthresh, 2.0)
        self.assertEqual(controller.cwnd, 1.0)

    def test_first_timeout_of_a_segment_recalcs_ssthresh(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_timeout(12, 5)

        self.assertEqual(controller.ssthresh, 6.0)

    def test_subsequent_timeouts_of_the_same_segment_retain_ssthresh(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_timeout(12, 5)  # ssthresh 6, cwnd 1
        controller.on_timeout(12, 5)  # same segment: retained
        controller.on_timeout(12, 5)

        self.assertEqual(controller.ssthresh, 6.0)
        self.assertEqual(controller.cwnd, 1.0)

    def test_an_ack_moving_to_a_new_segment_begins_a_new_episode(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_timeout(12, 5)  # ssthresh 6
        controller.on_timeout(12, 5)  # retained
        controller.on_ack(1)          # cumulative ACK moves past segment 5
        controller.on_timeout(20, 9)  # a different segment: recalculated

        self.assertEqual(controller.ssthresh, 10.0)
        self.assertEqual(controller.cwnd, 1.0)

    def test_loss_event_does_not_reset_the_timeout_episode(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_timeout(12, 5)  # ssthresh 6, cwnd 1, episode = 5
        controller.on_loss(20)        # fast recovery: ssthresh 10, cwnd 10
        controller.on_timeout(4, 5)   # same segment 5: retain, no recalc

        # A recalculation from FlightSize 4 would have set ssthresh 2.
        self.assertEqual(controller.ssthresh, 10.0)
        self.assertEqual(controller.cwnd, 1.0)
        self.assertFalse(controller.in_fast_recovery)

    def test_reenters_slow_start_after_timeout(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_timeout(16, 0)
        controller.on_ack(3)  # one ACK covering three segments: +1 SMSS

        self.assertEqual(controller.cwnd, 2.0)

    def test_recovery_flag_is_cleared_by_timeout(self):
        controller = RenoController(initial_cwnd=8)

        controller.on_loss(8)
        self.assertTrue(controller.in_fast_recovery)
        controller.on_timeout(8, 3)

        self.assertFalse(controller.in_fast_recovery)


class SlowStartPhaseTests(unittest.TestCase):
    def test_ack_updates_the_operating_phase(self):
        controller = RenoController(initial_cwnd=4, ssthresh=6)

        controller.on_ack(1)  # 5 < 6: slow start
        self.assertTrue(controller.in_slow_start)
        controller.on_ack(1)  # 6 == ssthresh, via the slow-start branch
        self.assertTrue(controller.in_slow_start)
        controller.on_ack(1)  # 6 == ssthresh: congestion avoidance
        self.assertFalse(controller.in_slow_start)

    def test_loss_and_timeout_update_the_operating_phase(self):
        controller = RenoController(initial_cwnd=8)

        controller.on_loss(8)  # recovery: congestion avoidance regime
        self.assertFalse(controller.in_slow_start)
        controller.on_timeout(8, 0)  # re-enters slow start with cwnd 1
        self.assertTrue(controller.in_slow_start)

    def test_initial_phase_is_slow_start_for_finite_and_infinite_ssthresh(self):
        self.assertTrue(RenoController().in_slow_start)
        self.assertTrue(RenoController(ssthresh=float("inf")).in_slow_start)


class PhaseTelemetryTests(unittest.TestCase):
    def test_phase_reports_slow_start_then_congestion_avoidance(self):
        controller = RenoController(initial_cwnd=4, ssthresh=5)

        self.assertEqual(controller.phase, "slow_start")
        controller.on_ack(1)
        self.assertEqual(controller.phase, "slow_start")
        controller.on_ack(1)
        self.assertEqual(controller.phase, "congestion_avoidance")

    def test_phase_reports_fast_recovery_after_loss(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_loss(16)

        self.assertEqual(controller.phase, "fast_recovery")
        controller.on_ack(1)
        self.assertEqual(controller.phase, "congestion_avoidance")

    def test_phase_returns_to_slow_start_after_timeout(self):
        controller = RenoController(initial_cwnd=16)

        controller.on_timeout(16, 0)

        self.assertEqual(controller.phase, "slow_start")


class FeedbackTests(unittest.TestCase):
    def test_loss_based_mode_ignores_congestion_feedback(self):
        controller = RenoController(initial_cwnd=8)

        controller.on_feedback(ce_count=5, q_delay=0.05)

        self.assertEqual(controller.cwnd, 8.0)
        self.assertEqual(controller.ssthresh, INITIAL_SSTHRESH)
        self.assertFalse(controller.in_fast_recovery)


if __name__ == "__main__":
    unittest.main()
