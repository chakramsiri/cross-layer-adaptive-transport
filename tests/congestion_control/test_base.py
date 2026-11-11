"""Interface tests for the CongestionController contract"""

import unittest

from transport.cc import CongestionController, RenoController


class MinimalController(CongestionController):
    def __init__(self):
        self._cwnd = 1.0
        self.losses = 0
        self.timeouts = 0

    @property
    def cwnd(self):
        return self._cwnd

    def on_ack(self, acked_data):
        self._cwnd += acked_data

    def on_loss(self, flight_size):
        self.losses += 1
        self._cwnd = max(flight_size / 2.0, 2.0)

    def on_timeout(self, flight_size, segment_id):
        self.timeouts += 1
        self._cwnd = 1.0

    def on_feedback(self, ce_count, q_delay):
        pass


class InterfaceTests(unittest.TestCase):
    def test_interface_cannot_be_instantiated(self):
        with self.assertRaises(TypeError):
            CongestionController()

    def test_interface_declares_every_required_member(self):
        required = {"cwnd", "on_ack", "on_loss", "on_timeout", "on_feedback"}
        self.assertEqual(set(CongestionController.__abstractmethods__), required)

    def test_reno_follows_the_interface(self):
        self.assertIsInstance(RenoController(), CongestionController)

    def test_minimal_implementation_tracks_the_window(self):
        controller = MinimalController()
        controller.on_ack(2)
        self.assertEqual(controller.cwnd, 3.0)

    def test_minimal_implementation_exercises_the_loss_callback(self):
        controller = MinimalController()
        controller.on_ack(8)  # cwnd 9
        controller.on_loss(9)

        self.assertEqual(controller.losses, 1)
        self.assertAlmostEqual(controller.cwnd, 4.5)

    def test_minimal_implementation_exercises_the_timeout_callback(self):
        controller = MinimalController()
        controller.on_ack(8)
        controller.on_timeout(9, 3)

        self.assertEqual(controller.timeouts, 1)
        self.assertEqual(controller.cwnd, 1.0)

    def test_generic_controllers_have_no_phase_telemetry(self):
        self.assertIsNone(MinimalController().phase)

    def test_generic_controllers_have_no_ssthresh_telemetry(self):
        self.assertIsNone(MinimalController().ssthresh)

    def test_congestion_feedback_is_ignored_without_phase_seven_behaviour(self):
        controller = MinimalController()
        before = controller.cwnd
        controller.on_feedback(ce_count=3, q_delay=0.02)
        self.assertEqual(controller.cwnd, before)


if __name__ == "__main__":
    unittest.main()
