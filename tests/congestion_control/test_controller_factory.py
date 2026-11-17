import unittest

from transport.cc.base import CongestionController
from transport.cc.delay_aware import DelayAwareController
from transport.cc.dctcp_style import DctcpController
from transport.cc.factory import MODES, make_controller
from transport.cc.reno import RenoController


class ControllerFactoryTests(unittest.TestCase):
    def test_modes_are_the_three_guide_modes(self):
        self.assertEqual(MODES, ("reno", "dctcp", "delay"))

    def test_each_mode_builds_its_controller(self):
        self.assertIsInstance(make_controller("reno"), RenoController)
        self.assertIsInstance(make_controller("dctcp"), DctcpController)
        self.assertIsInstance(make_controller("delay"), DelayAwareController)

    def test_every_controller_implements_the_interface(self):
        for mode in MODES:
            with self.subTest(mode=mode):
                self.assertIsInstance(make_controller(mode),
                                      CongestionController)

    def test_controllers_report_their_mode_name(self):
        for mode in MODES:
            with self.subTest(mode=mode):
                self.assertEqual(make_controller(mode).name, mode)

    def test_initial_window_and_threshold_are_passed_through(self):
        for mode in MODES:
            with self.subTest(mode=mode):
                controller = make_controller(mode, initial_cwnd=4.0,
                                             ssthresh=12.0)
                self.assertEqual(controller.cwnd, 4.0)
                self.assertEqual(controller.ssthresh, 12.0)

    def test_mode_parameters_are_forwarded(self):
        dctcp = make_controller("dctcp", g=0.25)
        self.assertEqual(dctcp.g, 0.25)

        delay = make_controller("delay", target_seconds=0.005,
                                increase_step=2.0)
        self.assertEqual(delay.target_seconds, 0.005)
        self.assertEqual(delay.increase_step, 2.0)

    def test_unknown_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            make_controller("cubic")

    def test_parameters_for_another_mode_are_rejected(self):
        with self.assertRaises(ValueError):
            make_controller("reno", g=0.5)
        with self.assertRaises(ValueError):
            make_controller("dctcp", target_seconds=0.01)


if __name__ == "__main__":
    unittest.main()
