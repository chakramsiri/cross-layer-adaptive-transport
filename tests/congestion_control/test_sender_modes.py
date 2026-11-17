import unittest

from transport.cc.factory import make_controller

from tests.helpers import SenderTestBase


class RenoModeLogTests(SenderTestBase):
    def test_mode_is_logged_at_start(self):
        self.open_window()
        self.finish_transfer()

        rows = self.events("CC_MODE")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["details"], "mode=reno")


class DctcpModeLogTests(SenderTestBase):
    def make_controller(self):
        return make_controller("dctcp", initial_cwnd=float(self.INITIAL_CWND))

    def test_mode_is_logged_at_start(self):
        self.open_window()
        self.finish_transfer()

        rows = self.events("CC_MODE")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["details"], "mode=dctcp")


class DelayModeLogTests(SenderTestBase):
    def make_controller(self):
        return make_controller("delay", initial_cwnd=float(self.INITIAL_CWND))

    def test_mode_is_logged_at_start(self):
        self.open_window()
        self.finish_transfer()

        rows = self.events("CC_MODE")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["details"], "mode=delay")


class NoControllerModeLogTests(SenderTestBase):
    CONTROLLER_ENABLED = False

    def test_disabled_congestion_control_is_logged_as_none(self):
        self.open_window()
        self.finish_transfer()

        rows = self.events("CC_MODE")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["details"], "mode=none")


if __name__ == "__main__":
    unittest.main()
