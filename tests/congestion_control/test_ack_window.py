import unittest

from transport.cc.window import AckWindow


class AckWindowTests(unittest.TestCase):
    def test_window_closes_once_a_full_congestion_window_is_acknowledged(self):
        window = AckWindow()
        window.open(3.0)

        self.assertFalse(window.acknowledge(1.0))
        self.assertFalse(window.acknowledge(1.0))
        self.assertTrue(window.acknowledge(1.0))
        self.assertEqual(window.acked, 3.0)

    def test_fractional_acknowledgements_accumulate(self):
        window = AckWindow()
        window.open(2.0)

        self.assertFalse(window.acknowledge(0.5))
        self.assertFalse(window.acknowledge(0.5))
        self.assertTrue(window.acknowledge(1.0))

    def test_open_restarts_the_count_with_the_new_target(self):
        window = AckWindow()
        window.open(2.0)
        window.acknowledge(2.0)

        window.open(5.0)

        self.assertEqual(window.acked, 0.0)
        self.assertEqual(window.target, 5.0)
        self.assertFalse(window.acknowledge(4.0))

    def test_target_is_at_least_one_packet(self):
        window = AckWindow()
        window.open(0.2)

        self.assertEqual(window.target, 1.0)
        self.assertTrue(window.acknowledge(1.0))


if __name__ == "__main__":
    unittest.main()
