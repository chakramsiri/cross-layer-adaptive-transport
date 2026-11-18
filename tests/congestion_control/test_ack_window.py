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


class AckWindowExcessTests(unittest.TestCase):
    def test_closing_ack_counts_its_full_excess_in_the_closing_window(self):
        window = AckWindow()
        window.open(4.0)

        self.assertTrue(window.acknowledge(9.0))
        self.assertEqual(window.acked, 9.0)

    def test_next_window_starts_empty_after_an_overshooting_ack(self):
        window = AckWindow()
        window.open(4.0)
        window.acknowledge(9.0)

        window.open(4.0)

        self.assertFalse(window.acknowledge(3.0))
        self.assertTrue(window.acknowledge(1.0))

    def test_each_acknowledged_packet_is_counted_in_exactly_one_window(self):
        window = AckWindow()
        window.open(4.0)
        closed_totals = []
        for acks in (9.0, 1.0, 1.0, 1.0, 1.0, 2.0):
            if window.acknowledge(acks):
                closed_totals.append(window.acked)
                window.open(4.0)
        total_acked = 9.0 + 1.0 + 1.0 + 1.0 + 1.0 + 2.0

        self.assertEqual(closed_totals, [9.0, 4.0])
        self.assertEqual(sum(closed_totals) + window.acked, total_acked)
