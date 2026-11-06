"""Deterministic unit tests for transport.rtt (RFC 6298 estimator):
SRTT/RTTVAR/RTO (sections 2.1-2.5), Karn's algorithm (3), clock
granularity (4), and exponential backoff (5.5).
"""

import unittest

from transport.rtt import (
    ALPHA,
    BETA,
    K,
    CLOCK_GRANULARITY,
    INITIAL_RTO,
    MIN_RTO,
    MAX_RTO,
    UINT32_MASK,
    RttEstimator,
    TimestampAllocator,
    encode_timestamp,
    rtt_seconds,
)


class RttEstimatorTests(unittest.TestCase):
    def test_initial_state(self):
        # RFC 6298 (2.1): RTO is 1 second until the first measurement.
        estimator = RttEstimator()

        self.assertIsNone(estimator.srtt)
        self.assertIsNone(estimator.rttvar)
        self.assertEqual(estimator.rto, INITIAL_RTO)
        self.assertEqual(estimator.rto, 1.0)
        self.assertFalse(estimator.has_sample)

    def test_custom_initial_rto_for_retried_handshake(self):
        # RFC 6298 (5.7): a 3 second initial RTO is used for the data phase
        # when the handshake timer expired during connection setup.
        estimator = RttEstimator(initial_rto=3.0)

        self.assertEqual(estimator.rto, 3.0)
        self.assertFalse(estimator.has_sample)

    def test_first_sample_sets_srtt_rttvar_rto(self):
        # RFC 6298 (2.2): SRTT <- R, RTTVAR <- R/2,
        # RTO <- SRTT + max(G, K*RTTVAR).
        estimator = RttEstimator()
        estimator.update(0.5)

        self.assertAlmostEqual(estimator.srtt, 0.5)
        self.assertAlmostEqual(estimator.rttvar, 0.25)
        self.assertAlmostEqual(
            estimator.rto, 0.5 + max(CLOCK_GRANULARITY, K * 0.25)
        )
        self.assertTrue(estimator.has_sample)

    def test_first_sample_rto_rounded_up_to_minimum(self):
        # RFC 6298 (2.4): an RTO below 1 second is rounded up.
        estimator = RttEstimator()
        estimator.update(0.02)

        self.assertAlmostEqual(estimator.srtt, 0.02)
        self.assertAlmostEqual(estimator.rttvar, 0.01)
        self.assertAlmostEqual(estimator.rto, MIN_RTO)

    def test_subsequent_sample_updates_rttvar_before_srtt(self):
        # RFC 6298 (2.3): RTTVAR uses the SRTT value *before* its own
        # update, and the assignments MUST happen in that order.
        estimator = RttEstimator()
        estimator.update(0.5)
        estimator.update(1.0)

        expected_rttvar = (1 - BETA) * 0.25 + BETA * abs(0.5 - 1.0)
        expected_srtt = (1 - ALPHA) * 0.5 + ALPHA * 1.0

        self.assertAlmostEqual(estimator.rttvar, expected_rttvar)
        self.assertAlmostEqual(estimator.srtt, expected_srtt)
        self.assertAlmostEqual(
            estimator.rto,
            max(
                MIN_RTO,
                expected_srtt + max(CLOCK_GRANULARITY, K * expected_rttvar),
            ),
        )

        # Discriminator: updating SRTT first would yield a different RTTVAR.
        wrong_order_rttvar = (1 - BETA) * 0.25 + BETA * abs(
            expected_srtt - 1.0
        )
        self.assertNotAlmostEqual(estimator.rttvar, wrong_order_rttvar)

    def test_repeated_samples_converge(self):
        estimator = RttEstimator()

        for _ in range(50):
            estimator.update(0.4)

        self.assertAlmostEqual(estimator.srtt, 0.4, places=9)
        # RTTVAR decays geometrically: after 50 identical samples it is
        # below the 1 ms clock granularity G (RFC 6298 sections 2.3, 4).
        self.assertLess(estimator.rttvar, CLOCK_GRANULARITY)
        # K*RTTVAR collapses below G, so G provides the floor and the
        # 1 second minimum RTO applies (RFC 6298 2.4).
        self.assertAlmostEqual(estimator.rto, MIN_RTO)

    def test_rto_maximum_clamped(self):
        # RFC 6298 (2.5): a maximum of at least 60 seconds may be placed.
        estimator = RttEstimator()
        estimator.update(0.1)
        estimator.update(1000.0)

        self.assertEqual(estimator.rto, MAX_RTO)

    def test_backoff_doubles_and_caps(self):
        # RFC 6298 (5.5): RTO <- RTO * 2 on timeout, bounded by the max.
        estimator = RttEstimator()

        estimator.backoff()
        self.assertAlmostEqual(estimator.rto, 2.0)
        estimator.backoff()
        self.assertAlmostEqual(estimator.rto, 4.0)

        for _ in range(10):
            estimator.backoff()

        self.assertEqual(estimator.rto, MAX_RTO)

    def test_new_sample_recomputes_rto_and_collapses_backoff(self):
        # RFC 6298 (5.5) note: a new measurement recomputes RTO from
        # SRTT/RTTVAR, collapsing the doubled value.
        estimator = RttEstimator()
        estimator.update(0.5)          # RTO = 1.5
        estimator.backoff()            # RTO = 3.0
        estimator.update(0.55)

        expected_rttvar = (1 - BETA) * 0.25 + BETA * abs(0.5 - 0.55)
        expected_srtt = (1 - ALPHA) * 0.5 + ALPHA * 0.55
        expected_rto = max(
            MIN_RTO,
            expected_srtt + max(CLOCK_GRANULARITY, K * expected_rttvar),
        )

        self.assertAlmostEqual(estimator.rttvar, expected_rttvar)
        self.assertAlmostEqual(estimator.srtt, expected_srtt)
        self.assertAlmostEqual(estimator.rto, expected_rto)
        self.assertLess(estimator.rto, 3.0)

    def test_first_sample_after_backoff_still_uses_first_sample_rule(self):
        estimator = RttEstimator(initial_rto=3.0)
        estimator.backoff()  # 6.0 before any sample (e.g. SYN retry path)
        estimator.update(0.5)

        self.assertAlmostEqual(estimator.srtt, 0.5)
        self.assertAlmostEqual(estimator.rttvar, 0.25)
        self.assertAlmostEqual(estimator.rto, 1.5)


class TimestampCodecTests(unittest.TestCase):
    def test_encode_uses_microseconds(self):
        allocator = TimestampAllocator()
        # Stamps are the true send time; zero can only occur at the exact
        # uint32 wraparound and is then treated as "no echo" by the sender.
        self.assertEqual(allocator.encode(0.0), 0)
        self.assertEqual(allocator.encode(1.5), 1_500_000)
        self.assertEqual(allocator.encode(2.25), 2_250_000)

    def test_encode_wraps_at_uint32(self):
        # 4,294,968.0 s = 4,294,968,000,000 us; modulo 2**32 leaves 704,000.
        allocator = TimestampAllocator()
        self.assertEqual(allocator.encode(4294968.0), 704_000)

    def test_same_microsecond_sends_share_the_true_stamp(self):
        # No stamp is fabricated later than the send instant: two sends in
        # one microsecond share a stamp, which the sender must treat as
        # ambiguous and skip.
        allocator = TimestampAllocator()
        first = allocator.encode(1.5)
        second = allocator.encode(1.5)

        self.assertEqual(first, second)
        self.assertEqual(first, 1_500_000)

    def test_encode_is_stateless_across_allocators(self):
        # A fresh allocator yields the identical stamp: no call can push a
        # stamp past the actual monotonic send time.
        self.assertEqual(
            TimestampAllocator().encode(1.5),
            TimestampAllocator().encode(1.5),
        )
        self.assertEqual(
            TimestampAllocator().encode(4294968.0), 704_000
        )

    def test_rtt_seconds_simple(self):
        self.assertAlmostEqual(
            rtt_seconds(1_000_000, 1.25), 0.25, places=9
        )
        self.assertAlmostEqual(
            rtt_seconds(100_000_000, 100.1), 0.1, places=9
        )

    def test_rtt_seconds_handles_timestamp_wraparound(self):
        # Echo was encoded 100 ms before the uint32 wrap; now is 50 ms after.
        echo = UINT32_MASK - 99_999  # 100,000 us before 2**32
        self.assertAlmostEqual(rtt_seconds(echo, 0.05), 0.15, places=9)

    def test_rtt_seconds_across_large_gap(self):
        # A 90 second RTT remains exact despite the wraparound modulo.
        echo = UINT32_MASK - 29_999_999  # 30,000,000 us before 2**32
        self.assertAlmostEqual(rtt_seconds(echo, 60.0), 90.0, places=9)

    def test_zero_and_uint32_max_boundaries_are_arithmetically_valid(self):
        # Zero is a valid encoded value for the codec; the sender's
        # absence sentinel is a policy layered on top of it.
        self.assertAlmostEqual(rtt_seconds(0, 1.0), 1.0, places=9)

        # UINT32_MAX boundary: exactly one microsecond after the maximum.
        self.assertAlmostEqual(
            rtt_seconds(UINT32_MASK, (UINT32_MASK + 1) / 1_000_000.0),
            1e-06,
            places=9,
        )

        # Wrap boundary: echo = 1 us, now = 2**32 + 3 us -> 2 us elapsed.
        self.assertAlmostEqual(
            rtt_seconds(1, (UINT32_MASK + 1 + 3) / 1_000_000.0),
            2e-06,
            places=9,
        )


if __name__ == "__main__":
    unittest.main()
