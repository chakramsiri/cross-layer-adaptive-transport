"""Deterministic unit tests for the RED queue."""

import math
import unittest

from router.queues.base import (
    DequeueResult,
    EnqueueResult,
    Queue,
    QueuedPacket,
)
from router.queues.red import (
    DEFAULT_RATE_BPS,
    DEFAULT_TYPICAL_PACKET_BYTES,
    RED,
)
from transport.packet import DATA, Packet
from transport.sender import CHUNK_SIZE


class FakeClock:
    """Controllable monotonic clock for deterministic sojourn tests."""

    def __init__(self, now=100.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class FixedRandom:
    def __init__(self, value=0.5):
        self.value = value
        self.calls = 0

    def random(self):
        self.calls += 1
        return self.value


def packet(payload=b"data", source=("10.0.0.1", 5000)):
    return QueuedPacket(payload=payload, source=source)


def make_red(capacity=16, **overrides):
    options = dict(
        w_q=1.0,
        min_th=4.0,
        max_th=8.0,
        max_p=0.5,
        rng=FixedRandom(1.0),
        typical_packet_bytes=1000.0,
        rate_bps=1_000_000.0,
        clock=FakeClock(),
    )
    options.update(overrides)
    return RED(capacity, **options)


def idle_red(capacity=64, clock=None):
    return make_red(
        capacity=capacity,
        w_q=0.25,
        min_th=60.0,
        max_th=64.0,
        clock=clock or FakeClock(now=100.0),
    )


def fill_and_drain(red, count=20):
    for _ in range(count):
        red.enqueue(packet())
    average = red.average_queue
    while red.length:
        red.dequeue()
    return average


class TestRedInterface(unittest.TestCase):
    def test_implements_queue_interface(self):
        self.assertIsInstance(make_red(), Queue)

    def test_dequeue_always_forwards_in_fifo_order(self):
        red = make_red(capacity=8, min_th=6, max_th=8, w_q=1.0)

        for index in range(3):
            self.assertIs(
                red.enqueue(packet(payload=f"p{index}".encode())),
                EnqueueResult.ACCEPTED,
            )

        for index in range(3):
            dequeued, sojourn, result = red.dequeue()
            self.assertEqual(dequeued.payload, f"p{index}".encode())
            self.assertIs(result, DequeueResult.FORWARD)
            self.assertGreaterEqual(sojourn, 0.0)

        self.assertIsNone(red.dequeue())

    def test_average_queue_starts_at_zero(self):
        self.assertEqual(make_red().average_queue, 0.0)

    def test_parameters_reported(self):
        red = make_red(
            capacity=10,
            w_q=0.1,
            min_th=2.0,
            max_th=6.0,
            max_p=0.25,
            gentle=True,
            mode="mark",
            seed=5,
            rng=None,
        )

        self.assertEqual(red.capacity, 10)
        self.assertEqual(
            red.parameters,
            {
                "w_q": 0.1,
                "min_th": 2.0,
                "max_th": 6.0,
                "max_p": 0.25,
                "gentle": True,
                "mode": "mark",
                "seed": 5,
                "typical_packet_bytes": 1000.0,
                "rate_bps": 1_000_000.0,
                "typical_tx_ms": 1.0,
            },
        )


class TestRedAverage(unittest.TestCase):
    def test_ewma_follows_weighted_equation(self):
        red = make_red(capacity=4, w_q=0.25, min_th=3.0, max_th=4.0)

        expected = []
        average = 0.0
        for length in range(4):
            average = 0.75 * average + 0.25 * length
            expected.append(average)
            self.assertIs(red.enqueue(packet()), EnqueueResult.ACCEPTED)
            self.assertAlmostEqual(red.average_queue, expected[-1])

        self.assertEqual(expected, [0.0, 0.25, 0.6875, 1.265625])

        red.dequeue()
        red.enqueue(packet())
        self.assertAlmostEqual(
            red.average_queue, 0.75 * 1.265625 + 0.25 * 3
        )

    def test_capacity_overflow_does_not_update_average(self):
        rng = FixedRandom(1.0)
        red = make_red(capacity=2, w_q=1.0, min_th=1.5, max_th=2.0, rng=rng)

        red.enqueue(packet())
        red.enqueue(packet())
        self.assertEqual(red.average_queue, 1.0)

        self.assertIs(red.enqueue(packet()), EnqueueResult.DROPPED)
        self.assertEqual(red.average_queue, 1.0)
        self.assertEqual(rng.calls, 0)

    def test_below_min_threshold_never_signals(self):
        rng = FixedRandom(0.0)
        red = make_red(capacity=4, w_q=0.25, min_th=3.0, max_th=4.0, rng=rng)

        for _ in range(4):
            self.assertIs(red.enqueue(packet()), EnqueueResult.ACCEPTED)

        self.assertEqual(red.average_queue, 1.265625)
        self.assertEqual(red.drops, 0)
        self.assertEqual(red.marks, 0)
        self.assertEqual(rng.calls, 0)


class TestRedProbability(unittest.TestCase):
    def test_ramp_and_boundaries(self):
        red = make_red(
            capacity=100, min_th=10.0, max_th=30.0, max_p=0.5
        )

        self.assertEqual(red.signal_probability(9.99), 0.0)
        self.assertEqual(red.signal_probability(10.0), 0.0)
        self.assertAlmostEqual(red.signal_probability(11.0), 0.025)
        self.assertAlmostEqual(red.signal_probability(20.0), 0.25)
        self.assertAlmostEqual(red.signal_probability(29.0), 0.475)
        self.assertEqual(red.signal_probability(30.0), 1.0)
        self.assertEqual(red.signal_probability(40.0), 1.0)

    def test_probability_increases_linearly_between_thresholds(self):
        red = make_red(
            capacity=100, min_th=10.0, max_th=30.0, max_p=0.5
        )

        self.assertLess(
            red.signal_probability(12.0),
            red.signal_probability(18.0),
        )
        self.assertLess(
            red.signal_probability(18.0),
            red.signal_probability(28.0),
        )
        midpoint = (
            red.signal_probability(14.0) + red.signal_probability(26.0)
        ) / 2
        self.assertAlmostEqual(midpoint, red.signal_probability(20.0))

    def test_gentle_ramp_reaches_certainty_at_twice_max(self):
        red = make_red(
            capacity=100,
            min_th=10.0,
            max_th=20.0,
            max_p=0.4,
            gentle=True,
        )

        self.assertAlmostEqual(red.signal_probability(20.0), 0.4)
        self.assertAlmostEqual(red.signal_probability(25.0), 0.55)
        self.assertAlmostEqual(red.signal_probability(30.0), 0.7)
        self.assertEqual(red.signal_probability(40.0), 1.0)
        self.assertEqual(red.signal_probability(100.0), 1.0)
        self.assertLess(
            red.signal_probability(21.0),
            red.signal_probability(29.0),
        )

    def test_non_gentle_signals_every_arrival_above_max(self):
        rng = FixedRandom(0.99)
        red = make_red(
            capacity=16,
            w_q=1.0,
            min_th=3.0,
            max_th=5.0,
            max_p=0.05,
            rng=rng,
        )

        for _ in range(5):
            self.assertIs(red.enqueue(packet()), EnqueueResult.ACCEPTED)
        self.assertEqual(red.average_queue, 4.0)

        for _ in range(12):
            self.assertIs(red.enqueue(packet()), EnqueueResult.DROPPED)

        self.assertEqual(red.drops, 12)
        self.assertEqual(red.length, 5)

    def test_gentle_spreads_signalling_above_max(self):
        rng = FixedRandom(0.99)
        red = make_red(
            capacity=16,
            w_q=1.0,
            min_th=3.0,
            max_th=5.0,
            max_p=0.05,
            gentle=True,
            rng=rng,
        )

        for _ in range(6):
            self.assertIs(red.enqueue(packet()), EnqueueResult.ACCEPTED)

        outcomes = []
        for _ in range(12):
            result = red.enqueue(packet())
            outcomes.append(
                "A" if result is EnqueueResult.ACCEPTED else "D"
            )
            if result is EnqueueResult.ACCEPTED:
                red.dequeue()

        self.assertEqual("".join(outcomes), "ADAAADAAADAA")
        self.assertEqual(red.drops, 3)


class TestRedDecisionModes(unittest.TestCase):
    def _saturated_config(self, mode, rng=None):
        return make_red(
            capacity=4,
            w_q=1.0,
            min_th=2.0,
            max_th=3.0,
            max_p=0.5,
            mode=mode,
            rng=rng or FixedRandom(1.0),
        )

    def test_drop_mode_rejects_signalled_arrival(self):
        rng = FixedRandom(1.0)
        red = self._saturated_config("drop", rng=rng)

        for _ in range(3):
            red.enqueue(packet())
        self.assertEqual(red.length, 3)

        self.assertIs(red.enqueue(packet(b"late")), EnqueueResult.DROPPED)
        self.assertEqual(red.length, 3)
        self.assertEqual(red.drops, 1)
        self.assertEqual(red.marks, 0)
        self.assertEqual(rng.calls, 0)

        for index in range(3):
            dequeued, _sojourn, _result = red.dequeue()
            self.assertNotEqual(dequeued.payload, b"late")

    def test_mark_mode_retains_signalled_arrival(self):
        red = self._saturated_config("mark")

        for _ in range(3):
            red.enqueue(packet())

        self.assertIs(red.enqueue(packet(b"marked")), EnqueueResult.MARKED)
        self.assertEqual(red.length, 4)
        self.assertEqual(red.marks, 1)
        self.assertEqual(red.drops, 0)

        payloads = []
        while red.length:
            dequeued, _sojourn, _result = red.dequeue()
            payloads.append(dequeued.payload)
        self.assertEqual(payloads[-1], b"marked")

    def test_mark_mode_still_drops_at_capacity(self):
        red = self._saturated_config("mark")

        for _ in range(3):
            red.enqueue(packet())
        red.enqueue(packet())
        self.assertEqual(red.marks, 1)
        self.assertEqual(red.length, 4)

        self.assertIs(red.enqueue(packet(b"overflow")), EnqueueResult.DROPPED)
        self.assertEqual(red.drops, 1)
        self.assertEqual(red.length, 4)


class TestRedReproducibility(unittest.TestCase):
    def _drive(self, red, arrivals):
        return [
            red.enqueue(packet(payload=bytes([index % 256])))
            for index in range(arrivals)
        ]

    def test_seeded_runs_are_identical(self):
        first = make_red(
            capacity=16,
            w_q=1.0,
            min_th=8.0,
            max_th=12.0,
            max_p=0.4,
            rng=None,
            seed=42,
        )
        second = make_red(
            capacity=16,
            w_q=1.0,
            min_th=8.0,
            max_th=12.0,
            max_p=0.4,
            rng=None,
            seed=42,
        )

        first_outcomes = self._drive(first, 200)
        second_outcomes = self._drive(second, 200)

        self.assertEqual(first_outcomes, second_outcomes)
        self.assertIn(EnqueueResult.DROPPED, first_outcomes)
        self.assertIn(EnqueueResult.ACCEPTED, first_outcomes)

    def test_scripted_rng_episode_follows_count_adjustment(self):
        rng = FixedRandom(0.5)
        red = make_red(
            capacity=8,
            w_q=1.0,
            min_th=4.0,
            max_th=8.0,
            max_p=0.5,
            rng=rng,
        )

        for _ in range(5):
            self.assertIs(red.enqueue(packet()), EnqueueResult.ACCEPTED)
        self.assertEqual(red.average_queue, 4.0)
        self.assertEqual(rng.calls, 0)

        outcomes = []
        for _ in range(15):
            result = red.enqueue(packet())
            outcomes.append(
                "A" if result is EnqueueResult.ACCEPTED else "D"
            )
            if result is EnqueueResult.ACCEPTED:
                red.dequeue()

        self.assertEqual("".join(outcomes), "AAAAAADAAAAAADA")
        self.assertEqual(red.drops, 2)
        self.assertEqual(red.marks, 0)
        self.assertEqual(rng.calls, 15)
        self.assertEqual(red._count, 1)


class TestRedCapacity(unittest.TestCase):
    def test_full_queue_drops_and_never_exceeds_capacity(self):
        red = make_red(
            capacity=8,
            w_q=1.0,
            min_th=7.0,
            max_th=8.0,
            rng=FixedRandom(1.0),
        )

        for _ in range(8):
            self.assertIs(red.enqueue(packet()), EnqueueResult.ACCEPTED)
        self.assertEqual(red.length, 8)

        for index in range(20):
            self.assertIs(
                red.enqueue(packet()), EnqueueResult.DROPPED
            )
            self.assertLessEqual(red.length, red.capacity)

        self.assertEqual(red.drops, 20)

    def test_length_never_exceeds_capacity_under_flood(self):
        red = make_red(
            capacity=8,
            w_q=1.0,
            min_th=6.0,
            max_th=8.0,
            rng=FixedRandom(0.999999),
        )

        accepted = 0
        for _ in range(100):
            result = red.enqueue(packet())
            self.assertLessEqual(red.length, red.capacity)
            if result is EnqueueResult.ACCEPTED:
                accepted += 1

        self.assertEqual(red.length, accepted)
        self.assertEqual(accepted + red.drops, 100)


class TestRedSojourn(unittest.TestCase):
    def test_sojourn_measured_at_dequeue(self):
        clock = FakeClock(now=500.0)
        red = make_red(capacity=8, min_th=6, max_th=8, clock=clock)

        red.enqueue(packet(b"first"))
        clock.advance(0.1)
        red.enqueue(packet(b"second"))
        clock.advance(0.2)

        _packet, sojourn_first, _result = red.dequeue()
        self.assertAlmostEqual(sojourn_first, 0.3)

        clock.advance(0.4)
        _packet, sojourn_second, _result = red.dequeue()
        self.assertAlmostEqual(sojourn_second, 0.6)


class TestRedIdleDecay(unittest.TestCase):
    def test_zero_idle_duration_keeps_average_unchanged(self):
        clock = FakeClock(now=100.0)
        red = idle_red(clock=clock)
        average = fill_and_drain(red)

        clock.advance(0.0)
        red.enqueue(packet())

        self.assertAlmostEqual(red.average_queue, average)

    def test_increasing_idle_duration_decays_progressively(self):
        values = []
        for idle_ms in (0.0, 1.0, 2.0, 5.0, 10.0):
            clock = FakeClock(now=100.0)
            red = idle_red(clock=clock)
            fill_and_drain(red)
            clock.advance(idle_ms / 1000.0)
            red.enqueue(packet())
            values.append(red.average_queue)

        for first, second in zip(values, values[1:]):
            self.assertGreater(first, second)

    def test_long_idle_period_decays_average_substantially(self):
        clock = FakeClock(now=100.0)
        red = idle_red(clock=clock)
        average = fill_and_drain(red)

        clock.advance(10.0)
        red.enqueue(packet())

        self.assertLess(red.average_queue, 1e-6)
        self.assertLess(red.average_queue, average * 1e-6)

    def test_decay_follows_exponential_formula(self):
        for idle_ms in (3.0, 2.5, 7.0):
            clock = FakeClock(now=100.0)
            red = idle_red(clock=clock)
            average = fill_and_drain(red)

            idle_seconds = idle_ms / 1000.0
            clock.advance(idle_seconds)
            red.enqueue(packet())

            slots = idle_seconds / (1000.0 / 1_000_000.0)
            expected = average * 0.75 ** slots
            self.assertAlmostEqual(
                red.average_queue, expected, places=9
            )

    def test_nonempty_arrivals_still_follow_ewma(self):
        clock = FakeClock(now=100.0)
        red = idle_red(clock=clock)
        fill_and_drain(red)

        clock.advance(0.005)
        red.enqueue(packet())
        decayed = red.average_queue

        red.enqueue(packet())
        self.assertAlmostEqual(
            red.average_queue, 0.75 * decayed + 0.25 * 1
        )

    def test_repeated_drops_while_empty_do_not_double_count(self):
        clock = FakeClock(now=100.0)
        red = make_red(
            capacity=64,
            w_q=0.25,
            min_th=2.0,
            max_th=4.0,
            max_p=0.0001,
            rng=FixedRandom(0.0),
            clock=clock,
        )

        for _ in range(20):
            red.enqueue(packet())
        average = red.average_queue
        self.assertGreaterEqual(average, 4.0)
        while red.length:
            red.dequeue()

        clock.advance(0.002)
        self.assertIs(red.enqueue(packet()), EnqueueResult.DROPPED)
        self.assertAlmostEqual(red.average_queue, average * 0.75 ** 2)

        clock.advance(0.001)
        self.assertIs(red.enqueue(packet()), EnqueueResult.DROPPED)
        self.assertAlmostEqual(
            red.average_queue, average * 0.75 ** 3, places=9
        )
        self.assertNotAlmostEqual(
            red.average_queue, average * 0.75 ** 5, places=6
        )
        self.assertEqual(red.length, 0)

    def test_transitions_reset_the_idle_timer(self):
        clock = FakeClock(now=100.0)
        red = idle_red(clock=clock)
        average = fill_and_drain(red)

        clock.advance(0.004)
        red.enqueue(packet())
        self.assertAlmostEqual(red.average_queue, average * 0.75 ** 4)

        clock.advance(0.001)
        red.enqueue(packet())
        after_second = 0.75 * (average * 0.75 ** 4) + 0.25 * 1
        self.assertAlmostEqual(red.average_queue, after_second)

        red.dequeue()
        red.dequeue()
        self.assertEqual(red.length, 0)

        clock.advance(0.002)
        red.enqueue(packet())
        self.assertAlmostEqual(
            red.average_queue, 0.75 ** 2 * after_second
        )


class TestRedIdleConfiguration(unittest.TestCase):
    def test_default_packet_bytes_match_transport_data_datagram(self):
        datagram = Packet(
            packet_type=DATA, payload=b"x" * CHUNK_SIZE
        ).pack()

        self.assertEqual(len(datagram), DEFAULT_TYPICAL_PACKET_BYTES)

    def test_default_rate_matches_project_bottleneck_rate(self):
        self.assertEqual(DEFAULT_RATE_BPS, 5.0 * 1_000_000 / 8)

    def test_default_typical_tx_seconds_derived_from_defaults(self):
        red = RED(16, w_q=1.0, min_th=4.0, max_th=8.0)

        self.assertEqual(
            red.parameters["typical_packet_bytes"],
            DEFAULT_TYPICAL_PACKET_BYTES,
        )
        self.assertEqual(red.parameters["rate_bps"], DEFAULT_RATE_BPS)
        self.assertAlmostEqual(
            red.parameters["typical_tx_ms"],
            1029.0 / 625_000.0 * 1000.0,
        )


class TestRedValidation(unittest.TestCase):
    def test_invalid_capacity_rejected(self):
        for bad_capacity in (0, -5, 2.5, True, "10", None):
            with self.assertRaises(ValueError):
                RED(bad_capacity)

    def test_invalid_w_q_rejected(self):
        for bad in (0, -0.1, 1.1, 2, float("nan"), float("inf"),
                    True, "0.5", None):
            with self.assertRaises(ValueError):
                make_red(w_q=bad)

    def test_invalid_thresholds_rejected(self):
        for bad_min in (0, -1, 0.5, float("nan"), float("inf"), True,
                        "4", None):
            with self.assertRaises(ValueError):
                make_red(min_th=bad_min)

        for bad_max in (4, 3, float("nan"), float("inf"), True, "8",
                        None):
            with self.assertRaises(ValueError):
                make_red(min_th=4.0, max_th=bad_max)

    def test_thresholds_above_capacity_rejected(self):
        with self.assertRaises(ValueError):
            make_red(capacity=5, min_th=4.0, max_th=6.0)

        with self.assertRaises(ValueError):
            RED(5, min_th=4.0, max_th=6.0)

    def test_invalid_max_p_rejected(self):
        for bad in (0, -0.1, 1.5, float("nan"), float("inf"), True,
                    "0.1", None):
            with self.assertRaises(ValueError):
                make_red(max_p=bad)

    def test_invalid_gentle_rejected(self):
        for bad in (1, 0, "yes", None):
            with self.assertRaises(ValueError):
                make_red(gentle=bad)

    def test_invalid_idle_decay_parameters_rejected(self):
        for bad in (0, -1, float("nan"), float("inf"), True, "x", None):
            with self.assertRaises(ValueError):
                make_red(rate_bps=bad)
            with self.assertRaises(ValueError):
                make_red(typical_packet_bytes=bad)

    def test_invalid_mode_rejected(self):
        for bad in ("bogus", "DROP", "", None):
            with self.assertRaises(ValueError):
                make_red(mode=bad)

    def test_invalid_seed_and_rng_rejected(self):
        for bad in (1.5, True, "42"):
            with self.assertRaises(ValueError):
                make_red(seed=bad)

        with self.assertRaises(ValueError):
            make_red(seed=1, rng=FixedRandom(0.5))

        with self.assertRaises(ValueError):
            make_red(rng=object())

    def test_valid_edge_values_accepted(self):
        red = make_red(capacity=4, w_q=1.0, min_th=1.0, max_th=4.0,
                       max_p=1.0)
        self.assertEqual(red.capacity, 4)

        red = make_red(capacity=100, w_q=0.000001, min_th=99.0,
                       max_th=100.0, max_p=0.0001, gentle=True,
                       mode="mark", seed=0, rng=None)
        self.assertEqual(red.parameters["max_th"], 100.0)


if __name__ == "__main__":
    unittest.main()
