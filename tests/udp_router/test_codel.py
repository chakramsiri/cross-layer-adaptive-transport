"""Deterministic unit tests for the CoDel queue."""

import math
import unittest

from router.queues.base import (
    DequeueResult,
    EnqueueResult,
    Queue,
    QueuedPacket,
)
from router.queues.codel import (
    DEFAULT_INTERVAL_SECONDS,
    DEFAULT_MAX_PACKET_BYTES,
    DEFAULT_TARGET_SECONDS,
    CoDel,
)


class FakeClock:
    """Controllable monotonic clock starting at zero."""

    def __init__(self, now=0.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def packet(payload=b"x" * 100, source=("10.0.0.1", 5000)):
    return QueuedPacket(payload=payload, source=source)


def make_codel(capacity=32, clock=None, **overrides):
    options = dict(
        target_seconds=0.005,
        interval_seconds=0.1,
        max_packet_bytes=0.0,
        clock=clock or FakeClock(),
    )
    options.update(overrides)
    return CoDel(capacity, **options)


def chain_from(entry_drop_next, last_count):
    expected = entry_drop_next
    for count in range(2, last_count + 1):
        expected += 0.1 / math.sqrt(count)
    return expected


class TestCoDelInterface(unittest.TestCase):
    def test_implements_queue_interface(self):
        self.assertIsInstance(make_codel(), Queue)

    def test_defaults_match_rfc_8289(self):
        self.assertEqual(DEFAULT_TARGET_SECONDS, 0.005)
        self.assertEqual(DEFAULT_INTERVAL_SECONDS, 0.1)
        self.assertEqual(DEFAULT_MAX_PACKET_BYTES, 512.0)

        codel = CoDel(8)
        self.assertEqual(
            codel.parameters,
            {
                "target_ms": 5.0,
                "interval_ms": 100.0,
                "mode": "drop",
                "max_packet_bytes": 512.0,
            },
        )

    def test_parameters_reported(self):
        codel = CoDel(
            10,
            target_seconds=0.01,
            interval_seconds=0.2,
            mode="mark",
            max_packet_bytes=1000,
        )
        self.assertEqual(
            codel.parameters,
            {
                "target_ms": 10.0,
                "interval_ms": 200.0,
                "mode": "mark",
                "max_packet_bytes": 1000.0,
            },
        )

    def test_fifo_order_and_per_packet_sojourn(self):
        clock = FakeClock(now=100.0)
        codel = make_codel(clock=clock)

        codel.enqueue(packet(b"first"))
        clock.advance(0.1)
        codel.enqueue(packet(b"second"))
        clock.advance(0.2)

        dequeued, sojourn_first, result = codel.dequeue()
        self.assertEqual(dequeued.payload, b"first")
        self.assertAlmostEqual(sojourn_first, 0.3)
        self.assertIs(result, DequeueResult.FORWARD)

        clock.advance(0.1)
        dequeued, sojourn_second, result = codel.dequeue()
        self.assertEqual(dequeued.payload, b"second")
        self.assertAlmostEqual(sojourn_second, 0.3)
        self.assertIs(result, DequeueResult.FORWARD)

        self.assertIsNone(codel.dequeue())


class TestCoDelAboveTargetGate(unittest.TestCase):
    def test_low_sojourn_never_drops(self):
        clock = FakeClock()
        codel = make_codel(clock=clock)

        for _ in range(3):
            codel.enqueue(packet())

        clock.advance(0.001)
        for _ in range(2):
            _packet, _sojourn, result = codel.dequeue()
            self.assertIs(result, DequeueResult.FORWARD)

        self.assertFalse(codel._dropping)
        self.assertEqual(codel.drops, 0)
        self.assertEqual(codel.marks, 0)

    def test_above_target_for_full_interval_before_entering(self):
        clock = FakeClock()
        codel = make_codel(clock=clock)

        for _ in range(4):
            codel.enqueue(packet())

        clock.now = 0.05
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.FORWARD)
        self.assertAlmostEqual(codel._first_above_time, 0.15)

        codel.enqueue(packet())
        clock.now = 0.14
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.FORWARD)
        self.assertFalse(codel._dropping)

        clock.now = 0.16
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.DROP)
        self.assertTrue(codel._dropping)
        self.assertEqual(codel._count, 1)
        self.assertAlmostEqual(codel._drop_next, 0.26)

    def test_exempt_packet_after_entry_is_forwarded(self):
        clock = FakeClock()
        codel = make_codel(clock=clock)

        for _ in range(4):
            codel.enqueue(packet())

        clock.now = 0.05
        codel.dequeue()
        clock.now = 0.16
        codel.dequeue()

        codel.enqueue(packet())
        clock.now = 0.16
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.FORWARD)
        self.assertTrue(codel._dropping)
        self.assertEqual(codel.drops, 1)
        self.assertAlmostEqual(codel._first_above_time, 0.15)

    def test_empty_queue_resets_timer(self):
        clock = FakeClock()
        codel = make_codel(clock=clock)

        codel.enqueue(packet())
        codel.enqueue(packet())
        clock.now = 0.2
        codel.dequeue()
        self.assertAlmostEqual(codel._first_above_time, 0.3)

        codel.dequeue()
        self.assertEqual(codel._first_above_time, 0.0)

        self.assertIsNone(codel.dequeue())
        self.assertEqual(codel._first_above_time, 0.0)

        for _ in range(3):
            codel.enqueue(packet())
        clock.now = 0.25
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.FORWARD)
        self.assertAlmostEqual(codel._first_above_time, 0.35)

        clock.now = 0.36
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.DROP)

    def test_sojourn_below_target_exits_dropping_state(self):
        clock = FakeClock()
        codel = make_codel(clock=clock)

        for _ in range(4):
            codel.enqueue(packet())

        clock.now = 0.05
        codel.dequeue()
        clock.now = 0.16
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.DROP)
        self.assertTrue(codel._dropping)

        clock.now = 0.16
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.FORWARD)
        self.assertTrue(codel._dropping)

        codel.enqueue(packet(b"w"))
        codel.enqueue(packet(b"z"))
        clock.now = 0.161
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.FORWARD)
        self.assertTrue(codel._dropping)

        clock.now = 0.162
        dequeued, sojourn, result = codel.dequeue()
        self.assertEqual(dequeued.payload, b"w")
        self.assertLess(sojourn, 0.005)
        self.assertIs(result, DequeueResult.FORWARD)
        self.assertFalse(codel._dropping)
        self.assertEqual(codel._first_above_time, 0.0)
        self.assertEqual(codel._count, 1)

    def test_empty_dequeue_clears_dropping_state(self):
        clock = FakeClock()
        codel = make_codel(clock=clock)

        for _ in range(3):
            codel.enqueue(packet())

        clock.now = 0.05
        codel.dequeue()
        clock.now = 0.16
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.DROP)
        self.assertTrue(codel._exempt_next)

        clock.now = 0.16
        codel.dequeue()
        self.assertEqual(codel.length, 0)

        self.assertIsNone(codel.dequeue())
        self.assertFalse(codel._dropping)
        self.assertEqual(codel._count, 1)


class TestCoDelControlLaw(unittest.TestCase):
    def _play_to_entry(self, codel, clock):
        for _ in range(4):
            codel.enqueue(packet())
        clock.now = 0.05
        codel.dequeue()
        clock.now = 0.14
        codel.dequeue()
        clock.now = 0.16
        return codel.dequeue()

    def test_entry_drop_schedules_next_drop_by_control_law(self):
        clock = FakeClock()
        codel = make_codel(clock=clock)

        _packet, _sojourn, result = self._play_to_entry(codel, clock)
        self.assertIs(result, DequeueResult.DROP)
        self.assertEqual(codel._count, 1)
        self.assertAlmostEqual(
            codel._drop_next, 0.16 + 0.1 / math.sqrt(1)
        )

    def test_repeated_drops_follow_control_law_chain(self):
        clock = FakeClock()
        codel = make_codel(clock=clock)

        self._play_to_entry(codel, clock)
        codel.enqueue(packet())
        clock.now = 0.16
        self.assertIs(codel.dequeue()[2], DequeueResult.FORWARD)

        codel.enqueue(packet())
        clock.now = 0.27
        self.assertIs(codel.dequeue()[2], DequeueResult.DROP)
        self.assertEqual(codel._count, 2)
        self.assertAlmostEqual(codel._drop_next, 0.26)

        codel.enqueue(packet())
        clock.now = 0.27
        self.assertIs(codel.dequeue()[2], DequeueResult.FORWARD)
        self.assertAlmostEqual(codel._drop_next, 0.330710678)

        codel.enqueue(packet())
        clock.now = 0.34
        self.assertIs(codel.dequeue()[2], DequeueResult.DROP)
        self.assertEqual(codel._count, 3)

        codel.enqueue(packet())
        clock.now = 0.34
        self.assertIs(codel.dequeue()[2], DequeueResult.FORWARD)
        self.assertAlmostEqual(codel._drop_next, 0.388445705)

        codel.enqueue(packet())
        clock.now = 0.42
        self.assertIs(codel.dequeue()[2], DequeueResult.DROP)
        self.assertEqual(codel._count, 4)

        codel.enqueue(packet())
        clock.now = 0.42
        self.assertIs(codel.dequeue()[2], DequeueResult.FORWARD)
        self.assertAlmostEqual(codel._drop_next, 0.438445705)

        codel.enqueue(packet())
        clock.now = 0.44
        self.assertIs(codel.dequeue()[2], DequeueResult.DROP)
        self.assertEqual(codel._count, 5)

        codel.enqueue(packet())
        clock.now = 0.44
        self.assertIs(codel.dequeue()[2], DequeueResult.FORWARD)
        self.assertAlmostEqual(codel._drop_next, 0.483167065)
        self.assertAlmostEqual(codel._drop_next, chain_from(0.26, 5))
        self.assertEqual(codel.drops, 5)
        self.assertEqual(codel.marks, 0)

    def test_reentry_resumes_recent_control_level(self):
        clock = FakeClock()
        codel = make_codel(clock=clock)

        for _ in range(6):
            codel.enqueue(packet())

        clock.now = 0.05
        codel.dequeue()
        clock.now = 0.16
        self.assertIs(codel.dequeue()[2], DequeueResult.DROP)

        codel.enqueue(packet())
        clock.now = 0.16
        codel.dequeue()
        codel.enqueue(packet())
        clock.now = 0.27
        self.assertIs(codel.dequeue()[2], DequeueResult.DROP)
        codel.enqueue(packet())
        clock.now = 0.27
        codel.dequeue()
        codel.enqueue(packet())
        clock.now = 0.34
        self.assertIs(codel.dequeue()[2], DequeueResult.DROP)
        self.assertEqual(codel._count, 3)
        self.assertEqual(codel._lastcount, 1)

        codel.enqueue(packet())
        clock.now = 0.34
        codel.dequeue()

        clock.now = 0.35
        for _ in range(codel.length):
            self.assertIs(codel.dequeue()[2], DequeueResult.FORWARD)

        self.assertIsNone(codel.dequeue())
        self.assertFalse(codel._dropping)

        for _ in range(4):
            codel.enqueue(packet())

        clock.now = 0.47
        self.assertIs(codel.dequeue()[2], DequeueResult.FORWARD)
        self.assertAlmostEqual(codel._first_above_time, 0.57)

        clock.now = 0.58
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.DROP)
        self.assertEqual(codel._count, 2)
        self.assertEqual(codel._lastcount, 2)
        self.assertAlmostEqual(codel._drop_next, 0.650710678)


class TestCoDelModes(unittest.TestCase):
    def _dropped_payload_flow(self, mode):
        clock = FakeClock()
        codel = make_codel(clock=clock, mode=mode)

        payloads = [bytes([index]) * 100 for index in range(1, 5)]
        for payload in payloads:
            codel.enqueue(packet(payload=payload))

        returned = []
        clock.now = 0.05
        returned.append(codel.dequeue()[0].payload)
        clock.now = 0.14
        returned.append(codel.dequeue()[0].payload)
        clock.now = 0.16

        signalled = codel.dequeue()
        if signalled[2] is DequeueResult.MARK:
            returned.append(signalled[0].payload)

        codel.enqueue(packet(payload=b"\x05" * 100))
        while codel.length:
            returned.append(codel.dequeue()[0].payload)

        return codel, signalled, payloads, returned

    def test_drop_mode_discards_the_signalled_packet(self):
        codel, signalled, payloads, returned = self._dropped_payload_flow(
            "drop"
        )

        self.assertIs(signalled[2], DequeueResult.DROP)
        self.assertEqual(signalled[0].payload, payloads[2])
        self.assertEqual(codel.drops, 1)
        self.assertEqual(codel.marks, 0)
        self.assertNotIn(payloads[2], returned)
        self.assertEqual(
            returned, [payloads[0], payloads[1], payloads[3], b"\x05" * 100]
        )

    def test_mark_mode_forwards_while_recording_signal(self):
        codel, signalled, payloads, returned = self._dropped_payload_flow(
            "mark"
        )

        self.assertIs(signalled[2], DequeueResult.MARK)
        self.assertEqual(signalled[0].payload, payloads[2])
        self.assertEqual(codel.marks, 1)
        self.assertEqual(codel.drops, 0)
        self.assertEqual(
            returned,
            [payloads[0], payloads[1], payloads[2], payloads[3],
             b"\x05" * 100],
        )


class TestCoDelSafeguards(unittest.TestCase):
    def test_capacity_overflow_drops_without_signalling(self):
        codel = make_codel(capacity=2)

        codel.enqueue(packet())
        codel.enqueue(packet())
        self.assertIs(codel.enqueue(packet()), EnqueueResult.DROPPED)

        self.assertEqual(codel.length, 2)
        self.assertEqual(codel.drops, 1)
        self.assertFalse(codel._dropping)

    def test_safeguard_suppresses_drop_below_one_mtu(self):
        clock = FakeClock()
        codel = make_codel(
            capacity=8,
            clock=clock,
            max_packet_bytes=512.0,
        )

        codel.enqueue(packet(payload=b"y" * 1000))
        clock.now = 0.2

        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.FORWARD)
        self.assertEqual(codel.drops, 0)
        self.assertEqual(codel._first_above_time, 0.0)

    def test_drop_allowed_above_one_mtu_of_backlog(self):
        clock = FakeClock()
        codel = make_codel(
            capacity=8,
            clock=clock,
            max_packet_bytes=512.0,
        )

        for _ in range(3):
            codel.enqueue(packet(payload=b"y" * 1000))

        clock.now = 0.05
        codel.dequeue()
        self.assertAlmostEqual(codel._first_above_time, 0.15)

        clock.now = 0.16
        _packet, _sojourn, result = codel.dequeue()
        self.assertIs(result, DequeueResult.DROP)

    def test_length_never_exceeds_capacity_under_flood(self):
        codel = make_codel(capacity=8)

        accepted = 0
        for _ in range(100):
            result = codel.enqueue(packet())
            self.assertLessEqual(codel.length, codel.capacity)
            if result is EnqueueResult.ACCEPTED:
                accepted += 1

        self.assertEqual(codel.length, accepted)
        self.assertEqual(accepted + codel.drops, 100)


class TestCoDelRobustness(unittest.TestCase):
    def test_one_dequeue_call_makes_at_most_one_decision(self):
        clock = FakeClock()
        codel = make_codel(capacity=16, clock=clock)

        for _ in range(10):
            codel.enqueue(packet())

        clock.now = 0.05
        codel.dequeue()
        clock.now = 0.16
        self.assertIs(codel.dequeue()[2], DequeueResult.DROP)

        clock.now = 5.0
        self.assertIs(codel.dequeue()[2], DequeueResult.FORWARD)
        drops_before = codel.drops
        length_before = codel.length

        for step in range(6):
            _packet, _sojourn, result = codel.dequeue()
            self.assertIs(result, DequeueResult.DROP)
            self.assertEqual(codel.drops, drops_before + step + 1)
            self.assertEqual(codel.length, length_before - step - 1)

        self.assertEqual(codel._count, 7)

    def test_empty_queue_repeated_dequeue_is_safe(self):
        codel = make_codel()

        for _ in range(5):
            self.assertIsNone(codel.dequeue())

        self.assertFalse(codel._dropping)
        self.assertEqual(codel.drops, 0)

    def test_sojourn_never_negative(self):
        clock = FakeClock(now=100.0)
        codel = make_codel(clock=clock)

        codel.enqueue(packet())
        clock.now = 50.0

        _packet, sojourn, _result = codel.dequeue()
        self.assertGreaterEqual(sojourn, 0.0)


class TestCoDelValidation(unittest.TestCase):
    def test_invalid_capacity_rejected(self):
        for bad in (0, -5, 2.5, True, "10", None):
            with self.assertRaises(ValueError):
                CoDel(bad)

    def test_invalid_target_rejected(self):
        for bad in (0, -0.001, float("nan"), float("inf"), True,
                    "0.005", None):
            with self.assertRaises(ValueError):
                make_codel(target_seconds=bad)

    def test_invalid_interval_rejected(self):
        for bad in (0, -1, 0.005, 0.004, float("nan"), float("inf"),
                    True, "0.1", None):
            with self.assertRaises(ValueError):
                make_codel(interval_seconds=bad)

    def test_invalid_mode_rejected(self):
        for bad in ("bogus", "DROP", "", None):
            with self.assertRaises(ValueError):
                make_codel(mode=bad)

    def test_invalid_max_packet_bytes_rejected(self):
        for bad in (-1, float("nan"), float("inf"), True, "512", None):
            with self.assertRaises(ValueError):
                make_codel(max_packet_bytes=bad)

    def test_valid_edge_values_accepted(self):
        codel = make_codel(interval_seconds=0.006)
        self.assertAlmostEqual(codel.parameters["interval_ms"], 6.0)

        codel = make_codel(target_seconds=0.000001, mode="mark")
        self.assertEqual(codel.parameters["mode"], "mark")

        codel = make_codel(max_packet_bytes=0)
        self.assertEqual(codel.parameters["max_packet_bytes"], 0.0)


if __name__ == "__main__":
    unittest.main()
