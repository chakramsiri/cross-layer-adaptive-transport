
import unittest

from router.queues.base import EnqueueResult, Queue, QueuedPacket
from router.queues.droptail import DropTail


class FakeClock:
    """Controllable monotonic clock for deterministic sojourn tests."""

    def __init__(self, now=100.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make_queue(capacity=4, clock=None):
    return DropTail(capacity, clock=clock or FakeClock())


def packet(payload=b"data", source=("10.0.0.1", 5000)):
    return QueuedPacket(payload=payload, source=source)


class TestQueueInterface(unittest.TestCase):
    def test_droptail_implements_queue_interface(self):
        self.assertIsInstance(DropTail(4), Queue)

    def test_enqueue_result_values(self):
        self.assertEqual(EnqueueResult.ACCEPTED.value, "accepted")
        self.assertEqual(EnqueueResult.DROPPED.value, "dropped")
        self.assertEqual(EnqueueResult.MARKED.value, "marked")


class TestDropTailBasics(unittest.TestCase):
    def test_empty_dequeue_returns_none(self):
        queue = make_queue()

        self.assertIsNone(queue.dequeue())
        self.assertEqual(queue.length, 0)

    def test_single_packet_roundtrip_preserves_bytes_and_source(self):
        queue = make_queue()
        original = packet(payload=b"payload-bytes", source=("10.0.0.9", 7777))

        self.assertIs(queue.enqueue(original), EnqueueResult.ACCEPTED)
        self.assertEqual(queue.length, 1)

        dequeued, _sojourn = queue.dequeue()
        self.assertEqual(dequeued.payload, b"payload-bytes")
        self.assertEqual(dequeued.source, ("10.0.0.9", 7777))
        self.assertEqual(queue.length, 0)

    def test_fifo_ordering(self):
        queue = make_queue(capacity=8)

        for index in range(5):
            queue.enqueue(packet(payload=f"p{index}".encode()))

        for index in range(5):
            dequeued, _sojourn = queue.dequeue()
            self.assertEqual(dequeued.payload, f"p{index}".encode())

    def test_capacity_counts_waiting_packets(self):
        queue = make_queue(capacity=2)

        queue.enqueue(packet(b"a"))
        queue.enqueue(packet(b"b"))
        self.assertEqual(queue.length, 2)

        queue.dequeue()
        self.assertEqual(queue.length, 1)

        self.assertIs(queue.enqueue(packet(b"c")), EnqueueResult.ACCEPTED)
        self.assertEqual(queue.length, 2)


class TestDropTailDrops(unittest.TestCase):
    def test_full_queue_drops_arrivals(self):
        queue = make_queue(capacity=3)

        for index in range(3):
            self.assertIs(
                queue.enqueue(packet(f"in{index}".encode())),
                EnqueueResult.ACCEPTED,
            )

        self.assertIs(queue.enqueue(packet(b"late")), EnqueueResult.DROPPED)
        self.assertEqual(queue.drops, 1)
        self.assertEqual(queue.length, 3)

    def test_length_never_exceeds_capacity(self):
        queue = make_queue(capacity=3)

        for index in range(100):
            queue.enqueue(packet(f"p{index}".encode()))
            self.assertLessEqual(queue.length, queue.capacity)

        self.assertEqual(queue.length, 3)
        self.assertEqual(queue.drops, 97)

    def test_dropped_packets_are_not_stored(self):
        queue = make_queue(capacity=1)
        queue.enqueue(packet(b"kept"))

        queue.enqueue(packet(b"dropped-1"))
        queue.enqueue(packet(b"dropped-2"))

        dequeued, _sojourn = queue.dequeue()
        self.assertEqual(dequeued.payload, b"kept")
        self.assertIsNone(queue.dequeue())


class TestDropTailTiming(unittest.TestCase):
    def test_enqueue_stamps_monotonic_enqueue_time(self):
        clock = FakeClock(now=500.0)
        queue = make_queue(clock=clock)

        queue.enqueue(packet())
        dequeued, _sojourn = queue.dequeue()
        self.assertEqual(dequeued.enqueued_at, 500.0)

    def test_sojourn_measured_at_dequeue(self):
        clock = FakeClock(now=500.0)
        queue = make_queue(clock=clock)

        queue.enqueue(packet(b"a"))
        clock.advance(0.25)

        _dequeued, sojourn = queue.dequeue()
        self.assertAlmostEqual(sojourn, 0.25)

    def test_sojourn_per_packet(self):
        clock = FakeClock()
        queue = make_queue(clock=clock)

        queue.enqueue(packet(b"first"))
        clock.advance(0.1)
        queue.enqueue(packet(b"second"))

        clock.advance(0.2)
        _packet, sojourn_first = queue.dequeue()
        self.assertAlmostEqual(sojourn_first, 0.3)

        clock.advance(0.4)
        _packet, sojourn_second = queue.dequeue()
        self.assertAlmostEqual(sojourn_second, 0.6)

    def test_sojourn_is_never_negative(self):
        class BackwardsClock(FakeClock):
            def __call__(self):
                return -self.now

        queue = make_queue(clock=BackwardsClock())
        queue.enqueue(packet())

        _packet, sojourn = queue.dequeue()
        self.assertGreaterEqual(sojourn, 0.0)

    def test_default_clock_is_monotonic(self):
        queue = DropTail(2)
        queue.enqueue(packet())

        dequeued, sojourn = queue.dequeue()
        self.assertGreaterEqual(sojourn, 0.0)
        self.assertLess(sojourn, 5.0)


class TestDropTailMarks(unittest.TestCase):
    def test_marks_stay_zero(self):
        queue = make_queue(capacity=2)

        for _ in range(10):
            queue.enqueue(packet())

        while queue.length:
            queue.dequeue()

        self.assertEqual(queue.marks, 0)


class TestDropTailValidation(unittest.TestCase):
    def test_invalid_capacity_rejected(self):
        for bad_capacity in (0, -5, 2.5, True, "10", None):
            with self.assertRaises(ValueError):
                DropTail(bad_capacity)

    def test_capacity_property_reflects_configuration(self):
        self.assertEqual(DropTail(7).capacity, 7)


if __name__ == "__main__":
    unittest.main()
