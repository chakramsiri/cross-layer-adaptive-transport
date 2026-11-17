"""DropTail queue: FIFO with packet-count capacity and tail drops."""

import time
from collections import deque

from router.queues.base import (
    DequeueResult,
    EnqueueResult,
    Queue,
    validate_capacity,
)


class DropTail(Queue):
    """Drops arrivals when full and never marks packets."""

    def __init__(self, capacity, clock=time.monotonic):
        self._capacity = validate_capacity(capacity)
        self._clock = clock
        self._packets = deque()
        self.drops = 0
        self.marks = 0

    @property
    def capacity(self):
        return self._capacity

    @property
    def length(self):
        return len(self._packets)

    def enqueue(self, packet):
        if len(self._packets) >= self._capacity:
            self.drops += 1
            return EnqueueResult.DROPPED

        packet.enqueued_at = self._clock()
        self._packets.append(packet)
        return EnqueueResult.ACCEPTED

    def dequeue(self):
        if not self._packets:
            return None

        packet = self._packets.popleft()
        sojourn = max(0.0, self._clock() - packet.enqueued_at)
        return packet, sojourn, DequeueResult.FORWARD
