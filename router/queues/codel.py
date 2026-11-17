"""CoDel queue: RFC 8289 sojourn-time controller with drop/mark signalling."""

import math
import time
from collections import deque

from router.queues.base import (
    DequeueResult,
    EnqueueResult,
    Queue,
    validate_capacity,
)

MODES = ("drop", "mark")

DEFAULT_TARGET_SECONDS = 0.005
DEFAULT_INTERVAL_SECONDS = 0.1
DEFAULT_MAX_PACKET_BYTES = 512.0


class CoDel(Queue):
    """Drops or marks at dequeue using the RFC 8289 state machine."""

    def __init__(
        self,
        capacity,
        target_seconds=DEFAULT_TARGET_SECONDS,
        interval_seconds=DEFAULT_INTERVAL_SECONDS,
        mode="drop",
        max_packet_bytes=DEFAULT_MAX_PACKET_BYTES,
        clock=time.monotonic,
    ):
        capacity = validate_capacity(capacity)

        if (
            isinstance(target_seconds, bool)
            or not isinstance(target_seconds, (int, float))
            or not math.isfinite(target_seconds)
            or target_seconds <= 0.0
        ):
            raise ValueError(
                f"target_seconds must be positive and finite, "
                f"got {target_seconds!r}"
            )

        if (
            isinstance(interval_seconds, bool)
            or not isinstance(interval_seconds, (int, float))
            or not math.isfinite(interval_seconds)
            or interval_seconds <= target_seconds
        ):
            raise ValueError(
                f"interval_seconds must be finite and greater than "
                f"target_seconds, got {interval_seconds!r}"
            )

        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {mode!r}")

        if (
            isinstance(max_packet_bytes, bool)
            or not isinstance(max_packet_bytes, (int, float))
            or not math.isfinite(max_packet_bytes)
            or max_packet_bytes < 0.0
        ):
            raise ValueError(
                f"max_packet_bytes must be non-negative and finite, "
                f"got {max_packet_bytes!r}"
            )

        self._capacity = capacity
        self._clock = clock
        self._packets = deque()
        self._bytes = 0
        self._target = float(target_seconds)
        self._interval = float(interval_seconds)
        self._mode = mode
        self._max_packet_bytes = float(max_packet_bytes)
        self._first_above_time = 0.0
        self._drop_next = 0.0
        self._count = 0
        self._lastcount = 0
        self._dropping = False
        self._exempt_next = False
        self._pending_reschedule = False
        self.drops = 0
        self.marks = 0

    @property
    def capacity(self):
        return self._capacity

    @property
    def length(self):
        return len(self._packets)

    @property
    def parameters(self):
        return {
            "target_ms": self._target * 1000.0,
            "interval_ms": self._interval * 1000.0,
            "mode": self._mode,
            "max_packet_bytes": self._max_packet_bytes,
        }

    def enqueue(self, packet):
        if len(self._packets) >= self._capacity:
            self.drops += 1
            return EnqueueResult.DROPPED

        packet.enqueued_at = self._clock()
        self._packets.append(packet)
        self._bytes += len(packet.payload)
        return EnqueueResult.ACCEPTED

    def dequeue(self):
        if not self._packets:
            self._first_above_time = 0.0
            self._dropping = False
            self._pending_reschedule = False
            return None

        now = self._clock()
        packet = self._packets.popleft()
        self._bytes -= len(packet.payload)
        sojourn = max(0.0, now - packet.enqueued_at)
        ok_to_drop = self._update_first_above_time(now, sojourn)

        if self._exempt_next:
            self._exempt_next = False
            return packet, sojourn, DequeueResult.FORWARD

        if self._dropping:
            if not ok_to_drop:
                self._dropping = False
                self._pending_reschedule = False
                return packet, sojourn, DequeueResult.FORWARD

            if self._pending_reschedule:
                self._drop_next = self._control_law(
                    self._drop_next, self._count
                )
                self._pending_reschedule = False

            if now >= self._drop_next:
                self._count += 1
                self._pending_reschedule = True
                return self._signal(packet, sojourn)

            return packet, sojourn, DequeueResult.FORWARD

        if not ok_to_drop:
            return packet, sojourn, DequeueResult.FORWARD

        self._dropping = True
        delta = self._count - self._lastcount
        self._count = 1

        if delta > 1 and (now - self._drop_next) < 16.0 * self._interval:
            self._count = delta

        self._drop_next = self._control_law(now, self._count)
        self._lastcount = self._count
        self._pending_reschedule = False
        self._exempt_next = bool(self._packets)
        return self._signal(packet, sojourn)

    def _update_first_above_time(self, now, sojourn):
        """True once the sojourn stayed above target for a full interval."""
        if sojourn < self._target or self._bytes <= self._max_packet_bytes:
            self._first_above_time = 0.0
            return False

        if self._first_above_time == 0.0:
            self._first_above_time = now + self._interval
            return False

        return now >= self._first_above_time

    def _control_law(self, base_time, count):
        """RFC 8289 control law: base + interval / sqrt(count)."""
        return base_time + self._interval / math.sqrt(count)

    def _signal(self, packet, sojourn):
        if self._mode == "drop":
            self.drops += 1
            return packet, sojourn, DequeueResult.DROP

        self.marks += 1
        return packet, sojourn, DequeueResult.MARK
