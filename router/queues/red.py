"""RED queue: EWMA average queue length with linear drop/mark signalling."""

import math
import random
import time
from collections import deque

from router.queues.base import (
    DequeueResult,
    EnqueueResult,
    Queue,
    validate_capacity,
)

MODES = ("drop", "mark")

DEFAULT_TYPICAL_PACKET_BYTES = 1029.0
DEFAULT_RATE_BPS = 625_000.0


class RED(Queue):
    """Average sampled at enqueue; capacity overflow never signals."""

    def __init__(
        self,
        capacity,
        w_q=0.002,
        min_th=20.0,
        max_th=80.0,
        max_p=0.1,
        gentle=False,
        mode="drop",
        seed=None,
        rng=None,
        typical_packet_bytes=DEFAULT_TYPICAL_PACKET_BYTES,
        rate_bps=DEFAULT_RATE_BPS,
        clock=time.monotonic,
    ):
        capacity = validate_capacity(capacity)

        if (
            isinstance(w_q, bool)
            or not isinstance(w_q, (int, float))
            or not math.isfinite(w_q)
            or not 0.0 < w_q <= 1.0
        ):
            raise ValueError(f"w_q must be in (0, 1], got {w_q!r}")

        if (
            isinstance(min_th, bool)
            or not isinstance(min_th, (int, float))
            or not math.isfinite(min_th)
            or min_th < 1.0
        ):
            raise ValueError(
                f"min_th must be at least 1 packet, got {min_th!r}"
            )

        if (
            isinstance(max_th, bool)
            or not isinstance(max_th, (int, float))
            or not math.isfinite(max_th)
            or max_th <= min_th
        ):
            raise ValueError(
                f"max_th must be greater than min_th, got {max_th!r}"
            )

        if max_th > capacity:
            raise ValueError(
                f"max_th {max_th!r} exceeds queue capacity {capacity}"
            )

        if (
            isinstance(max_p, bool)
            or not isinstance(max_p, (int, float))
            or not math.isfinite(max_p)
            or not 0.0 < max_p <= 1.0
        ):
            raise ValueError(f"max_p must be in (0, 1], got {max_p!r}")

        if (
            isinstance(typical_packet_bytes, bool)
            or not isinstance(typical_packet_bytes, (int, float))
            or not math.isfinite(typical_packet_bytes)
            or typical_packet_bytes <= 0.0
        ):
            raise ValueError(
                f"typical_packet_bytes must be positive and finite, "
                f"got {typical_packet_bytes!r}"
            )

        if (
            isinstance(rate_bps, bool)
            or not isinstance(rate_bps, (int, float))
            or not math.isfinite(rate_bps)
            or rate_bps <= 0.0
        ):
            raise ValueError(
                f"rate_bps must be positive and finite, got {rate_bps!r}"
            )

        if not isinstance(gentle, bool):
            raise ValueError(f"gentle must be a boolean, got {gentle!r}")

        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}, got {mode!r}")

        if seed is not None and (
            isinstance(seed, bool) or not isinstance(seed, int)
        ):
            raise ValueError(f"seed must be an integer or None, got {seed!r}")

        if rng is not None and seed is not None:
            raise ValueError("pass either rng or seed, not both")

        if rng is not None and not callable(getattr(rng, "random", None)):
            raise ValueError(
                f"rng must provide a random() method, got {rng!r}"
            )

        self._capacity = capacity
        self._clock = clock
        self._packets = deque()
        self._w_q = float(w_q)
        self._min_th = float(min_th)
        self._max_th = float(max_th)
        self._max_p = float(max_p)
        self._gentle = gentle
        self._mode = mode
        self._seed = seed
        self._rng = rng if rng is not None else random.Random(seed)
        self._typical_packet_bytes = float(typical_packet_bytes)
        self._rate_bps = float(rate_bps)
        self._typical_tx_seconds = (
            self._typical_packet_bytes / self._rate_bps
        )
        self._average = 0.0
        self._count = -1
        self._empty_at = None
        self.drops = 0
        self.marks = 0

    @property
    def capacity(self):
        return self._capacity

    @property
    def length(self):
        return len(self._packets)

    @property
    def average_queue(self):
        return self._average

    @property
    def parameters(self):
        return {
            "w_q": self._w_q,
            "min_th": self._min_th,
            "max_th": self._max_th,
            "max_p": self._max_p,
            "gentle": self._gentle,
            "mode": self._mode,
            "seed": self._seed,
            "typical_packet_bytes": self._typical_packet_bytes,
            "rate_bps": self._rate_bps,
            "typical_tx_ms": self._typical_tx_seconds * 1000.0,
        }

    def signal_probability(self, average):
        """RED signalling probability for an average queue length."""
        if average < self._min_th:
            return 0.0

        if average < self._max_th:
            return (
                self._max_p
                * (average - self._min_th)
                / (self._max_th - self._min_th)
            )

        if not self._gentle:
            return 1.0

        if average >= 2.0 * self._max_th:
            return 1.0

        return (
            self._max_p
            + (1.0 - self._max_p)
            * (average - self._max_th)
            / self._max_th
        )

    def enqueue(self, packet):
        if len(self._packets) >= self._capacity:
            self.drops += 1
            return EnqueueResult.DROPPED

        self._update_average()

        if self._signal(self._average):
            if self._mode == "drop":
                self.drops += 1
                return EnqueueResult.DROPPED
            self.marks += 1
            result = EnqueueResult.MARKED
        else:
            result = EnqueueResult.ACCEPTED

        packet.enqueued_at = self._clock()
        self._packets.append(packet)
        self._empty_at = None
        return result

    def dequeue(self):
        if not self._packets:
            return None

        now = self._clock()
        packet = self._packets.popleft()
        sojourn = max(0.0, now - packet.enqueued_at)
        if not self._packets:
            self._empty_at = now
        return packet, sojourn, DequeueResult.FORWARD

    def _update_average(self):
        """Exponential idle decay while empty; EWMA sample while nonempty."""
        if self._packets:
            self._average = (
                (1.0 - self._w_q) * self._average
                + self._w_q * len(self._packets)
            )
            return

        now = self._clock()
        if self._empty_at is not None:
            idle_seconds = max(0.0, now - self._empty_at)
            idle_slots = idle_seconds / self._typical_tx_seconds
            self._average = (1.0 - self._w_q) ** idle_slots * self._average
        self._empty_at = now

    def _signal(self, average):
        if average < self._min_th:
            self._count = -1
            return False

        probability = self.signal_probability(average)

        self._count += 1

        if probability <= 0.0:
            return False

        if probability >= 1.0:
            self._count = 0
            return True

        denominator = 1.0 - self._count * probability

        if denominator <= 0.0:
            self._count = 0
            return True

        if self._rng.random() < probability / denominator:
            self._count = 0
            return True

        return False
