"""Generic packet-queue interface for the router: enqueue outcome
and sojourn timing; capacity counts waiting packets only."""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum


def validate_capacity(capacity):
    """Return a validated integer packet capacity."""

    if isinstance(capacity, bool) or not isinstance(capacity, int):
        raise ValueError(
            f"capacity must be an integer number of packets, got {capacity!r}"
        )

    if capacity < 1:
        raise ValueError(
            f"capacity must be at least 1 packet, got {capacity}"
        )

    return capacity


class EnqueueResult(Enum):
    """Outcome of an enqueue attempt."""

    ACCEPTED = "accepted"
    DROPPED = "dropped"
    MARKED = "marked"


class DequeueResult(Enum):
    """Congestion outcome of a dequeue operation."""

    FORWARD = "forward"
    DROP = "drop"
    MARK = "mark"


@dataclass
class QueuedPacket:
    """Datagram awaiting transmission, with source endpoint and enqueue stamp."""

    payload: bytes
    source: tuple
    enqueued_at: float = field(default_factory=time.monotonic)
    marked: bool = False


class Queue(ABC):
    @property
    @abstractmethod
    def capacity(self):
        """Configured maximum number of waiting packets."""

    @property
    @abstractmethod
    def length(self):
        """Current number of waiting packets."""

    @property
    def parameters(self):
        """Configured algorithm parameters recorded with the experiment."""
        return {}

    @property
    def average_queue(self):
        """Current average queue length in packets, or None."""
        return None

    @abstractmethod
    def enqueue(self, packet):
        """Store ``packet`` and return an EnqueueResult."""

    @abstractmethod
    def dequeue(self):
        """Returns (packet, sojourn_seconds, DequeueResult) or None."""
