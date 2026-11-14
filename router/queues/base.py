"""Generic packet-queue interface for the router: enqueue outcome
and sojourn timing; capacity counts waiting packets only."""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum


class EnqueueResult(Enum):
    """Outcome of an enqueue attempt."""

    ACCEPTED = "accepted"
    DROPPED = "dropped"
    MARKED = "marked"


@dataclass
class QueuedPacket:
    """Datagram awaiting transmission, with source endpoint and enqueue stamp."""

    payload: bytes
    source: tuple
    enqueued_at: float = field(default_factory=time.monotonic)


class Queue(ABC):
    @property
    @abstractmethod
    def capacity(self):
        """Configured maximum number of waiting packets."""

    @property
    @abstractmethod
    def length(self):
        """Current number of waiting packets."""

    @abstractmethod
    def enqueue(self, packet):
        """Store ``packet`` and return an EnqueueResult."""

    @abstractmethod
    def dequeue(self):
        """Remove the oldest packet; returns (packet, sojourn_seconds) or None."""
