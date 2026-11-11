"""Congestion controller interface"""

from abc import ABC, abstractmethod


class CongestionController(ABC):
    """Pluggable sender-side congestion control.

    The window is carried in packets (MSS units) and is always at least
    one, so a sender using int(cwnd) in-flight packets can always make
    progress.  The sender reports transport events through the callbacks
    and never inspects controller internals.
    """

    @property
    @abstractmethod
    def cwnd(self):
        """Current congestion window, in packets."""

    @property
    def phase(self):
        return None

    @property
    def ssthresh(self):
        return None

    @abstractmethod
    def on_ack(self, acked_data):
        """Report that acked_data packets (SMSS units, fractional for
        partial-sized segments) of previously unacknowledged data were
        cumulatively acknowledged by one incoming ACK."""

    @abstractmethod
    def on_loss(self, flight_size):
        """Report a fast-retransmit loss event (three duplicate ACKs).

        flight_size is the outstanding (sent, unacknowledged) data in
        packets, the same units as cwnd.
        """

    @abstractmethod
    def on_timeout(self, flight_size, segment_id):
        """Report a retransmission timeout.

        flight_size is the outstanding data in packets, the same units
        as cwnd.  segment_id identifies the retransmitted segment, so
        implementations can distinguish the first timeout of a segment
        from subsequent timeouts of the same segment.
        """

    @abstractmethod
    def on_feedback(self, ce_count, q_delay):
        """Report explicit congestion feedback (CE count, queue delay)."""
