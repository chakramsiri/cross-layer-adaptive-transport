"""Reno-like loss-based congestion control (RFC 5681, Mode A)."""

import math

from transport.cc.base import CongestionController

INITIAL_CWND = 1.0  # packets
INITIAL_SSTHRESH = 8.0  # packets; bounds the slow-start overshoot
MIN_SSTHRESH = 2.0  # packets (2 * SMSS, expressed in packet units)


class RenoController(CongestionController):
    """Slow start, AIMD congestion avoidance, simplified fast recovery.

    ssthresh is recalculated from the sender-reported FlightSize (RFC
    5681): on the third duplicate ACK and on the first timeout of a
    segment, ssthresh = max(FlightSize / 2, 2 * SMSS); further timeouts
    of the same segment retain it, tracked by segment identity rather
    than a flag.  Explicit congestion feedback is ignored: this mode is
    purely loss-based.
    """

    def __init__(self, initial_cwnd=INITIAL_CWND, ssthresh=INITIAL_SSTHRESH):
        if not math.isfinite(initial_cwnd) or initial_cwnd < 1:
            raise ValueError("initial_cwnd must be finite and at least 1")
        if not math.isfinite(ssthresh) and ssthresh != float("inf"):
            raise ValueError("ssthresh must be finite or positive infinity")

        self._cwnd = float(initial_cwnd)
        self._ssthresh = float(ssthresh)
        self.in_fast_recovery = False
        self.in_slow_start = self._cwnd < self._ssthresh
        self._timeout_segment = None

    @property
    def cwnd(self):
        return self._cwnd

    @property
    def phase(self):
        if self.in_fast_recovery:
            return "fast_recovery"
        if self.in_slow_start:
            return "slow_start"
        return "congestion_avoidance"

    @property
    def ssthresh(self):
        return self._ssthresh

    def on_ack(self, acked_data):
        """One incoming ACK acknowledging acked_data packets (SMSS
        units) of new data, per RFC 5681 3.1: at most one SMSS of
        growth per ACK (the min(N, SMSS) byte-counting increment), and
        one additive-increase step per ACK during congestion avoidance.
        """

        self.in_fast_recovery = False

        if self._cwnd < self._ssthresh:
            self.in_slow_start = True
            self._cwnd += min(acked_data, 1.0)
        else:
            self.in_slow_start = False
            self._cwnd += 1.0 / self._cwnd

    def on_loss(self, flight_size):
        if self.in_fast_recovery:
            return

        self._ssthresh = max(flight_size / 2.0, MIN_SSTHRESH)
        self._cwnd = self._ssthresh
        self.in_fast_recovery = True
        self.in_slow_start = False

    def on_timeout(self, flight_size, segment_id):
        if segment_id != self._timeout_segment:
            self._ssthresh = max(flight_size / 2.0, MIN_SSTHRESH)
            self._timeout_segment = segment_id

        self._cwnd = 1.0
        self.in_fast_recovery = False
        self.in_slow_start = True

    def on_feedback(self, ce_count, q_delay):
        pass
