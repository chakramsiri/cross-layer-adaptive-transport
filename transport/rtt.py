"""RFC 6298 RTT/RTO estimator and uint32 microsecond timestamps.

Covers RFC 6298 sections 2.1-2.5 (SRTT, RTTVAR, RTO bounds), 3 (Karn's
algorithm, enforced by the sender's sample selection), 4 (granularity)
and 5.5 (exponential backoff).  Wire timestamps are monotonic-clock
microseconds truncated to uint32 (`rtt_seconds()` differences modulo
2**32, so the ~71.6 min wrap is harmless); estimator state is float
seconds with G = 1 ms.  Stamps are the true send time: same-microsecond
sends share one, the sender skips such ambiguous echoes, and a zero
stamp (exact uint32 wraparound) is treated as "no echo".
"""

import time

ALPHA = 1 / 8      # RFC 6298 (2.3) smoothing factor for SRTT
BETA = 1 / 4       # RFC 6298 (2.3) smoothing factor for RTTVAR
K = 4              # RFC 6298 (2.2), (2.3) RTTVAR multiplier

CLOCK_GRANULARITY = 0.001  # G: conservative 1 ms floor for K*RTTVAR near 0

INITIAL_RTO = 1.0  # RFC 6298 (2.1)
MIN_RTO = 1.0      # RFC 6298 (2.4) round RTO up to 1 second
MAX_RTO = 60.0     # RFC 6298 (2.5) the maximum must be at least 60 seconds

UINT32_MASK = 0xFFFFFFFF

# Reserved value meaning "no echo" (carried by SYN-ACK / FIN-ACK).
ZERO_SENTINEL = 0


class TimestampAllocator:
    """Allocator for uint32 microsecond send stamps.

    Returns the true monotonic time: no stamp is ever fabricated later
    than the actual send instant, so two sends within the same
    microsecond share a stamp and the sender skips such ambiguous echoes.
    """

    def encode(self, now_seconds):
        """Return the true uint32 microsecond stamp for *now_seconds*."""

        return int(now_seconds * 1_000_000) & UINT32_MASK


# Module-level allocator backing encode_timestamp(); stamping is stateless,
# so one shared instance is sufficient.
_timestamp_allocator = TimestampAllocator()


def encode_timestamp(now_seconds):
    """Return the true uint32 microsecond stamp for a monotonic time.

    Same-microsecond sends share a stamp; the sender treats such echoes
    as ambiguous and skips them.
    """

    return _timestamp_allocator.encode(now_seconds)


def rtt_seconds(echo_timestamp, now_seconds):
    """RTT in seconds from an echoed uint32 microsecond stamp.

    The subtraction is done modulo 2**32 so a stamp encoded shortly before
    the ~71.6 minute wraparound still yields the correct elapsed time.
    """
    elapsed_us = (int(now_seconds * 1_000_000) - echo_timestamp) & UINT32_MASK
    return elapsed_us / 1_000_000.0


class RttEstimator:
    """SRTT / RTTVAR / RTO per RFC 6298 sections 2 and 5.5.

    Pure state and arithmetic with no I/O, so it can be unit-tested
    independently of the sender loop (Phase 2 requirement).
    """

    def __init__(self, initial_rto=INITIAL_RTO):
        self.srtt = None
        self.rttvar = None
        self.rto = initial_rto

    @property
    def has_sample(self):
        return self.srtt is not None

    def update(self, rtt):
        """Fold one RTT sample into SRTT/RTTVAR and recompute RTO.

        First sample sets SRTT <- R and RTTVAR <- R/2 (RFC 6298 2.2); later
        samples update RTTVAR first, using the SRTT value from before its
        own update (2.3).  Recomputing RTO also collapses any exponential
        backoff (note under 5.5).
        """

        if self.srtt is None:
            self.srtt = rtt
            self.rttvar = rtt / 2
        else:
            self.rttvar = (
                (1 - BETA) * self.rttvar + BETA * abs(self.srtt - rtt)
            )
            self.srtt = (1 - ALPHA) * self.srtt + ALPHA * rtt

        self.rto = self._compute_rto()

    def backoff(self):
        """Double RTO after a retransmission timeout (RFC 6298 (5.5))."""

        self.rto = min(self.rto * 2, MAX_RTO)

    def _compute_rto(self):
        # RFC 6298 (2.2)/(2.3): RTO <- SRTT + max(G, K*RTTVAR);
        # (2.4) round up to 1 second; (2.5) cap at the chosen maximum.
        rto = self.srtt + max(CLOCK_GRANULARITY, K * self.rttvar)
        return min(max(MIN_RTO, rto), MAX_RTO)
