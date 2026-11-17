"""Explicit congestion feedback: router CE stamping and echo tracking."""

from transport.packet import CE, DATA, Packet

CE_MODULUS = 1 << 16
MAX_Q_DELAY_US = (1 << 32) - 1
_HALF_CE_MODULUS = CE_MODULUS // 2


def stamp_forwarded_datagram(payload, sojourn_seconds, marked):
    """Write queue delay into a DATA header; set CE when marked."""

    try:
        packet = Packet.unpack(payload)
    except ValueError:
        return payload, False

    if packet.packet_type != DATA:
        return payload, False

    packet.q_delay_us = min(
        MAX_Q_DELAY_US, round(sojourn_seconds * 1_000_000)
    )
    if marked:
        packet.flags |= CE

    return packet.pack(), bool(marked)


class FeedbackTracker:
    """Turns the receiver's cumulative CE echo into per-ACK increments."""

    def __init__(self):
        self.ce_total = 0
        self.q_delay_us = 0
        self._last_count = 0

    def observe(self, ce_count, q_delay_us):
        """Return the CE packets newly echoed, or None for an older echo."""

        step = (ce_count - self._last_count) % CE_MODULUS
        if step >= _HALF_CE_MODULUS:
            return None

        self._last_count = ce_count
        self.ce_total += step
        self.q_delay_us = q_delay_us
        return step
