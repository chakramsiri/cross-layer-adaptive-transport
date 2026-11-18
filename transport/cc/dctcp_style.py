"""DCTCP-style ECN controller (Mode B): alpha-proportional reduction."""

import math

from transport.cc.reno import INITIAL_CWND, INITIAL_SSTHRESH, RenoController
from transport.cc.window import AckWindow

DEFAULT_G = 1 / 4


def _validate_gain(g):
    if (
        isinstance(g, bool)
        or not isinstance(g, (int, float))
        or not math.isfinite(g)
        or not 0.0 < g <= 1.0
    ):
        raise ValueError(f"g must be in (0, 1], got {g!r}")
    return float(g)


class DctcpController(RenoController):
    """Reno growth and loss response, plus once-per-window alpha reduction."""

    name = "dctcp"

    def __init__(self, initial_cwnd=INITIAL_CWND, ssthresh=INITIAL_SSTHRESH,
                 g=DEFAULT_G):
        super().__init__(initial_cwnd=initial_cwnd, ssthresh=ssthresh)
        self._g = _validate_gain(g)
        self._alpha = 0.0
        self._marked = 0
        self._window = AckWindow()
        self._window.open(self._cwnd)

    @property
    def g(self):
        return self._g

    @property
    def alpha(self):
        return self._alpha

    @property
    def window_target(self):
        return self._window.target

    @property
    def window_acked(self):
        return self._window.acked

    def on_ack(self, acked_data):
        super().on_ack(acked_data)
        if self._window.acknowledge(acked_data):
            self._end_window()

    def on_feedback(self, ce_count, q_delay):
        self._marked += ce_count

    def on_loss(self, flight_size):
        super().on_loss(flight_size)
        self._restart_window()

    def on_timeout(self, flight_size, segment_id):
        super().on_timeout(flight_size, segment_id)
        self._restart_window()

    def _restart_window(self):
        self._marked = 0
        self._window.open(self._cwnd)

    def _end_window(self):
        fraction = min(1.0, self._marked / self._window.acked)
        self._alpha = (1.0 - self._g) * self._alpha + self._g * fraction
        if self._marked > 0:
            self._cwnd = max(1.0, self._cwnd * (1.0 - self._alpha / 2.0))
            self._ssthresh = self._cwnd
        self._marked = 0
        self._window.open(self._cwnd)
