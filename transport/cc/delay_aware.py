"""Queue-delay-aware controller (Mode C): holds queue delay near a target."""

import math

from transport.cc.reno import INITIAL_CWND, INITIAL_SSTHRESH, RenoController
from transport.cc.window import AckWindow

DEFAULT_TARGET_SECONDS = 0.005
DEFAULT_INCREASE_STEP = 1.0
DELAY_GAIN = 0.5


def _validate_positive(value, name):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0.0
    ):
        raise ValueError(f"{name} must be positive and finite, got {value!r}")
    return float(value)


class DelayAwareController(RenoController):
    """Additive growth below target; a proportional cut above, once per RTT."""

    name = "delay"

    def __init__(self, initial_cwnd=INITIAL_CWND, ssthresh=INITIAL_SSTHRESH,
                 target_seconds=DEFAULT_TARGET_SECONDS,
                 increase_step=DEFAULT_INCREASE_STEP):
        super().__init__(initial_cwnd=initial_cwnd, ssthresh=ssthresh)
        self._target = _validate_positive(target_seconds, "target_seconds")
        self._step = _validate_positive(increase_step, "increase_step")
        self._min_delay = None
        self._congested = False
        self._window = AckWindow()
        self._window.open(self._cwnd)

    @property
    def target_seconds(self):
        return self._target

    @property
    def increase_step(self):
        return self._step

    @property
    def congested(self):
        return self._congested

    @property
    def window_target(self):
        return self._window.target

    @property
    def window_acked(self):
        return self._window.acked

    def on_feedback(self, ce_count, q_delay):
        if self._min_delay is None or q_delay < self._min_delay:
            self._min_delay = q_delay

    def on_ack(self, acked_data):
        self.in_fast_recovery = False
        self._grow(acked_data)
        if self._window.acknowledge(acked_data):
            self._end_window()

    def _grow(self, acked_data):
        if self._cwnd < self._ssthresh:
            self.in_slow_start = True
            if not self._congested:
                self._cwnd += min(acked_data, 1.0)
        else:
            self.in_slow_start = False
            if not self._congested:
                self._cwnd += self._step / self._cwnd

    def _end_window(self):
        delay = self._min_delay
        self._min_delay = None

        if delay is None:
            self._window.open(self._cwnd)
            return

        self._congested = delay > self._target
        if self._congested:
            excess = (delay - self._target) / delay
            self._cwnd = max(1.0, self._cwnd * (1.0 - DELAY_GAIN * excess))
            self._ssthresh = self._cwnd

        self._window.open(self._cwnd)
