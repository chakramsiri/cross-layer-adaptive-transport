class AckWindow:
    """Counts acknowledged packets until one congestion window has passed."""

    def __init__(self):
        self._target = 1.0
        self._acked = 0.0

    @property
    def target(self):
        return self._target

    @property
    def acked(self):
        return self._acked

    def open(self, cwnd):
        self._target = max(1.0, float(cwnd))
        self._acked = 0.0

    def acknowledge(self, packets):
        self._acked += packets
        return self._acked >= self._target
