"""CSV telemetry for router queue events; see FIELDS for the schema.
Counters are cumulative; elapsed_s is monotonic since router start."""

import csv
import threading
import time
from pathlib import Path

FIELDS = [
    "elapsed_s",
    "event",
    "queue_length_packets",
    "sojourn_ms",
    "drops",
    "marks",
    "bytes",
    "forwarded_bytes",
]

EVENTS = ("enqueue", "forward", "drop", "error")


class TelemetryLogger:
    def __init__(self, filename, clock=None):
        self.path = Path(filename)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = time.monotonic if clock is None else clock
        self._start = self._clock()
        self._lock = threading.Lock()
        self.rows_written = 0
        self._file = self.path.open("w", newline="")
        self._writer = csv.DictWriter(self._file, fieldnames=FIELDS)
        self._writer.writeheader()
        self._file.flush()

    def record(
        self,
        event,
        queue_length,
        datagram_bytes,
        drops,
        marks,
        forwarded_bytes,
        sojourn_seconds=None,
    ):
        if event not in EVENTS:
            raise ValueError(f"unknown telemetry event: {event!r}")

        row = {
            "elapsed_s": f"{self._clock() - self._start:.6f}",
            "event": event,
            "queue_length_packets": int(queue_length),
            "sojourn_ms": (
                ""
                if sojourn_seconds is None
                else f"{sojourn_seconds * 1000.0:.3f}"
            ),
            "drops": int(drops),
            "marks": int(marks),
            "bytes": int(datagram_bytes),
            "forwarded_bytes": int(forwarded_bytes),
        }

        with self._lock:
            self._writer.writerow(row)
            self._file.flush()
            self.rows_written += 1

    def close(self):
        with self._lock:
            if not self._file.closed:
                self._file.flush()
                self._file.close()
