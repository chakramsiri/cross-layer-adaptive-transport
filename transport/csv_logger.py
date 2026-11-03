import csv
import os
from datetime import datetime, timezone
from pathlib import Path


FIELDS = [
    "timestamp",
    "event",
    "packet_type",
    "seq",
    "ack",
    "bytes",
    "attempt",
    "details"
]


class CSVLogger:
    def __init__(self, filename):
        self.path = Path(filename)
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if not self.path.exists() or self.path.stat().st_size == 0:
            with self.path.open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=FIELDS)
                writer.writeheader()

    def log(
        self,
        event,
        packet_type="",
        seq="",
        ack="",
        byte_count=0,
        attempt="",
        details=""
    ):
        row = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
            "packet_type": packet_type,
            "seq": seq,
            "ack": ack,
            "bytes": byte_count,
            "attempt": attempt,
            "details": details
        }

        with self.path.open("a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=FIELDS)
            writer.writerow(row)
