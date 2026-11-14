
import csv
import threading
import tempfile
import unittest
from pathlib import Path

from router.telemetry import FIELDS, TelemetryLogger


class FakeClock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def read_rows(path):
    with open(path, newline="") as file:
        return list(csv.DictReader(file))


class TestTelemetrySchema(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="telemetry_test_"))
        self.path = self.tmp / "router_telemetry.csv"

    def test_header_written_on_creation(self):
        TelemetryLogger(self.path).close()

        with open(self.path, newline="") as file:
            header = next(csv.reader(file))

        self.assertEqual(header, FIELDS)

    def test_record_writes_all_columns(self):
        clock = FakeClock(now=10.0)
        logger = TelemetryLogger(self.path, clock=clock)

        clock.advance(1.5)
        logger.record(
            "forward",
            queue_length=3,
            datagram_bytes=1029,
            drops=2,
            marks=0,
            forwarded_bytes=5145,
            sojourn_seconds=0.0123456,
        )
        logger.close()

        rows = read_rows(self.path)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["elapsed_s"], "1.500000")
        self.assertEqual(row["event"], "forward")
        self.assertEqual(row["queue_length_packets"], "3")
        self.assertEqual(row["sojourn_ms"], "12.346")
        self.assertEqual(row["drops"], "2")
        self.assertEqual(row["marks"], "0")
        self.assertEqual(row["bytes"], "1029")
        self.assertEqual(row["forwarded_bytes"], "5145")

    def test_sojourn_empty_where_not_applicable(self):
        logger = TelemetryLogger(self.path)
        logger.record("enqueue", 1, 1029, 0, 0, 0)
        logger.record("drop", 2, 1029, 1, 0, 0)
        logger.close()

        for row in read_rows(self.path):
            self.assertEqual(row["sojourn_ms"], "")

    def test_unknown_event_rejected(self):
        logger = TelemetryLogger(self.path)

        with self.assertRaises(ValueError):
            logger.record("nonsense", 0, 0, 0, 0, 0)

        logger.close()

    def test_elapsed_uses_monotonic_start(self):
        clock = FakeClock(now=100.0)
        logger = TelemetryLogger(self.path, clock=clock)

        clock.advance(0.5)
        logger.record("enqueue", 1, 100, 0, 0, 0)
        clock.advance(0.5)
        logger.record("enqueue", 2, 100, 0, 0, 0)
        logger.close()

        elapsed = [row["elapsed_s"] for row in read_rows(self.path)]
        self.assertEqual(elapsed, ["0.500000", "1.000000"])

    def test_missing_parent_directory_created(self):
        nested = self.tmp / "run" / "sub" / "router_telemetry.csv"
        TelemetryLogger(nested).close()

        self.assertTrue(nested.exists())


class TestTelemetryConcurrency(unittest.TestCase):
    def test_concurrent_records_produce_all_rows(self):
        path = Path(tempfile.mkdtemp(prefix="telemetry_test_")) / "t.csv"
        logger = TelemetryLogger(path)

        def record_many(count):
            for index in range(count):
                logger.record("enqueue", index, 100, 0, 0, 0)

        threads = [
            threading.Thread(target=record_many, args=(50,))
            for _ in range(4)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        logger.close()

        self.assertEqual(logger.rows_written, 200)
        self.assertEqual(len(read_rows(path)), 200)


class TestTelemetryClose(unittest.TestCase):
    def test_close_is_idempotent_and_flushes(self):
        path = Path(tempfile.mkdtemp(prefix="telemetry_test_")) / "t.csv"
        logger = TelemetryLogger(path)

        logger.record("enqueue", 1, 100, 0, 0, 0)
        logger.close()
        logger.close()

        self.assertEqual(len(read_rows(path)), 1)


if __name__ == "__main__":
    unittest.main()
