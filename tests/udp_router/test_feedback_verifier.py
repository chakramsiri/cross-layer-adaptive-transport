import contextlib
import csv
import importlib.util
import io
import shutil
import tempfile
import unittest
from pathlib import Path

from transport.csv_logger import FIELDS as SENDER_FIELDS
from router.telemetry import FIELDS as TELEMETRY_FIELDS

MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "verify_feedback.py"
)


def load_verifier():
    spec = importlib.util.spec_from_file_location(
        "verify_feedback", MODULE_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


verifier = load_verifier()


def sender_row(event, details=""):
    row = {field: "" for field in SENDER_FIELDS}
    row.update(event=event, details=details)
    return row


def telemetry_row(event, sojourn_ms=""):
    row = {field: "" for field in TELEMETRY_FIELDS}
    row.update(event=event, sojourn_ms=sojourn_ms)
    return row


def feedback(ce_total, q_delay_us):
    return sender_row(
        "FEEDBACK",
        f"ce_total={ce_total} ce_new=0 q_delay_us={q_delay_us}",
    )


class VerifyFeedbackTests(unittest.TestCase):
    def test_matching_run_passes(self):
        sender = [
            sender_row("ACK_RECEIVED"),
            feedback(1, 1200),
            feedback(3, 2500),
        ]
        telemetry = [
            telemetry_row("forward", "1.200"),
            telemetry_row("mark", "0.900"),
            telemetry_row("forward", "2.500"),
            telemetry_row("mark", "3.000"),
            telemetry_row("mark", "3.100"),
            telemetry_row("forward", "0.100"),
        ]

        result = verifier.verify(sender, telemetry)

        self.assertTrue(result["passed"])
        self.assertEqual(result["sender_ce_total"], 3)
        self.assertEqual(result["router_marks"], 3)
        self.assertEqual(result["unmatched_queue_delays"], 0)

    def test_ce_total_disagreeing_with_router_marks_fails(self):
        sender = [feedback(2, 1000)]
        telemetry = [
            telemetry_row("forward", "1.000"),
            telemetry_row("mark"),
            telemetry_row("mark"),
            telemetry_row("mark"),
        ]

        result = verifier.verify(sender, telemetry)

        self.assertFalse(result["ce_match"])
        self.assertFalse(result["passed"])

    def test_echoed_queue_delay_without_a_forwarded_sojourn_fails(self):
        sender = [feedback(0, 7777)]
        telemetry = [telemetry_row("forward", "1.000")]

        result = verifier.verify(sender, telemetry)

        self.assertEqual(result["unmatched_queue_delays"], 1)
        self.assertFalse(result["passed"])

    def test_run_without_any_feedback_does_not_pass(self):
        result = verifier.verify([sender_row("ACK_RECEIVED")],
                                 [telemetry_row("forward", "1.000")])

        self.assertFalse(result["passed"])

    def test_main_reads_files_and_sets_exit_status(self):
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp, True)
        sender_path = tmp / "sender_log.csv"
        telemetry_path = tmp / "router_telemetry.csv"

        with open(sender_path, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=SENDER_FIELDS)
            writer.writeheader()
            writer.writerow(sender_row("FEEDBACK",
                                       "ce_total=1 ce_new=1 q_delay_us=500"))

        with open(telemetry_path, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=TELEMETRY_FIELDS)
            writer.writeheader()
            writer.writerow(telemetry_row("forward", "0.500"))
            writer.writerow(telemetry_row("mark"))

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            status = verifier.main([
                "--sender-log", str(sender_path),
                "--telemetry", str(telemetry_path),
            ])

        self.assertEqual(status, 0)
        self.assertIn("PASS", output.getvalue())

        with open(telemetry_path, "a", newline="") as file:
            csv.DictWriter(file, fieldnames=TELEMETRY_FIELDS).writerow(
                telemetry_row("mark")
            )

        with contextlib.redirect_stdout(io.StringIO()):
            status = verifier.main([
                "--sender-log", str(sender_path),
                "--telemetry", str(telemetry_path),
            ])

        self.assertEqual(status, 1)


if __name__ == "__main__":
    unittest.main()
