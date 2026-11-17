import contextlib
import csv
import importlib.util
import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from router.telemetry import FIELDS as TELEMETRY_FIELDS
from transport.csv_logger import FIELDS as SENDER_FIELDS

MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "summarize_cc_modes.py"
)


def load_summary_module():
    spec = importlib.util.spec_from_file_location("summarize_cc_modes",
                                                  MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


summary = load_summary_module()


def write_rows(path, fields, rows):
    with open(path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            full = {field: "" for field in fields}
            full.update(row)
            writer.writerow(full)


def make_run(root, name, controller, sojourns_ms, cwnds, file_bytes=1_000_000,
             duration=2.0):
    run_dir = root / name / "red"
    run_dir.mkdir(parents=True)

    (run_dir / "run_metadata.json").write_text(json.dumps({
        "algorithm": "red",
        "mode": "mark",
        "network": {"file_bytes": file_bytes},
        "congestion_control": {"mode": controller, "parameters": {}},
        "result": {"success": True, "duration_s": duration},
    }))

    telemetry = [
        {"event": "forward", "sojourn_ms": f"{value:.3f}",
         "queue_length_packets": 3}
        for value in sojourns_ms
    ]
    telemetry.append({"event": "mark", "queue_length_packets": 9})
    write_rows(run_dir / "router_telemetry.csv", TELEMETRY_FIELDS, telemetry)

    sender = [{"event": "CC_MODE", "details": f"mode={controller}"},
              {"event": "CWND_UPDATE",
               "details": "cwnd=1.000 reason=init phase=slow_start"}]
    sender += [
        {"event": "CWND_UPDATE", "details": f"cwnd={value:.3f} reason=ack"}
        for value in cwnds
    ]
    write_rows(run_dir / "sender_log.csv", SENDER_FIELDS, sender)
    return run_dir


class CcModeSummaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p7summary_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_run_metrics_are_computed_from_the_logs(self):
        run_dir = make_run(self.tmp, "delay_red", "delay",
                           sojourns_ms=[1.0, 3.0, 5.0],
                           cwnds=[10.0, 12.0, 8.0])

        result = summary.summarise_run(run_dir, self.tmp)

        self.assertEqual(result["controller"], "delay")
        self.assertEqual(result["logged_controller"], "delay")
        self.assertEqual(result["run"], str(Path("delay_red") / "red"))
        self.assertAlmostEqual(result["goodput_mbps"], 4.0)
        self.assertAlmostEqual(result["mean_delay_ms"], 3.0)
        self.assertEqual(result["router_marks"], 1)
        self.assertEqual(result["delay_samples"], 3)

    def test_window_statistics_exclude_the_initial_window(self):
        run_dir = make_run(self.tmp, "reno_red", "reno",
                           sojourns_ms=[2.0], cwnds=[10.0, 4.0, 6.0])

        result = summary.summarise_run(run_dir, self.tmp)

        self.assertEqual(result["cwnd_min"], 4.0)
        self.assertEqual(result["cwnd_max"], 10.0)
        self.assertAlmostEqual(result["cwnd_mean"], 20.0 / 3.0)

    def test_percentiles_use_the_nearest_rank(self):
        values = list(range(1, 101))

        self.assertEqual(summary.percentile(values, 50), 50)
        self.assertEqual(summary.percentile(values, 95), 95)
        self.assertEqual(summary.percentile(values, 99), 99)

    def test_runs_are_found_recursively(self):
        make_run(self.tmp, "a", "reno", [1.0], [2.0])
        make_run(self.tmp, "b", "delay", [1.0], [2.0])

        self.assertEqual(len(summary.find_runs(self.tmp)), 2)

    def test_main_writes_csv_and_reports_an_empty_root(self):
        make_run(self.tmp, "delay_red", "delay", [1.0, 2.0], [3.0])
        output = self.tmp / "summary.csv"

        with contextlib.redirect_stdout(io.StringIO()):
            status = summary.main([str(self.tmp), "--csv", str(output)])

        self.assertEqual(status, 0)
        with open(output, newline="") as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(rows[0]["controller"], "delay")

        empty = Path(tempfile.mkdtemp(prefix="p7empty_"))
        self.addCleanup(shutil.rmtree, empty, True)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(summary.main([str(empty)]), 1)


if __name__ == "__main__":
    unittest.main()
