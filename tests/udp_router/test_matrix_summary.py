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
    Path(__file__).resolve().parents[2] / "scripts" / "summarize_matrix.py"
)


def load_matrix_module():
    spec = importlib.util.spec_from_file_location("summarize_matrix",
                                                  MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


matrix = load_matrix_module()


def write_rows(path, fields, rows):
    with open(path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            full = {field: "" for field in fields}
            full.update(row)
            writer.writerow(full)


def make_run(root, seed, goodput=4.0, success=True, rtts_ms=(40.0,),
             flow_sojourns=(1.0,), cross_sojourns=(), marks=0, cross_marks=0,
             extra_sender=()):
    run_dir = root / "S1" / "buf200" / "red_mark_dctcp" / f"rep{seed}"
    run_dir.mkdir(parents=True)
    cell = {"scenario": "S1", "buffer": 200, "aqm": "red",
            "aqm_mode": "mark", "controller": "dctcp", "seed": seed}
    result = {
        "success": success,
        "failure_reasons": [] if success else ["sender exited with 1"],
        "goodput_mbps": goodput if success else None,
        "duration_s": 10.0,
    }
    (run_dir / "run_metadata.json").write_text(
        json.dumps({"cell": cell, "result": result})
    )

    sender = [
        {"event": "RTT_SAMPLE", "details": f"rtt={rtt / 1000:.6f} srtt=0"}
        for rtt in rtts_ms
    ]
    sender += [
        {"event": "CWND_UPDATE", "details": "cwnd=1.000 reason=init"},
        {"event": "CWND_UPDATE", "details": "cwnd=6.000 reason=ack"},
        {"event": "CWND_UPDATE", "details": "cwnd=2.000 reason=timeout"},
    ]
    sender += list(extra_sender)
    write_rows(run_dir / "sender_log.csv", SENDER_FIELDS, sender)

    telemetry = [{"event": "forward", "sojourn_ms": f"{value:.3f}",
                  "kind": "data", "queue_length_packets": 3}
                 for value in flow_sojourns]
    telemetry += [{"event": "forward", "sojourn_ms": f"{value:.3f}",
                   "kind": "cross", "queue_length_packets": 9}
                  for value in cross_sojourns]
    telemetry += [{"event": "mark", "kind": "data", "queue_length_packets": 4}
                  for _ in range(marks)]
    telemetry += [{"event": "mark", "kind": "cross",
                   "queue_length_packets": 4} for _ in range(cross_marks)]
    write_rows(run_dir / "router_telemetry.csv", TELEMETRY_FIELDS, telemetry)
    return run_dir


class RunMetricTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p8sum_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_rtt_percentiles_come_from_the_sender_samples(self):
        run_dir = make_run(self.tmp, 1,
                           rtts_ms=(40.0, 50.0, 60.0, 70.0, 80.0))

        row = matrix.summarise_run(run_dir)

        self.assertAlmostEqual(row["rtt_mean_ms"], 60.0)
        self.assertAlmostEqual(row["rtt_p50_ms"], 60.0)
        self.assertAlmostEqual(row["rtt_p95_ms"], 80.0)

    def test_queue_delay_counts_only_flow_packets(self):
        run_dir = make_run(self.tmp, 1, flow_sojourns=(2.0, 4.0),
                           cross_sojourns=(100.0, 200.0))

        row = matrix.summarise_run(run_dir)

        self.assertAlmostEqual(row["queue_mean_ms"], 3.0)
        self.assertAlmostEqual(row["queue_p95_ms"], 4.0)
        self.assertEqual(row["cross_forwarded"], 2)

    def test_marks_are_split_between_flow_and_cross_traffic(self):
        run_dir = make_run(self.tmp, 1, marks=5, cross_marks=3)

        row = matrix.summarise_run(run_dir)

        self.assertEqual(row["flow_marks"], 5)

    def test_retransmissions_timeouts_and_windows_are_counted(self):
        run_dir = make_run(self.tmp, 1, extra_sender=(
            {"event": "RETRANSMISSION", "details": "Timeout"},
            {"event": "RETRANSMISSION",
             "details": "Fast retransmit (3 duplicate ACKs)"},
            {"event": "RETRANSMISSION", "details": "Timeout"},
        ))

        row = matrix.summarise_run(run_dir)

        self.assertEqual(row["retransmissions"], 3)
        self.assertEqual(row["timeouts"], 2)
        self.assertEqual(row["fast_retransmits"], 1)
        self.assertEqual(row["cwnd_max"], 6.0)
        self.assertAlmostEqual(row["cwnd_mean"], 4.0)


class CellAggregationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p8agg_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_repetitions_give_median_and_range(self):
        runs = [
            matrix.summarise_run(make_run(self.tmp, seed, goodput=goodput))
            for seed, goodput in ((1, 3.0), (2, 4.0), (3, 5.0))
        ]

        cells = matrix.aggregate_cells(runs)

        self.assertEqual(len(cells), 1)
        cell = cells[0]
        self.assertEqual(cell["runs"], 3)
        self.assertEqual(cell["successes"], 3)
        self.assertEqual(cell["goodput_mbps_median"], 4.0)
        self.assertEqual(cell["goodput_mbps_min"], 3.0)
        self.assertEqual(cell["goodput_mbps_max"], 5.0)

    def test_failed_runs_are_counted_but_not_averaged(self):
        runs = [
            matrix.summarise_run(make_run(self.tmp, 1, goodput=4.0)),
            matrix.summarise_run(make_run(self.tmp, 2, success=False)),
        ]

        cell = matrix.aggregate_cells(runs)[0]

        self.assertEqual(cell["runs"], 2)
        self.assertEqual(cell["successes"], 1)
        self.assertEqual(cell["goodput_mbps_median"], 4.0)

    def test_cli_writes_run_and_cell_tables(self):
        make_run(self.tmp, 1)
        make_run(self.tmp, 2)
        runs_csv = self.tmp / "runs.csv"
        cells_csv = self.tmp / "cells.csv"

        with contextlib.redirect_stdout(io.StringIO()) as output:
            status = matrix.main([str(self.tmp), "--runs-csv", str(runs_csv),
                                  "--cells-csv", str(cells_csv)])

        self.assertEqual(status, 0)
        self.assertIn("runs: 2", output.getvalue())
        with open(cells_csv, newline="") as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(rows[0]["runs"], "2")

    def test_empty_root_is_reported(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(matrix.main([str(self.tmp)]), 1)


if __name__ == "__main__":
    unittest.main()
