import csv
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from router.telemetry import FIELDS as TELEMETRY_FIELDS

MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "phase9_analysis.py"
)


def load_analysis():
    spec = importlib.util.spec_from_file_location(
        "phase9_analysis", MODULE_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


analysis = load_analysis()

SENDER_FIELDS = ["timestamp", "event", "packet_type", "seq", "ack", "bytes",
                 "attempt", "details"]


def write_rows(path, fields, rows):
    with open(path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            full = {field: "" for field in fields}
            full.update(row)
            writer.writerow(full)


def make_run(root, scenario, buffer, aqm, mode, controller, seed,
             goodput=4.0, rtts=(40.0, 50.0), queue=(1.0, 2.0),
             cwnds=(1.0, 2.0, 3.0), drops=0, marks=0, flow_packets=10):
    run_dir = (root / scenario / f"buf{buffer}"
               / f"{aqm}_{mode}_{controller}" / f"rep{seed}")
    run_dir.mkdir(parents=True)
    cell = {"scenario": scenario, "buffer": buffer, "aqm": aqm,
            "aqm_mode": mode, "controller": controller, "seed": seed}
    result = {"success": True, "failure_reasons": [],
              "goodput_mbps": goodput, "duration_s": 10.0}
    (run_dir / "run_metadata.json").write_text(
        json.dumps({"cell": cell, "result": result,
                    "network": {"file_bytes": 5_000_000}})
    )

    sender = [{"timestamp": f"2026-11-18T00:00:0{index}.000000+00:00",
               "event": "CWND_UPDATE",
               "details": f"cwnd={value:.3f} reason=ack"}
              for index, value in enumerate(cwnds)]
    sender += [{"event": "RTT_SAMPLE", "details": f"rtt={value / 1000:.6f}"}
               for value in rtts]
    write_rows(run_dir / "sender_log.csv", SENDER_FIELDS, sender)

    telemetry = [{"elapsed_s": f"{index * 0.5:.6f}", "event": "enqueue",
                  "queue_length_packets": int(value), "kind": "data"}
                 for index, value in enumerate(queue)]
    telemetry += [{"elapsed_s": "1.0", "event": "forward",
                   "sojourn_ms": "4.000", "kind": "data",
                   "queue_length_packets": 2}
                  for _ in range(flow_packets)]
    telemetry += [{"elapsed_s": "1.0", "event": "forward",
                   "sojourn_ms": "50.0", "kind": "cross",
                   "queue_length_packets": 2}]
    telemetry += [{"elapsed_s": "1.0", "event": "drop", "kind": "data",
                   "queue_length_packets": 3} for _ in range(drops)]
    telemetry += [{"elapsed_s": "1.0", "event": "mark", "kind": "data",
                   "queue_length_packets": 3} for _ in range(marks)]
    write_rows(run_dir / "router_telemetry.csv", TELEMETRY_FIELDS, telemetry)
    return run_dir


class SampleTests(unittest.TestCase):
    def test_cdf_points_are_sorted_with_cumulative_fraction(self):
        xs, ys = analysis.cdf_points([3.0, 1.0, 2.0])

        self.assertEqual(xs, [1.0, 2.0, 3.0])
        self.assertAlmostEqual(ys[0], 1 / 3)
        self.assertAlmostEqual(ys[-1], 1.0)

    def test_empty_cdf_is_empty(self):
        self.assertEqual(analysis.cdf_points([]), ([], []))

    def test_queue_timeline_starts_at_zero(self):
        rows = [{"elapsed_s": "2.0", "queue_length_packets": "3"},
                {"elapsed_s": "2.5", "queue_length_packets": "7"}]

        times, lengths = analysis.queue_timeline(rows)

        self.assertEqual(times, [0.0, 0.5])
        self.assertEqual(lengths, [3, 7])

    def test_cwnd_timeline_is_relative_to_the_first_update(self):
        rows = [
            {"timestamp": "2026-11-18T00:00:01.000000+00:00",
             "event": "CWND_UPDATE", "details": "cwnd=1.000 reason=init"},
            {"timestamp": "2026-11-18T00:00:01.250000+00:00",
             "event": "CWND_UPDATE", "details": "cwnd=2.500 reason=ack"},
            {"timestamp": "2026-11-18T00:00:01.500000+00:00",
             "event": "RTT_SAMPLE", "details": "rtt=0.04"},
        ]

        times, windows = analysis.cwnd_timeline(rows)

        self.assertEqual(times, [0.0, 0.25])
        self.assertEqual(windows, [1.0, 2.5])

    def test_rtt_samples_are_reported_in_milliseconds(self):
        rows = [{"event": "RTT_SAMPLE", "details": "rtt=0.041106 srtt=0.04"}]

        samples = analysis.rtt_samples(rows)

        self.assertEqual(len(samples), 1)
        self.assertAlmostEqual(samples[0], 41.106)

    def test_error_range_gives_median_and_extremes(self):
        self.assertEqual(
            analysis.error_range([3.0, 5.0, 4.0]), (4.0, 3.0, 5.0)
        )

    def test_error_range_of_nothing_is_none(self):
        self.assertEqual(analysis.error_range([]), (None, None, None))


class RunStatisticsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p9run_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_drop_and_mark_rates_are_per_thousand_flow_packets(self):
        run_dir = make_run(self.tmp, "S1", 200, "red", "drop", "reno", 1,
                           drops=5, marks=2, flow_packets=1000)

        stats = analysis.run_statistics(run_dir)

        self.assertEqual(stats["flow_packets"], 1000)
        self.assertAlmostEqual(stats["drop_rate"], 5.0)
        self.assertAlmostEqual(stats["mark_rate"], 2.0)

    def test_run_statistics_carry_the_summary_metrics(self):
        run_dir = make_run(self.tmp, "S1", 200, "droptail", "drop", "reno", 1,
                           goodput=4.5, rtts=(40.0, 60.0))

        stats = analysis.run_statistics(run_dir)

        self.assertEqual(stats["goodput_mbps"], 4.5)
        self.assertAlmostEqual(stats["rtt_mean_ms"], 50.0)
        self.assertTrue(stats["success"])

    def test_cell_directories_are_found_by_combination(self):
        make_run(self.tmp, "S1", 200, "red", "mark", "dctcp", 1)
        make_run(self.tmp, "S1", 200, "red", "mark", "dctcp", 2)

        dirs = analysis.cell_directories(self.tmp, "S1", 200, "red", "mark",
                                         "dctcp")

        self.assertEqual([path.name for path in dirs], ["rep1", "rep2"])


class SummaryTableTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p9tab_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_combination_rows_take_medians_over_seeds(self):
        for seed, goodput in ((1, 3.0), (2, 4.0), (3, 5.0)):
            make_run(self.tmp, "S1", 200, "droptail", "drop", "reno", seed,
                     goodput=goodput)

        rows = analysis.summary_rows(self.tmp)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["runs"], 3)
        self.assertEqual(rows[0]["goodput_mbps"], 4.0)

    def test_summary_rows_are_written_to_csv_and_markdown(self):
        make_run(self.tmp, "S1", 200, "droptail", "drop", "reno", 1)
        rows = analysis.summary_rows(self.tmp)
        csv_path = self.tmp / "table.csv"
        md_path = self.tmp / "table.md"

        analysis.write_table(rows, csv_path, md_path)

        self.assertIn("goodput_mbps", csv_path.read_text())
        self.assertIn("| droptail", md_path.read_text())


class RenderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p9fig_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def _is_png(self, path):
        return path.exists() and path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"

    def test_queue_timeline_figure_is_written(self):
        series = {"DropTail reno": ([0.0, 1.0, 2.0], [0, 5, 3])}
        path = self.tmp / "queue.png"

        analysis.render_timelines(path, [("DropTail", series)],
                                  ylabel="queue (packets)")

        self.assertTrue(self._is_png(path))

    def test_cdf_figure_is_written(self):
        panels = [("DropTail", {"reno": [1.0, 2.0, 3.0, 4.0]})]
        path = self.tmp / "cdf.png"

        analysis.render_cdfs(path, [("RTT (ms)", panels)])

        self.assertTrue(self._is_png(path))

    def test_bar_figure_is_written(self):
        path = self.tmp / "bars.png"

        analysis.render_bars(path, ["a", "b"],
                             [("goodput", [4.0, 3.0], [3.5, 2.5], [4.5, 3.5])])

        self.assertTrue(self._is_png(path))

    def test_line_figure_is_written(self):
        path = self.tmp / "lines.png"

        panels = [("queue p95", {"reno": ([20, 50], [30, 78])})]
        analysis.render_lines(path, panels,
                              xlabel="buffer (packets)")

        self.assertTrue(self._is_png(path))


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p9ver_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_verification_compares_goodput_and_tail_rtt(self):
        reference = self.tmp / "reference"
        verification = self.tmp / "verification"
        make_run(reference, "S1", 200, "droptail", "drop", "reno", 1,
                 goodput=4.0, rtts=(40.0, 100.0))
        make_run(verification, "S1", 200, "droptail", "drop", "reno", 1,
                 goodput=4.4, rtts=(40.0, 110.0))

        rows = analysis.compare_runs(reference, verification)

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["goodput_ratio"], 1.1)
        self.assertAlmostEqual(rows[0]["rtt_p95_ratio"], 1.1)


class VerificationTextTests(unittest.TestCase):
    def test_verification_section_reports_agreement(self):
        rows = [
            {"run": "S1/a", "goodput_ratio": 1.01, "rtt_p95_ratio": 1.02,
             "reference_goodput_mbps": 4.0, "verification_goodput_mbps": 4.04},
            {"run": "S3/b", "goodput_ratio": 0.8, "rtt_p95_ratio": 1.1,
             "reference_goodput_mbps": 4.0, "verification_goodput_mbps": 3.2},
        ]

        text = "\n".join(analysis.verification_lines(rows))

        self.assertIn("Goodput is within 5% in 1 of 2 runs", text)
        self.assertIn("S3/b: 4.00 to 3.20 Mbps", text)

    def test_no_verification_adds_no_section(self):
        self.assertEqual(analysis.verification_lines([]), [])


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p9all_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_pipeline_writes_every_phase9_artifact(self):
        root = self.tmp / "phase8"
        for seed in (1, 2):
            make_run(root, "S1", 200, "droptail", "drop", "reno", seed)
            make_run(root, "S1", 200, "red", "mark", "dctcp", seed)
            make_run(root, "S3", 200, "codel", "mark", "delay", seed)
        out = self.tmp / "phase9"

        analysis.run_pipeline(root, out)

        names = {path.name for path in (out / "figures").iterdir()}
        self.assertIn("fig_queue_timeline.png", names)
        self.assertIn("fig_delay_cdfs.png", names)
        self.assertIn("fig_cwnd_modes.png", names)
        self.assertIn("fig_throughput_bars.png", names)
        self.assertTrue((out / "summary_table_s1.csv").exists())
        self.assertTrue((out / "summary_table_s1.md").exists())
        self.assertTrue((out / "summary_table_all.csv").exists())
        self.assertIn("Finding 1", (out / "findings.md").read_text())


if __name__ == "__main__":
    unittest.main()
