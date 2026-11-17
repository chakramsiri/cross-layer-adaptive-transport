"""Phase 5 harness support tests: args, commands, metadata, parsing."""

import csv
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT = REPO_ROOT / "tests" / "mininet" / "phase5_aqm_experiments.py"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


TELEMETRY_FIELDS = [
    "elapsed_s", "event", "queue_length_packets", "sojourn_ms",
    "drops", "marks", "bytes", "forwarded_bytes", "avg_queue_packets",
    "reason",
]

LOCKSTEP_FIELDS = [
    "timestamp", "event", "packet_type", "seq", "ack", "bytes",
    "attempt", "details",
]


def telemetry_row(elapsed, event, queue, sojourn="", drops=0, marks=0,
                  forwarded_bytes=0, avg="", reason=""):
    return {
        "elapsed_s": elapsed,
        "event": event,
        "queue_length_packets": str(queue),
        "sojourn_ms": sojourn,
        "drops": str(drops),
        "marks": str(marks),
        "bytes": "1000",
        "forwarded_bytes": str(forwarded_bytes),
        "avg_queue_packets": avg,
        "reason": reason,
    }


class Phase5TestCase(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module(
            "phase5_aqm_experiments", EXPERIMENT
        )
        self.tmp = Path(tempfile.mkdtemp(prefix="p5support_"))


class TestArgsValidation(Phase5TestCase):
    def test_defaults_are_valid(self):
        args = self.experiment.build_arg_parser().parse_args([])
        self.assertEqual(
            self.experiment.validate_args(args),
            ["droptail", "red", "codel"],
        )
        self.assertEqual(args.aqm, "droptail")
        self.assertEqual(args.aqm_mode, "drop")
        self.assertEqual(args.queue_capacity, 200)
        self.assertEqual(args.rate_mbps, 5.0)
        self.assertEqual(args.delay_ms, 20.0)
        self.assertEqual(args.file_bytes, 30 * 1000 * 1000)
        self.assertEqual(args.red_seed, 42)
        self.assertEqual(args.red_typical_packet_bytes, 1029.0)
        self.assertEqual(args.codel_target_ms, 5.0)
        self.assertEqual(args.codel_interval_ms, 100.0)

    def test_invalid_numeric_arguments_rejected(self):
        cases = (
            ("--rate-mbps", "0"),
            ("--queue-capacity", "0"),
            ("--file-bytes", "100"),
            ("--sender-window", "0"),
            ("--rwnd-bytes", "0"),
            ("--timeout-s", "5"),
        )
        for flag, value in cases:
            args = self.experiment.build_arg_parser().parse_args(
                [flag, value]
            )
            with self.assertRaises(SystemExit):
                self.experiment.validate_args(args)

    def test_unknown_run_rejected(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--runs", "droptail,bogus"]
        )
        with self.assertRaises(SystemExit):
            self.experiment.validate_args(args)

    def test_run_selection_subset(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--runs", "red,codel,red"]
        )
        self.assertEqual(
            self.experiment.validate_args(args), ["red", "codel"]
        )

    def test_interactive_requires_mininet(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--interactive", "--topology", "relay"]
        )
        with self.assertRaises(SystemExit):
            self.experiment.validate_args(args)

    def test_mark_mode_rejected_with_droptail(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--aqm-mode", "mark"]
        )
        with self.assertRaises(SystemExit):
            self.experiment.validate_args(args)

        args = self.experiment.build_arg_parser().parse_args(
            ["--aqm-mode", "mark", "--runs", "red,codel"]
        )
        self.assertEqual(
            self.experiment.validate_args(args), ["red", "codel"]
        )

    def test_invalid_red_parameters_rejected(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--runs", "red", "--red-w-q", "0"]
        )
        with self.assertRaises(SystemExit):
            self.experiment.validate_args(args)

        args = self.experiment.build_arg_parser().parse_args(
            ["--runs", "red", "--queue-capacity", "50"]
        )
        with self.assertRaises(SystemExit):
            self.experiment.validate_args(args)

    def test_invalid_codel_parameters_rejected(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--runs", "codel", "--codel-interval-ms", "5"]
        )
        with self.assertRaises(SystemExit):
            self.experiment.validate_args(args)


class TestRouterCommand(Phase5TestCase):
    def _args(self, *extra):
        return self.experiment.build_arg_parser().parse_args(list(extra))

    def test_red_command_carries_red_flags_only(self):
        args = self._args()
        plan = self.experiment.phase4.MININET_PLAN

        command = self.experiment.build_router_command(
            "python3", plan, args, self.tmp, "red"
        )
        text = " ".join(command)

        self.assertIn("-m router.forwarder", text)
        self.assertIn("--aqm red", text)
        self.assertIn("--aqm-mode drop", text)
        self.assertIn("--red-w-q 0.002", text)
        self.assertIn("--red-min-th 20.0", text)
        self.assertIn("--red-max-th 80.0", text)
        self.assertIn("--red-max-p 0.1", text)
        self.assertIn("--red-seed 42", text)
        self.assertIn("--red-typical-packet-bytes 1029.0", text)
        self.assertIn("--queue-capacity 200", text)
        self.assertIn(str(self.tmp / "router_telemetry.csv"), text)
        self.assertIn(str(self.tmp / "router.ready"), text)
        self.assertIn("--max-seconds 360.0", text)
        self.assertNotIn("--codel", text)
        self.assertIn("10.0.0.2:5001", text)

    def test_codel_command_converts_defaults(self):
        args = self._args()
        plan = self.experiment.phase4.MININET_PLAN

        command = self.experiment.build_router_command(
            "python3", plan, args, self.tmp, "codel"
        )
        text = " ".join(command)

        self.assertIn("--aqm codel", text)
        self.assertIn("--codel-target-ms 5.0", text)
        self.assertIn("--codel-interval-ms 100.0", text)
        self.assertNotIn("--red", text)

    def test_droptail_command_has_no_aqm_parameters(self):
        args = self._args()
        plan = self.experiment.phase4.MININET_PLAN

        command = self.experiment.build_router_command(
            "python3", plan, args, self.tmp, "droptail"
        )
        text = " ".join(command)

        self.assertIn("--aqm droptail", text)
        self.assertNotIn("--red", text)
        self.assertNotIn("--codel", text)

    def test_mark_mode_and_overrides_carried(self):
        args = self._args(
            "--aqm-mode", "mark",
            "--red-gentle",
            "--codel-target-ms", "10",
        )
        plan = self.experiment.phase4.MININET_PLAN

        text = " ".join(
            self.experiment.build_router_command(
                "python3", plan, args, self.tmp, "red"
            )
        )
        self.assertIn("--aqm-mode mark", text)
        self.assertIn("--red-gentle", text)

        text = " ".join(
            self.experiment.build_router_command(
                "python3", plan, args, self.tmp, "codel",
                max_seconds=86400.0,
            )
        )
        self.assertIn("--codel-target-ms 10.0", text)
        self.assertIn("--max-seconds 86400.0", text)

    def test_builder_for_matches_direct_command(self):
        args = self._args()
        plan = self.experiment.phase4.MININET_PLAN

        direct = self.experiment.build_router_command(
            "python3", plan, args, self.tmp, "red"
        )
        builder = self.experiment.router_command_builder_for(args, "red")
        built = builder("python3", plan, args, self.tmp, 200)

        self.assertEqual(direct, built)


class TestManualCommands(Phase5TestCase):
    def _interfaces(self):
        return {
            "sender": "h1-eth0",
            "router_sender": "r-eth0",
            "router_receiver": "r-eth1",
            "receiver": "h2-eth0",
        }

    def test_sections_are_ordered(self):
        args = self.experiment.build_arg_parser().parse_args([])
        commands = self.experiment.build_manual_endpoint_commands(
            args, self.experiment.phase4.MININET_PLAN,
            self.tmp, self._interfaces(),
        )
        self.assertEqual(
            list(commands),
            ["captures", "receiver", "sender", "progress", "cleanup"],
        )

    def test_capture_commands_use_discovered_interfaces(self):
        args = self.experiment.build_arg_parser().parse_args([])
        commands = self.experiment.build_manual_endpoint_commands(
            args, self.experiment.phase4.MININET_PLAN,
            self.tmp, self._interfaces(),
        )
        text = "\n".join(commands["captures"])

        for interface in self._interfaces().values():
            self.assertIn(f"-i {interface}", text)
        for pid_file in ("h1_capture", "router_sender_capture",
                         "router_receiver_capture", "h2_capture"):
            self.assertIn(f"& echo $! > {self.tmp}/{pid_file}.pid", text)

    def test_receiver_and_sender_redirect_and_record_pid(self):
        args = self.experiment.build_arg_parser().parse_args([])
        commands = self.experiment.build_manual_endpoint_commands(
            args, self.experiment.phase4.MININET_PLAN,
            self.tmp, self._interfaces(),
        )
        receiver = commands["receiver"][0]
        sender = commands["sender"][0]

        self.assertIn("</dev/null", receiver)
        self.assertIn(f"> {self.tmp}/manual_receiver_stdout.log 2>&1",
                      receiver)
        self.assertTrue(
            receiver.endswith(f"& echo $! > {self.tmp}/receiver.pid")
        )
        self.assertIn("--rwnd-bytes 800000", receiver)

        self.assertIn("</dev/null", sender)
        self.assertIn(f"> {self.tmp}/manual_sender_stdout.log 2>&1", sender)
        self.assertTrue(
            sender.endswith(f"& echo $! > {self.tmp}/sender.pid")
        )
        self.assertIn("--window-size 400", sender)
        self.assertIn("--initial-ssthresh 20.0", sender)

    def test_progress_uses_tail_n_not_tail_f(self):
        args = self.experiment.build_arg_parser().parse_args([])
        commands = self.experiment.build_manual_endpoint_commands(
            args, self.experiment.phase4.MININET_PLAN,
            self.tmp, self._interfaces(),
        )
        text = "\n".join(commands["progress"])

        self.assertIn("tail -n 5", text)
        self.assertNotIn("tail -f", text)
        self.assertIn("out.bin", text)

    def test_cleanup_uses_exact_pids_and_verification(self):
        args = self.experiment.build_arg_parser().parse_args([])
        commands = self.experiment.build_manual_endpoint_commands(
            args, self.experiment.phase4.MININET_PLAN,
            self.tmp, self._interfaces(),
        )
        text = "\n".join(commands["cleanup"])

        self.assertNotIn("pkill", text)
        for pid_file in ("h1_capture", "router_sender_capture",
                         "router_receiver_capture", "h2_capture"):
            self.assertIn(f"cat {self.tmp}/{pid_file}.pid", text)
        self.assertIn("grep -aq tcpdump /proc/$p/cmdline && kill $p", text)
        self.assertIn(
            "grep -aq phase2_node.py /proc/$p/cmdline && kill $p", text
        )
        self.assertIn(f"sha256sum {self.tmp}/out.bin", text)
        self.assertIn(f"cmp {self.tmp}/input.bin {self.tmp}/out.bin", text)
        for capture in ("h1_capture", "router_sender_capture",
                        "router_receiver_capture", "h2_capture"):
            self.assertIn(f"tcpdump -r {self.tmp}/{capture}.pcap", text)
        self.assertIn(
            f"awk -F, ", text
        )

    def test_missing_interface_rejected(self):
        args = self.experiment.build_arg_parser().parse_args([])
        interfaces = self._interfaces()
        interfaces["router_receiver"] = None

        with self.assertRaises(ValueError):
            self.experiment.build_manual_endpoint_commands(
                args, self.experiment.phase4.MININET_PLAN,
                self.tmp, interfaces,
            )


class TestRunMetadata(Phase5TestCase):
    def test_metadata_contents(self):
        args = self.experiment.build_arg_parser().parse_args([])
        plan = self.experiment.phase4.MININET_PLAN
        parameters = self.experiment.build_queue_parameters(args, "red")

        metadata = self.experiment.build_run_metadata(
            args, plan, self.tmp / "red", "red", "abc123", parameters
        )

        self.assertEqual(metadata["run_id"], "red")
        self.assertEqual(metadata["algorithm"], "red")
        self.assertEqual(metadata["mode"], "drop")
        self.assertEqual(metadata["queue"]["capacity"], 200)
        self.assertEqual(metadata["queue"]["parameters"]["min_th"], 20.0)
        self.assertEqual(metadata["queue"]["parameters"]["max_th"], 80.0)
        self.assertEqual(metadata["queue"]["parameters"]["seed"], 42)
        self.assertEqual(metadata["network"]["rate_mbps"], 5.0)
        self.assertEqual(metadata["network"]["access_link_delay_ms"], 20.0)
        self.assertEqual(metadata["network"]["file_bytes"], 30 * 1000 * 1000)
        self.assertEqual(metadata["network"]["input_sha256"], "abc123")
        self.assertEqual(metadata["sender"]["window_size"], 400)
        self.assertEqual(metadata["receiver"]["rwnd_bytes"], 800000)
        self.assertEqual(
            metadata["address_plan"]["router_sender_face"], "10.0.0.2:5001"
        )
        self.assertIn("started_at", metadata)

    def test_write_and_merge_metadata(self):
        run_dir = self.tmp / "run"
        self.experiment.write_json(
            run_dir / "run_metadata.json", {"algorithm": "codel"}
        )
        self.experiment.merge_run_metadata(
            run_dir, {"result": {"success": True}}
        )

        payload = json.loads(
            (run_dir / "run_metadata.json").read_text()
        )
        self.assertEqual(payload["algorithm"], "codel")
        self.assertTrue(payload["result"]["success"])

    def test_queue_parameters_match_router_configuration(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--aqm-mode", "mark", "--red-gentle"]
        )
        parameters = self.experiment.build_queue_parameters(args, "red")

        self.assertEqual(parameters["mode"], "mark")
        self.assertTrue(parameters["gentle"])
        self.assertEqual(parameters["typical_packet_bytes"], 1029.0)
        self.assertEqual(parameters["rate_bps"], 625_000.0)

        codel = self.experiment.build_queue_parameters(args, "codel")
        self.assertEqual(codel["target_ms"], 5.0)
        self.assertEqual(codel["interval_ms"], 100.0)
        self.assertEqual(codel["mode"], "mark")

    def test_red_packet_size_override_reaches_parameters(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--red-typical-packet-bytes", "1100"]
        )

        parameters = self.experiment.build_queue_parameters(args, "red")

        self.assertEqual(parameters["typical_packet_bytes"], 1100.0)


class TestTelemetryParsing(Phase5TestCase):
    def test_new_schema_counts_and_statistics(self):
        path = self.tmp / "router_telemetry.csv"
        write_csv(
            path, TELEMETRY_FIELDS,
            [
                telemetry_row("0.001", "enqueue", 1, avg="0.500"),
                telemetry_row("0.002", "enqueue", 2, avg="1.000"),
                telemetry_row("0.003", "forward", 1, sojourn="2.000",
                              forwarded_bytes=1000, avg="1.000"),
                telemetry_row("0.004", "mark", 2, reason="red",
                              marks=1, avg="1.500"),
                telemetry_row("0.005", "forward", 1, sojourn="4.000",
                              drops=1, marks=1, forwarded_bytes=2000,
                              avg="1.500"),
                telemetry_row("0.006", "drop", 1, drops=2, marks=1,
                              reason="codel", avg="1.500"),
                telemetry_row("0.007", "drop", 1, drops=3, marks=1,
                              reason="full", avg="1.500"),
                telemetry_row("0.008", "forward", 0, sojourn="6.000",
                              drops=3, marks=1, forwarded_bytes=3000,
                              avg="1.000"),
            ],
        )

        stats = self.experiment.parse_telemetry(path)

        self.assertEqual(stats["rows"], 8)
        self.assertEqual(stats["enqueue"], 2)
        self.assertEqual(stats["forward"], 3)
        self.assertEqual(stats["drop"], 2)
        self.assertEqual(stats["mark"], 1)
        self.assertEqual(stats["drops_codel"], 1)
        self.assertEqual(stats["drops_full"], 1)
        self.assertEqual(stats["drops_red"], 0)
        self.assertEqual(stats["marks_red"], 1)
        self.assertEqual(stats["marks_codel"], 0)
        self.assertEqual(stats["max_queue"], 2)
        self.assertAlmostEqual(stats["mean_queue"], 1.5)
        self.assertEqual(stats["sojourn_samples"], 3)
        self.assertAlmostEqual(stats["mean_sojourn_ms"], 4.0)
        self.assertAlmostEqual(stats["median_sojourn_ms"], 4.0)
        self.assertAlmostEqual(stats["p95_sojourn_ms"], 5.8)
        self.assertAlmostEqual(stats["p99_sojourn_ms"], 5.96)
        self.assertAlmostEqual(stats["max_sojourn_ms"], 6.0)
        self.assertAlmostEqual(stats["avg_queue_mean"],
                               (0.5 + 1 + 1 + 1.5 + 1.5 + 1.5 + 1.5 + 1) / 8)
        self.assertAlmostEqual(stats["avg_queue_max"], 1.5)
        self.assertEqual(stats["forwarded_bytes"], 3000)
        self.assertAlmostEqual(stats["elapsed_s"], 0.008)

    def test_phase4_schema_rows_parse_with_defaults(self):
        path = self.tmp / "legacy_router_telemetry.csv"
        legacy_fields = [
            "elapsed_s", "event", "queue_length_packets", "sojourn_ms",
            "drops", "marks", "bytes", "forwarded_bytes",
        ]
        write_csv(
            path, legacy_fields,
            [
                {"elapsed_s": "0.1", "event": "enqueue",
                 "queue_length_packets": "1", "sojourn_ms": "",
                 "drops": "0", "marks": "0", "bytes": "1000",
                 "forwarded_bytes": "0"},
                {"elapsed_s": "0.2", "event": "forward",
                 "queue_length_packets": "0", "sojourn_ms": "3.000",
                 "drops": "0", "marks": "0", "bytes": "1000",
                 "forwarded_bytes": "1000"},
            ],
        )

        stats = self.experiment.parse_telemetry(path)

        self.assertEqual(stats["forward"], 1)
        self.assertEqual(stats["drops_codel"], 0)
        self.assertEqual(stats["avg_queue_samples"], 0)
        self.assertIsNone(stats["avg_queue_mean"])
        self.assertAlmostEqual(stats["median_sojourn_ms"], 3.0)

    def test_unknown_event_rejected(self):
        path = self.tmp / "router_telemetry.csv"
        write_csv(
            path, TELEMETRY_FIELDS,
            [telemetry_row("0.1", "bogus", 0)],
        )

        with self.assertRaises(ValueError):
            self.experiment.parse_telemetry(path)


class TestSenderTimeoutParsing(Phase5TestCase):
    def test_timeout_rows_counted(self):
        path = self.tmp / "sender_log.csv"
        write_csv(
            path, LOCKSTEP_FIELDS,
            [
                {"timestamp": "t0", "event": "PACKET_SENT",
                 "packet_type": "DATA", "seq": 0, "ack": "", "bytes": 1000,
                 "attempt": 1, "details": ""},
                {"timestamp": "t1", "event": "RETRANSMISSION",
                 "packet_type": "DATA", "seq": 0, "ack": "", "bytes": 1000,
                 "attempt": 2, "details": "attempt=2"},
                {"timestamp": "t2", "event": "CWND_UPDATE",
                 "packet_type": "", "seq": "", "ack": "", "bytes": "",
                 "attempt": "",
                 "details": "cwnd=1.000 reason=timeout phase=slow_start"},
                {"timestamp": "t3", "event": "CWND_UPDATE",
                 "packet_type": "", "seq": "", "ack": "", "bytes": "",
                 "attempt": "",
                 "details": "cwnd=2.000 reason=ack phase=slow_start"},
            ],
        )

        self.assertEqual(
            self.experiment.parse_sender_timeouts(path), 1
        )

    def test_missing_log_returns_zero(self):
        self.assertEqual(
            self.experiment.parse_sender_timeouts(
                self.tmp / "missing.csv"
            ),
            0,
        )


class TestSummaryBuilding(Phase5TestCase):
    def _fake_telemetry(self, median, mean_queue, avg_queue_mean):
        return {
            "median_sojourn_ms": median,
            "mean_sojourn_ms": median,
            "p95_sojourn_ms": median,
            "p99_sojourn_ms": median,
            "max_queue": int(mean_queue) + 5,
            "mean_queue": mean_queue,
            "drop": 2,
            "drops_full": 1,
            "drops_red": 1,
            "drops_codel": 0,
            "mark": 0,
            "marks_red": 0,
            "marks_codel": 0,
            "avg_queue_mean": avg_queue_mean,
            "avg_queue_max": avg_queue_mean,
        }

    def test_summary_criteria_and_files(self):
        args = self.experiment.build_arg_parser().parse_args([])
        plan = self.experiment.phase4.MININET_PLAN
        parameters = self.experiment.build_queue_parameters(args, "red")

        results = {}
        for aqm, median, mean_queue, avg_queue in (
            ("droptail", 40.0, 100.0, None),
            ("red", 30.0, 50.0, 40.0),
            ("codel", 6.0, 10.0, None),
        ):
            results[aqm] = {
                "queue_parameters": parameters,
                "success": True,
                "failure_reasons": [],
                "duration_s": 60.0,
                "sha256_match": True,
                "sender": None,
                "timeouts": 0,
                "telemetry": self._fake_telemetry(
                    median, mean_queue, avg_queue
                ),
            }

        summary = self.experiment.build_summary(
            args, plan, results, 30_000_000, "abc"
        )

        self.assertTrue(summary["overall_success"])
        self.assertAlmostEqual(
            summary["runs"]["red"]["goodput_mbps"], 4.0
        )
        criteria = summary["criteria"]
        self.assertTrue(criteria["codel_median_within_4x_target"])
        self.assertTrue(criteria["red_average_between_half_min_and_double_max"])
        self.assertTrue(criteria["droptail_queue_largest"])

        run_dir = self.tmp / "run"
        self.experiment.write_summary_files(run_dir, summary)
        self.assertTrue(
            (run_dir / "comparison" / "summary.json").exists()
        )
        text = (run_dir / "comparison" / "summary.txt").read_text()
        self.assertIn("CoDel median sojourn", text)
        self.assertIn("DropTail expected largest", text)


class TestPlotScriptSupport(unittest.TestCase):
    def setUp(self):
        self.plot = load_module(
            "plot_aqm_comparison",
            REPO_ROOT / "scripts" / "plot_aqm_comparison.py",
        )
        self.tmp = Path(tempfile.mkdtemp(prefix="p5plot_"))

    def _manual_run(self, name, sojourn="5.000", signal_event=None,
                    reason="", drops=0, marks=0, sender_complete=True,
                    receiver_complete=True, with_output=True,
                    output_bytes=b"payload-bytes", input_bytes=None,
                    network=None):
        run = self.tmp / name
        run.mkdir()
        rows = [
            telemetry_row("0.001", "enqueue", 1, avg="0.500"),
            telemetry_row("0.002", "forward", 0, sojourn=sojourn,
                          forwarded_bytes=1000),
        ]
        if signal_event:
            rows.append(
                telemetry_row("0.003", signal_event, 0, sojourn="1.000",
                              drops=drops, marks=marks,
                              forwarded_bytes=1000, reason=reason)
            )
        write_csv(run / "router_telemetry.csv", TELEMETRY_FIELDS, rows)

        sender_log = [
            {"timestamp": "2026-11-15T10:00:00", "event": "PACKET_SENT",
             "packet_type": "DATA", "seq": 0, "ack": "", "bytes": 13,
             "attempt": 1, "details": ""},
            {"timestamp": "2026-11-15T10:00:01", "event": "RETRANSMISSION",
             "packet_type": "DATA", "seq": 0, "ack": "", "bytes": 13,
             "attempt": 2, "details": "attempt=2"},
            {"timestamp": "2026-11-15T10:00:05", "event": "CWND_UPDATE",
             "packet_type": "", "seq": "", "ack": "", "bytes": "",
             "attempt": "",
             "details": "cwnd=1.000 reason=timeout phase=slow_start"},
        ]
        if sender_complete:
            sender_log.append(
                {"timestamp": "2026-11-15T10:00:10",
                 "event": "TRANSFER_COMPLETED", "packet_type": "FIN",
                 "seq": 1, "ack": "", "bytes": 13, "attempt": "",
                 "details": ""}
            )
        write_csv(run / "manual_sender_log.csv", LOCKSTEP_FIELDS,
                  sender_log)

        receiver_log = [
            {"timestamp": "2026-11-15T10:00:00", "event": "SYN_RECEIVED",
             "packet_type": "SYN", "seq": "", "ack": "", "bytes": "",
             "attempt": "", "details": ""},
            {"timestamp": "2026-11-15T10:00:05", "event": "DATA_RECEIVED",
             "packet_type": "DATA", "seq": "", "ack": "", "bytes": 13,
             "attempt": "", "details": ""},
        ]
        if receiver_complete:
            receiver_log.append(
                {"timestamp": "2026-11-15T10:00:10",
                 "event": "TRANSFER_COMPLETED", "packet_type": "",
                 "seq": "", "ack": "", "bytes": 13, "attempt": "",
                 "details": ""}
            )
        write_csv(run / "manual_receiver_log.csv", LOCKSTEP_FIELDS,
                  receiver_log)

        (run / "input.bin").write_bytes(
            input_bytes if input_bytes is not None else b"payload-bytes"
        )
        if with_output:
            (run / "out.bin").write_bytes(output_bytes)
        metadata = {"network": {"file_bytes": 13}}
        if network:
            metadata["network"].update(network)
        (run / "run_metadata.json").write_text(json.dumps(metadata))
        return run

    def _make_three(self):
        return {
            "droptail": self._manual_run("manual_droptail"),
            "red": self._manual_run(
                "manual_red", signal_event="mark", marks=3, reason="red"
            ),
            "codel": self._manual_run(
                "manual_codel", signal_event="drop", drops=2,
                reason="codel",
            ),
        }

    def test_resolve_layout_explicit_directories(self):
        parser = self.plot.build_arg_parser()
        runs = self._make_three()
        args = parser.parse_args([
            "--droptail-dir", str(runs["droptail"]),
            "--red-dir", str(runs["red"]),
            "--codel-dir", str(runs["codel"]),
        ])

        layout, run_dir = self.plot.resolve_layout(args, parser)

        self.assertIsNone(run_dir)
        self.assertEqual(layout["red"], runs["red"])

    def test_resolve_layout_requires_all_or_run_directory(self):
        parser = self.plot.build_arg_parser()
        args = parser.parse_args(["--red-dir", str(self.tmp)])
        with self.assertRaises(SystemExit):
            self.plot.resolve_layout(args, parser)

    def test_run_metrics_manual_style_directory(self):
        run = self._manual_run(
            "manual_red", sojourn="7.500", signal_event="mark", marks=1,
            reason="red",
        )
        metrics = self.plot.run_metrics(run, "red")

        self.assertTrue(metrics["success"])
        self.assertAlmostEqual(metrics["goodput_mbps"], 13 * 8 / 10 / 1e6)
        self.assertTrue(metrics["sha256_match"])
        self.assertEqual(metrics["sojourn_samples"], 1)
        self.assertAlmostEqual(metrics["median_sojourn_ms"], 7.5)
        self.assertEqual(metrics["marks"], 1)
        self.assertEqual(metrics["marks_red"], 1)
        self.assertEqual(metrics["retransmissions"], 1)
        self.assertEqual(metrics["timeouts"], 1)

    def test_manual_success_false_without_sender_completion(self):
        run = self._manual_run("no_sender", sender_complete=False)
        self.assertFalse(self.plot.run_metrics(run, "red")["success"])

    def test_manual_success_false_without_receiver_completion(self):
        run = self._manual_run("no_receiver", receiver_complete=False)
        self.assertFalse(self.plot.run_metrics(run, "red")["success"])

    def test_manual_success_false_without_output_file(self):
        run = self._manual_run("no_output", with_output=False)
        metrics = self.plot.run_metrics(run, "red")

        self.assertFalse(metrics["success"])
        self.assertIsNone(metrics["sha256_match"])

    def test_manual_success_false_with_wrong_output_size(self):
        run = self._manual_run("short_output", output_bytes=b"payload")
        metrics = self.plot.run_metrics(run, "red")

        self.assertFalse(metrics["success"])
        self.assertFalse(metrics["sha256_match"])

    def test_manual_success_false_on_sha256_mismatch(self):
        run = self._manual_run(
            "corrupt_output", output_bytes=b"payload-byteX"
        )
        metrics = self.plot.run_metrics(run, "red")

        self.assertFalse(metrics["success"])
        self.assertFalse(metrics["sha256_match"])

    def test_automated_result_success_takes_precedence(self):
        run = self._manual_run("automated")
        path = run / "run_metadata.json"
        metadata = json.loads(path.read_text())

        metadata["result"] = {"success": False, "sha256_match": False}
        path.write_text(json.dumps(metadata))
        metrics = self.plot.run_metrics(run, "red")
        self.assertFalse(metrics["success"])
        self.assertFalse(metrics["sha256_match"])

        metadata["result"] = {"success": True, "sha256_match": None}
        path.write_text(json.dumps(metadata))
        metrics = self.plot.run_metrics(run, "red")
        self.assertTrue(metrics["success"])
        self.assertTrue(metrics["sha256_match"])

    def test_main_writes_plot_and_metrics_for_explicit_dirs(self):
        runs = self._make_three()
        output = self.tmp / "comparison" / "aqm_comparison.png"

        exit_code = self.plot.main([
            "--droptail-dir", str(runs["droptail"]),
            "--red-dir", str(runs["red"]),
            "--codel-dir", str(runs["codel"]),
            "--output", str(output),
        ])

        self.assertEqual(exit_code, 0)
        self.assertTrue(output.exists())
        metrics_text = (output.parent / "metrics.txt").read_text()
        self.assertIn("CoDel", metrics_text)
        self.assertIn("time-weighted", metrics_text)


class TestMainRunId(Phase5TestCase):
    def test_automated_run_id_defaults_to_timestamp(self):
        captured = {}

        def fake_run(args):
            captured["run_id"] = args.run_id
            return 0

        original = self.experiment.run_experiments
        self.experiment.run_experiments = fake_run
        try:
            exit_code = self.experiment.main([])
        finally:
            self.experiment.run_experiments = original

        self.assertEqual(exit_code, 0)
        self.assertNotIn("manual_", captured["run_id"])

    def test_interactive_run_id_prefixes_algorithm(self):
        captured = {}

        def fake_run(args):
            captured["run_id"] = args.run_id
            return 0

        original = self.experiment.run_interactive
        self.experiment.run_interactive = fake_run
        try:
            exit_code = self.experiment.main(
                ["--interactive", "--aqm", "codel"]
            )
        finally:
            self.experiment.run_interactive = original

        self.assertEqual(exit_code, 0)
        self.assertTrue(captured["run_id"].startswith("manual_codel_"))


if __name__ == "__main__":
    unittest.main()
