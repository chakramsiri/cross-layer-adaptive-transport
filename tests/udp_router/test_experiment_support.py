
import csv
import importlib.util
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT = REPO_ROOT / "tests" / "mininet" / "phase4_experiments.py"
NODE = REPO_ROOT / "tests" / "mininet" / "phase2_node.py"


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


class TestPercentile(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase4_experiments", EXPERIMENT)

    def test_exact_ranks(self):
        self.assertEqual(
            self.experiment.percentile([1, 2, 3, 4, 5], 50), 3
        )

    def test_interpolated_rank(self):
        self.assertAlmostEqual(
            self.experiment.percentile([10, 20, 30, 40], 25), 17.5
        )

    def test_extremes(self):
        values = [5, 1, 9, 3]
        self.assertEqual(self.experiment.percentile(values, 0), 1)
        self.assertEqual(self.experiment.percentile(values, 100), 9)

    def test_empty_rejected(self):
        with self.assertRaises(ValueError):
            self.experiment.percentile([], 50)


class TestRttStats(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase4_experiments", EXPERIMENT)

    def test_none_for_no_samples(self):
        self.assertIsNone(self.experiment.rtt_stats([]))

    def test_summary_fields(self):
        stats = self.experiment.rtt_stats([10.0, 20.0, 30.0, 40.0])
        self.assertEqual(stats["n"], 4)
        self.assertEqual(stats["min_ms"], 10.0)
        self.assertEqual(stats["max_ms"], 40.0)
        self.assertAlmostEqual(stats["mean_ms"], 25.0)
        self.assertAlmostEqual(stats["p50_ms"], 25.0)
        self.assertAlmostEqual(stats["p95_ms"], 38.5)


class TestSenderLogParsing(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase4_experiments", EXPERIMENT)
        self.tmp = Path(tempfile.mkdtemp(prefix="p4support_"))

    def test_rtt_samples_retransmissions_and_completion(self):
        path = self.tmp / "sender_log.csv"
        write_csv(
            path,
            ["timestamp", "event", "packet_type", "seq", "ack", "bytes",
             "attempt", "details"],
            [
                {"timestamp": "t0", "event": "PACKET_SENT",
                 "packet_type": "DATA", "seq": 0, "ack": "", "bytes": 1000,
                 "attempt": 1, "details": ""},
                {"timestamp": "t1", "event": "RTT_SAMPLE",
                 "packet_type": "DATA", "seq": 0, "ack": "", "bytes": "",
                 "attempt": "", "details": "rtt=0.042000 srtt=0.042000 "
                 "rttvar=0.000000 rto=1.000000"},
                {"timestamp": "t2", "event": "RTT_SAMPLE",
                 "packet_type": "DATA", "seq": 1, "ack": "", "bytes": "",
                 "attempt": "", "details": "rtt=0.060000 srtt=0.044000 "
                 "rttvar=0.004500 rto=1.018000"},
                {"timestamp": "t3", "event": "RETRANSMISSION",
                 "packet_type": "DATA", "seq": 2, "ack": "", "bytes": 1000,
                 "attempt": 2, "details": "attempt=2"},
                {"timestamp": "t4", "event": "TRANSFER_COMPLETED",
                 "packet_type": "FIN", "seq": 5, "ack": "", "bytes": 5000,
                 "attempt": "", "details": ""},
            ],
        )

        parsed = self.experiment.parse_sender_log(path)
        self.assertEqual(parsed["rtt_samples_ms"], [42.0, 60.0])
        self.assertEqual(parsed["retransmissions"], 1)
        self.assertTrue(parsed["completed"])

    def test_empty_log_yields_no_completion(self):
        path = self.tmp / "sender_log.csv"
        write_csv(
            path,
            ["timestamp", "event", "packet_type", "seq", "ack", "bytes",
             "attempt", "details"],
            [],
        )

        parsed = self.experiment.parse_sender_log(path)
        self.assertFalse(parsed["completed"])
        self.assertEqual(parsed["retransmissions"], 0)


class TestReceiverLogParsing(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase4_experiments", EXPERIMENT)
        self.tmp = Path(tempfile.mkdtemp(prefix="p4support_"))

    def test_completion_and_bytes(self):
        path = self.tmp / "receiver_log.csv"
        write_csv(
            path,
            ["timestamp", "event", "packet_type", "seq", "ack", "bytes",
             "attempt", "details"],
            [
                {"timestamp": "t0", "event": "TRANSFER_COMPLETED",
                 "packet_type": "", "seq": "", "ack": "", "bytes": 10485760,
                 "attempt": "", "details": "done"},
            ],
        )

        parsed = self.experiment.parse_receiver_log(path)
        self.assertTrue(parsed["completed"])
        self.assertEqual(parsed["bytes"], 10485760)


class TestTelemetryParsing(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase4_experiments", EXPERIMENT)
        self.tmp = Path(tempfile.mkdtemp(prefix="p4support_"))

    def test_counts_and_statistics(self):
        path = self.tmp / "router_telemetry.csv"
        rows = [
            ["0.000000", "enqueue", "1", "", "0", "0", "1000", "0"],
            ["0.001000", "enqueue", "2", "", "0", "0", "1000", "0"],
            ["0.002000", "forward", "1", "2.500", "0", "0", "1000", "1000"],
            ["0.003000", "enqueue", "2", "", "0", "0", "1000", "1000"],
            ["0.004000", "drop", "2", "", "1", "0", "1000", "1000"],
            ["0.005000", "forward", "1", "4.000", "1", "0", "1000", "2000"],
            ["0.006000", "forward", "0", "1.000", "1", "0", "1000", "3000"],
        ]
        write_csv(
            path,
            ["elapsed_s", "event", "queue_length_packets", "sojourn_ms",
             "drops", "marks", "bytes", "forwarded_bytes"],
            [
                dict(zip(
                    ["elapsed_s", "event", "queue_length_packets",
                     "sojourn_ms", "drops", "marks", "bytes",
                     "forwarded_bytes"],
                    row,
                ))
                for row in rows
            ],
        )

        stats = self.experiment.parse_telemetry(path)
        self.assertEqual(stats["rows"], 7)
        self.assertEqual(stats["enqueue"], 3)
        self.assertEqual(stats["forward"], 3)
        self.assertEqual(stats["drop"], 1)
        self.assertEqual(stats["max_queue"], 2)
        self.assertAlmostEqual(stats["mean_queue"], 5 / 3)
        self.assertAlmostEqual(stats["mean_sojourn_ms"], 2.5)
        self.assertEqual(stats["max_sojourn_ms"], 4.0)
        self.assertEqual(stats["marks"], 0)
        self.assertEqual(stats["forwarded_bytes"], 3000)

    def test_unknown_event_rejected(self):
        path = self.tmp / "router_telemetry.csv"
        write_csv(
            path,
            ["elapsed_s", "event", "queue_length_packets", "sojourn_ms",
             "drops", "marks", "bytes", "forwarded_bytes"],
            [dict(zip(
                ["elapsed_s", "event", "queue_length_packets", "sojourn_ms",
                 "drops", "marks", "bytes", "forwarded_bytes"],
                ["0.0", "bogus", "0", "", "0", "0", "0", "0"],
            ))],
        )

        with self.assertRaises(ValueError):
            self.experiment.parse_telemetry(path)


class TestRunDirReservation(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase4_experiments", EXPERIMENT)
        self.root = Path(tempfile.mkdtemp(prefix="p4runs_"))

    def test_creates_unique_directory(self):
        run_dir = self.experiment.prepare_run_dir(self.root, "runA")
        self.assertTrue(run_dir.is_dir())

    def test_refuses_existing_directory(self):
        self.experiment.prepare_run_dir(self.root, "runB")

        with self.assertRaises(SystemExit):
            self.experiment.prepare_run_dir(self.root, "runB")


class TestArgValidation(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase4_experiments", EXPERIMENT)

    def test_defaults_are_valid(self):
        args = self.experiment.build_arg_parser().parse_args([])
        self.experiment.validate_args(args)


    def test_invalid_rate_rejected(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--rate-mbps", "0"]
        )
        with self.assertRaises(SystemExit):
            self.experiment.validate_args(args)

    def test_invalid_capacity_rejected(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--small-capacity", "0"]
        )
        with self.assertRaises(SystemExit):
            self.experiment.validate_args(args)

    def test_address_plans_are_distinct_per_topology(self):
        self.assertNotEqual(
            self.experiment.MININET_PLAN, self.experiment.LOOPBACK_PLAN
        )
        self.assertEqual(
            self.experiment.MININET_PLAN["router_sender_face"],
            ("10.0.0.2", 5001),
        )
        self.assertEqual(
            self.experiment.LOOPBACK_PLAN["router_sender_face"],
            ("127.0.0.1", 5001),
        )


class TestPhase2NodeWindowFlag(unittest.TestCase):
    """--window-size must be opt-in and leave defaults untouched."""

    def setUp(self):
        self.node = load_module("phase2_node_test_target", NODE)
        import transport.sender as sender_module

        self.sender_module = sender_module
        self.original_window = sender_module.WINDOW_SIZE
        self.original_send_file = sender_module.send_file
        self.original_cwd = os.getcwd()
        self.addCleanup(self.restore)

    def restore(self):
        self.sender_module.WINDOW_SIZE = self.original_window
        self.sender_module.send_file = self.original_send_file
        os.chdir(self.original_cwd)

    def test_default_leaves_transport_window_untouched(self):
        self.sender_module.send_file = lambda *args, **kwargs: True

        args = self.node.build_parser().parse_args(
            ["sender", "file.bin", "127.0.0.1", "5001", "log.csv"]
        )
        self.assertIsNone(args.window_size)

        self.node.run_sender(args)
        self.assertEqual(self.sender_module.WINDOW_SIZE, 32)

    def test_flag_patches_sender_window(self):
        self.sender_module.send_file = lambda *args, **kwargs: True

        args = self.node.build_parser().parse_args(
            ["sender", "file.bin", "127.0.0.1", "5001", "log.csv",
             "--window-size", "400"]
        )

        self.node.run_sender(args)
        self.assertEqual(self.sender_module.WINDOW_SIZE, 400)

    def test_invalid_window_rejected(self):
        self.sender_module.send_file = lambda *args, **kwargs: True

        args = self.node.build_parser().parse_args(
            ["sender", "file.bin", "127.0.0.1", "5001", "log.csv",
             "--window-size", "0"]
        )

        with self.assertRaises(SystemExit):
            self.node.run_sender(args)


class _StubIntf:
    def __init__(self, name):
        self.name = name


class _StubNode:
    def __init__(self, name, interfaces):
        self.name = name
        self._ips = dict(interfaces)
        self._intfs = [_StubIntf(name_) for name_, _ in interfaces]

    def intfList(self):
        return self._intfs

    def IP(self, intf):
        return self._ips.get(intf.name)


class _RecordingNode:
    def __init__(self):
        self.commands = []

    def cmd(self, command):
        self.commands.append(command)


class _StubContext:
    def __init__(self):
        self.teardown_called = False
        self.nodes = {
            "sender": _RecordingNode(),
            "receiver": _RecordingNode(),
            "router": _RecordingNode(),
        }

    def teardown(self):
        self.teardown_called = True


class _StubProcess:
    def __init__(self, running=True):
        self._running = running
        self.terminated = False
        self.killed = False
        self.returncode = 0

    def poll(self):
        return None if self._running else 0

    def terminate(self):
        self.terminated = True
        self._running = False

    def wait(self, timeout=None):
        return 0

    def kill(self):
        self.killed = True


class TestInteractiveModeSupport(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase4_interactive", EXPERIMENT)
        self.tmp = Path(tempfile.mkdtemp(prefix="p4interactive_"))

    def test_interactive_requires_mininet_topology(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--interactive", "--topology", "relay"]
        )
        with self.assertRaises(SystemExit):
            self.experiment.validate_args(args)

    def test_interactive_accepts_mininet_default(self):
        args = self.experiment.build_arg_parser().parse_args(["--interactive"])
        self.assertEqual(
            self.experiment.validate_args(args),
            ["base", "large", "small"],
        )
        self.assertTrue(args.interactive)

    def test_queue_capacity_validation(self):
        for bad in ("0", "-3"):
            args = self.experiment.build_arg_parser().parse_args(
                ["--interactive", "--queue-capacity", bad]
            )
            with self.assertRaises(SystemExit):
                self.experiment.validate_args(args)

        args = self.experiment.build_arg_parser().parse_args(
            ["--interactive", "--queue-capacity", "75"]
        )
        self.assertEqual(
            self.experiment.validate_args(args),
            ["base", "large", "small"],
        )

    def test_queue_capacity_defaults_to_large_capacity(self):
        args = self.experiment.build_arg_parser().parse_args(
            ["--interactive"]
        )
        self.assertIsNone(args.queue_capacity)
        capacity = (
            args.queue_capacity
            if args.queue_capacity is not None
            else args.large_capacity
        )
        self.assertEqual(capacity, 500)

    def test_run_id_defaults_to_none_for_resolution_in_main(self):
        args = self.experiment.build_arg_parser().parse_args([])
        self.assertIsNone(args.run_id)

    def test_router_command_builder_matches_automated_invocation(self):
        args = self.experiment.build_arg_parser().parse_args([])
        run_dir = self.tmp / "runA"

        command = self.experiment.build_router_command(
            "python3", self.experiment.MININET_PLAN, args, run_dir,
            queue_capacity=250,
        )

        self.assertIn("-m", command)
        self.assertIn("router.forwarder", command)
        self.assertIn("--sender-face", command)
        self.assertIn("10.0.0.2:5001", command)
        self.assertIn("--receiver-face", command)
        self.assertIn("10.0.1.2:5001", command)
        self.assertIn("--receiver", command)
        self.assertIn("10.0.1.1:5001", command)
        self.assertIn("--rate-mbps", command)
        self.assertIn("5.0", command)
        self.assertIn("--queue-capacity", command)
        self.assertIn("250", command)
        self.assertIn(str(run_dir / "router_telemetry.csv"), command)
        self.assertIn(str(run_dir / "router.ready"), command)
        self.assertIn("360.0", command)

    def test_router_command_builder_max_seconds_override(self):
        args = self.experiment.build_arg_parser().parse_args([])
        command = self.experiment.build_router_command(
            "python3", self.experiment.MININET_PLAN, args, self.tmp,
            queue_capacity=500, max_seconds=86400.0,
        )
        self.assertIn("86400.0", command)

    def test_manual_endpoint_commands(self):
        args = self.experiment.build_arg_parser().parse_args([])
        run_dir = str(self.tmp / "manualrun")
        interfaces = {
            "sender": "h1-eth0",
            "router_sender": "r-eth0",
            "router_receiver": "r-eth1",
            "receiver": "h2-eth0",
        }

        commands = self.experiment.build_manual_endpoint_commands(
            args, self.experiment.MININET_PLAN, run_dir, interfaces
        )
        cli_text = "\n".join(commands["cli"])
        shell_text = "\n".join(commands["shell"])

        for interface in interfaces.values():
            self.assertIn(f"-i {interface}", cli_text)

        self.assertIn(f"tcpdump -i h1-eth0 -w {run_dir}/h1_capture.pcap &",
                      cli_text)
        self.assertIn(
            f"tcpdump -i r-eth0 -w {run_dir}/router_sender_capture.pcap &",
            cli_text,
        )
        self.assertIn(
            f"tcpdump -i r-eth1 -w {run_dir}/router_receiver_capture.pcap &",
            cli_text,
        )
        self.assertIn(f"tcpdump -i h2-eth0 -w {run_dir}/h2_capture.pcap &",
                      cli_text)

        receiver_line = next(
            line for line in commands["cli"] if " receiver " in line
        )
        self.assertTrue(receiver_line.startswith("mininet> h2 python3 -u"))
        self.assertIn(str(self.experiment.NODE), receiver_line)
        self.assertIn(f"{run_dir}/out.bin", receiver_line)
        self.assertIn(" 0.0.0.0 5001 ", receiver_line)
        self.assertIn(f"{run_dir}/manual_receiver_log.csv", receiver_line)
        self.assertIn("--rwnd-bytes 800000", receiver_line)
        self.assertIn(
            f"</dev/null > {run_dir}/manual_receiver_stdout.log 2>&1",
            receiver_line,
        )
        self.assertTrue(
            receiver_line.endswith(f"& echo $! > {run_dir}/receiver.pid")
        )

        sender_line = next(
            line for line in commands["cli"] if " sender " in line
        )
        self.assertTrue(sender_line.startswith("mininet> h1 python3 -u"))
        self.assertIn(f"{run_dir}/input.bin", sender_line)
        self.assertIn(" 10.0.0.2 5001 ", sender_line)
        self.assertIn(f"{run_dir}/manual_sender_log.csv", sender_line)
        self.assertIn("--window-size 400", sender_line)
        self.assertIn("--initial-ssthresh 20", sender_line)
        self.assertIn(
            f"</dev/null > {run_dir}/manual_sender_stdout.log 2>&1",
            sender_line,
        )
        self.assertTrue(
            sender_line.endswith(f"& echo $! > {run_dir}/sender.pid")
        )

        for pid_file in ("h1_capture", "router_sender_capture",
                         "router_receiver_capture", "h2_capture"):
            self.assertIn(
                f"& echo $! > {run_dir}/{pid_file}.pid", cli_text
            )

        self.assertIn(f"sh tail -f {run_dir}/router_telemetry.csv", cli_text)

        self.assertNotIn("pkill", cli_text)
        self.assertNotIn("pkill", shell_text)
        for pid_file in ("h1_capture", "router_sender_capture",
                         "router_receiver_capture", "h2_capture"):
            self.assertIn(f"cat {run_dir}/{pid_file}.pid", shell_text)
        self.assertIn("grep -aq tcpdump /proc/$p/cmdline && kill $p",
                      shell_text)
        self.assertIn("grep -aq phase2_node.py /proc/$p/cmdline && kill $p",
                      shell_text)

        self.assertIn(f"sha256sum {run_dir}/out.bin", shell_text)
        self.assertIn(f"tcpdump -r {run_dir}/h1_capture.pcap", shell_text)
        self.assertIn(f"head -30 {run_dir}/router_telemetry.csv", shell_text)

    def _stub_topology_nodes(self):
        """Stub nodes mirroring MininetContext's live topology."""
        return {
            "sender": _StubNode(
                "h1", [("lo", "127.0.0.1"), ("h1-eth0", "10.0.0.1")]
            ),
            "router": _StubNode(
                "r",
                [
                    ("lo", "127.0.0.1"),
                    ("r-eth0", "10.0.0.2"),
                    ("r-eth1", "10.0.1.2"),
                ],
            ),
            "receiver": _StubNode(
                "h2", [("lo", "127.0.0.1"), ("h2-eth0", "10.0.1.1")]
            ),
        }

    def test_capture_interface_discovery_matches_live_topology(self):
        nodes = self._stub_topology_nodes()

        interfaces = self.experiment.discover_capture_interfaces(
            nodes, self.experiment.MININET_PLAN
        )

        self.assertEqual(interfaces["sender"], "h1-eth0")
        self.assertEqual(interfaces["router_sender"], "r-eth0")
        self.assertEqual(interfaces["router_receiver"], "r-eth1")
        self.assertEqual(interfaces["receiver"], "h2-eth0")

    def test_interface_discovery_failure_aborts_with_clear_error(self):
        nodes = self._stub_topology_nodes()
        nodes["sender"] = _StubNode(
            "h1", [("lo", "127.0.0.1"), ("h1-eth9", "192.168.99.1")]
        )

        with self.assertRaises(SystemExit) as caught:
            self.experiment.discover_capture_interfaces(
                nodes, self.experiment.MININET_PLAN
            )

        message = str(caught.exception)
        self.assertIn("h1", message)
        self.assertIn("sender capture", message)
        self.assertIn("aborting", message)

    def test_interface_discovery_skips_interfaces_without_ip(self):
        nodes = self._stub_topology_nodes()
        nodes["router"] = _StubNode(
            "r",
            [("r-eth0", None), ("r-eth1", "10.0.1.2")],
        )

        with self.assertRaises(SystemExit):
            self.experiment.discover_capture_interfaces(
                nodes, self.experiment.MININET_PLAN
            )

        nodes["router"] = _StubNode(
            "r", [("r-eth0", "10.0.0.2"), ("r-eth1", "10.0.1.2")]
        )
        interfaces = self.experiment.discover_capture_interfaces(
            nodes, self.experiment.MININET_PLAN
        )
        self.assertEqual(interfaces["router_sender"], "r-eth0")

    def test_manual_launch_commands_redirect_output_and_record_pid(self):
        args = self.experiment.build_arg_parser().parse_args([])
        run_dir = str(self.tmp / "launchrun")
        interfaces = {
            "sender": "h1-eth0",
            "router_sender": "r-eth0",
            "router_receiver": "r-eth1",
            "receiver": "h2-eth0",
        }
        commands = self.experiment.build_manual_endpoint_commands(
            args, self.experiment.MININET_PLAN, run_dir, interfaces
        )
        receiver_line = next(
            line for line in commands["cli"] if " receiver " in line
        )
        sender_line = next(
            line for line in commands["cli"] if " sender " in line
        )

        for line, stdout_log, pid_file in (
            (receiver_line, "manual_receiver_stdout.log", "receiver.pid"),
            (sender_line, "manual_sender_stdout.log", "sender.pid"),
        ):
            self.assertIn("</dev/null", line)
            self.assertIn(f"> {run_dir}/{stdout_log} 2>&1", line)
            background = line.index("& echo $! >")
            self.assertLess(line.index("</dev/null"), background)
            self.assertLess(line.index(f"> {run_dir}/{stdout_log}"), background)
            self.assertLess(line.index("2>&1"), background)
            self.assertLess(
                background,
                line.index(f"echo $! > {run_dir}/{pid_file}"),
            )
            self.assertTrue(
                line.endswith(f"& echo $! > {run_dir}/{pid_file}")
            )

    def test_generated_capture_commands_never_contain_i_none(self):
        nodes = self._stub_topology_nodes()
        interfaces = self.experiment.discover_capture_interfaces(
            nodes, self.experiment.MININET_PLAN
        )
        args = self.experiment.build_arg_parser().parse_args([])

        commands = self.experiment.build_manual_endpoint_commands(
            args, self.experiment.MININET_PLAN, str(self.tmp), interfaces
        )
        cli_text = "\n".join(commands["cli"])

        self.assertNotIn("-i None", cli_text)
        self.assertIn("-i h1-eth0", cli_text)
        self.assertIn("-i r-eth0", cli_text)
        self.assertIn("-i r-eth1", cli_text)
        self.assertIn("-i h2-eth0", cli_text)

    def test_manual_command_builder_rejects_missing_interface(self):
        args = self.experiment.build_arg_parser().parse_args([])
        interfaces = {
            "sender": "h1-eth0",
            "router_sender": "r-eth0",
            "router_receiver": None,
            "receiver": "h2-eth0",
        }

        with self.assertRaises(ValueError):
            self.experiment.build_manual_endpoint_commands(
                args, self.experiment.MININET_PLAN, str(self.tmp),
                interfaces,
            )

    def test_make_deterministic_input_is_reproducible(self):
        first = self.tmp / "first.bin"
        second = self.tmp / "second.bin"

        self.experiment.make_deterministic_input(first, 4096)
        self.experiment.make_deterministic_input(second, 4096)

        self.assertEqual(
            self.experiment.sha256_file(first),
            self.experiment.sha256_file(second),
        )
        self.assertEqual(first.stat().st_size, 4096)

    def test_make_deterministic_input_seed_changes_bytes(self):
        first = self.tmp / "a.bin"
        second = self.tmp / "b.bin"

        self.experiment.make_deterministic_input(first, 4096)
        self.experiment.make_deterministic_input(
            second, 4096, seed=999
        )

        self.assertNotEqual(
            self.experiment.sha256_file(first),
            self.experiment.sha256_file(second),
        )

    def test_topology_lines_lists_names_interfaces_and_ips(self):
        nodes = {
            "sender": _StubNode("h1", [("h1-eth0", "10.0.0.1")]),
            "router": _StubNode(
                "r", [("r-eth0", "10.0.0.2"), ("r-eth1", "10.0.1.2")]
            ),
            "receiver": _StubNode("h2", [("h2-eth0", "10.0.1.1")]),
        }

        lines = self.experiment.topology_lines(nodes)
        text = "\n".join(lines)

        self.assertIn("node h1", text)
        self.assertIn("node r", text)
        self.assertIn("node h2", text)
        self.assertIn("h1-eth0: 10.0.0.1", text)
        self.assertIn("r-eth0: 10.0.0.2", text)
        self.assertIn("r-eth1: 10.0.1.2", text)
        self.assertIn("h2-eth0: 10.0.1.1", text)

    def test_shutdown_interactive_terminates_exact_session_processes(self):
        context = _StubContext()
        router = _StubProcess(running=True)
        sleeper = subprocess.Popen(
            ["bash", "-c", "exec -a phase2_node.py sleep 30"]
        )
        pid_file = self.tmp / "receiver.pid"
        pid_file.write_text(f"{sleeper.pid}\n")

        try:
            self.experiment.shutdown_interactive(context, router, self.tmp)

            self.assertEqual(sleeper.wait(timeout=5), -15)
            self.assertTrue(router.terminated)
            self.assertFalse(router.killed)
            self.assertTrue(context.teardown_called)
            self.assertFalse(pid_file.exists())
        finally:
            if sleeper.poll() is None:
                sleeper.kill()
                sleeper.wait(timeout=5)

    def test_shutdown_interactive_never_targets_unrelated_processes(self):
        context = _StubContext()
        router = _StubProcess(running=True)
        unrelated = subprocess.Popen(["sleep", "30"])
        pid_file = self.tmp / "h1_capture.pid"
        pid_file.write_text(f"{unrelated.pid}\n")

        try:
            self.experiment.shutdown_interactive(context, router, self.tmp)

            self.assertIsNone(unrelated.poll())
            for node in context.nodes.values():
                for command in node.commands:
                    self.assertNotIn("pkill", command)
            self.assertTrue(router.terminated)
            self.assertTrue(context.teardown_called)
            self.assertFalse(pid_file.exists())
        finally:
            unrelated.kill()
            unrelated.wait(timeout=5)

    def test_shutdown_interactive_without_live_router(self):
        context = _StubContext()
        router = _StubProcess(running=False)

        self.experiment.shutdown_interactive(context, router, self.tmp)

        self.assertFalse(router.terminated)
        self.assertFalse(router.killed)
        self.assertTrue(context.teardown_called)

    def test_terminate_pid_file_signals_the_exact_recorded_process(self):
        sleeper = subprocess.Popen(["sleep", "30"])
        pid_file = self.tmp / "sender.pid"
        pid_file.write_text(f"{sleeper.pid}\n")

        try:
            self.assertTrue(
                self.experiment.terminate_pid_file(pid_file, "sleep")
            )
            self.assertEqual(sleeper.wait(timeout=5), -15)
            self.assertFalse(pid_file.exists())
        finally:
            if sleeper.poll() is None:
                sleeper.kill()
                sleeper.wait(timeout=5)

    def test_terminate_pid_file_refuses_a_reused_or_wrong_pid(self):
        unrelated = subprocess.Popen(["sleep", "30"])
        pid_file = self.tmp / "h2_capture.pid"
        pid_file.write_text(f"{unrelated.pid}\n")

        try:
            self.assertFalse(
                self.experiment.terminate_pid_file(pid_file, "tcpdump")
            )
            self.assertIsNone(unrelated.poll())
            self.assertFalse(pid_file.exists())
        finally:
            unrelated.kill()
            unrelated.wait(timeout=5)

    def test_terminate_pid_file_escapes_to_sigkill(self):
        ready_flag = self.tmp / "sigterm_ignored.ready"
        stubborn = subprocess.Popen(
            [
                sys.executable, "-c",
                "import signal, time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                f"open({str(ready_flag)!r}, 'w').write('ok'); "
                "time.sleep(30)",
            ]
        )
        pid_file = self.tmp / "sender.pid"
        pid_file.write_text(f"{stubborn.pid}\n")

        try:
            deadline = time.monotonic() + 5.0
            while not ready_flag.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(ready_flag.exists())

            self.assertTrue(
                self.experiment.terminate_pid_file(
                    pid_file, "python", timeout=0.5
                )
            )
            self.assertEqual(stubborn.wait(timeout=5), -9)
            self.assertFalse(pid_file.exists())
        finally:
            if stubborn.poll() is None:
                stubborn.kill()
                stubborn.wait(timeout=5)

    def test_terminate_pid_file_handles_missing_and_corrupt_files(self):
        missing = self.tmp / "missing.pid"
        self.assertFalse(
            self.experiment.terminate_pid_file(missing, "tcpdump")
        )
        self.assertFalse(missing.exists())

        corrupt = self.tmp / "corrupt.pid"
        corrupt.write_text("not-a-pid\n")
        self.assertFalse(
            self.experiment.terminate_pid_file(corrupt, "tcpdump")
        )
        self.assertFalse(corrupt.exists())

    def test_main_prefixes_interactive_run_id(self):
        captured = {}

        def fake_interactive(args):
            captured["run_id"] = args.run_id
            return 0

        original = self.experiment.run_interactive
        self.experiment.run_interactive = fake_interactive
        try:
            exit_code = self.experiment.main(["--interactive"])
        finally:
            self.experiment.run_interactive = original

        self.assertEqual(exit_code, 0)
        self.assertTrue(captured["run_id"].startswith("manual_"))


if __name__ == "__main__":
    unittest.main()
