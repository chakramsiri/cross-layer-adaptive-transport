"""Experiment orchestration: metadata generation and failure reporting.

run_experiment() is exercised with fake Mininet hosts and a mocked plot
subprocess so the metadata construction runs against the real command
lists without Mininet.
"""

import ast
import csv
import importlib.util
import os
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENT = (
    REPO_ROOT / "tests" / "mininet" / "congestion_control_experiments.py"
)


def _load_experiment_module():
    spec = importlib.util.spec_from_file_location(
        "congestion_control_experiments", EXPERIMENT
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeTime:
    def __init__(self):
        self._now = 1000.0

    def sleep(self, seconds):
        self._now += seconds

    def monotonic(self):
        self._now += 0.01
        return self._now


class _FakeSubprocess:
    STDOUT = -2
    TimeoutExpired = subprocess.TimeoutExpired

    def __init__(self, plot_returncode):
        self.plot_returncode = plot_returncode
        self.plot_args = None

    def run(self, cmd, check=False):
        self.plot_args = list(cmd)
        return types.SimpleNamespace(returncode=self.plot_returncode)


class _FakeProcess:
    def __init__(self, exit_code, on_wait=None):
        self._exit_code = exit_code
        self._on_wait = on_wait
        self.returncode = None

    def wait(self, timeout=None):
        if self._on_wait is not None:
            self._on_wait()
        self.returncode = self._exit_code
        return self._exit_code

    def kill(self):
        pass


class _FakeHost:
    def __init__(self, role, exit_code, corrupt_output=False,
                 write_output=True, transfer_event="TRANSFER_COMPLETED"):
        self._role = role
        self._exit_code = exit_code
        self._corrupt_output = corrupt_output
        self._write_output = write_output
        self._transfer_event = transfer_event

    def cmd(self, command):
        return ""

    def popen(self, cmd, stdout=None, stderr=None):
        if self._role == "receiver":
            output_path = Path(cmd[3])

            def complete_output():
                if not self._write_output:
                    return
                experiment_input = output_path.parent / "input.bin"
                payload = experiment_input.read_bytes()
                if self._corrupt_output:
                    payload = payload[:-1] + bytes([payload[-1] ^ 0xFF])
                output_path.write_bytes(payload)

            return _FakeProcess(self._exit_code, complete_output)

        log_path = Path(cmd[6])

        def write_sender_log():
            with open(log_path, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([
                    "timestamp", "event", "packet_type", "seq", "ack",
                    "bytes", "attempt", "details",
                ])
                writer.writerow([
                    "2026-01-01T00:00:00+00:00", "PACKET_SENT", "DATA",
                    "0", "", "1000", "1", "",
                ])
                if self._transfer_event is not None:
                    writer.writerow([
                        "2026-01-01T00:00:00+00:00", self._transfer_event,
                        "FIN", "3", "", "3000", "", "",
                    ])

        return _FakeProcess(self._exit_code, write_sender_log)


class _FakeNet:
    def __init__(self):
        self.stop_calls = 0

    def stop(self):
        self.stop_calls += 1


def _make_hosts(corrupt_output=False):
    return (
        _FakeHost("sender", 0),
        _FakeHost("receiver", 0, corrupt_output=corrupt_output),
    )


class RunExperimentTests(unittest.TestCase):
    def setUp(self):
        self.module = _load_experiment_module()
        self.addCleanup(setattr, self.module, "time", self.module.time)
        self.addCleanup(
            setattr, self.module, "subprocess", self.module.subprocess
        )
        self.module.time = _FakeTime()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.results_root = Path(self.tmp.name) / "phase3"
        self.results_root.mkdir(parents=True)

        self.plot = _FakeSubprocess(0)
        self.module.subprocess = self.plot
        self.module.apply_netem = lambda *args: None
        self.module.qdisc_state = lambda *args: ""

    def _run(self, sender_exit=0, receiver_exit=0, corrupt_output=False,
             transfer_event="TRANSFER_COMPLETED"):
        h1 = _FakeHost(
            "sender", sender_exit, transfer_event=transfer_event
        )
        h2 = _FakeHost(
            "receiver", receiver_exit, corrupt_output=corrupt_output
        )
        run_dir = self.module.resolve_results_dir(
            self.results_root, "orchestration_test"
        )
        info = self.module.run_experiment(h1, h2, run_dir, 3000)
        return info, run_dir

    def test_metadata_records_the_actual_plot_command(self):
        info, run_dir = self._run()

        self.assertTrue(info["ok"])
        with open(run_dir / "run_metadata.txt") as file:
            metadata = file.read()

        recorded = [
            line for line in metadata.splitlines()
            if line.startswith("  plot: ")
        ][0][len("  plot: "):]
        self.assertEqual(recorded, " ".join(self.plot.plot_args))
        self.assertIn("cwnd.png", recorded)

    def test_successful_experiment_reports_ok(self):
        info, run_dir = self._run()

        self.assertTrue(info["ok"])
        self.assertTrue((run_dir / "run_metadata.txt").exists())
        self.assertTrue((run_dir / "sha256_verification.txt").exists())
        self.assertTrue((run_dir / "cwnd_stats.txt").exists())

    def test_plot_failure_reports_experiment_failure_but_keeps_metadata(self):
        self.plot.plot_returncode = 1
        info, run_dir = self._run()

        self.assertFalse(info["ok"])
        self.assertTrue((run_dir / "run_metadata.txt").exists())
        self.assertTrue((run_dir / "sha256_verification.txt").exists())

    def test_sender_failure_reports_experiment_failure(self):
        info, run_dir = self._run(sender_exit=1)

        self.assertFalse(info["ok"])
        self.assertTrue((run_dir / "run_metadata.txt").exists())

    def test_receiver_failure_reports_experiment_failure(self):
        info, run_dir = self._run(receiver_exit=1)

        self.assertFalse(info["ok"])
        self.assertTrue((run_dir / "run_metadata.txt").exists())

    def test_failed_transfer_event_rejects_experiment(self):
        info, run_dir = self._run(transfer_event="TRANSFER_FAILED")

        self.assertFalse(info["ok"])
        self.assertTrue(info["sha256_match"])
        self.assertTrue((run_dir / "run_metadata.txt").exists())

    def test_missing_completion_event_rejects_experiment(self):
        info, run_dir = self._run(transfer_event=None)

        self.assertFalse(info["ok"])
        self.assertTrue(info["sha256_match"])
        self.assertTrue((run_dir / "run_metadata.txt").exists())

    def test_missing_receiver_output_reports_integrity_failure(self):
        h1 = _FakeHost("sender", 0)
        h2 = _FakeHost("receiver", 0, write_output=False)
        run_dir = self.module.resolve_results_dir(
            self.results_root, "missing_output_test"
        )

        info = self.module.run_experiment(h1, h2, run_dir, 3000)

        self.assertFalse(info["ok"])
        self.assertEqual(info["sender_exit"], 0)
        self.assertEqual(info["receiver_exit"], 0)
        sha_text = (run_dir / "sha256_verification.txt").read_text()
        self.assertIn("destination missing", sha_text)
        self.assertIn("FAIL", sha_text)
        self.assertFalse((run_dir / "received.bin").exists())
        self.assertTrue((run_dir / "run_metadata.txt").exists())
        self.assertTrue((run_dir / "cwnd_stats.txt").exists())

    def test_sha256_mismatch_reports_experiment_failure(self):
        info, run_dir = self._run(corrupt_output=True)

        self.assertFalse(info["ok"])
        self.assertIn("FAIL", (run_dir / "sha256_verification.txt").read_text())
        self.assertTrue((run_dir / "run_metadata.txt").exists())
        self.assertTrue((run_dir / "sender.out").exists())
        self.assertTrue((run_dir / "receiver.out").exists())
        self.assertTrue((run_dir / "cwnd_stats.txt").exists())


class MainExitStatusTests(unittest.TestCase):
    def setUp(self):
        self.module = _load_experiment_module()
        self.plot = _FakeSubprocess(0)
        self.fake_net = _FakeNet()
        self.addCleanup(setattr, self.module, "subprocess", self.module.subprocess)
        self.addCleanup(setattr, self.module, "time", self.module.time)
        self.addCleanup(setattr, self.module, "build_network", self.module.build_network)
        self.addCleanup(setattr, self.module, "RESULTS_ROOT", self.module.RESULTS_ROOT)
        self.module.subprocess = self.plot
        self.module.time = _FakeTime()
        self.module.apply_netem = lambda *args: None
        self.module.qdisc_state = lambda *args: ""
        self.module.build_network = lambda: (self.fake_net, *_make_hosts())
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.results_root = Path(self.tmp.name) / "phase3"
        self.results_root.mkdir(parents=True)
        self.module.RESULTS_ROOT = self.results_root

    def _call_main(self, results_name):
        original_cwd = os.getcwd()
        original_argv = sys.argv
        sys.argv = [
            "congestion_control_experiments.py",
            "--results-name", results_name,
            "3000",
        ]
        os.chdir(REPO_ROOT)
        self.addCleanup(os.chdir, original_cwd)
        self.addCleanup(setattr, sys, "argv", original_argv)

        with self.assertRaises(SystemExit) as captured:
            self.module.main()

        return captured.exception.code

    def test_successful_experiment_exits_zero(self):
        self.module.build_network = lambda: (
            self.fake_net, *_make_hosts()
        )

        self.assertEqual(self._call_main("main_success"), 0)
        self.assertEqual(self.fake_net.stop_calls, 1)

    def test_failed_experiment_exits_nonzero_and_stops_the_network(self):
        self.module.build_network = lambda: (
            self.fake_net, *_make_hosts(corrupt_output=True)
        )

        self.assertEqual(self._call_main("main_failure"), 1)
        self.assertEqual(self.fake_net.stop_calls, 1)


class PlotCommandOrderTests(unittest.TestCase):
    def test_plot_command_is_assigned_before_the_metadata_write(self):
        tree = ast.parse(EXPERIMENT.read_text())
        run_experiment = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "run_experiment"
        )

        store_lines = []
        load_lines = []
        metadata_line = None

        for node in ast.walk(run_experiment):
            if isinstance(node, ast.Name) and node.id == "plot_cmd":
                if isinstance(node.ctx, ast.Store):
                    store_lines.append(node.lineno)
                else:
                    load_lines.append(node.lineno)
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "write_text"
            ):
                for nested in ast.walk(node):
                    if (
                        isinstance(nested, ast.Constant)
                        and nested.value == "Exact commands used:"
                    ):
                        metadata_line = node.lineno

        self.assertTrue(store_lines, "plot_cmd is never assigned")
        self.assertTrue(load_lines, "plot_cmd is never used")
        self.assertTrue(
            metadata_line, "metadata write_text call not found"
        )
        self.assertEqual(len(store_lines), 1)
        self.assertLess(store_lines[0], metadata_line)
        for load_line in load_lines:
            self.assertGreaterEqual(load_line, store_lines[0])


class ResolveResultsDirTests(unittest.TestCase):
    def setUp(self):
        self.module = _load_experiment_module()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "phase3"
        self.root.mkdir(parents=True)

    def test_creates_a_fresh_nested_directory(self):
        run_dir = self.module.resolve_results_dir(
            self.root, "ssthresh_investigation/ssthresh_8"
        )

        self.assertEqual(
            run_dir, self.root / "ssthresh_investigation" / "ssthresh_8"
        )
        self.assertTrue(run_dir.is_dir())

    def test_rejects_an_existing_directory(self):
        (self.root / "existing").mkdir()

        with self.assertRaises(FileExistsError):
            self.module.resolve_results_dir(self.root, "existing")

    def test_rejects_absolute_paths(self):
        outside = Path(self.tmp.name) / "outside"
        outside.mkdir()

        with self.assertRaises(ValueError):
            self.module.resolve_results_dir(self.root, str(outside))

    def test_rejects_path_traversal(self):
        with self.assertRaises(ValueError):
            self.module.resolve_results_dir(self.root, "../escape")

        self.assertFalse((self.root.parent / "escape").exists())

    def test_existing_siblings_remain_untouched(self):
        sibling = self.module.resolve_results_dir(self.root, "sibling")
        (sibling / "sender_log.csv").write_text("evidence\n")

        self.module.resolve_results_dir(self.root, "other")

        self.assertEqual(
            (sibling / "sender_log.csv").read_text(), "evidence\n"
        )


if __name__ == "__main__":
    unittest.main()
