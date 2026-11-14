
import csv
import importlib.util
import os
import subprocess
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path

from tests.helpers import pick_test_port

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE = REPO_ROOT / "tests" / "mininet" / "phase2_node.py"
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


class Phase2NodeDefaultsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.input_path = Path(self.tmp.name) / "input.bin"
        self.input_path.write_bytes(b"".join(
            bytes([65 + i % 26]) * 1000 for i in range(5)
        ))
        self.output_path = Path(self.tmp.name) / "out.bin"

    @staticmethod
    def _reap(process):
        if process.poll() is None:
            process.kill()
        process.wait()

    def _run_transfer(self, sender_flags, receiver_flags=None):
        port = pick_test_port()
        receiver_log = Path(self.tmp.name) / "receiver.csv"
        sender_log = Path(self.tmp.name) / "sender.csv"

        receiver_cmd = [
            sys.executable, str(NODE), "receiver", str(self.output_path),
            "0.0.0.0", str(port), str(receiver_log),
            "--fin-linger", "0.5",
        ] + (receiver_flags or [])

        receiver = subprocess.Popen(
            receiver_cmd,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        sender = None
        try:
            time.sleep(0.7)
            sender = subprocess.Popen(
                [
                    sys.executable, str(NODE), "sender", str(self.input_path),
                    "127.0.0.1", str(port), str(sender_log),
                ] + sender_flags,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            sender.wait(timeout=30)
            try:
                receiver.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
        finally:
            for process in (sender, receiver):
                if process is not None:
                    self._reap(process)

        with open(sender_log) as file:
            rows = list(csv.DictReader(file))
        return rows, sender.returncode, receiver.returncode

    def _assert_transfer_ok(self, sender_exit, receiver_exit):
        self.assertEqual(sender_exit, 0)
        self.assertEqual(receiver_exit, 0)
        self.assertTrue(self.output_path.exists())
        self.assertEqual(
            self.output_path.read_bytes(), self.input_path.read_bytes()
        )

    def test_node_defaults_do_not_enable_reno(self):
        rows, sender_exit, receiver_exit = self._run_transfer([])

        self._assert_transfer_ok(sender_exit, receiver_exit)
        self.assertEqual(
            [r["event"] for r in rows if r["event"] == "CWND_UPDATE"], []
        )
        self.assertIn("TRANSFER_COMPLETED", [r["event"] for r in rows])

    def test_phase3_flags_enable_reno(self):
        rows, sender_exit, receiver_exit = self._run_transfer(
            ["--initial-cwnd", "1", "--initial-ssthresh", "8"]
        )

        self._assert_transfer_ok(sender_exit, receiver_exit)
        cwnd_rows = [r for r in rows if r["event"] == "CWND_UPDATE"]
        self.assertTrue(cwnd_rows, "no CWND_UPDATE rows with Reno flags")
        self.assertIn("cwnd=1.000", cwnd_rows[0]["details"])
        self.assertIn("TRANSFER_COMPLETED", [r["event"] for r in rows])

    def test_failed_transfer_exits_nonzero(self):
        rows, sender_exit, receiver_exit = self._run_transfer(
            [], receiver_flags=["--rwnd-bytes", "0"]
        )

        self.assertNotEqual(sender_exit, 0)
        self.assertIsNotNone(receiver_exit)
        events = [r["event"] for r in rows]
        self.assertIn("TRANSFER_FAILED", events)
        self.assertNotIn("TRANSFER_COMPLETED", events)

    def test_zero_initial_cwnd_reaches_controller_validation(self):
        rows, sender_exit, receiver_exit = self._run_transfer(
            ["--initial-cwnd", "0"]
        )

        self.assertNotEqual(sender_exit, 0)
        self.assertIsNotNone(receiver_exit)
        self.assertNotIn(
            "TRANSFER_COMPLETED", [r["event"] for r in rows]
        )


class SenderCreationFailureTests(Phase2NodeDefaultsTests):
    def test_receiver_is_reaped_when_sender_creation_fails(self):
        real_popen = subprocess.Popen
        started = []

        class FailingSubprocess:
            DEVNULL = subprocess.DEVNULL
            TimeoutExpired = subprocess.TimeoutExpired

            @staticmethod
            def Popen(cmd, **kwargs):
                if "sender" in cmd:
                    raise OSError("sender spawn failed")
                process = real_popen(cmd, **kwargs)
                started.append(process)
                return process

        module = sys.modules[__name__]
        original = module.subprocess
        module.subprocess = FailingSubprocess
        self.addCleanup(setattr, module, "subprocess", original)

        with self.assertRaises(OSError):
            self._run_transfer([])

        self.assertEqual(len(started), 1)
        self.assertIsNotNone(started[0].poll())


class RunSenderPropagationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.original_cwd = os.getcwd()
        self.addCleanup(os.chdir, self.original_cwd)

        spec = importlib.util.spec_from_file_location(
            "phase2_node", NODE
        )
        self.node = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.node)

        import transport.sender as sender_module
        self.sender_module = sender_module
        self.original_send_file = sender_module.send_file
        self.addCleanup(
            setattr, sender_module, "send_file", self.original_send_file
        )

    def _run_node_sender(self, result):
        self.sender_module.send_file = lambda *args, **kwargs: result
        args = types.SimpleNamespace(
            file="input.bin", host="127.0.0.1", port=1,
            log=str(Path(self.tmp.name) / "sender.csv"),
            initial_cwnd=None, initial_ssthresh=None,
            window_size=None,
        )
        try:
            self.node.run_sender(args)
        except SystemExit as exit_signal:
            return exit_signal.code
        return None

    def test_failed_transfer_exits_nonzero(self):
        self.assertEqual(self._run_node_sender(False), 1)

    def test_successful_transfer_exits_normally(self):
        self.assertIsNone(self._run_node_sender(True))


class ExperimentConfigurationTests(unittest.TestCase):
    def test_phase3_experiment_passes_explicit_reno_flags(self):
        module = _load_experiment_module()

        self.assertEqual(
            module.reno_sender_flags(None),
            ["--initial-cwnd", "1.0", "--initial-ssthresh", "8.0"],
        )

    def test_experiment_ssthresh_override_is_forwarded(self):
        module = _load_experiment_module()

        self.assertEqual(
            module.reno_sender_flags(float("inf"))[-1], "inf"
        )
        self.assertEqual(
            module.reno_sender_flags(16)[-1], "16"
        )


if __name__ == "__main__":
    unittest.main()
