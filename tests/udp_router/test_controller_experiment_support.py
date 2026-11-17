"""Phase 7 experiment plumbing: the sender controller flags reach the
transport node, the harness commands, the run metadata and the summary."""

import argparse
import importlib.util
import tempfile
import unittest
from pathlib import Path

from transport.cc.delay_aware import DelayAwareController
from transport.cc.dctcp_style import DctcpController
from transport.cc.reno import RenoController

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE = REPO_ROOT / "tests" / "mininet" / "phase2_node.py"
EXPERIMENT = REPO_ROOT / "tests" / "mininet" / "phase5_aqm_experiments.py"


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SenderNodeControllerTests(unittest.TestCase):
    def setUp(self):
        self.node = load_module("phase2_node_under_test", NODE)

    def _args(self, *extra):
        return self.node.build_parser().parse_args(
            ["sender", "in.bin", "127.0.0.1", "5001", "log.csv", *extra]
        )

    def test_without_controller_flags_congestion_control_stays_disabled(self):
        self.assertIsNone(self.node.build_sender_controller(self._args()))

    def test_initial_threshold_alone_keeps_selecting_reno(self):
        controller = self.node.build_sender_controller(
            self._args("--initial-ssthresh", "5")
        )
        self.assertIsInstance(controller, RenoController)
        self.assertEqual(controller.ssthresh, 5.0)

    def test_mode_flag_selects_the_controller(self):
        self.assertIsInstance(
            self.node.build_sender_controller(self._args("--cc-mode", "reno")),
            RenoController,
        )
        self.assertIsInstance(
            self.node.build_sender_controller(
                self._args("--cc-mode", "dctcp")
            ),
            DctcpController,
        )
        self.assertIsInstance(
            self.node.build_sender_controller(
                self._args("--cc-mode", "delay")
            ),
            DelayAwareController,
        )

    def test_mode_parameters_are_forwarded(self):
        dctcp = self.node.build_sender_controller(
            self._args("--cc-mode", "dctcp", "--dctcp-g", "0.25")
        )
        self.assertEqual(dctcp.g, 0.25)

        delay = self.node.build_sender_controller(
            self._args("--cc-mode", "delay", "--delay-target-ms", "5",
                       "--delay-increase-step", "2")
        )
        self.assertEqual(delay.target_seconds, 0.005)
        self.assertEqual(delay.increase_step, 2.0)

    def test_window_flags_still_apply_to_the_selected_controller(self):
        controller = self.node.build_sender_controller(
            self._args("--cc-mode", "dctcp", "--initial-cwnd", "3",
                       "--initial-ssthresh", "9")
        )
        self.assertEqual(controller.cwnd, 3.0)
        self.assertEqual(controller.ssthresh, 9.0)

    def test_parameter_for_another_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            self.node.build_sender_controller(
                self._args("--cc-mode", "dctcp", "--delay-target-ms", "5")
            )

    def test_unknown_mode_is_rejected_by_the_parser(self):
        with self.assertRaises(SystemExit):
            self._args("--cc-mode", "cubic")


class SenderOptionsTests(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase5_for_options", EXPERIMENT)
        self.phase4 = self.experiment.phase4

    def test_sender_options_without_a_mode_are_unchanged(self):
        args = argparse.Namespace(sender_window=400, initial_ssthresh=20.0)

        self.assertEqual(
            self.phase4.sender_options(args, True),
            ["--window-size", "400", "--initial-ssthresh", "20.0"],
        )

    def test_mode_and_parameters_are_appended(self):
        args = argparse.Namespace(
            sender_window=400, initial_ssthresh=20.0, cc_mode="dctcp",
            dctcp_g=0.0625, delay_target_ms=10.0, delay_increase_step=1.0,
        )
        options = self.phase4.sender_options(args, True)

        self.assertEqual(options[-4:], ["--cc-mode", "dctcp",
                                        "--dctcp-g", "0.0625"])

    def test_delay_mode_passes_its_parameters(self):
        args = argparse.Namespace(
            sender_window=400, initial_ssthresh=20.0, cc_mode="delay",
            dctcp_g=0.0625, delay_target_ms=5.0, delay_increase_step=2.0,
        )
        options = self.phase4.sender_options(args, True)

        self.assertEqual(options[-6:], [
            "--cc-mode", "delay", "--delay-target-ms", "5.0",
            "--delay-increase-step", "2.0",
        ])

    def test_non_bulk_runs_carry_no_controller_options(self):
        args = argparse.Namespace(
            sender_window=400, initial_ssthresh=20.0, cc_mode="dctcp",
            dctcp_g=0.0625, delay_target_ms=10.0, delay_increase_step=1.0,
        )

        self.assertEqual(self.phase4.sender_options(args, False), [])


class HarnessControllerTests(unittest.TestCase):
    def setUp(self):
        self.experiment = load_module("phase5_under_test", EXPERIMENT)
        self.tmp = Path(tempfile.mkdtemp(prefix="p7support_"))

    def _parse(self, *argv):
        return self.experiment.build_arg_parser().parse_args(list(argv))

    def test_defaults_select_loss_only_reno_with_guide_parameters(self):
        args = self._parse()

        self.assertEqual(args.cc_mode, "reno")
        self.assertEqual(args.dctcp_g, 0.25)
        self.assertEqual(args.delay_target_ms, 5.0)
        self.assertEqual(args.delay_increase_step, 1.0)

    def test_invalid_controller_parameters_are_rejected(self):
        cases = (
            ("--dctcp-g", "0"),
            ("--dctcp-g", "1.5"),
            ("--delay-target-ms", "0"),
            ("--delay-increase-step", "0"),
        )
        for flag, value in cases:
            with self.subTest(flag=flag, value=value):
                args = self._parse(flag, value)
                with self.assertRaises(SystemExit):
                    self.experiment.validate_args(args)

    def test_metadata_records_the_dctcp_controller(self):
        args = self._parse("--cc-mode", "dctcp")
        plan = self.experiment.phase4.MININET_PLAN
        parameters = self.experiment.build_queue_parameters(args, "red")

        metadata = self.experiment.build_run_metadata(
            args, plan, self.tmp / "red", "red", "abc", parameters
        )

        self.assertEqual(
            metadata["congestion_control"],
            {"mode": "dctcp", "parameters": {"g": 0.25}},
        )

    def test_metadata_records_the_delay_controller(self):
        args = self._parse("--cc-mode", "delay", "--delay-target-ms", "5")
        plan = self.experiment.phase4.MININET_PLAN
        parameters = self.experiment.build_queue_parameters(args, "red")

        metadata = self.experiment.build_run_metadata(
            args, plan, self.tmp / "red", "red", "abc", parameters
        )

        self.assertEqual(
            metadata["congestion_control"],
            {"mode": "delay",
             "parameters": {"target_ms": 5.0, "increase_step": 1.0}},
        )

    def test_summary_records_the_controller(self):
        args = self._parse("--cc-mode", "delay")
        plan = self.experiment.phase4.MININET_PLAN

        summary = self.experiment.build_summary(
            args, plan, {}, 1000, "abc"
        )

        self.assertEqual(summary["congestion_control"]["mode"], "delay")

    def test_manual_sender_command_carries_the_controller(self):
        args = self._parse("--cc-mode", "dctcp", "--dctcp-g", "0.5")
        interfaces = {
            "sender": "h1-eth0",
            "router_sender": "r-eth0",
            "router_receiver": "r-eth1",
            "receiver": "h2-eth0",
        }
        commands = self.experiment.build_manual_endpoint_commands(
            args, self.experiment.phase4.MININET_PLAN, self.tmp, interfaces,
        )

        sender = commands["sender"][0]
        self.assertIn("--cc-mode dctcp --dctcp-g 0.5", sender)


if __name__ == "__main__":
    unittest.main()
