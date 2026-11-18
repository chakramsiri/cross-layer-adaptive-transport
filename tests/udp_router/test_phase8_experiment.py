"""Phase 8 experiment definitions: matrix, cell identity, commands and
completion rules. Mininet itself is exercised by the pilot and matrix runs."""

import importlib.util
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "tests" / "mininet"
    / "run_experiment.py"
)


def load_runner():
    spec = importlib.util.spec_from_file_location(
        "run_experiment", MODULE_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runner = load_runner()


class MatrixDefinitionTests(unittest.TestCase):
    def test_queue_and_controller_combinations(self):
        combos = runner.controller_combinations()

        self.assertEqual(len(combos), 11)
        for aqm, mode in (("droptail", "drop"), ("red", "mark"),
                          ("codel", "mark")):
            for controller in runner.CONTROLLERS:
                self.assertIn((aqm, mode, controller), combos)
        self.assertIn(("red", "drop", "reno"), combos)
        self.assertIn(("codel", "drop", "reno"), combos)

    def test_scenarios_match_the_guide_rows(self):
        self.assertEqual(sorted(runner.SCENARIOS), ["S1", "S2", "S3", "S4"])
        self.assertEqual(runner.SCENARIOS["S1"].buffers, (200,))
        self.assertEqual(runner.SCENARIOS["S2"].buffers,
                         (20, 50, 100, 500))
        self.assertTrue(runner.SCENARIOS["S3"].cross)
        self.assertEqual(runner.SCENARIOS["S4"].loss_percent, 1.0)
        self.assertFalse(runner.SCENARIOS["S1"].cross)
        self.assertEqual(runner.SCENARIOS["S1"].loss_percent, 0.0)

    def test_full_matrix_has_three_repetitions_per_cell(self):
        cells = runner.enumerate_cells()

        self.assertEqual(len(cells), 231)
        self.assertEqual({cell.seed for cell in cells}, {1, 2, 3})
        self.assertEqual(len({cell.key() for cell in cells}), 231)

    def test_scenario_filter_selects_only_requested_rows(self):
        cells = runner.enumerate_cells(scenarios=["S3"], seeds=[2])

        self.assertEqual(len(cells), 11)
        self.assertTrue(all(cell.scenario == "S3" for cell in cells))
        self.assertTrue(all(cell.seed == 2 for cell in cells))

    def test_sweep_row_at_200_packets_is_the_bulk_scenario(self):
        with self.assertRaises(ValueError):
            runner.Cell.parse("S2/200/red/mark/dctcp/1")
        self.assertIn(
            runner.Cell.parse("S1/200/red/mark/dctcp/1"),
            runner.enumerate_cells(["S1"]),
        )


class CellIdentityTests(unittest.TestCase):
    def test_cell_round_trips_through_its_text_form(self):
        cell = runner.Cell.parse("S2/50/red/mark/dctcp/3")

        self.assertEqual(cell.scenario, "S2")
        self.assertEqual(cell.buffer, 50)
        self.assertEqual(cell.aqm, "red")
        self.assertEqual(cell.aqm_mode, "mark")
        self.assertEqual(cell.controller, "dctcp")
        self.assertEqual(cell.seed, 3)
        self.assertEqual(str(cell), "S2/50/red/mark/dctcp/3")

    def test_run_directory_layout(self):
        cell = runner.Cell.parse("S4/200/codel/mark/delay/2")

        self.assertEqual(
            cell.relative_dir(),
            Path("S4") / "buf200" / "codel_mark_delay" / "rep2",
        )

    def test_invalid_cell_text_is_rejected(self):
        for text in ("S9/200/red/mark/dctcp/1", "S1/200/red/mark/cubic/1",
                     "S1/200/red/mark/dctcp", "S1/200/droptail/mark/reno/1"):
            with self.subTest(text=text):
                with self.assertRaises(ValueError):
                    runner.Cell.parse(text)


class QueueParameterTests(unittest.TestCase):
    def test_red_thresholds_scale_with_the_buffer(self):
        self.assertEqual(runner.red_thresholds(200), (20.0, 80.0))
        self.assertEqual(runner.red_thresholds(20), (2.0, 8.0))
        self.assertEqual(runner.red_thresholds(500), (50.0, 200.0))

    def test_red_thresholds_stay_valid_for_every_buffer(self):
        for buffer in (20, 50, 100, 200, 500):
            with self.subTest(buffer=buffer):
                low, high = runner.red_thresholds(buffer)
                self.assertGreaterEqual(low, 1.0)
                self.assertLess(low, high)
                self.assertLessEqual(high, buffer)

    def test_aqm_flags_follow_the_queue_variant(self):
        red = runner.aqm_flags(runner.Cell.parse("S1/200/red/mark/reno/1"))
        self.assertIn("--red-min-th", red)
        self.assertEqual(red[red.index("--red-min-th") + 1], "20.0")
        self.assertEqual(red[red.index("--red-seed") + 1], "1")

        codel = runner.aqm_flags(
            runner.Cell.parse("S1/200/codel/mark/reno/1")
        )
        self.assertEqual(codel[codel.index("--codel-target-ms") + 1], "5.0")

        self.assertEqual(
            runner.aqm_flags(runner.Cell.parse("S1/200/droptail/drop/reno/1")),
            [],
        )


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p8cmd_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_router_command_carries_queue_and_topology(self):
        cell = runner.Cell.parse("S2/500/red/mark/dctcp/1")
        command = runner.router_command("python", cell, self.tmp, 300.0)

        self.assertIn("router.forwarder", command)
        self.assertEqual(command[command.index("--sender-face") + 1],
                         "10.0.0.2:5001")
        self.assertEqual(command[command.index("--receiver") + 1],
                         "10.0.1.1:5001")
        self.assertEqual(command[command.index("--queue-capacity") + 1], "500")
        self.assertEqual(command[command.index("--rate-mbps") + 1], "5.0")
        self.assertEqual(command[command.index("--aqm-mode") + 1], "mark")
        self.assertEqual(command[command.index("--red-max-th") + 1], "200.0")

    def test_sender_command_selects_the_controller(self):
        cell = runner.Cell.parse("S1/200/droptail/drop/delay/1")
        command = runner.sender_command(
            "python", cell, self.tmp / "input.bin", self.tmp / "s.csv"
        )

        self.assertEqual(command[command.index("--cc-mode") + 1], "delay")
        self.assertEqual(
            command[command.index("--delay-target-ms") + 1], "5.0"
        )
        self.assertEqual(command[command.index("--window-size") + 1], "400")
        self.assertEqual(command[command.index("--initial-ssthresh") + 1],
                         "20.0")

    def test_dctcp_sender_uses_the_frozen_gain(self):
        cell = runner.Cell.parse("S1/200/red/mark/dctcp/1")
        command = runner.sender_command(
            "python", cell, self.tmp / "input.bin", self.tmp / "s.csv"
        )

        self.assertEqual(command[command.index("--dctcp-g") + 1], "0.25")

    def test_reno_sender_names_the_loss_only_controller(self):
        cell = runner.Cell.parse("S1/200/droptail/drop/reno/1")
        command = runner.sender_command(
            "python", cell, self.tmp / "input.bin", self.tmp / "s.csv"
        )

        self.assertEqual(command[command.index("--cc-mode") + 1], "reno")

    def test_cross_command_matches_the_burst_pattern(self):
        cell = runner.Cell.parse("S3/200/red/mark/reno/1")
        command = runner.cross_command(
            "python", cell, self.tmp / "cross.csv", 300.0
        )

        self.assertEqual(command[command.index("--rate-mbps") + 1], "2.5")
        self.assertEqual(command[command.index("--on-ms") + 1], "200.0")
        self.assertEqual(command[command.index("--period-ms") + 1], "1000.0")
        self.assertEqual(command[3], "10.0.0.2:5001")

    def test_receiver_command_binds_the_plan_port(self):
        command = runner.receiver_command(
            "python", self.tmp / "out.bin", self.tmp / "r.csv"
        )

        self.assertIn("0.0.0.0", command)
        self.assertIn("5001", command)
        self.assertEqual(command[command.index("--rwnd-bytes") + 1],
                         "800000")
        self.assertEqual(command[command.index("--fin-linger") + 1], "2.0")


class CompletionAndVerdictTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="p8done_"))
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_run_with_a_recorded_result_is_complete(self):
        (self.tmp / "run_metadata.json").write_text(
            json.dumps({"result": {"success": True}})
        )

        self.assertTrue(runner.is_complete(self.tmp))

    def test_missing_or_unfinished_run_is_not_complete(self):
        self.assertFalse(runner.is_complete(self.tmp))

        (self.tmp / "run_metadata.json").write_text(json.dumps({}))
        self.assertFalse(runner.is_complete(self.tmp))

    def test_clean_transfer_is_a_success(self):
        self.assertEqual(
            runner.evaluate_transfer(sender_exit=0, receiver_exit=0,
                                     router_exit=0, sha256_match=True,
                                     sender_completed=True),
            [],
        )

    def test_each_failure_mode_is_reported(self):
        reasons = runner.evaluate_transfer(
            sender_exit=1, receiver_exit=None, router_exit=0,
            sha256_match=False, sender_completed=False,
        )

        self.assertIn("sender exited with 1", reasons)
        self.assertIn("receiver did not exit cleanly", reasons)
        self.assertIn("output hash mismatch", reasons)
        self.assertIn("sender log has no TRANSFER_COMPLETED", reasons)

    def test_ownership_is_handed_back_only_under_sudo(self):
        tmp = Path(tempfile.mkdtemp(prefix="p8own_"))
        self.addCleanup(shutil.rmtree, tmp, True)

        self.assertFalse(runner.hand_back_ownership(tmp, environ={}))
        self.assertTrue(runner.hand_back_ownership(
            tmp, environ={"SUDO_UID": str(os.getuid()),
                          "SUDO_GID": str(os.getgid())}))

    def test_root_is_required_for_mininet(self):
        with self.assertRaises(SystemExit):
            runner.require_root(1000)
        runner.require_root(0)


if __name__ == "__main__":
    unittest.main()
