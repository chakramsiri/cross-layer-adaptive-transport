"""Sender integration tests: cwnd governs transmission alongside rwnd.

The controller instance is reachable as self.controller; window values are
asserted from the CWND_UPDATE log rows, which the sender writes after each
controller update.
"""

import contextlib
import csv
import hashlib
import io
import runpy
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from transport import receiver as receiver_module
from transport import sender as sender_module
from transport.cc.base import CongestionController
from transport.csv_logger import CSVLogger

from tests.helpers import ScriptedPeer, SenderTestBase, pick_test_port

REPO_ROOT = Path(__file__).resolve().parents[2]
NODE = REPO_ROOT / "tests" / "mininet" / "phase2_node.py"


def parse_details(row):
    values = {}
    for token in row["details"].split():
        key, _, value = token.partition("=")
        try:
            values[key] = float(value)
        except ValueError:
            values[key] = value
    return values


class CwndTrackingMixin:
    def cwnd_events(self):
        return [parse_details(row) for row in self.events("CWND_UPDATE")]

    def wait_for_cwnd(self, reason, cwnd, timeout=2.0):
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            for values in self.cwnd_events():
                if (
                    values.get("reason") == reason
                    and abs(values["cwnd"] - cwnd) < 1e-6
                ):
                    return values
            time.sleep(0.02)

        self.fail(f"no CWND_UPDATE {reason} cwnd={cwnd}")


class FlightLimitTests(CwndTrackingMixin, SenderTestBase):
    CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(12)]
    INITIAL_CWND = 4

    def test_sender_holds_back_until_acks_raise_cwnd(self):
        self.peer.accept_syn()

        self.assertEqual(self.peer.collect_data(4), [0, 1, 2, 3])
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "sent beyond cwnd=4"
        )

        # One cumulative ACK covering four segments: +1 SMSS.
        self.peer.send_ack(4)
        self.wait_for_cwnd("ack", 5.0)
        self.assertAlmostEqual(self.controller.cwnd, 5.0)

        self.assertEqual(self.peer.collect_data(5), list(range(4, 9)))
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "sent beyond cwnd=5"
        )

        self.peer.send_ack(9)
        self.wait_for_cwnd("ack", 6.0)
        self.assertAlmostEqual(self.controller.cwnd, 6.0)

        self.assertEqual(self.peer.collect_data(3), [9, 10, 11])
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "sent beyond cwnd=6"
        )

        self.finish_transfer()


class EffectiveWindowTests(CwndTrackingMixin, SenderTestBase):
    CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(8)]
    INITIAL_CWND = 3

    def test_effective_limit_is_min_of_cwnd_and_rwnd(self):
        self.peer.rwnd = 2000
        self.peer.accept_syn()

        # rwnd (2 packets) binds before cwnd (3).
        self.assertEqual(self.peer.collect_data(2), [0, 1])
        self.assertIsNone(self.peer.expect_no_data(0.3))

        self.peer.send_ack(2, rwnd=10000)
        self.wait_for_cwnd("ack", 4.0)

        # With rwnd out of the way, cwnd (4) is the binding limit.
        self.assertEqual(self.peer.collect_data(4), [2, 3, 4, 5])
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "sent beyond cwnd=4"
        )

        self.peer.send_ack(6)
        self.wait_for_cwnd("ack", 5.0)
        self.assertEqual(self.peer.collect_data(2), [6, 7])

        self.finish_transfer()


class HardCapTests(CwndTrackingMixin, SenderTestBase):
    CHUNKS = [bytes([65]) * 1000 for _ in range(40)]
    INITIAL_CWND = 64

    def test_packet_cap_still_binds_when_cwnd_is_larger(self):
        self.peer.accept_syn()

        self.assertEqual(self.peer.collect_data(32), list(range(32)))
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "32-packet cap not enforced"
        )

        self.peer.send_ack(32)
        self.assertEqual(
            self.peer.collect_data(8, timeout=2.0), list(range(32, 40))
        )

        self.finish_transfer()


class AckEventDisciplineTests(CwndTrackingMixin, SenderTestBase):
    INITIAL_CWND = 4

    def test_duplicate_stale_and_future_acks_leave_cwnd_alone(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(4), [0, 1, 2, 3])

        self.peer.send_ack(0)
        self.peer.send_ack(0)
        time.sleep(0.15)
        self.assertEqual(len(self.cwnd_events()), 1)  # init row only
        self.assertAlmostEqual(self.controller.cwnd, 4.0)

        self.peer.send_ack(99)  # future ACK
        time.sleep(0.15)
        self.assertEqual(len(self.cwnd_events()), 1)
        self.assertAlmostEqual(self.controller.cwnd, 4.0)

        self.peer.send_ack(4)
        self.wait_for_cwnd("ack", 5.0)

        self.peer.send_ack(2)  # stale ACK
        time.sleep(0.15)
        self.assertEqual(len(self.cwnd_events()), 2)
        self.assertAlmostEqual(self.controller.cwnd, 5.0)

        self.finish_transfer()


class FastRecoveryIntegrationTests(CwndTrackingMixin, SenderTestBase):
    CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(8)]
    INITIAL_CWND = 8

    def test_three_duplicate_acks_halve_cwnd_and_retransmit(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(8), list(range(8)))

        self.peer.send_ack(0)
        self.peer.send_ack(0)
        self.assertIsNone(
            self.peer.expect_no_data(0.4), "retransmit before 3 dup ACKs"
        )
        self.assertEqual(self.events("RETRANSMISSION"), [])
        self.assertAlmostEqual(self.controller.cwnd, 8.0)

        self.peer.send_ack(0)
        retransmission = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(retransmission, "no fast retransmit")
        self.assertEqual(retransmission.seq, 0)
        self.assertEqual(retransmission.payload, self.CHUNKS[0])

        values = self.wait_for_cwnd("fast_recovery", 4.0)
        self.assertAlmostEqual(values["ssthresh"], 4.0)
        self.assertAlmostEqual(self.controller.cwnd, 4.0)
        self.assertEqual(values["phase"], "fast_recovery")

        # More duplicates belong to the same loss event: no second
        # reduction and no second retransmission.
        self.peer.send_ack(0)
        self.peer.send_ack(0)
        time.sleep(0.15)
        recovery_rows = [
            v for v in self.cwnd_events() if v["reason"] == "fast_recovery"
        ]
        self.assertEqual(len(recovery_rows), 1)
        self.assertEqual(len(self.retransmissions("fast")), 1)
        self.assertAlmostEqual(self.controller.cwnd, 4.0)

        # A new advancing ACK ends recovery; growth resumes additively.
        self.peer.send_ack(1)
        self.wait_for_cwnd("ack", 4.25)
        self.assertFalse(self.controller.in_fast_recovery)

        self.finish_transfer()

    def test_new_loss_event_after_recovery_reduces_again(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(8), list(range(8)))

        for _ in range(3):
            self.peer.send_ack(0)
        self.assertIsNotNone(self.peer.recv_data(timeout=0.5))
        self.wait_for_cwnd("fast_recovery", 4.0)

        self.peer.send_ack(1)
        self.wait_for_cwnd("ack", 4.25)

        for _ in range(3):
            self.peer.send_ack(1)
        retransmission = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(retransmission, "no second fast retransmit")
        self.assertEqual(retransmission.seq, 1)

        # FlightSize is the outstanding data (packets 1..7), not cwnd.
        values = self.wait_for_cwnd("fast_recovery", 3.5)
        self.assertAlmostEqual(values["ssthresh"], 3.5)
        self.assertAlmostEqual(self.controller.cwnd, 3.5)

        self.finish_transfer()


class TimeoutIntegrationTests(CwndTrackingMixin, SenderTestBase):
    INITIAL_CWND = 4

    def test_timeout_resets_cwnd_and_reenters_slow_start(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(4), [0, 1, 2, 3])

        retransmission = self.peer.recv_data(timeout=1.6)
        self.assertIsNotNone(retransmission, "no timeout retransmission")
        self.assertEqual(retransmission.seq, 0)

        self.wait_for_cwnd("timeout", 1.0)
        timeout_row = [
            v for v in self.cwnd_events() if v["reason"] == "timeout"
        ][-1]
        self.assertAlmostEqual(timeout_row["ssthresh"], 2.0)
        self.assertEqual(len(self.events("RTO_BACKOFF")), 1)

        # Slow start re-entry: 1 -> 2, then congestion avoidance at the
        # threshold: 2 -> 2.5.
        self.peer.send_ack(1)
        self.wait_for_cwnd("ack", 2.0)
        self.peer.send_ack(2)
        self.wait_for_cwnd("ack", 2.5)

        # Drain the outstanding window so the final ACK is not a future ACK.
        self.peer.send_ack(3)
        self.assertEqual(self.peer.collect_data(1, timeout=2.0), [4])

        self.finish_transfer()


class DefaultControllerTransferTests(unittest.TestCase):
    """Real sender (default Reno) + real receiver on localhost."""

    CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(40)]

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.input_path = Path(self.tmp.name) / "input.bin"
        self.input_path.write_bytes(b"".join(self.CHUNKS))
        self.output_path = Path(self.tmp.name) / "out.bin"
        self.sender_log = Path(self.tmp.name) / "sender.csv"
        self.receiver_log = Path(self.tmp.name) / "receiver.csv"

        receiver_port = pick_test_port()

        # The sender sends to the receiver's port directly (no relay).
        self.orig_sender = (sender_module.PORT, sender_module.logger)
        sender_module.PORT = receiver_port
        sender_module.logger = CSVLogger(str(self.sender_log))

        self.orig_receiver = (
            receiver_module.HOST,
            receiver_module.PORT,
            receiver_module.FIN_LINGER,
            receiver_module.logger,
        )
        receiver_module.HOST = "127.0.0.1"
        receiver_module.PORT = receiver_port
        receiver_module.FIN_LINGER = 0.5
        receiver_module.logger = CSVLogger(str(self.receiver_log))

        self.errors = {}
        self.receiver_thread = threading.Thread(
            target=self._run_receiver, daemon=True
        )
        self.sender_thread = threading.Thread(
            target=self._run_sender, daemon=True
        )

        self.addCleanup(self._restore_modules)
        self.addCleanup(self._join_threads)

        self.receiver_thread.start()
        time.sleep(0.2)
        self.sender_thread.start()

    def _restore_modules(self):
        sender_module.PORT, sender_module.logger = self.orig_sender
        (
            receiver_module.HOST,
            receiver_module.PORT,
            receiver_module.FIN_LINGER,
            receiver_module.logger,
        ) = self.orig_receiver

    def _join_threads(self):
        self.sender_thread.join(timeout=10.0)
        self.receiver_thread.join(timeout=5.0)

    def _run_receiver(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                receiver_module.receive_file(str(self.output_path))
        except Exception as error:  # pragma: no cover - failure path
            self.errors["receiver"] = error

    def _run_sender(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                sender_module.send_file(
                    str(self.input_path), "127.0.0.1"
                )
        except Exception as error:  # pragma: no cover - failure path
            self.errors["sender"] = error

    @staticmethod
    def _sha256(path):
        digest = hashlib.sha256()
        with open(path, "rb") as file:
            for chunk in iter(lambda: file.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def test_default_reno_transfer_preserves_integrity_and_grows(self):
        self.sender_thread.join(timeout=15.0)
        self.receiver_thread.join(timeout=5.0)

        self.assertFalse(self.sender_thread.is_alive())
        self.assertFalse(self.receiver_thread.is_alive())
        self.assertEqual(self.errors, {})

        self.assertEqual(
            self._sha256(self.input_path), self._sha256(self.output_path)
        )

        with open(self.sender_log) as file:
            rows = list(csv.DictReader(file))

        events = [row["event"] for row in rows]
        self.assertIn("TRANSFER_COMPLETED", events)
        self.assertNotIn("TRANSFER_FAILED", events)

        cwnd_rows = [parse_details(row) for row in rows
                     if row["event"] == "CWND_UPDATE"]
        self.assertEqual(cwnd_rows[0]["cwnd"], 1.0)  # default initial cwnd
        self.assertGreater(max(r["cwnd"] for r in cwnd_rows), 1.0)
        self.assertGreater(len(cwnd_rows), 5)


class GenericContractController(CongestionController):
    def __init__(self):
        self._cwnd = float(sender_module.WINDOW_SIZE)

    @property
    def cwnd(self):
        return self._cwnd

    def on_ack(self, acked_data):
        pass

    def on_loss(self, flight_size):
        pass

    def on_timeout(self, flight_size, segment_id):
        pass

    def on_feedback(self, ce_count, q_delay):
        pass


class GenericControllerSenderTests(unittest.TestCase):
    """A controller implementing only the documented interface must run
    without crashing and without a false Reno phase classification."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.input_path = Path(self.tmp.name) / "input.bin"
        self.input_path.write_bytes(
            b"".join(bytes([65 + i]) * 1000 for i in range(3))
        )
        self.log_path = Path(self.tmp.name) / "sender.csv"

        self.port = pick_test_port()
        self.orig_port = sender_module.PORT
        self.orig_logger = sender_module.logger
        sender_module.PORT = self.port
        sender_module.logger = CSVLogger(str(self.log_path))
        self.addCleanup(self._restore_sender_module)

        self.peer = ScriptedPeer(self.port)
        self.peer.rwnd = 64000
        self.addCleanup(self.peer.close)

        self.error = None
        self.thread = threading.Thread(target=self._run_sender, daemon=True)
        self.thread.start()

    def _restore_sender_module(self):
        sender_module.PORT = self.orig_port
        sender_module.logger = self.orig_logger

    def _run_sender(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                sender_module.send_file(
                    str(self.input_path), "127.0.0.1",
                    GenericContractController(),
                )
        except Exception as error:
            self.error = error

    def test_documented_contract_controller_runs_and_logs_unknown_phase(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(3), [0, 1, 2])
        self.peer.send_ack(3)
        self.assertTrue(self.peer.wait_for_fin(), "FIN handshake failed")
        self.thread.join(timeout=5.0)

        self.assertFalse(self.thread.is_alive())
        self.assertIsNone(self.error)

        with open(self.log_path) as file:
            rows = list(csv.DictReader(file))
        cwnd_rows = [r for r in rows if r["event"] == "CWND_UPDATE"]
        self.assertTrue(cwnd_rows, "no CWND_UPDATE rows")
        for row in cwnd_rows:
            self.assertIn("phase=unknown", row["details"])
        self.assertIn("TRANSFER_COMPLETED", [r["event"] for r in rows])


class HandshakeFailureOutcomeTests(SenderTestBase):
    def setUp(self):
        self.orig_max_retries = sender_module.MAX_RETRIES
        self.orig_timeout = sender_module.TIMEOUT
        sender_module.MAX_RETRIES = 3
        sender_module.TIMEOUT = 0.2
        self.addCleanup(
            setattr, sender_module, "MAX_RETRIES", self.orig_max_retries
        )
        self.addCleanup(
            setattr, sender_module, "TIMEOUT", self.orig_timeout
        )
        super().setUp()

    def test_syn_handshake_exhaustion_returns_false(self):
        self.thread.join(timeout=5.0)

        self.assertFalse(self.thread.is_alive())
        self.assertIs(self.sender_result, False)
        self.assertIsNone(self.sender_error)
        failed = self.events("TRANSFER_FAILED")
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["packet_type"], "SYN")
        self.assertEqual(self.events("CWND_UPDATE"), [])

    def test_data_retry_exhaustion_returns_false(self):
        self.open_window()
        self.thread.join(timeout=5.0)

        self.assertFalse(self.thread.is_alive())
        self.assertIs(self.sender_result, False)
        self.assertIsNone(self.sender_error)
        failed = self.events("TRANSFER_FAILED")
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["packet_type"], "DATA")
        self.assertEqual(failed[0]["details"], "Maximum retries exceeded")

    def test_fin_handshake_exhaustion_returns_false(self):
        self.open_window()
        self.peer.send_ack(len(self.CHUNKS))
        self.thread.join(timeout=5.0)

        self.assertFalse(self.thread.is_alive())
        self.assertIs(self.sender_result, False)
        self.assertIsNone(self.sender_error)
        failed = self.events("TRANSFER_FAILED")
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0]["packet_type"], "FIN")


class TransferOutcomeTests(SenderTestBase):
    def test_successful_transfer_returns_true(self):
        self.open_window()
        self.finish_transfer()

        self.assertIs(self.sender_result, True)


class WindowStallOutcomeTests(SenderTestBase):
    PEER_RWND = 0

    def test_window_stall_returns_false(self):
        self.peer.accept_syn()
        self.thread.join(timeout=5.0)

        self.assertFalse(self.thread.is_alive())
        self.assertIs(self.sender_result, False)
        self.assertIsNone(self.sender_error)
        self.assertTrue(self.events("TRANSFER_FAILED"))
        self.assertEqual(self.events("TRANSFER_COMPLETED"), [])


class SenderCliExitTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.workdir = Path(self.tmp.name) / "work"
        self.workdir.mkdir()
        self.input_path = self.workdir / "input.bin"
        self.input_path.write_bytes(
            b"".join(bytes([65 + i]) * 1000 for i in range(3))
        )
        self.output_path = Path(self.tmp.name) / "received.bin"

    def _start_receiver_process(self, rwnd):
        receiver_log = Path(self.tmp.name) / "receiver.csv"
        receiver_out = open(Path(self.tmp.name) / "receiver.out", "w")
        self.addCleanup(receiver_out.close)
        process = subprocess.Popen(
            [
                sys.executable, str(NODE), "receiver",
                str(self.output_path), "127.0.0.1", "5001",
                str(receiver_log),
                "--rwnd-bytes", str(rwnd), "--fin-linger", "0.5",
            ],
            stdout=receiver_out, stderr=subprocess.STDOUT,
        )
        self.addCleanup(self._terminate_receiver, process)
        return process

    @staticmethod
    def _terminate_receiver(receiver):
        if receiver.poll() is None:
            receiver.terminate()
        try:
            receiver.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            receiver.kill()
            receiver.wait()

    def _run_cli_main(self):
        original_cwd = os.getcwd()
        os.chdir(self.workdir)
        self.addCleanup(os.chdir, original_cwd)

        original_argv = sys.argv
        sys.argv = [
            "transport/sender.py", str(self.input_path), "127.0.0.1",
        ]
        self.addCleanup(setattr, sys, "argv", original_argv)

        try:
            runpy.run_path(
                str(REPO_ROOT / "transport" / "sender.py"),
                run_name="__main__",
            )
        except SystemExit as exit_signal:
            return exit_signal.code
        return None

    def _sender_log_events(self):
        sender_log = self.workdir / "results" / "sender_log.csv"
        with open(sender_log) as file:
            return [row["event"] for row in csv.DictReader(file)]

    def test_successful_transfer_exits_zero(self):
        receiver = self._start_receiver_process(rwnd=64000)

        # Success returns True: the CLI never calls sys.exit and the
        # process exits zero implicitly.
        self.assertIsNone(self._run_cli_main())

        # The receiver lingers briefly after the FIN; let it exit so its
        # buffered writes reach received.bin before the assertions.
        try:
            receiver.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            pass
        self.assertEqual(
            self._sender_log_events()[-1], "TRANSFER_COMPLETED"
        )
        self.assertTrue(self.output_path.exists())
        self.assertEqual(
            self.output_path.read_bytes(), self.input_path.read_bytes()
        )

    def test_failed_transfer_exits_nonzero(self):
        receiver = self._start_receiver_process(rwnd=0)

        self.assertEqual(self._run_cli_main(), 1)
        events = self._sender_log_events()
        self.assertIn("TRANSFER_FAILED", events)
        self.assertNotIn("TRANSFER_COMPLETED", events)


class CongestionControlDisabledTests(CwndTrackingMixin, SenderTestBase):
    """controller=None restores the Phase 2 window behaviour."""

    CHUNKS = [bytes([65]) * 1000 for _ in range(40)]
    CONTROLLER_ENABLED = False

    def test_explicit_none_disables_congestion_control(self):
        self.assertIsNone(self.controller)
        self.peer.accept_syn()

        # The 32-packet cap is the only packet limit remaining.
        self.assertEqual(self.peer.collect_data(32), list(range(32)))
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "cap not enforced"
        )

        # Limited transmit belongs to congestion control: none without it.
        self.peer.send_ack(0)
        self.assertIsNone(
            self.peer.expect_no_data(0.4), "limited transmit while disabled"
        )

        self.peer.send_ack(32)
        self.assertEqual(
            self.peer.collect_data(8, timeout=2.0), list(range(32, 40))
        )

        self.finish_transfer()
        self.assertEqual(self.events("CWND_UPDATE"), [])

    def test_rwnd_remains_enforced_without_congestion_control(self):
        self.peer.rwnd = 2000
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(2), [0, 1])
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "rwnd not enforced"
        )

        # Reopening the window: the cap (32) and rwnd are the only limits.
        self.peer.send_ack(2, rwnd=64000)
        self.assertEqual(
            self.peer.collect_data(32, timeout=2.0), list(range(2, 34))
        )

        self.peer.send_ack(34, rwnd=64000)
        self.assertEqual(
            self.peer.collect_data(6, timeout=2.0), list(range(34, 40))
        )

        self.finish_transfer()


class LimitedTransmitTests(CwndTrackingMixin, SenderTestBase):
    """RFC 3042: one fresh segment per early duplicate ACK."""

    CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(12)]
    INITIAL_CWND = 8

    def test_first_and_second_duplicate_acks_send_one_segment_each(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(8), list(range(8)))

        self.peer.send_ack(0)
        first = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(first, "no limited transmit on the 1st dup ACK")
        self.assertEqual(first.seq, 8)

        self.peer.send_ack(0)
        second = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(second, "no limited transmit on the 2nd dup ACK")
        self.assertEqual(second.seq, 9)

        # Limited transmit must not inflate the window.
        self.assertAlmostEqual(self.controller.cwnd, 8.0)
        self.assertEqual(len(self.cwnd_events()), 1)

        self.peer.send_ack(0)
        retransmission = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(retransmission, "no fast retransmit")
        self.assertEqual(retransmission.seq, 0)
        self.assertEqual(retransmission.payload, self.CHUNKS[0])

        # RFC 5681 3.2: the two limited-transmit segments are excluded
        # from the ssthresh FlightSize: (10 - 2) / 2 = 4.
        values = self.wait_for_cwnd("fast_recovery", 4.0)
        self.assertAlmostEqual(values["ssthresh"], 4.0)
        self.assertAlmostEqual(self.controller.cwnd, 4.0)

        self.peer.send_ack(10)
        self.assertEqual(self.peer.collect_data(2, timeout=2.0), [10, 11])
        self.finish_transfer()

    def test_further_duplicate_acks_send_nothing_more(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(8), list(range(8)))

        self.peer.send_ack(0)
        self.assertIsNotNone(self.peer.recv_data(timeout=0.5))
        self.peer.send_ack(0)
        self.assertIsNotNone(self.peer.recv_data(timeout=0.5))

        # The third duplicate ACK fast-retransmits the lost segment.
        self.peer.send_ack(0)
        retransmission = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(retransmission)
        self.assertEqual(retransmission.seq, 0)

        # Limited transmit must not fire on the fourth and fifth
        # duplicate ACKs, and the retransmission must not repeat.
        for _ in range(2):
            self.peer.send_ack(0)
        self.assertIsNone(
            self.peer.expect_no_data(0.4),
            "limited transmit fired beyond the second duplicate ACK",
        )
        self.assertEqual(len(self.retransmissions("fast")), 1)

        self.peer.send_ack(10)
        self.assertEqual(self.peer.collect_data(2, timeout=2.0), [10, 11])
        self.finish_transfer()

    def test_limited_transmit_respects_the_receiver_window(self):
        self.peer.rwnd = 2000
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(2), [0, 1])

        # Two duplicate ACKs, but the advertised window is exhausted.
        self.peer.send_ack(0)
        self.peer.send_ack(0)
        self.assertIsNone(
            self.peer.expect_no_data(0.4),
            "limited transmit ignored the advertised window",
        )
        self.assertAlmostEqual(self.controller.cwnd, 8.0)

        self.peer.send_ack(2, rwnd=10000)
        self.assertEqual(
            self.peer.collect_data(8, timeout=2.0), list(range(2, 10))
        )
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "sent beyond cwnd"
        )

        self.peer.send_ack(8, rwnd=10000)
        self.assertEqual(self.peer.collect_data(2, timeout=2.0), [10, 11])
        self.finish_transfer()


class SmallFileSmssTests(CwndTrackingMixin, SenderTestBase):
    """A sub-CHUNK_SIZE file must not redefine SMSS (RFC 5681: SMSS is
    the sender's maximum segment size, not the partial segment size)."""

    CHUNKS = [bytes([65]) * 400]
    INITIAL_CWND = 1

    def test_ack_of_the_partial_segment_grows_by_acked_bytes_over_smss(self):
        self.peer.accept_syn()

        packet = self.peer.recv_data(timeout=2.0)
        self.assertEqual(len(packet.payload), 400)

        self.peer.send_ack(1)
        self.wait_for_cwnd("ack", 1.4)  # N = 400 B / 1000 B = 0.4
        self.assertAlmostEqual(self.controller.cwnd, 1.4)

        self.finish_transfer()


class TimeoutFlightSizeSmssTests(CwndTrackingMixin, SenderTestBase):
    CHUNKS = [bytes([65]) * 1000 for _ in range(6)] + [bytes([90]) * 400]
    INITIAL_CWND = 32

    def test_timeout_flight_size_counts_the_partial_segment_fractionally(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(7), list(range(7)))

        # FlightSize = 6400 B of full segments + 400 B partial = 6.4 SMSS.
        retransmission = self.peer.recv_data(timeout=1.6)
        self.assertIsNotNone(retransmission, "no timeout retransmission")
        self.assertEqual(retransmission.seq, 0)

        values = self.wait_for_cwnd("timeout", 1.0)
        self.assertAlmostEqual(values["ssthresh"], 3.2)  # 6.4 / 2

        self.finish_transfer()


class RechunkSmssTests(CwndTrackingMixin, SenderTestBase):
    """Rechunked payloads define the effective SMSS for ACK growth."""

    CHUNKS = [bytes([65 + i]) * 1000 for i in range(5)]
    PEER_RWND = 600
    INITIAL_CWND = 1

    def test_rechunked_ack_grows_by_chunks_over_the_rechunk_size(self):
        self.peer.accept_syn()

        packet = self.peer.recv_data(timeout=2.0)
        self.assertEqual(len(packet.payload), 600)

        self.peer.send_ack(1)
        self.wait_for_cwnd("ack", 2.0)  # N = 600 B / 600 B = 1
        self.assertAlmostEqual(self.controller.cwnd, 2.0)

        for ack in range(2, 10):
            self.peer.send_ack(ack)
        self.assertEqual(
            self.peer.collect_data(8, timeout=2.0), list(range(1, 9))
        )
        self.finish_transfer()


class DuplicateAckWindowTests(CwndTrackingMixin, SenderTestBase):
    """The protocol's duplicate ACKs carry the receiver's current rwnd
    (buffer occupancy); a decreased window does not stop them from
    counting toward fast retransmit."""

    CHUNKS = [bytes([65 + i]) * 1000 for i in range(8)]
    INITIAL_CWND = 8

    def test_duplicate_acks_with_decreasing_rwnd_still_trigger_fast_retransmit(
        self,
    ):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(8), list(range(8)))

        # The real receiver's rwnd shrinks with every out-of-order
        # buffered segment; these ACKs mirror that.
        self.peer.send_ack(0, rwnd=7000)
        self.peer.send_ack(0, rwnd=6000)
        self.peer.send_ack(0, rwnd=5000)

        retransmission = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(retransmission, "no fast retransmit")
        self.assertEqual(retransmission.seq, 0)

        values = self.wait_for_cwnd("fast_recovery", 4.0)
        self.assertAlmostEqual(values["ssthresh"], 4.0)

        self.finish_transfer()


class LimitedTransmitSingleTests(CwndTrackingMixin, SenderTestBase):
    """Exactly one unsent segment: only one limited transmit is possible."""

    CHUNKS = [bytes([65 + i % 26]) * 1000 for i in range(9)]
    INITIAL_CWND = 8

    def test_one_limited_transmit_segment_is_excluded(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(8), list(range(8)))

        self.peer.send_ack(0)
        self.assertIsNotNone(self.peer.recv_data(timeout=0.5))
        self.peer.send_ack(0)
        self.assertIsNone(
            self.peer.expect_no_data(0.3), "second limited transmit"
        )

        self.peer.send_ack(0)
        retransmission = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(retransmission, "no fast retransmit")
        self.assertEqual(retransmission.seq, 0)

        # FlightSize 9 minus the limited-transmit segment = 8 -> ssthresh 4.
        values = self.wait_for_cwnd("fast_recovery", 4.0)
        self.assertAlmostEqual(values["ssthresh"], 4.0)
        self.assertAlmostEqual(self.controller.cwnd, 4.0)

        self.finish_transfer()


class LimitedTransmitPartialTests(CwndTrackingMixin, SenderTestBase):
    CHUNKS = [bytes([65 + i % 26]) * 1000 for i in range(8)] + [
        bytes([90]) * 400
    ]
    INITIAL_CWND = 8

    def test_partial_sized_limited_transmit_segment_is_excluded(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(8), list(range(8)))

        # The limited-transmit segment is the 400-byte final packet:
        # FlightSize 8.4, excluded down to 8.0 -> ssthresh 4.0.
        self.peer.send_ack(0)
        limited = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(limited)
        self.assertEqual(limited.seq, 8)
        self.assertEqual(limited.payload, self.CHUNKS[8])

        for _ in range(2):
            self.peer.send_ack(0)
        retransmission = self.peer.recv_data(timeout=0.5)
        self.assertIsNotNone(retransmission, "no fast retransmit")
        self.assertEqual(retransmission.seq, 0)

        values = self.wait_for_cwnd("fast_recovery", 4.0)
        self.assertAlmostEqual(values["ssthresh"], 4.0)
        self.assertAlmostEqual(self.controller.cwnd, 4.0)

        self.finish_transfer()


class LimitedTransmitCapTests(CwndTrackingMixin, SenderTestBase):
    CHUNKS = [bytes([65]) * 1000 for _ in range(40)]
    INITIAL_CWND = 32

    def test_limited_transmit_respects_the_packet_cap(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(32), list(range(32)))

        self.peer.send_ack(0)
        self.assertIsNone(
            self.peer.expect_no_data(0.4),
            "limited transmit exceeded the 32-packet cap",
        )

        self.peer.send_ack(32)
        self.assertEqual(
            self.peer.collect_data(8, timeout=2.0), list(range(32, 40))
        )
        self.finish_transfer()


class FlightSizePropagationTests(CwndTrackingMixin, SenderTestBase):
    CHUNKS = [bytes([65 + index % 26]) * 1000 for index in range(8)]
    INITIAL_CWND = 4

    def setUp(self):
        self.orig_timeout = sender_module.TIMEOUT
        sender_module.TIMEOUT = 3.0
        self.addCleanup(
            setattr, sender_module, "TIMEOUT", self.orig_timeout
        )
        super().setUp()

    def test_timeout_flight_size_includes_limited_transmit_segments(self):
        self.peer.accept_syn()
        self.assertEqual(self.peer.collect_data(4), [0, 1, 2, 3])

        # Two duplicate ACKs: limited transmit raises FlightSize to 6.
        self.peer.send_ack(0)
        limited = self.peer.recv_data(timeout=2.0)
        self.assertIsNotNone(limited)
        self.assertEqual(limited.seq, 4)

        self.peer.send_ack(0)
        limited = self.peer.recv_data(timeout=2.0)
        self.assertIsNotNone(limited)
        self.assertEqual(limited.seq, 5)

        # No third duplicate ACK: the loss is recovered by the timeout,
        # whose FlightSize (6) sets ssthresh = 3.
        retransmission = self.peer.recv_data(timeout=6.0)
        self.assertIsNotNone(retransmission, "no timeout retransmission")
        self.assertEqual(retransmission.seq, 0)

        values = self.wait_for_cwnd("timeout", 1.0)
        self.assertAlmostEqual(values["ssthresh"], 3.0)

        # A second timeout of the same segment retains ssthresh (RFC 5681).
        second = self.peer.recv_data(timeout=12.0)
        self.assertIsNotNone(second, "no second timeout retransmission")
        self.assertEqual(second.seq, 0)
        retained = [
            v for v in self.cwnd_events() if v["reason"] == "timeout"
        ][-1]
        self.assertAlmostEqual(retained["ssthresh"], 3.0)

        self.peer.send_ack(1)
        self.peer.send_ack(6)
        self.assertEqual(self.peer.collect_data(2, timeout=2.0), [6, 7])
        self.finish_transfer()


if __name__ == "__main__":
    unittest.main()
