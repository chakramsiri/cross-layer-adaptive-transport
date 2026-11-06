"""Deterministic test helpers shared by the transport test suite."""

import contextlib
import csv
import io
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from transport.packet import Packet, DATA, ACK, SYN, FIN
from transport.csv_logger import CSVLogger
from transport import sender as sender_module


TEST_PORT_RANGE_START = 22100
TEST_PORT_RANGE_SIZE = 100

DEFAULT_RWND = 64000  # bytes, mirrors receiver.RECV_BUFFER_BYTES

_port_cursor = TEST_PORT_RANGE_START


def pick_test_port():
    """Return the next free UDP port from a local, non-ephemeral range.

    The range stays below Linux's ephemeral ports (>= 32768), so stray
    packets from lingering threads can never arrive here.
    """

    global _port_cursor

    for _ in range(TEST_PORT_RANGE_SIZE):
        port = _port_cursor
        _port_cursor = TEST_PORT_RANGE_START + (
            (_port_cursor - TEST_PORT_RANGE_START + 1) % TEST_PORT_RANGE_SIZE
        )

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port

    raise RuntimeError("no free test port in the test port range")


class ScriptedPeer:
    """Scripted UDP peer impersonating the receiver in sender tests.

    Every ACK is sent explicitly by the test, so duplicate-ACK and
    retransmission scenarios stay deterministic with no real network.
    """

    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", port))
        self.sender_addr = None
        self.payloads = {}      # seq -> payload of first arrival
        self.timestamps = {}    # seq -> ts_send of first arrival
        self.arrivals = []      # seq of every DATA receipt, retransmits included
        self.rwnd = DEFAULT_RWND  # advertised window used by send_ack()

    def close(self):
        self.sock.close()

    #  receiving

    def _recv_raw(self, timeout):
        self.sock.settimeout(max(0.01, timeout))

        try:
            raw, addr = self.sock.recvfrom(65535)
        except (socket.timeout, OSError):
            return None, None

        try:
            return Packet.unpack(raw), addr
        except ValueError:
            return None, None

    def _record_data(self, packet):
        self.arrivals.append(packet.seq)
        self.payloads.setdefault(packet.seq, packet.payload)
        self.timestamps.setdefault(packet.seq, packet.ts_send)

    def recv_any(self, timeout=2.0):
        """Return (packet, address) for the next UDP datagram, or (None, None)."""

        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            packet, addr = self._recv_raw(deadline - time.monotonic())
            if packet is not None:
                return packet, addr

        return None, None

    def accept_syn(self, timeout=5.0):
        """Wait for SYN and acknowledge it. Raises if it never arrives."""

        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            packet, addr = self._recv_raw(deadline - time.monotonic())

            if packet is None:
                continue

            if packet.packet_type == SYN:
                self.sender_addr = addr
                self.send_ack(0)
                return True

        raise AssertionError("SYN was not received by the scripted peer")

    def recv_data(self, timeout=2.0):
        """Return the next DATA packet (recording it), or None on timeout."""

        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            packet, _ = self._recv_raw(deadline - time.monotonic())

            if packet is None:
                continue

            if packet.packet_type == DATA:
                self._record_data(packet)
                return packet

        return None

    def collect_data(self, count, timeout=2.0):
        """Collect until `count` distinct DATA sequence numbers have arrived."""

        deadline = time.monotonic() + timeout
        seen = set()

        while len(seen) < count and time.monotonic() < deadline:
            packet = self.recv_data(deadline - time.monotonic())
            if packet is None:
                break
            seen.add(packet.seq)

        return sorted(seen)

    def expect_no_data(self, timeout):
        """Return a DATA packet if one unexpectedly arrives, else None."""

        return self.recv_data(timeout)

    def wait_for_fin(self, timeout=5.0):
        """Wait for FIN and acknowledge it. Returns True on success."""

        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            packet, _ = self._recv_raw(deadline - time.monotonic())

            if packet is None:
                continue

            if packet.packet_type == DATA:
                self._record_data(packet)
            elif packet.packet_type == FIN:
                self.send_ack(packet.seq)
                return True

        return False

    #  sending

    def send_ack(self, ack, ts_echo=0, rwnd=None):
        """Send an ACK, defaulting to the peer's currently advertised rwnd."""

        assert self.sender_addr is not None, "accept_syn() must run first"

        if rwnd is None:
            rwnd = self.rwnd

        packet = Packet(
            packet_type=ACK, ack=ack, rwnd=rwnd, ts_echo=ts_echo
        )
        self.sock.sendto(packet.pack(), self.sender_addr)


class _DiscardLogger:
    """Logger stand-in that drops every row (protects results/ logs)."""

    def log(self, *args, **kwargs):
        pass


class SenderTestBase(unittest.TestCase):
    """Deterministic harness for sender-side tests.

    Runs transport.sender.send_file in a background thread against a
    ScriptedPeer, isolating module globals (port, logger) per test.
    Subclasses set CHUNKS (payloads) and PEER_RWND (advertised window).
    """

    CHUNKS = [bytes([65 + index]) * 1000 for index in range(5)]
    PEER_RWND = DEFAULT_RWND

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.input_path = Path(self.tmp.name) / "input.bin"
        self.input_path.write_bytes(b"".join(self.CHUNKS))
        self.log_path = Path(self.tmp.name) / "sender.csv"

        self.port = pick_test_port()
        self.orig_port = sender_module.PORT
        self.orig_logger = sender_module.logger
        sender_module.PORT = self.port
        sender_module.logger = CSVLogger(str(self.log_path))
        self.addCleanup(self._restore_sender_module)

        # Bind the peer before the sender starts so the SYN cannot be lost.
        self.peer = ScriptedPeer(self.port)
        self.peer.rwnd = self.PEER_RWND
        self.addCleanup(self.peer.close)

        self.sender_error = None
        self.thread = threading.Thread(target=self._run_sender, daemon=True)
        self.addCleanup(self._stop_sender_quietly)
        self.thread.start()

    #  setup

    def _restore_sender_module(self):
        # Restore globals unconditionally so a failed test cannot poison
        # later ones; a still-alive thread gets a discarding logger.
        if self.thread.is_alive():
            sender_module.logger = _DiscardLogger()

        sender_module.PORT = self.orig_port

        if not self.thread.is_alive():
            sender_module.logger = self.orig_logger

    def _run_sender(self):
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                sender_module.send_file(str(self.input_path), "127.0.0.1")
        except Exception as error:  # pragma: no cover - failure path
            self.sender_error = error

    def _stop_sender_quietly(self):
        """Best-effort completion so a failed test cannot leak a thread.

        The ACK sweep is state-agnostic: it acknowledges every cumulative
        point until the sender completes, whatever state the test left.
        """

        if not self.thread.is_alive():
            return

        try:
            if self.peer.sender_addr is None:
                # With SYN backoff the first retry waits 2 s (RFC 6298 5.5).
                self.peer.accept_syn(timeout=2.6)
        except Exception:
            # Still run the sweep and join below so globals can be restored.
            pass

        deadline = time.monotonic() + 2.0

        try:
            while self.thread.is_alive() and time.monotonic() < deadline:
                for ack in range(len(self.CHUNKS) + 1):
                    self.peer.send_ack(ack, rwnd=DEFAULT_RWND)
                self.peer.wait_for_fin(timeout=0.2)
        except Exception:
            pass

        self.thread.join(timeout=0.5)

    #  scenario helpers

    def open_window(self):
        """SYN handshake plus the initial window of DATA packets."""

        self.peer.accept_syn()
        seqs = self.peer.collect_data(len(self.CHUNKS))
        self.assertEqual(seqs, list(range(len(self.CHUNKS))))

    def finish_transfer(self):
        self.peer.send_ack(len(self.CHUNKS))
        self.assertTrue(
            self.peer.wait_for_fin(), "FIN handshake did not complete"
        )
        self.thread.join(timeout=5.0)
        self.assertFalse(self.thread.is_alive(), "sender thread did not exit")
        self.assertIsNone(
            self.sender_error, f"sender raised: {self.sender_error!r}"
        )

    def sender_rows(self):
        with open(self.log_path) as file:
            return list(csv.DictReader(file))

    def events(self, name):
        return [row for row in self.sender_rows() if row["event"] == name]

    def wait_for_event(self, name, count=1, timeout=2.0):
        """Poll the sender log until `count` events have appeared."""

        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            rows = self.events(name)
            if len(rows) >= count:
                return rows
            time.sleep(0.02)

        return self.events(name)

    def retransmissions(self, kind=None):
        rows = self.events("RETRANSMISSION")

        if kind == "timeout":
            rows = [row for row in rows if row["details"] == "Timeout"]
        elif kind == "fast":
            rows = [
                row
                for row in rows
                if row["details"].startswith("Fast retransmit")
            ]

        return rows
