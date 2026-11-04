"""Deterministic test helpers shared by the transport test suite."""

import socket
import time

from transport.packet import Packet, DATA, ACK, SYN, FIN


TEST_PORT_RANGE_START = 22100
TEST_PORT_RANGE_SIZE = 100


def pick_test_port():
    """Return a free UDP port from a local, non-ephemeral range.

    The range stays below Linux's ephemeral port range (>= 32768) so that
    stray packets from lingering test threads can never arrive here.
    """

    for port in range(
        TEST_PORT_RANGE_START, TEST_PORT_RANGE_START + TEST_PORT_RANGE_SIZE
    ):
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
    retransmission scenarios are deterministic and need no real network.
    """

    def __init__(self, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", port))
        self.sender_addr = None
        self.payloads = {}   # seq -> payload of first arrival
        self.arrivals = []   # seq of every DATA receipt, retransmits included

    def close(self):
        self.sock.close()

    # -- receiving ---------------------------------------------------------

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

    # -- sending -----------------------------------------------------------

    def send_ack(self, ack):
        assert self.sender_addr is not None, "accept_syn() must run first"

        packet = Packet(packet_type=ACK, ack=ack)
        self.sock.sendto(packet.pack(), self.sender_addr)
