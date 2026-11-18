import importlib.util
import shutil
import socket
import tempfile
import threading
import unittest
from pathlib import Path

from transport.packet import CROSS, HEADER_SIZE, Packet

MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "tests" / "mininet"
    / "cross_traffic.py"
)


def load_source():
    spec = importlib.util.spec_from_file_location("cross_traffic", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


source = load_source()


class OnOffScheduleTests(unittest.TestCase):
    def test_bursts_start_at_each_period_boundary(self):
        self.assertEqual(
            source.on_windows(duration_s=3.0, period_s=1.0, on_s=0.2),
            [(0.0, 0.2), (1.0, 1.2), (2.0, 2.2)],
        )

    def test_no_burst_starts_at_or_after_the_duration(self):
        windows = source.on_windows(duration_s=2.0, period_s=1.0, on_s=0.2)

        self.assertEqual(len(windows), 2)

    def test_packets_fill_a_burst_at_the_configured_rate(self):
        interval = source.datagram_interval_s(
            rate_bps=2_500_000, payload_bytes=1000
        )
        expected = (HEADER_SIZE + 1000) * 8 / 2_500_000

        self.assertAlmostEqual(interval, expected)

        times = source.send_times(0.0, 0.2, interval)
        self.assertEqual(len(times), 61)
        self.assertLess(times[-1], 0.2)

    def test_send_times_stay_inside_the_window(self):
        times = source.send_times(5.0, 5.01, 0.004)

        self.assertTrue(all(5.0 <= value < 5.01 for value in times))
        self.assertEqual(times[0], 5.0)


class CrossDatagramTests(unittest.TestCase):
    def test_datagrams_are_cross_packets_of_the_requested_size(self):
        raw = source.build_datagram(seq=7, payload_bytes=1000)
        packet = Packet.unpack(raw)

        self.assertEqual(packet.packet_type, CROSS)
        self.assertEqual(packet.seq, 7)
        self.assertEqual(len(packet.payload), 1000)
        self.assertEqual(len(raw), HEADER_SIZE + 1000)


class CrossSourceRunTests(unittest.TestCase):
    def test_short_run_sends_the_scheduled_bursts_to_the_target(self):
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(0.2)
        self.addCleanup(receiver.close)
        target = receiver.getsockname()

        tmp = Path(tempfile.mkdtemp(prefix="cross_"))
        self.addCleanup(shutil.rmtree, tmp, True)
        log_path = tmp / "cross_log.csv"

        received = []
        done = threading.Event()

        def collect():
            while not done.is_set():
                try:
                    data, _ = receiver.recvfrom(65535)
                except socket.timeout:
                    continue
                received.append(data)

        collector = threading.Thread(target=collect, daemon=True)
        collector.start()
        self.addCleanup(done.set)

        bursts = source.run(
            target=target,
            rate_bps=2_500_000,
            on_s=0.1,
            period_s=0.25,
            duration_s=0.6,
            log_path=log_path,
        )
        done.set()
        collector.join(timeout=2.0)

        self.assertEqual(bursts, 3)
        self.assertGreater(len(received), 60)
        self.assertTrue(
            all(Packet.unpack(data).packet_type == CROSS for data in received)
        )
        self.assertIn("burst", log_path.read_text())


if __name__ == "__main__":
    unittest.main()
