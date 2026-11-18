"""On/off CROSS traffic source: fixed bursts of CROSS datagrams."""

import argparse
import csv
import socket
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from transport.packet import CROSS, HEADER_SIZE, Packet

PAYLOAD_BYTES = 1000


def on_windows(duration_s, period_s, on_s):
    windows = []
    index = 0
    while index * period_s < duration_s:
        start = index * period_s
        windows.append((start, start + on_s))
        index += 1
    return windows


def datagram_interval_s(rate_bps, payload_bytes):
    return (HEADER_SIZE + payload_bytes) * 8 / rate_bps


def send_times(start, end, interval):
    times = []
    step = 0
    while start + step * interval < end:
        times.append(start + step * interval)
        step += 1
    return times


def build_datagram(seq, payload_bytes):
    return Packet(
        packet_type=CROSS, seq=seq, payload=b"\x00" * payload_bytes
    ).pack()


def run(target, rate_bps, on_s, period_s, duration_s, log_path):
    interval = datagram_interval_s(rate_bps, PAYLOAD_BYTES)
    windows = on_windows(duration_s, period_s, on_s)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    origin = time.monotonic()
    seq = 0

    with open(log_path, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["burst", "start_s", "packets"])
        for index, (start, end) in enumerate(windows):
            count = 0
            for offset in send_times(start, end, interval):
                delay = origin + offset - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                sock.sendto(build_datagram(seq, PAYLOAD_BYTES), target)
                seq += 1
                count += 1
            writer.writerow([index, f"{start:.3f}", count])
            file.flush()

    sock.close()
    return len(windows)


def parse_address(text):
    host, _, port = text.rpartition(":")
    return host, int(port)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", type=parse_address,
                        help="router sender face ip:port")
    parser.add_argument("--rate-mbps", type=float, default=2.5,
                        help="rate during each burst in Mbps")
    parser.add_argument("--on-ms", type=float, default=200.0)
    parser.add_argument("--period-ms", type=float, default=1000.0)
    parser.add_argument("--duration-s", type=float, required=True)
    parser.add_argument("--log", required=True)
    args = parser.parse_args(argv)

    bursts = run(
        target=args.target,
        rate_bps=args.rate_mbps * 1_000_000,
        on_s=args.on_ms / 1000.0,
        period_s=args.period_ms / 1000.0,
        duration_s=args.duration_s,
        log_path=args.log,
    )
    print(f"cross: {bursts} bursts sent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
