import socket
import sys
import time

from transport.packet import Packet, DATA, ACK, SYN, FIN
from transport.csv_logger import CSVLogger

PORT = 5001
CHUNK_SIZE = 1000
WINDOW_SIZE = 32
TIMEOUT = 1.0
SOCKET_POLL_INTERVAL = 0.05
MAX_RETRIES = 10

logger = CSVLogger("results/sender_log.csv")


def wait_for_ack(sock, expected_ack, receiver_address):
    while True:
        try:
            data, address = sock.recvfrom(2048)
        except socket.timeout:
            return False

        if address != receiver_address:
            continue

        try:
            packet = Packet.unpack(data)
        except ValueError:
            continue

        if packet.packet_type == ACK and packet.ack == expected_ack:
            logger.log(
                "ACK_RECEIVED",
                packet_type="ACK",
                ack=packet.ack
            )
            return True


def send_control_packet(sock, packet, expected_ack, receiver_address):
    for attempt in range(1, MAX_RETRIES + 1):
        sock.sendto(packet.pack(), receiver_address)

        event = "RETRANSMISSION" if attempt > 1 else "PACKET_SENT"

        logger.log(
            event,
            packet_type=packet.packet_type,
            seq=packet.seq,
            attempt=attempt,
            byte_count=len(packet.payload)
        )

        print(
            f"Sent control packet {packet.packet_type}, "
            f"attempt {attempt}/{MAX_RETRIES}"
        )

        sock.settimeout(TIMEOUT)

        if wait_for_ack(sock, expected_ack, receiver_address):
            print(f"ACK {expected_ack} received.")
            return True

        print("Control packet timeout. Retransmitting...")

    return False


def receive_ack(sock, receiver_address):
    try:
        data, address = sock.recvfrom(2048)
    except socket.timeout:
        return None

    if address != receiver_address:
        return None

    try:
        packet = Packet.unpack(data)
    except ValueError:
        return None

    if packet.packet_type == ACK:
        logger.log(
            "ACK_RECEIVED",
            packet_type="ACK",
            ack=packet.ack
        )
        return packet.ack

    return None


def send_file(input_file, receiver_ip):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    receiver_address = (receiver_ip, PORT)

    with open(input_file, "rb") as file:
        chunks = []
        while True:
            chunk = file.read(CHUNK_SIZE)
            if not chunk:
                break
            chunks.append(chunk)

    total_packets = len(chunks)
    total_bytes = sum(len(chunk) for chunk in chunks)

    print(f"File size: {total_bytes} bytes")
    print(f"Total DATA packets: {total_packets}")
    print(f"Window size: {WINDOW_SIZE}")

    # SYN handshake
    print("\nStarting SYN handshake...")

    syn_packet = Packet(packet_type=SYN, seq=0)

    if not send_control_packet(sock, syn_packet, 0, receiver_address):
        print("SYN handshake failed.")
        sock.close()
        return

    print("SYN handshake successful.\n")

    # Sliding Window transfer
    base = 0
    next_seq = 0

    outstanding = {}
    send_times = {}
    retries = {}

    sock.settimeout(SOCKET_POLL_INTERVAL)

    while base < total_packets:

        # Fill the sender window
        while next_seq < total_packets and next_seq < base + WINDOW_SIZE:
            packet = Packet(
                packet_type=DATA,
                seq=next_seq,
                payload=chunks[next_seq]
            )

            sock.sendto(packet.pack(), receiver_address)

            outstanding[next_seq] = packet
            send_times[next_seq] = time.monotonic()
            retries[next_seq] = 0

            logger.log(
                "PACKET_SENT",
                packet_type="DATA",
                seq=next_seq,
                byte_count=len(chunks[next_seq]),
                attempt=1
            )

            print(f"Sent DATA packet {next_seq}")
            next_seq += 1

        # Process cumulative ACK
        ack_number = receive_ack(sock, receiver_address)

        if ack_number is not None:
            if base < ack_number <= next_seq:
                print(
                    f"Cumulative ACK {ack_number} received. "
                    f"Window base: {base} -> {ack_number}"
                )

                for seq in range(base, ack_number):
                    outstanding.pop(seq, None)
                    send_times.pop(seq, None)
                    retries.pop(seq, None)

                base = ack_number

        # Retransmit expired packets
        current_time = time.monotonic()

        for seq in list(outstanding.keys()):
            if current_time - send_times[seq] >= TIMEOUT:

                if retries[seq] >= MAX_RETRIES:
                    print(f"Packet {seq} exceeded maximum retries.")
                    logger.log(
                        "TRANSFER_FAILED",
                        packet_type="DATA",
                        seq=seq,
                        details="Maximum retries exceeded"
                    )
                    sock.close()
                    return

                retries[seq] += 1
                attempt = retries[seq] + 1

                print(
                    f"Timeout! Retransmitting packet {seq}, "
                    f"attempt {attempt}"
                )

                sock.sendto(outstanding[seq].pack(), receiver_address)

                logger.log(
                    "RETRANSMISSION",
                    packet_type="DATA",
                    seq=seq,
                    byte_count=len(outstanding[seq].payload),
                    attempt=attempt,
                    details="Timeout"
                )

                send_times[seq] = time.monotonic()

    print("\nAll DATA packets acknowledged.")

    # FIN handshake
    print("Starting FIN handshake...")

    fin_packet = Packet(packet_type=FIN, seq=total_packets)

    if send_control_packet(
        sock, fin_packet, total_packets, receiver_address
    ):
        print("\nTransfer completed successfully.")
        print(f"Total packets sent: {total_packets}")
        print(f"Total bytes sent: {total_bytes}")
        logger.log(
            "TRANSFER_COMPLETED",
            packet_type="FIN",
            seq=total_packets,
            byte_count=total_bytes
        )
    else:
        print("FIN handshake failed.")
        logger.log(
            "TRANSFER_FAILED",
            packet_type="FIN",
            seq=total_packets,
            details="FIN handshake failed"
        )

    sock.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python -m transport.sender <input_file> <receiver_ip>")
        sys.exit(1)

    send_file(sys.argv[1], sys.argv[2])
