import socket
import sys
from pathlib import Path

from transport.packet import Packet, DATA, ACK, SYN, FIN
from transport.csv_logger import CSVLogger

HOST = "0.0.0.0"
PORT = 5001
BUFFER_SIZE = 2048
FIN_LINGER = 11.0

    # Capacity of the out-of-order buffer advertised through rwnd (bytes).
    # Larger than the sender's default 32-packet window.
RECV_BUFFER_BYTES = 64000

logger = CSVLogger("results/receiver_log.csv")


def send_ack(sock, address, ack_number, rwnd=0, ts_echo=0):
    """Send a cumulative ACK with receiver's free buffer capacity (rwnd, bytes) and trigger packet's timestamp echo (ts_echo, monotonic microseconds)."""
    packet = Packet(
        packet_type=ACK,
        ack=ack_number,
        rwnd=rwnd,
        ts_echo=ts_echo
    )
    sock.sendto(packet.pack(), address)

    logger.log(
        "ACK_SENT",
        packet_type="ACK",
        ack=ack_number,
        details=f"rwnd={rwnd}"
    )


def receive_file(output_file):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))

    print(f"Receiver listening on port {PORT}...")

    expected_seq = 0
    received_bytes = 0
    out_of_order = {}
    buffered_bytes = 0
    sender_address = None

    def current_rwnd():
        """Free receive-buffer capacity in bytes (Phase 2 flow control)."""

        return max(0, RECV_BUFFER_BYTES - buffered_bytes)

    with open(output_file, "wb") as file:
        while True:
            data, address = sock.recvfrom(BUFFER_SIZE)

            try:
                packet = Packet.unpack(data)
            except ValueError as error:
                print(f"Invalid packet: {error}")
                logger.log("INVALID_PACKET", details=str(error))
                continue

            # SYN handshake
            if packet.packet_type == SYN:
                if sender_address is None:
                    sender_address = address

                if address == sender_address:
                    print("SYN received. Sending SYN ACK.")

                    logger.log(
                        "SYN_RECEIVED",
                        packet_type="SYN",
                        seq=packet.seq
                    )

                    send_ack(sock, address, 0, rwnd=current_rwnd())

                continue

            if sender_address is None or address != sender_address:
                continue

            # DATA packet
            if packet.packet_type == DATA:

                logger.log(
                    "DATA_RECEIVED",
                    packet_type="DATA",
                    seq=packet.seq,
                    byte_count=len(packet.payload)
                )

                if packet.seq == expected_seq:
                    file.write(packet.payload)
                    received_bytes += len(packet.payload)

                    print(
                        f"Received packet {packet.seq} "
                        f"({len(packet.payload)} bytes)"
                    )

                    expected_seq += 1

                    # Flush consecutively buffered packets
                    while expected_seq in out_of_order:
                        buffered_payload = out_of_order.pop(expected_seq)
                        buffered_bytes -= len(buffered_payload)

                        file.write(buffered_payload)
                        received_bytes += len(buffered_payload)

                        print(f"Writing buffered packet {expected_seq}")

                        logger.log(
                            "BUFFERED_PACKET_WRITTEN",
                            packet_type="DATA",
                            seq=expected_seq,
                            byte_count=len(buffered_payload)
                        )

                        expected_seq += 1

                elif packet.seq > expected_seq:
                    if packet.seq in out_of_order:
                        print(f"Duplicate buffered packet {packet.seq}")

                        logger.log(
                            "DUPLICATE_BUFFERED",
                            packet_type="DATA",
                            seq=packet.seq
                        )
                    elif buffered_bytes + len(packet.payload) > (
                        RECV_BUFFER_BYTES
                    ):
                        # Defensive guard: sender ignoring advertised rwnd must not grow buffer
                        # without bound; gap packet will be retransmitted later.
                        print(
                            f"Receive buffer full; dropping packet "
                            f"{packet.seq}"
                        )

                        logger.log(
                            "BUFFER_OVERFLOW_DROPPED",
                            packet_type="DATA",
                            seq=packet.seq,
                            byte_count=len(packet.payload),
                            details=f"Expected {expected_seq}"
                        )
                    else:
                        out_of_order[packet.seq] = packet.payload
                        buffered_bytes += len(packet.payload)

                        print(
                            f"Buffered out-of-order packet {packet.seq}; "
                            f"expected {expected_seq}"
                        )

                        logger.log(
                            "OUT_OF_ORDER_BUFFERED",
                            packet_type="DATA",
                            seq=packet.seq,
                            byte_count=len(packet.payload),
                            details=f"Expected {expected_seq}"
                        )

                else:
                    print(f"Duplicate packet {packet.seq} received.")

                    logger.log(
                        "DUPLICATE_RECEIVED",
                        packet_type="DATA",
                        seq=packet.seq
                    )

                # Cumulative ACK: next expected sequence, free capacity (rwnd),
                # and the trigger packet's timestamp echo (ts_echo).
                send_ack(
                    sock,
                    address,
                    expected_seq,
                    rwnd=current_rwnd(),
                    ts_echo=packet.ts_send
                )

            # FIN handshake
            elif packet.packet_type == FIN:
                if packet.seq == expected_seq and not out_of_order:
                    print("FIN received. Sending FIN ACK.")

                    logger.log(
                        "FIN_RECEIVED",
                        packet_type="FIN",
                        seq=packet.seq
                    )

                    send_ack(sock, address, expected_seq,
                             rwnd=current_rwnd())

                    # Remain available for retransmitted FIN packets
                    sock.settimeout(FIN_LINGER)

                    while True:
                        try:
                            fin_data, fin_address = sock.recvfrom(BUFFER_SIZE)
                        except socket.timeout:
                            break

                        if fin_address != sender_address:
                            continue

                        try:
                            fin_packet = Packet.unpack(fin_data)
                        except ValueError:
                            continue

                        if (
                            fin_packet.packet_type == FIN
                            and fin_packet.seq == expected_seq
                        ):
                            print("Duplicate FIN received. Sending ACK again.")

                            logger.log(
                                "DUPLICATE_FIN_RECEIVED",
                                packet_type="FIN",
                                seq=fin_packet.seq
                            )

                            send_ack(sock, sender_address, expected_seq,
                                     rwnd=current_rwnd())

                    break

                else:
                    send_ack(sock, address, expected_seq,
                             rwnd=current_rwnd())

    sock.close()

    print("\nTransfer finished.")
    print(f"File saved to: {Path(output_file).resolve()}")
    print(f"Total bytes received: {received_bytes}")

    logger.log(
        "TRANSFER_COMPLETED",
        byte_count=received_bytes,
        details="Receiver finished writing file"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m transport.receiver <output_file>")
        sys.exit(1)

    receive_file(sys.argv[1])
