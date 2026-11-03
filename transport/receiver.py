import socket
import sys
from pathlib import Path

from transport.packet import Packet, DATA, ACK, SYN, FIN
from transport.csv_logger import CSVLogger

HOST = "0.0.0.0"
PORT = 5001
BUFFER_SIZE = 2048
FIN_LINGER = 11.0

logger = CSVLogger("results/receiver_log.csv")


def send_ack(sock, address, ack_number):
    packet = Packet(packet_type=ACK, ack=ack_number)
    sock.sendto(packet.pack(), address)

    logger.log(
        "ACK_SENT",
        packet_type="ACK",
        ack=ack_number
    )


def receive_file(output_file):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))

    print(f"Receiver listening on port {PORT}...")

    expected_seq = 0
    received_bytes = 0
    out_of_order = {}
    sender_address = None

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

                    send_ack(sock, address, 0)

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
                    if packet.seq not in out_of_order:
                        out_of_order[packet.seq] = packet.payload

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
                        print(f"Duplicate buffered packet {packet.seq}")

                        logger.log(
                            "DUPLICATE_BUFFERED",
                            packet_type="DATA",
                            seq=packet.seq
                        )

                else:
                    print(f"Duplicate packet {packet.seq} received.")

                    logger.log(
                        "DUPLICATE_RECEIVED",
                        packet_type="DATA",
                        seq=packet.seq
                    )

                # Cumulative ACK: next expected sequence number
                send_ack(sock, address, expected_seq)

            # FIN handshake
            elif packet.packet_type == FIN:
                if packet.seq == expected_seq and not out_of_order:
                    print("FIN received. Sending FIN ACK.")

                    logger.log(
                        "FIN_RECEIVED",
                        packet_type="FIN",
                        seq=packet.seq
                    )

                    send_ack(sock, address, expected_seq)

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

                            send_ack(sock, sender_address, expected_seq)

                    break

                else:
                    send_ack(sock, address, expected_seq)

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
