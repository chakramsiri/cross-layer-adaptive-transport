import socket
import sys
import time

from transport.packet import Packet, DATA, ACK, SYN, FIN
from transport.csv_logger import CSVLogger
from transport.rtt import MAX_RTO, RttEstimator, encode_timestamp, rtt_seconds

PORT = 5001
CHUNK_SIZE = 1000
WINDOW_SIZE = 32
TIMEOUT = 1.0   # Control retry interval and initial DATA RTO
SOCKET_POLL_INTERVAL = 0.05
MAX_RETRIES = 10

logger = CSVLogger("results/sender_log.csv")


def wait_for_ack(sock, expected_ack, receiver_address):
    """Return the matching ACK packet, or None when the wait times out."""

    while True:
        try:
            data, address = sock.recvfrom(2048)
        except socket.timeout:
            return None

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
            return packet


def send_control_packet(sock, packet, expected_ack, receiver_address,
                        backoff=False):
    """Send a control packet until acknowledged, retrying SYN with
    exponential backoff (RFC 6298 2.1/5.5) and FIN at the fixed TIMEOUT.

    Returns (ack_packet, attempts); ack_packet is None when retries are
    exhausted.
    """

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

        if backoff:
            sock.settimeout(min(TIMEOUT * 2 ** (attempt - 1), MAX_RTO))
        else:
            sock.settimeout(TIMEOUT)

        ack_packet = wait_for_ack(sock, expected_ack, receiver_address)

        if ack_packet is not None:
            print(f"ACK {expected_ack} received.")
            return ack_packet, attempt

        print("Control packet timeout. Retransmitting...")

    return None, MAX_RETRIES


def receive_ack(sock, receiver_address):
    """Return (packet, arrival_time) for the next ACK, or (None, None).
    arrival_time captured immediately after recvfrom(), before parsing
    or logging; RTT sampling measures datagram arrival."""

    try:
        data, address = sock.recvfrom(2048)
    except socket.timeout:
        return None, None

    arrival = time.monotonic()

    if address != receiver_address:
        return None, None

    try:
        packet = Packet.unpack(data)
    except ValueError:
        return None, None

    if packet.packet_type == ACK:
        logger.log(
            "ACK_RECEIVED",
            packet_type="ACK",
            ack=packet.ack
        )
        return packet, arrival

    return None, None


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

    syn_ack, syn_attempts = send_control_packet(
        sock, syn_packet, 0, receiver_address, backoff=True
    )

    if syn_ack is None:
        print("SYN handshake failed.")
        sock.close()
        return

    print("SYN handshake successful.\n")

    # A window smaller than one default payload cannot carry CHUNK_SIZE
    # bytes; re-chunk to the advertised window instead.  The receiver
    # concatenates payloads in sequence order, so file integrity holds.
    if chunks and 0 < syn_ack.rwnd < len(chunks[0]):
        data = b"".join(chunks)
        chunks = [
            data[i:i + syn_ack.rwnd]
            for i in range(0, len(data), syn_ack.rwnd)
        ]
        total_packets = len(chunks)

        print(
            f"Advertised window {syn_ack.rwnd} is below one payload; "
            f"re-chunked into {total_packets} packets."
        )

    # RFC 6298 (5.7): re-initialize data-phase RTO to 3 s only if the RTO
    # in use is below 3 s; a backed-off RTO of 3 s or more is kept.
    if syn_attempts > 1:
        handshake_rto = min(TIMEOUT * 2 ** (syn_attempts - 1), MAX_RTO)
        initial_rto = 3.0 if handshake_rto < 3.0 else handshake_rto
    else:
        initial_rto = TIMEOUT

    estimator = RttEstimator(initial_rto=initial_rto)

    logger.log(
        "RTO_INITIAL",
        details=f"rto={initial_rto:.6f} handshake_attempts={syn_attempts}"
    )

    # Sliding Window transfer
    base = 0
    next_seq = 0

    outstanding = {}
    retries = {}

    # Prevent multiple RTT samples from the same packet.
    sampled_seqs = set()

    # Stamps proven ambiguous (an echo matched several outstanding packets)
    # can never be associated with one packet again, so they are never
    # sampled -- not even when a later echo happens to match exactly one.
    ambiguous_stamps = set()

    # Track the advertised byte window and payload bytes currently in flight.
    # Retransmissions reuse bytes already counted in flight.
    last_rwnd = syn_ack.rwnd
    in_flight_bytes = 0

    # Absolute deadline for the oldest unacknowledged packet.
    # Non-advancing ACKs may update RTO but must not move this deadline.
    rto_deadline = None

    # Suppress further fast retransmits for this base until it advances.
    duplicate_acks = 0
    base_retx_done = False

    sock.settimeout(SOCKET_POLL_INTERVAL)

    while base < total_packets:

        # Bound new transmissions by the packet limit and advertised byte window.
        while (
            next_seq < total_packets
            and next_seq < base + WINDOW_SIZE
            and in_flight_bytes + len(chunks[next_seq]) <= last_rwnd
        ):
            send_time = time.monotonic()
            packet = Packet(
                packet_type=DATA,
                seq=next_seq,
                payload=chunks[next_seq],
                ts_send=encode_timestamp(send_time)
            )

            sock.sendto(packet.pack(), receiver_address)

            outstanding[next_seq] = packet
            retries[next_seq] = 0
            in_flight_bytes += len(chunks[next_seq])

            if next_seq == base:
                rto_deadline = send_time + estimator.rto

            logger.log(
                "PACKET_SENT",
                packet_type="DATA",
                seq=next_seq,
                byte_count=len(chunks[next_seq]),
                attempt=1
            )

            print(f"Sent DATA packet {next_seq}")
            next_seq += 1


        # Nothing is in flight and the next payload cannot fit the
        # advertised window.  The receiver only ACKs received data, so no
        # window update can ever arrive: progress is impossible.
        if (
            not outstanding
            and next_seq < total_packets
            and len(chunks[next_seq]) > last_rwnd
        ):
            print(
                f"Stalled: advertised window {last_rwnd} cannot fit "
                f"payload {len(chunks[next_seq])}."
            )
            logger.log(
                "TRANSFER_FAILED",
                packet_type="DATA",
                seq=next_seq,
                details=(
                    f"receiver window {last_rwnd} smaller than payload "
                    f"{len(chunks[next_seq])} with nothing in flight"
                )
            )
            sock.close()
            return


        ack_packet, ack_arrival = receive_ack(sock, receiver_address)

        if ack_packet is not None:
            ack_number = ack_packet.ack

            # Ignore future ACKs; stale ACKs must not change sender state.
            ack_is_valid = ack_number <= next_seq

            if ack_is_valid:
                # Advancing ACKs may update rwnd; duplicate ACKs may only reduce it.
                # Stale ACKs never change the advertised window.
                if base < ack_number <= next_seq:
                    if ack_packet.rwnd != last_rwnd:
                        last_rwnd = ack_packet.rwnd
                        logger.log(
                            "RWND_UPDATE",
                            packet_type="ACK",
                            ack=ack_number,
                            details=f"rwnd={last_rwnd}"
                        )
                elif ack_number == base and base < next_seq:
                    if ack_packet.rwnd < last_rwnd:
                        last_rwnd = ack_packet.rwnd
                        logger.log(
                            "RWND_UPDATE",
                            packet_type="ACK",
                            ack=ack_number,
                            details=f"rwnd={last_rwnd}"
                        )


                    # Match the echo to one outstanding, never-retransmitted packet.
                    # Accept at most one RTT sample per packet (Karn's algorithm).
                    # A zero echo means "no timestamp" and is skipped.
                    if ack_packet.ts_echo:
                        matches = [
                            seq
                            for seq, packet in outstanding.items()
                            if packet.ts_send == ack_packet.ts_echo
                        ]

                        if len(matches) > 1:
                            # Ambiguous echo: poison the stamp so no later
                            # ACK echoing it can be mis-associated.
                            ambiguous_stamps.add(ack_packet.ts_echo)
                        elif len(matches) == 1:
                            candidate = matches[0]

                            if (
                                retries.get(candidate, 0) == 0
                                and candidate not in sampled_seqs
                                and ack_packet.ts_echo not in ambiguous_stamps
                            ):
                                rtt = rtt_seconds(
                                    ack_packet.ts_echo, ack_arrival
                                )
                                estimator.update(rtt)
                                sampled_seqs.add(candidate)

                                logger.log(
                                    "RTT_SAMPLE",
                                    packet_type="DATA",
                                    seq=candidate,
                                    details=(
                                        f"rtt={rtt:.6f} "
                                        f"srtt={estimator.srtt:.6f} "
                                        f"rttvar={estimator.rttvar:.6f} "
                                        f"rto={estimator.rto:.6f}"
                                    )
                                )

                if base < ack_number <= next_seq:
                    # Advance the window and re-arm the timer for the new base.
                    print(
                        f"Cumulative ACK {ack_number} received. "
                        f"Window base: {base} -> {ack_number}"
                    )

                    # RTT sampling must happen BEFORE popping outstanding packets,
                    # since the echo identifies a specific outstanding packet.
                    if ack_packet.ts_echo:
                        matches = [
                            seq
                            for seq, packet in outstanding.items()
                            if packet.ts_send == ack_packet.ts_echo
                        ]

                        if len(matches) > 1:
                            # Ambiguous echo: poison the stamp so no later
                            # ACK echoing it can be mis-associated.
                            ambiguous_stamps.add(ack_packet.ts_echo)
                        elif len(matches) == 1:
                            candidate = matches[0]

                            if (
                                retries.get(candidate, 0) == 0
                                and candidate not in sampled_seqs
                                and ack_packet.ts_echo not in ambiguous_stamps
                            ):
                                rtt = rtt_seconds(
                                    ack_packet.ts_echo, ack_arrival
                                )
                                estimator.update(rtt)
                                sampled_seqs.add(candidate)

                                logger.log(
                                    "RTT_SAMPLE",
                                    packet_type="DATA",
                                    seq=candidate,
                                    details=(
                                        f"rtt={rtt:.6f} "
                                        f"srtt={estimator.srtt:.6f} "
                                        f"rttvar={estimator.rttvar:.6f} "
                                        f"rto={estimator.rto:.6f}"
                                    )
                                )

                    for seq in range(base, ack_number):
                        in_flight_bytes -= len(outstanding[seq].payload)
                        outstanding.pop(seq, None)
                        retries.pop(seq, None)
                        sampled_seqs.discard(seq)

                    base = ack_number
                    duplicate_acks = 0
                    base_retx_done = False

                    if base < total_packets:
                        rto_deadline = ack_arrival + estimator.rto

                elif ack_number == base and base < next_seq:
                    # Three duplicate ACKs trigger fast retransmission of the oldest missing packet.
                    duplicate_acks += 1

                    logger.log(
                        "DUPLICATE_ACK",
                        packet_type="ACK",
                        ack=ack_number,
                        details=f"count={duplicate_acks}"
                    )

                    if (
                        duplicate_acks == 3
                        and not base_retx_done
                        and retries[base] < MAX_RETRIES
                    ):
                        retries[base] += 1
                        attempt = retries[base] + 1

                        print(
                            f"3 duplicate ACKs: fast retransmitting "
                            f"packet {base}, attempt {attempt}"
                        )

                        sock.sendto(
                            outstanding[base].pack(), receiver_address
                        )

                        logger.log(
                            "RETRANSMISSION",
                            packet_type="DATA",
                            seq=base,
                            byte_count=len(outstanding[base].payload),
                            attempt=attempt,
                            details="Fast retransmit (3 duplicate ACKs)"
                        )

                        base_retx_done = True
                        if rto_deadline is None:
                            rto_deadline = time.monotonic() + estimator.rto

        # On timeout, retransmit only the oldest unacknowledged packet.
        # Apply exponential backoff and re-arm its deadline.
        if base < next_seq and rto_deadline is not None:
            current_time = time.monotonic()

            if current_time >= rto_deadline:

                if retries[base] >= MAX_RETRIES:
                    print(f"Packet {base} exceeded maximum retries.")
                    logger.log(
                        "TRANSFER_FAILED",
                        packet_type="DATA",
                        seq=base,
                        details="Maximum retries exceeded"
                    )
                    sock.close()
                    return

                retries[base] += 1
                attempt = retries[base] + 1

                print(
                    f"Timeout! Retransmitting packet {base}, "
                    f"attempt {attempt}"
                )

                sock.sendto(outstanding[base].pack(), receiver_address)

                logger.log(
                    "RETRANSMISSION",
                    packet_type="DATA",
                    seq=base,
                    byte_count=len(outstanding[base].payload),
                    attempt=attempt,
                    details="Timeout"
                )

                # Double the RTO after timeout and re-arm the timer (RFC 6298 §§5.5–5.6).
                estimator.backoff()

                logger.log(
                    "RTO_BACKOFF",
                    packet_type="DATA",
                    seq=base,
                    details=f"rto={estimator.rto:.6f}"
                )

                rto_deadline = time.monotonic() + estimator.rto
                base_retx_done = True

    print("\nAll DATA packets acknowledged.")


    print("Starting FIN handshake...")

    fin_packet = Packet(packet_type=FIN, seq=total_packets)

    fin_ack, _fin_attempts = send_control_packet(
        sock, fin_packet, total_packets, receiver_address
    )

    if fin_ack is not None:
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
