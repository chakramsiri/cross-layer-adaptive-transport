import struct
from dataclasses import dataclass

# Packet types
DATA = 1
ACK = 2
SYN = 3
FIN = 4
CROSS = 5

# Flags
CE = 0x10
ECE = 0x20

TYPE_MASK = 0x0F

# Header format:
# type/flags: 1 byte
# seq, ack, rwnd, ts_send, ts_echo, q_delay_us: 4 bytes each
# ce_count, payload_len: 2 bytes each

HEADER_FORMAT = "!BIIIIIIHH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


@dataclass
class Packet:
    packet_type: int
    seq: int = 0
    ack: int = 0
    rwnd: int = 0
    ts_send: int = 0
    ts_echo: int = 0
    q_delay_us: int = 0
    ce_count: int = 0
    payload: bytes = b""
    flags: int = 0

    def pack(self):
        """Convert a Packet object into bytes."""

        if not isinstance(self.payload, bytes):
            raise TypeError("Payload must be bytes")

        if not 0 <= self.packet_type <= TYPE_MASK:
            raise ValueError("Invalid packet type")

        if self.flags & TYPE_MASK:
            raise ValueError("Flags must not overlap packet type bits")

        payload_len = len(self.payload)

        if payload_len > 65535:
            raise ValueError("Payload is too large")

        type_flags = self.packet_type | self.flags

        header = struct.pack(
            HEADER_FORMAT,
            type_flags,
            self.seq,
            self.ack,
            self.rwnd,
            self.ts_send,
            self.ts_echo,
            self.q_delay_us,
            self.ce_count,
            payload_len
        )

        return header + self.payload

    @staticmethod
    def unpack(data):
        """Convert received bytes into a Packet object."""

        if len(data) < HEADER_SIZE:
            raise ValueError("Packet is smaller than the header")

        fields = struct.unpack(
            HEADER_FORMAT,
            data[:HEADER_SIZE]
        )

        type_flags = fields[0]
        packet_type = type_flags & TYPE_MASK
        flags = type_flags & ~TYPE_MASK

        (
            seq,
            ack,
            rwnd,
            ts_send,
            ts_echo,
            q_delay_us,
            ce_count,
            payload_len
        ) = fields[1:]

        payload = data[HEADER_SIZE:]

        if len(payload) != payload_len:
            raise ValueError("Payload length mismatch")

        return Packet(
            packet_type=packet_type,
            seq=seq,
            ack=ack,
            rwnd=rwnd,
            ts_send=ts_send,
            ts_echo=ts_echo,
            q_delay_us=q_delay_us,
            ce_count=ce_count,
            payload=payload,
            flags=flags
        )