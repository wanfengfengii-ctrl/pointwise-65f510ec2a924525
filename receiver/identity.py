from dataclasses import dataclass

from source.acquire import SourceOrigin
from source.dispatch import DispatchPacket, require_source_seq


@dataclass(frozen=True, slots=True)
class PacketIdentity:
    upstream_shift: str
    source_seq: int


def packet_identity(packet: DispatchPacket) -> PacketIdentity:
    return PacketIdentity(packet.pair.upstream_shift, require_source_seq(packet.source_seq))


def same_packet(original: DispatchPacket, incoming: DispatchPacket) -> bool:
    if (type(incoming.raw) is not int or type(incoming.microliters) is not int
            or type(incoming.point) is not str or type(incoming.origin) is not SourceOrigin):
        return False
    origin = incoming.origin
    if any(type(value) is not str for value in (origin.station, origin.shift, origin.book, origin.entry)):
        return False
    return original == incoming
