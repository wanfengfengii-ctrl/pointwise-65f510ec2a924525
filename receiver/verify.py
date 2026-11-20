from shift.calibration import require_integer
from shift.pairing import require_paired_shifts
from shift.roster import require_point
from shift.store import ShiftSnapshot
from source.acquire import validate_source_origin
from source.convert import convert_upstream
from source.dispatch import DispatchPacket


def verify_binding(snapshot: ShiftSnapshot, packet: DispatchPacket) -> None:
    if type(packet) is not DispatchPacket:
        raise ValueError('expected a dispatched packet')
    require_paired_shifts(snapshot.pair, packet.pair)


def verify_new_packet(snapshot: ShiftSnapshot, packet: DispatchPacket) -> None:
    require_point(snapshot.upstream_roster, packet.point)
    validate_source_origin(packet.origin, snapshot.pair)
    measured = require_integer(packet.microliters, 'packet microliters')
    if measured != convert_upstream(snapshot, packet.raw):
        raise ValueError('packet microliters disagree with frozen calibration')
