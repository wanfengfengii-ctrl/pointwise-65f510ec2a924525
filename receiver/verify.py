from shift.calibration import require_integer
from shift.pairing import require_identifier, require_paired_shifts, validate_pair
from shift.roster import require_point
from shift.store import ShiftSnapshot
from source.acquire import validate_source_origin
from source.convert import convert_upstream
from source.dispatch import CutNotice, DispatchPacket, require_source_seq


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


def validate_cut_notice(notice: CutNotice) -> None:
    if type(notice) is not CutNotice:
        raise ValueError('expected a cut notice')
    require_identifier(notice.cut_no, 'cut number')
    validate_pair(notice.pair)
    if type(notice.packets) is not tuple:
        raise ValueError('cut notice packets must be a tuple')
    previous = 0
    for packet in notice.packets:
        if type(packet) is not DispatchPacket:
            raise ValueError('expected a dispatched packet')
        source_seq = require_source_seq(packet.source_seq)
        if source_seq <= previous:
            raise ValueError('cut members must ascend by source sequence')
        previous = source_seq
