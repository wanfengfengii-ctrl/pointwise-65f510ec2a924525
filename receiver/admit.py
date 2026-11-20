from sqlite3 import Connection

from shift.pairing import require_paired_shifts
from shift.store import load_shift_snapshot, write_transaction
from source.dispatch import CutNotice, DispatchPacket
from .events import NOTICE, append_cut_event
from .identity import packet_identity, same_packet
from .ledger import append_acceptance, find_acceptance
from .notices import find_notice_shift, load_accepted_notice, store_accepted_notice
from .receipts import AdmissionReceipt
from .verify import validate_cut_notice, verify_binding, verify_new_packet


class PacketConflict(ValueError):
    pass


def admit(db: Connection, downstream_shift: str, packet: DispatchPacket) -> AdmissionReceipt:
    with write_transaction(db):
        snapshot = load_shift_snapshot(db, downstream_shift, 'downstream')
        verify_binding(snapshot, packet)
        identity = packet_identity(packet)
        existing = find_acceptance(db, downstream_shift, identity)
        if existing is not None:
            if not same_packet(existing.packet, packet):
                raise PacketConflict(f'conflicting payload for {identity}')
            return existing.receipt
        verify_new_packet(snapshot, packet)
        return append_acceptance(db, downstream_shift, packet).receipt


def deliver_notice(db: Connection, downstream_shift: str, notice: CutNotice) -> CutNotice:
    with write_transaction(db):
        snapshot = load_shift_snapshot(db, downstream_shift, 'downstream')
        validate_cut_notice(notice)
        require_paired_shifts(snapshot.pair, notice.pair)
        existing = load_accepted_notice(db, downstream_shift)
        if existing is not None:
            if existing == notice:
                return existing
            raise ValueError('a different cut notice is already preserved')
        for packet in notice.packets:
            verify_binding(snapshot, packet)
            verify_new_packet(snapshot, packet)
            identity = packet_identity(packet)
            accepted = find_acceptance(db, downstream_shift, identity)
            if accepted is not None and not same_packet(accepted.packet, packet):
                raise PacketConflict(f'conflicting payload for {identity}')
        owner = find_notice_shift(db, notice.cut_no)
        if owner is not None:
            raise ValueError(f'cut number {notice.cut_no!r} is already preserved by {owner!r}')
        store_accepted_notice(db, downstream_shift, notice)
        append_cut_event(db, downstream_shift, NOTICE, 1)
        return notice
