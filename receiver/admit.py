from sqlite3 import Connection

from shift.store import load_shift_snapshot, write_transaction
from source.dispatch import DispatchPacket
from .identity import packet_identity, same_packet
from .ledger import append_acceptance, find_acceptance
from .receipts import AdmissionReceipt
from .verify import verify_binding, verify_new_packet


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
