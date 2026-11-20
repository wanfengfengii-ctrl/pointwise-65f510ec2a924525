import json
from dataclasses import dataclass
from sqlite3 import Connection

from source.dispatch import DispatchPacket, decode_packet, encode_packet
from .events import ADMISSION, append_cut_event
from .identity import PacketIdentity, packet_identity
from .receipts import (
    AdmissionReceipt,
    decode_admission_receipt,
    encode_admission_receipt,
    make_admission_receipt,
)


@dataclass(frozen=True, slots=True)
class AcceptedPacket:
    packet: DispatchPacket
    receipt: AdmissionReceipt


def initialize_receipt_ledger(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS acceptances (
            downstream_shift TEXT NOT NULL REFERENCES shifts(shift_id),
            upstream_shift TEXT NOT NULL,
            source_seq INTEGER NOT NULL CHECK (source_seq > 0),
            admission_seq INTEGER NOT NULL CHECK (admission_seq > 0),
            packet TEXT NOT NULL,
            receipt TEXT NOT NULL,
            PRIMARY KEY (downstream_shift, upstream_shift, source_seq),
            UNIQUE (downstream_shift, admission_seq)
        )
    ''')


def _from_row(row: tuple[str, str]) -> AcceptedPacket:
    packet, receipt = row
    return AcceptedPacket(decode_packet(json.loads(packet)), decode_admission_receipt(json.loads(receipt)))


def find_acceptance(
    db: Connection, downstream_shift: str, identity: PacketIdentity,
) -> AcceptedPacket | None:
    row = db.execute('''
        SELECT packet, receipt FROM acceptances
        WHERE downstream_shift = ? AND upstream_shift = ? AND source_seq = ?
    ''', (downstream_shift, identity.upstream_shift, identity.source_seq)).fetchone()
    return None if row is None else _from_row(row)


def append_acceptance(db: Connection, downstream_shift: str, packet: DispatchPacket) -> AcceptedPacket:
    identity = packet_identity(packet)
    admission_seq = db.execute(
        'SELECT COALESCE(MAX(admission_seq), 0) FROM acceptances WHERE downstream_shift = ?',
        (downstream_shift,),
    ).fetchone()[0] + 1
    receipt = make_admission_receipt(downstream_shift, identity, admission_seq)
    db.execute(
        'INSERT INTO acceptances VALUES (?, ?, ?, ?, ?, ?)',
        (downstream_shift, identity.upstream_shift, identity.source_seq, admission_seq,
         json.dumps(encode_packet(packet)), json.dumps(encode_admission_receipt(receipt))),
    )
    append_cut_event(db, downstream_shift, ADMISSION, admission_seq)
    return AcceptedPacket(packet, receipt)


def list_acceptances(db: Connection, downstream_shift: str) -> tuple[AcceptedPacket, ...]:
    rows = db.execute('''
        SELECT packet, receipt FROM acceptances WHERE downstream_shift = ? ORDER BY admission_seq
    ''', (downstream_shift,))
    return tuple(_from_row(row) for row in rows)
