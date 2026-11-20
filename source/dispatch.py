import json
from dataclasses import asdict, dataclass
from sqlite3 import Connection

from shift.calibration import decode_integer, encode_integer, require_integer
from shift.lifecycle import require_open_station
from shift.pairing import ShiftPair
from shift.store import load_shift_snapshot, write_transaction
from .acquire import SourceOrigin, load_upstream_read


@dataclass(frozen=True, slots=True)
class DispatchPacket:
    pair: ShiftPair
    source_seq: int
    point: str
    raw: int
    microliters: int
    origin: SourceOrigin


def require_source_seq(value: object) -> int:
    seq = require_integer(value, 'source sequence')
    if not 1 <= seq <= (1 << 63) - 1:
        raise ValueError('source sequence is out of range')
    return seq


def encode_packet(packet: DispatchPacket) -> dict:
    return {
        'pair': asdict(packet.pair),
        'source_seq': packet.source_seq,
        'point': packet.point,
        'raw': encode_integer(packet.raw),
        'microliters': encode_integer(packet.microliters),
        'origin': asdict(packet.origin),
    }


def decode_packet(data: dict) -> DispatchPacket:
    return DispatchPacket(
        ShiftPair(**data['pair']), data['source_seq'], data['point'],
        decode_integer(data['raw']), decode_integer(data['microliters']),
        SourceOrigin(**data['origin']),
    )


def initialize_dispatch_ledger(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS dispatches (
            shift_id TEXT NOT NULL,
            source_seq INTEGER NOT NULL,
            packet TEXT NOT NULL,
            PRIMARY KEY (shift_id, source_seq),
            FOREIGN KEY (shift_id, source_seq) REFERENCES upstream_reads(shift_id, source_seq)
        )
    ''')


def find_dispatched_packet(
    db: Connection, upstream_shift: str, source_seq: int,
) -> DispatchPacket | None:
    row = db.execute(
        'SELECT packet FROM dispatches WHERE shift_id = ? AND source_seq = ?',
        (upstream_shift, require_source_seq(source_seq)),
    ).fetchone()
    return None if row is None else decode_packet(json.loads(row[0]))


def list_dispatched_packets(db: Connection, upstream_shift: str) -> tuple[DispatchPacket, ...]:
    rows = db.execute(
        'SELECT packet FROM dispatches WHERE shift_id = ? ORDER BY source_seq',
        (upstream_shift,),
    )
    return tuple(decode_packet(json.loads(row[0])) for row in rows)


def dispatch(db: Connection, upstream_shift: str, read_no: tuple[str, str, int]) -> DispatchPacket:
    with write_transaction(db):
        snapshot = load_shift_snapshot(db, upstream_shift, 'upstream')
        if type(read_no) is not tuple or len(read_no) != 3:
            raise ValueError('invalid upstream read number')
        station, shift_id, source_seq = read_no
        if (station, shift_id) != (snapshot.pair.upstream_station, upstream_shift):
            raise ValueError('read belongs to a different upstream shift')
        source_seq = require_source_seq(source_seq)
        existing = find_dispatched_packet(db, upstream_shift, source_seq)
        if existing is not None:
            return existing
        read = load_upstream_read(db, upstream_shift, source_seq)
        require_open_station(db, upstream_shift, 'upstream')
        packet = DispatchPacket(read.pair, read.source_seq, read.point, read.raw, read.microliters, read.origin)
        db.execute(
            'INSERT INTO dispatches VALUES (?, ?, ?)',
            (upstream_shift, source_seq, json.dumps(encode_packet(packet))),
        )
        return packet
