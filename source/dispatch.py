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


@dataclass(frozen=True, slots=True)
class CutNotice:
    cut_no: str
    pair: ShiftPair
    packets: tuple[DispatchPacket, ...]


def encode_cut_notice(notice: CutNotice) -> dict:
    return {
        'cut_no': notice.cut_no,
        'pair': asdict(notice.pair),
        'packets': [encode_packet(packet) for packet in notice.packets],
    }


def decode_cut_notice(data: dict) -> CutNotice:
    return CutNotice(
        data['cut_no'], ShiftPair(**data['pair']),
        tuple(decode_packet(packet) for packet in data['packets']),
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


def initialize_cut_notices(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS cut_notices (
            shift_id TEXT PRIMARY KEY NOT NULL REFERENCES shifts(shift_id),
            notice TEXT NOT NULL
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


def load_cut_notice(db: Connection, upstream_shift: str) -> CutNotice | None:
    row = db.execute(
        'SELECT notice FROM cut_notices WHERE shift_id = ?', (upstream_shift,),
    ).fetchone()
    return None if row is None else decode_cut_notice(json.loads(row[0]))


def open_cut(db: Connection, upstream_shift: str) -> CutNotice:
    with write_transaction(db):
        snapshot = load_shift_snapshot(db, upstream_shift, 'upstream')
        existing = load_cut_notice(db, upstream_shift)
        if existing is not None:
            return existing
        require_open_station(db, upstream_shift, 'upstream')
        notice = CutNotice(
            f'C/{snapshot.pair.downstream_shift}', snapshot.pair,
            list_dispatched_packets(db, upstream_shift),
        )
        db.execute(
            'INSERT INTO cut_notices VALUES (?, ?)',
            (upstream_shift, json.dumps(encode_cut_notice(notice))),
        )
        return notice


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
