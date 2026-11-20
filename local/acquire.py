from dataclasses import dataclass
from sqlite3 import Connection

from receiver.events import LOCAL, append_cut_event
from shift.calibration import decode_integer, encode_integer
from shift.lifecycle import require_open_station
from shift.store import load_shift_snapshot, write_transaction
from .convert import convert_local
from .qualify import qualify_local_read


@dataclass(frozen=True, slots=True)
class LocalRead:
    station: str
    shift: str
    local_seq: int
    point: str
    raw: int
    microliters: int


def encode_local_read(read: LocalRead) -> dict:
    return {
        'station': read.station,
        'shift': read.shift,
        'local_seq': read.local_seq,
        'point': read.point,
        'raw': encode_integer(read.raw),
        'microliters': encode_integer(read.microliters),
    }


def decode_local_read(data: dict) -> LocalRead:
    return LocalRead(
        data['station'], data['shift'], data['local_seq'], data['point'],
        decode_integer(data['raw']), decode_integer(data['microliters']),
    )


def initialize_local_reads(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS local_reads (
            shift_id TEXT NOT NULL REFERENCES shifts(shift_id),
            local_seq INTEGER NOT NULL CHECK (local_seq > 0),
            point TEXT NOT NULL,
            raw TEXT NOT NULL,
            microliters TEXT NOT NULL,
            PRIMARY KEY (shift_id, local_seq)
        )
    ''')


def record_local(db: Connection, downstream_shift: str, point: str, raw: int) -> LocalRead:
    with write_transaction(db):
        snapshot = require_open_station(db, downstream_shift, 'downstream')
        point = qualify_local_read(snapshot, point, raw)
        microliters = convert_local(snapshot, raw)
        local_seq = db.execute(
            'SELECT COALESCE(MAX(local_seq), 0) FROM local_reads WHERE shift_id = ?',
            (downstream_shift,),
        ).fetchone()[0] + 1
        db.execute(
            'INSERT INTO local_reads VALUES (?, ?, ?, ?, ?)',
            (downstream_shift, local_seq, point, encode_integer(raw), encode_integer(microliters)),
        )
        append_cut_event(db, downstream_shift, LOCAL, local_seq)
        return LocalRead(snapshot.pair.downstream_station, downstream_shift, local_seq, point, raw, microliters)


def list_local_reads(db: Connection, downstream_shift: str) -> tuple[LocalRead, ...]:
    snapshot = load_shift_snapshot(db, downstream_shift, 'downstream')
    rows = db.execute('''
        SELECT local_seq, point, raw, microliters FROM local_reads
        WHERE shift_id = ? ORDER BY local_seq
    ''', (downstream_shift,))
    station = snapshot.pair.downstream_station
    return tuple(
        LocalRead(station, downstream_shift, seq, point, decode_integer(raw), decode_integer(microliters))
        for seq, point, raw, microliters in rows
    )
