from dataclasses import dataclass
from sqlite3 import Connection

from shift.calibration import decode_integer, encode_integer
from shift.lifecycle import require_open_station
from shift.pairing import ShiftPair, require_identifier
from shift.roster import require_point
from shift.store import load_shift_snapshot, write_transaction
from .convert import convert_upstream


@dataclass(frozen=True, slots=True)
class SourceOrigin:
    station: str
    shift: str
    book: str
    entry: str


@dataclass(frozen=True, slots=True)
class UpstreamRead:
    pair: ShiftPair
    source_seq: int
    point: str
    raw: int
    microliters: int
    origin: SourceOrigin

    @property
    def read_no(self) -> tuple[str, str, int]:
        return self.pair.upstream_station, self.pair.upstream_shift, self.source_seq


def initialize_upstream_reads(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS upstream_reads (
            shift_id TEXT NOT NULL REFERENCES shifts(shift_id),
            source_seq INTEGER NOT NULL CHECK (source_seq > 0),
            point TEXT NOT NULL,
            raw TEXT NOT NULL,
            microliters TEXT NOT NULL,
            book TEXT NOT NULL,
            entry TEXT NOT NULL,
            PRIMARY KEY (shift_id, source_seq)
        )
    ''')


def validate_source_origin(origin: SourceOrigin, pair: ShiftPair) -> None:
    if type(origin) is not SourceOrigin:
        raise ValueError('expected a sampling book origin')
    require_identifier(origin.station, 'origin station')
    require_identifier(origin.shift, 'origin shift')
    require_identifier(origin.book, 'sampling book')
    require_identifier(origin.entry, 'sampling book entry')
    if (origin.station, origin.shift) != (pair.upstream_station, pair.upstream_shift):
        raise ValueError('origin does not belong to the paired upstream shift')


def record_upstream(
    db: Connection, upstream_shift: str, point: str, raw: int, origin: SourceOrigin,
) -> UpstreamRead:
    with write_transaction(db):
        snapshot = require_open_station(db, upstream_shift, 'upstream')
        require_point(snapshot.upstream_roster, point)
        validate_source_origin(origin, snapshot.pair)
        microliters = convert_upstream(snapshot, raw)
        source_seq = db.execute(
            'SELECT COALESCE(MAX(source_seq), 0) FROM upstream_reads WHERE shift_id = ?',
            (upstream_shift,),
        ).fetchone()[0] + 1
        db.execute(
            'INSERT INTO upstream_reads VALUES (?, ?, ?, ?, ?, ?, ?)',
            (upstream_shift, source_seq, point, encode_integer(raw),
             encode_integer(microliters), origin.book, origin.entry),
        )
        return UpstreamRead(snapshot.pair, source_seq, point, raw, microliters, origin)


def _from_row(pair: ShiftPair, row: tuple) -> UpstreamRead:
    source_seq, point, raw, microliters, book, entry = row
    origin = SourceOrigin(pair.upstream_station, pair.upstream_shift, book, entry)
    return UpstreamRead(pair, source_seq, point, decode_integer(raw), decode_integer(microliters), origin)


def load_upstream_read(db: Connection, upstream_shift: str, source_seq: int) -> UpstreamRead:
    snapshot = load_shift_snapshot(db, upstream_shift, 'upstream')
    row = db.execute('''
        SELECT source_seq, point, raw, microliters, book, entry FROM upstream_reads
        WHERE shift_id = ? AND source_seq = ?
    ''', (upstream_shift, source_seq)).fetchone()
    if row is None:
        raise KeyError((upstream_shift, source_seq))
    return _from_row(snapshot.pair, row)


def list_upstream_reads(db: Connection, upstream_shift: str) -> tuple[UpstreamRead, ...]:
    snapshot = load_shift_snapshot(db, upstream_shift, 'upstream')
    rows = db.execute('''
        SELECT source_seq, point, raw, microliters, book, entry FROM upstream_reads
        WHERE shift_id = ? ORDER BY source_seq
    ''', (upstream_shift,))
    return tuple(_from_row(snapshot.pair, row) for row in rows)
