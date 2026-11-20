import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from os import PathLike

from .calibration import Calibration, decode_integer, encode_integer
from .pairing import ShiftPair, require_identifier
from .roster import StationRoster


@dataclass(frozen=True, slots=True)
class ShiftSnapshot:
    pair: ShiftPair
    role: str
    upstream_roster: StationRoster
    downstream_roster: StationRoster
    upstream_calibration: Calibration
    downstream_calibration: Calibration


def _require_role(role: str) -> None:
    if role not in ('upstream', 'downstream'):
        raise ValueError('invalid station role')


def require_station_role(db: sqlite3.Connection, role: str) -> None:
    _require_role(role)
    row = db.execute('SELECT role FROM station_settings WHERE singleton = 1').fetchone()
    if row is None or row[0] != role:
        raise ValueError('station store has a different role')


def open_station_store(path: str | PathLike[str], role: str) -> sqlite3.Connection:
    _require_role(role)
    db = sqlite3.connect(path, isolation_level=None)
    try:
        initialize_station_store(db, role)
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def write_transaction(db: sqlite3.Connection) -> Iterator[None]:
    db.execute('BEGIN IMMEDIATE')
    try:
        yield
        db.commit()
    except BaseException:
        if db.in_transaction:
            db.rollback()
        raise


def initialize_station_store(db: sqlite3.Connection, role: str) -> None:
    _require_role(role)
    db.execute('PRAGMA foreign_keys = ON')
    with write_transaction(db):
        db.execute('''
            CREATE TABLE IF NOT EXISTS station_settings (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                role TEXT NOT NULL CHECK (role IN ('upstream', 'downstream'))
            )
        ''')
        db.execute('INSERT OR IGNORE INTO station_settings VALUES (1, ?)', (role,))
        require_station_role(db, role)
        db.execute('''
            CREATE TABLE IF NOT EXISTS shifts (
                shift_id TEXT PRIMARY KEY NOT NULL,
                peer_station TEXT NOT NULL,
                peer_shift TEXT NOT NULL,
                snapshot TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('open', 'closed')),
                UNIQUE (peer_station, peer_shift)
            )
        ''')
        if role == 'upstream':
            from source.acquire import initialize_upstream_reads
            from source.closeout import initialize_upstream_closeouts
            from source.dispatch import initialize_cut_notices, initialize_dispatch_ledger

            initialize_upstream_reads(db)
            initialize_dispatch_ledger(db)
            initialize_cut_notices(db)
            initialize_upstream_closeouts(db)
        else:
            from local.acquire import initialize_local_reads
            from local.closeout import initialize_local_closeouts
            from receiver.events import initialize_cut_events
            from receiver.ledger import initialize_receipt_ledger
            from receiver.notices import initialize_accepted_notices
            from report.archive import initialize_cut_archive, initialize_report_archive

            initialize_cut_events(db)
            initialize_receipt_ledger(db)
            initialize_accepted_notices(db)
            initialize_local_reads(db)
            initialize_local_closeouts(db)
            initialize_report_archive(db)
            initialize_cut_archive(db)


def _encode_calibration(calibration: Calibration) -> list[str]:
    return [encode_integer(calibration.scale), encode_integer(calibration.offset)]


def _decode_calibration(values: list[str]) -> Calibration:
    scale, offset = values
    return Calibration(decode_integer(scale), decode_integer(offset))


def load_shift_snapshot(db: sqlite3.Connection, shift_id: str, role: str) -> ShiftSnapshot:
    require_identifier(shift_id, 'shift')
    require_station_role(db, role)
    row = db.execute('SELECT snapshot FROM shifts WHERE shift_id = ?', (shift_id,)).fetchone()
    if row is None:
        raise KeyError(shift_id)
    data = json.loads(row[0])
    pair = ShiftPair(**data['pair'])
    return ShiftSnapshot(
        pair,
        role,
        StationRoster(pair.upstream_station, tuple(data['upstream_points'])),
        StationRoster(pair.downstream_station, tuple(data['downstream_points'])),
        _decode_calibration(data['upstream_calibration']),
        _decode_calibration(data['downstream_calibration']),
    )


def insert_shift_snapshot(db: sqlite3.Connection, snapshot: ShiftSnapshot) -> None:
    require_station_role(db, snapshot.role)
    pair = snapshot.pair
    if snapshot.role == 'upstream':
        shift_id = pair.upstream_shift
        peer_station, peer_shift = pair.downstream_station, pair.downstream_shift
    else:
        shift_id = pair.downstream_shift
        peer_station, peer_shift = pair.upstream_station, pair.upstream_shift
    data = {
        'pair': asdict(pair),
        'upstream_points': snapshot.upstream_roster.points,
        'downstream_points': snapshot.downstream_roster.points,
        'upstream_calibration': _encode_calibration(snapshot.upstream_calibration),
        'downstream_calibration': _encode_calibration(snapshot.downstream_calibration),
    }
    try:
        db.execute(
            "INSERT INTO shifts VALUES (?, ?, ?, ?, 'open')",
            (shift_id, peer_station, peer_shift, json.dumps(data)),
        )
    except sqlite3.IntegrityError as error:
        raise ValueError('station shift identity is already bound') from error


def get_station_status(db: sqlite3.Connection, shift_id: str) -> str:
    row = db.execute('SELECT status FROM shifts WHERE shift_id = ?', (shift_id,)).fetchone()
    if row is None:
        raise KeyError(shift_id)
    return row[0]


def set_station_closed(db: sqlite3.Connection, shift_id: str) -> None:
    changed = db.execute(
        "UPDATE shifts SET status = 'closed' WHERE shift_id = ? AND status = 'open'",
        (shift_id,),
    )
    if changed.rowcount != 1:
        get_station_status(db, shift_id)
        raise RuntimeError(f'station shift {shift_id!r} is closed')
