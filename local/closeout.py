import json
from dataclasses import dataclass
from sqlite3 import Connection

from shift.lifecycle import close_station
from shift.pairing import ShiftPair
from shift.store import load_shift_snapshot, write_transaction
from .acquire import LocalRead, decode_local_read, encode_local_read, list_local_reads


@dataclass(frozen=True, slots=True)
class LocalCloseout:
    pair: ShiftPair
    reads: tuple[LocalRead, ...]


def initialize_local_closeouts(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS local_closeouts (
            shift_id TEXT PRIMARY KEY NOT NULL REFERENCES shifts(shift_id),
            reads TEXT NOT NULL
        )
    ''')


def load_local_closeout(db: Connection, downstream_shift: str) -> LocalCloseout | None:
    snapshot = load_shift_snapshot(db, downstream_shift, 'downstream')
    row = db.execute('SELECT reads FROM local_closeouts WHERE shift_id = ?', (downstream_shift,)).fetchone()
    if row is None:
        return None
    return LocalCloseout(snapshot.pair, tuple(decode_local_read(read) for read in json.loads(row[0])))


def close_local(db: Connection, downstream_shift: str) -> LocalCloseout:
    with write_transaction(db):
        previous = load_local_closeout(db, downstream_shift)
        if previous is not None:
            return previous
        snapshot = load_shift_snapshot(db, downstream_shift, 'downstream')
        reads = list_local_reads(db, downstream_shift)
        close_station(db, downstream_shift)
        db.execute(
            'INSERT INTO local_closeouts VALUES (?, ?)',
            (downstream_shift, json.dumps([encode_local_read(read) for read in reads])),
        )
        return LocalCloseout(snapshot.pair, reads)
