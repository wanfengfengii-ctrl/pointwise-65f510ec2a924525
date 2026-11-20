import json
from dataclasses import dataclass
from sqlite3 import Connection

from shift.lifecycle import close_station
from shift.pairing import ShiftPair, validate_pair
from shift.store import load_shift_snapshot, write_transaction
from .acquire import list_upstream_reads
from .dispatch import list_dispatched_packets


@dataclass(frozen=True, slots=True)
class UpstreamCloseout:
    pair: ShiftPair
    source_seqs: tuple[int, ...]


class PendingDispatch(RuntimeError):
    def __init__(self, source_seqs: tuple[int, ...]):
        self.source_seqs = source_seqs
        super().__init__(f'unissued source reads {source_seqs}')


def validate_upstream_closeout(manifest: UpstreamCloseout) -> None:
    if type(manifest) is not UpstreamCloseout or type(manifest.source_seqs) is not tuple:
        raise ValueError('invalid upstream closeout')
    validate_pair(manifest.pair)
    for expected, seq in enumerate(manifest.source_seqs, 1):
        if type(seq) is not int or seq != expected:
            raise ValueError('upstream closeout has an invalid source sequence')


def initialize_upstream_closeouts(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS upstream_closeouts (
            shift_id TEXT PRIMARY KEY NOT NULL REFERENCES shifts(shift_id),
            source_seqs TEXT NOT NULL
        )
    ''')


def load_upstream_closeout(db: Connection, upstream_shift: str) -> UpstreamCloseout | None:
    snapshot = load_shift_snapshot(db, upstream_shift, 'upstream')
    row = db.execute(
        'SELECT source_seqs FROM upstream_closeouts WHERE shift_id = ?', (upstream_shift,),
    ).fetchone()
    if row is None:
        return None
    return UpstreamCloseout(snapshot.pair, tuple(json.loads(row[0])))


def close_upstream(db: Connection, upstream_shift: str) -> UpstreamCloseout:
    with write_transaction(db):
        previous = load_upstream_closeout(db, upstream_shift)
        if previous is not None:
            return previous
        snapshot = load_shift_snapshot(db, upstream_shift, 'upstream')
        sent = tuple(packet.source_seq for packet in list_dispatched_packets(db, upstream_shift))
        sent_set = set(sent)
        pending = tuple(read.source_seq for read in list_upstream_reads(db, upstream_shift)
                        if read.source_seq not in sent_set)
        if pending:
            raise PendingDispatch(pending)
        manifest = UpstreamCloseout(snapshot.pair, sent)
        close_station(db, upstream_shift)
        db.execute('INSERT INTO upstream_closeouts VALUES (?, ?)', (upstream_shift, json.dumps(sent)))
        return manifest
