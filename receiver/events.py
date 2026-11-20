from dataclasses import dataclass
from sqlite3 import Connection

ADMISSION = 'admission'
NOTICE = 'notice'
LOCAL = 'local'


@dataclass(frozen=True, slots=True)
class CutEvent:
    event_seq: int
    kind: str
    item_seq: int


def initialize_cut_events(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS cut_events (
            event_seq INTEGER PRIMARY KEY,
            downstream_shift TEXT NOT NULL REFERENCES shifts(shift_id),
            kind TEXT NOT NULL CHECK (kind IN ('admission', 'notice', 'local')),
            item_seq INTEGER NOT NULL
        )
    ''')


def append_cut_event(db: Connection, downstream_shift: str, kind: str, item_seq: int) -> None:
    db.execute(
        'INSERT INTO cut_events (downstream_shift, kind, item_seq) VALUES (?, ?, ?)',
        (downstream_shift, kind, item_seq),
    )


def list_cut_events(db: Connection, downstream_shift: str) -> tuple[CutEvent, ...]:
    rows = db.execute('''
        SELECT event_seq, kind, item_seq FROM cut_events
        WHERE downstream_shift = ? ORDER BY event_seq
    ''', (downstream_shift,))
    return tuple(CutEvent(seq, kind, item) for seq, kind, item in rows)
