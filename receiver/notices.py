import json
from sqlite3 import Connection

from source.dispatch import CutNotice, decode_cut_notice, encode_cut_notice


def initialize_accepted_notices(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS accepted_notices (
            downstream_shift TEXT PRIMARY KEY NOT NULL REFERENCES shifts(shift_id),
            cut_no TEXT NOT NULL UNIQUE,
            notice TEXT NOT NULL
        )
    ''')


def store_accepted_notice(db: Connection, downstream_shift: str, notice: CutNotice) -> None:
    db.execute(
        'INSERT INTO accepted_notices VALUES (?, ?, ?)',
        (downstream_shift, notice.cut_no, json.dumps(encode_cut_notice(notice))),
    )


def load_accepted_notice(db: Connection, downstream_shift: str) -> CutNotice | None:
    row = db.execute(
        'SELECT notice FROM accepted_notices WHERE downstream_shift = ?', (downstream_shift,),
    ).fetchone()
    return None if row is None else decode_cut_notice(json.loads(row[0]))


def find_notice_shift(db: Connection, cut_no: str) -> str | None:
    row = db.execute(
        'SELECT downstream_shift FROM accepted_notices WHERE cut_no = ?', (cut_no,),
    ).fetchone()
    return None if row is None else row[0]
