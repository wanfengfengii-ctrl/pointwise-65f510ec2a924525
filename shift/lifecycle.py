from collections.abc import Iterable
from sqlite3 import Connection

from .calibration import Calibration, freeze_calibration
from .pairing import ShiftPair, validate_pair
from .roster import freeze_roster
from .store import (
    ShiftSnapshot,
    get_station_status,
    insert_shift_snapshot,
    load_shift_snapshot,
    require_station_role,
    set_station_closed,
    write_transaction,
)


def open_shift(
    db: Connection,
    pair: ShiftPair,
    role: str,
    upstream_points: Iterable[str],
    downstream_points: Iterable[str],
    upstream_calibration: Calibration,
    downstream_calibration: Calibration,
) -> ShiftSnapshot:
    validate_pair(pair)
    require_station_role(db, role)
    if type(upstream_calibration) is not Calibration or type(downstream_calibration) is not Calibration:
        raise ValueError('both stations require a calibration')
    snapshot = ShiftSnapshot(
        pair,
        role,
        freeze_roster(pair.upstream_station, upstream_points),
        freeze_roster(pair.downstream_station, downstream_points),
        freeze_calibration(upstream_calibration.scale, upstream_calibration.offset),
        freeze_calibration(downstream_calibration.scale, downstream_calibration.offset),
    )
    shift_id = pair.upstream_shift if role == 'upstream' else pair.downstream_shift
    with write_transaction(db):
        try:
            existing = load_shift_snapshot(db, shift_id, role)
        except KeyError:
            insert_shift_snapshot(db, snapshot)
            return snapshot
        if existing != snapshot:
            raise ValueError(f'frozen configuration differs for shift {shift_id!r}')
        return existing


def require_open_station(db: Connection, shift_id: str, role: str) -> ShiftSnapshot:
    snapshot = load_shift_snapshot(db, shift_id, role)
    if get_station_status(db, shift_id) != 'open':
        raise RuntimeError(f'station shift {shift_id!r} is closed')
    return snapshot


def close_station(db: Connection, shift_id: str) -> None:
    # The caller commits this change together with the station's closeout.
    set_station_closed(db, shift_id)
