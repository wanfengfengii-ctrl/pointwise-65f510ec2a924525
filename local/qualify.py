from shift.calibration import require_integer
from shift.roster import require_point
from shift.store import ShiftSnapshot


def qualify_local_read(snapshot: ShiftSnapshot, point: str, raw: int) -> str:
    require_integer(raw, 'local raw reading')
    return require_point(snapshot.downstream_roster, point)
