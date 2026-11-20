from shift.calibration import to_microliters
from shift.store import ShiftSnapshot


def convert_upstream(snapshot: ShiftSnapshot, raw: int) -> int:
    return to_microliters(snapshot.upstream_calibration, raw)
