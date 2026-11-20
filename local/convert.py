from shift.calibration import to_microliters
from shift.store import ShiftSnapshot


def convert_local(snapshot: ShiftSnapshot, raw: int) -> int:
    return to_microliters(snapshot.downstream_calibration, raw)
