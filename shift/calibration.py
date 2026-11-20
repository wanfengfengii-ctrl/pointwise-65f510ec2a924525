from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Calibration:
    scale: int
    offset: int


def require_integer(value: object, label: str) -> int:
    if type(value) is not int:
        raise ValueError(f'{label} requires an integer')
    return value


def freeze_calibration(scale: int, offset: int) -> Calibration:
    return Calibration(require_integer(scale, 'scale'), require_integer(offset, 'offset'))


def to_microliters(calibration: Calibration, raw: int) -> int:
    return require_integer(raw, 'raw reading') * calibration.scale + calibration.offset


def encode_integer(value: int) -> str:
    # Hexadecimal text keeps Python's integer range through SQLite and JSON.
    return format(require_integer(value, 'stored measurement'), 'x')


def decode_integer(value: str) -> int:
    return int(value, 16)
