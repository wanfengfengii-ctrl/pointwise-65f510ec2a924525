from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ShiftPair:
    upstream_station: str
    upstream_shift: str
    downstream_station: str
    downstream_shift: str


def require_identifier(value: object, label: str) -> str:
    if type(value) is not str or not value or value.strip() != value or '\x00' in value:
        raise ValueError(f'invalid {label}')
    return value


def validate_pair(pair: ShiftPair) -> ShiftPair:
    if type(pair) is not ShiftPair:
        raise ValueError('expected a station pair')
    require_identifier(pair.upstream_station, 'upstream station')
    require_identifier(pair.upstream_shift, 'upstream shift')
    require_identifier(pair.downstream_station, 'downstream station')
    require_identifier(pair.downstream_shift, 'downstream shift')
    if pair.upstream_station == pair.downstream_station:
        raise ValueError('a pair requires two distinct stations')
    return pair


def require_paired_shifts(expected: ShiftPair, actual: ShiftPair) -> None:
    if validate_pair(actual) != expected:
        raise ValueError('station and shift pair does not match')
