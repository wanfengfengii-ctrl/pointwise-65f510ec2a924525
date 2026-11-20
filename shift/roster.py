from collections.abc import Iterable
from dataclasses import dataclass

from .pairing import require_identifier


@dataclass(frozen=True, slots=True)
class StationRoster:
    station: str
    points: tuple[str, ...]


def freeze_roster(station: str, points: Iterable[str]) -> StationRoster:
    require_identifier(station, 'roster station')
    if isinstance(points, (str, bytes)):
        raise ValueError('expected a collection of points')
    try:
        items = tuple(points)
    except TypeError as error:
        raise ValueError('expected a collection of points') from error
    for point in items:
        require_identifier(point, 'roster point')
    if not items or len(set(items)) != len(items):
        raise ValueError('roster requires distinct points')
    return StationRoster(station, tuple(sorted(items)))


def require_point(roster: StationRoster, point: str) -> str:
    require_identifier(point, 'point')
    if point not in roster.points:
        raise ValueError(f'unknown point {point!r} at station {roster.station!r}')
    return point
