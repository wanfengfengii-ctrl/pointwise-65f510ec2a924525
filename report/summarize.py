from dataclasses import dataclass
from typing import Generic, TypeVar

from local.acquire import LocalRead
from local.closeout import LocalCloseout
from receiver.identity import PacketIdentity
from receiver.ledger import AcceptedPacket
from shift.pairing import ShiftPair
from .reconcile import Coverage


Row = TypeVar('Row', AcceptedPacket, LocalRead)


@dataclass(frozen=True, slots=True)
class StationSummary(Generic[Row]):
    station: str
    shift: str
    rows: tuple[Row, ...]
    total_microliters: int


@dataclass(frozen=True, slots=True)
class NormalReportBody:
    pair: ShiftPair
    expected: tuple[PacketIdentity, ...]
    upstream: StationSummary[AcceptedPacket]
    downstream: StationSummary[LocalRead]


def summarize(coverage: Coverage, local: LocalCloseout) -> NormalReportBody:
    if coverage.missing:
        raise ValueError('cannot summarize incomplete coverage')
    pair = local.pair
    upstream_summary = StationSummary(
        pair.upstream_station, pair.upstream_shift, coverage.accepted,
        sum(row.packet.microliters for row in coverage.accepted),
    )
    downstream_summary = StationSummary(
        pair.downstream_station, pair.downstream_shift, local.reads,
        sum(row.microliters for row in local.reads),
    )
    return NormalReportBody(pair, coverage.expected, upstream_summary, downstream_summary)
