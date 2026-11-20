from collections.abc import Callable
from dataclasses import dataclass
from sqlite3 import Connection

from local.acquire import list_local_reads
from local.closeout import load_local_closeout
from receiver.events import ADMISSION, NOTICE, list_cut_events
from receiver.identity import PacketIdentity
from receiver.ledger import AcceptedPacket, list_acceptances
from receiver.notices import load_accepted_notice
from shift.pairing import require_paired_shifts
from shift.store import load_shift_snapshot, write_transaction
from source.closeout import UpstreamCloseout, validate_upstream_closeout
from source.dispatch import CutNotice
from .archive import (
    IN_TRANSIT,
    RECEIVED,
    CutReport,
    CutUpstreamRow,
    NormalReport,
    ReportReceipt,
    load_report,
    load_shift_cut_report,
    store_cut_report,
    store_report,
)
from .reconcile import reconcile
from .summarize import summarize


@dataclass(frozen=True, slots=True)
class MissingPackets:
    missing: tuple[PacketIdentity, ...]


def issue_normal(
    db: Connection,
    downstream_shift: str,
    upstream_manifest: UpstreamCloseout,
    *,
    before_commit: Callable[[str], None] | None = None,
) -> NormalReport | MissingPackets:
    with write_transaction(db):
        snapshot = load_shift_snapshot(db, downstream_shift, 'downstream')
        validate_upstream_closeout(upstream_manifest)
        require_paired_shifts(snapshot.pair, upstream_manifest.pair)
        local_closeout = load_local_closeout(db, downstream_shift)
        if local_closeout is None:
            raise RuntimeError('downstream sampling must close before signing')
        report_no = f'N/{downstream_shift}'
        previous = load_report(db, report_no)
        if previous is not None:
            archived_seqs = tuple(item.source_seq for item in previous.body.expected)
            if archived_seqs != upstream_manifest.source_seqs:
                raise ValueError('upstream closeout differs from the signed report')
            return previous
        coverage = reconcile(upstream_manifest, local_closeout, list_acceptances(db, downstream_shift))
        if coverage.missing:
            return MissingPackets(coverage.missing)
        body = summarize(coverage, local_closeout)
        receipt = ReportReceipt(report_no, f'R/{report_no}')
        report = NormalReport(report_no, body, receipt)
        store_report(db, report, before_commit=before_commit)
        return report


def _cut_section(
    db: Connection, downstream_shift: str, member_seqs: frozenset,
) -> tuple[int, dict[int, AcceptedPacket], dict[int, int], dict[int, int]]:
    admissions = {row.receipt.admission_seq: row for row in list_acceptances(db, downstream_shift)}
    notice_pos = None
    first_postcut = None
    admission_pos = {}
    local_pos = {}
    for event in list_cut_events(db, downstream_shift):
        if event.kind == NOTICE:
            notice_pos = event.event_seq
        elif event.kind == ADMISSION:
            admission_pos[event.item_seq] = event.event_seq
            accepted = admissions[event.item_seq]
            if first_postcut is None and accepted.packet.source_seq not in member_seqs:
                first_postcut = event.event_seq
        else:
            local_pos[event.item_seq] = event.event_seq
    if notice_pos is None:
        raise RuntimeError('accepted cut notice is missing its event')
    boundary = notice_pos if first_postcut is None else min(notice_pos, first_postcut)
    return boundary, admissions, admission_pos, local_pos


def _build_cut_report(db: Connection, downstream_shift: str, notice: CutNotice) -> CutReport:
    member_seqs = frozenset(packet.source_seq for packet in notice.packets)
    boundary, admissions, admission_pos, local_pos = _cut_section(db, downstream_shift, member_seqs)
    by_seq = {row.packet.source_seq: row for row in admissions.values()}
    upstream_rows = []
    for packet in notice.packets:
        accepted = by_seq.get(packet.source_seq)
        position = None if accepted is None else admission_pos.get(accepted.receipt.admission_seq)
        if accepted is not None and (position is None or position < boundary):
            upstream_rows.append(CutUpstreamRow(packet, RECEIVED, accepted.receipt))
        else:
            upstream_rows.append(CutUpstreamRow(packet, IN_TRANSIT, None))
    local_reads = tuple(
        read for read in list_local_reads(db, downstream_shift)
        if local_pos.get(read.local_seq) is None or local_pos[read.local_seq] < boundary
    )
    total = sum(read.microliters for read in local_reads)
    receipt = ReportReceipt(notice.cut_no, f'R/{notice.cut_no}')
    return CutReport(notice.cut_no, notice.pair, tuple(upstream_rows), local_reads, total, receipt)


def issue_cut(
    db: Connection,
    downstream_shift: str,
    *,
    before_commit: Callable[[str], None] | None = None,
) -> CutReport:
    with write_transaction(db):
        load_shift_snapshot(db, downstream_shift, 'downstream')
        previous = load_shift_cut_report(db, downstream_shift)
        if previous is not None:
            return previous
        notice = load_accepted_notice(db, downstream_shift)
        if notice is None:
            raise RuntimeError(f'no accepted cut notice for shift {downstream_shift!r}')
        report = _build_cut_report(db, downstream_shift, notice)
        store_cut_report(db, report, before_commit=before_commit)
        return report
