from collections.abc import Callable
from dataclasses import dataclass
from sqlite3 import Connection

from local.closeout import load_local_closeout
from receiver.identity import PacketIdentity
from receiver.ledger import list_acceptances
from shift.pairing import require_paired_shifts
from shift.store import load_shift_snapshot, write_transaction
from source.closeout import UpstreamCloseout, validate_upstream_closeout
from .archive import NormalReport, ReportReceipt, load_report, store_report
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
