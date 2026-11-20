import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from sqlite3 import Connection

from local.acquire import decode_local_read, encode_local_read
from receiver.identity import PacketIdentity
from receiver.ledger import AcceptedPacket
from receiver.receipts import decode_admission_receipt, encode_admission_receipt
from shift.calibration import decode_integer, encode_integer
from shift.pairing import ShiftPair
from source.dispatch import decode_packet, encode_packet
from .summarize import NormalReportBody, StationSummary


@dataclass(frozen=True, slots=True)
class ReportReceipt:
    report_no: str
    receipt_no: str


@dataclass(frozen=True, slots=True)
class NormalReport:
    report_no: str
    body: NormalReportBody
    receipt: ReportReceipt


def initialize_report_archive(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS normal_reports (
            report_no TEXT PRIMARY KEY NOT NULL,
            downstream_shift TEXT NOT NULL UNIQUE REFERENCES shifts(shift_id),
            body TEXT NOT NULL
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS report_receipts (
            report_no TEXT PRIMARY KEY NOT NULL REFERENCES normal_reports(report_no),
            receipt_no TEXT NOT NULL UNIQUE
        )
    ''')


def _serialize_body(body: NormalReportBody) -> str:
    return json.dumps({
        'pair': asdict(body.pair),
        'expected': [asdict(identity) for identity in body.expected],
        'upstream_total': encode_integer(body.upstream.total_microliters),
        'upstream_rows': [
            {'packet': encode_packet(row.packet), 'receipt': encode_admission_receipt(row.receipt)}
            for row in body.upstream.rows
        ],
        'local_total': encode_integer(body.downstream.total_microliters),
        'local_rows': [encode_local_read(row) for row in body.downstream.rows],
    })


def _deserialize_body(text: str) -> NormalReportBody:
    data = json.loads(text)
    pair = ShiftPair(**data['pair'])
    expected = tuple(PacketIdentity(**identity) for identity in data['expected'])
    upstream_rows = tuple(
        AcceptedPacket(decode_packet(item['packet']), decode_admission_receipt(item['receipt']))
        for item in data['upstream_rows']
    )
    local_rows = tuple(decode_local_read(item) for item in data['local_rows'])
    upstream = StationSummary(
        pair.upstream_station, pair.upstream_shift, upstream_rows, decode_integer(data['upstream_total']),
    )
    downstream = StationSummary(
        pair.downstream_station, pair.downstream_shift, local_rows, decode_integer(data['local_total']),
    )
    return NormalReportBody(pair, expected, upstream, downstream)


def store_report(
    db: Connection, report: NormalReport, *, before_commit: Callable[[str], None] | None = None,
) -> None:
    if not db.in_transaction:
        raise RuntimeError('archive writes require a report transaction')
    db.execute(
        'INSERT INTO normal_reports VALUES (?, ?, ?)',
        (report.report_no, report.body.pair.downstream_shift, _serialize_body(report.body)),
    )
    db.execute(
        'INSERT INTO report_receipts VALUES (?, ?)', (report.receipt.report_no, report.receipt.receipt_no),
    )
    if before_commit is not None:
        before_commit(report.report_no)


def load_report(db: Connection, report_no: str) -> NormalReport | None:
    row = db.execute('''
        SELECT reports.body, receipts.receipt_no
        FROM normal_reports AS reports LEFT JOIN report_receipts AS receipts
            ON reports.report_no = receipts.report_no
        WHERE reports.report_no = ?
    ''', (report_no,)).fetchone()
    if row is None:
        return None
    body, receipt_no = row
    if receipt_no is None:
        raise RuntimeError(f'archive is missing the receipt for report {report_no!r}')
    return NormalReport(report_no, _deserialize_body(body), ReportReceipt(report_no, receipt_no))
