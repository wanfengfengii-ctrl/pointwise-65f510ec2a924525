from sqlite3 import Connection

from shift.pairing import require_identifier
from .archive import NormalReport, load_report


def read_normal(db: Connection, report_no: str) -> NormalReport:
    require_identifier(report_no, 'report number')
    report = load_report(db, report_no)
    if report is None:
        raise KeyError(report_no)
    return report
