from sqlite3 import Connection

from shift.pairing import require_identifier
from .archive import CutReport, NormalReport, load_cut_report, load_report


def read_normal(db: Connection, report_no: str) -> NormalReport:
    require_identifier(report_no, 'report number')
    report = load_report(db, report_no)
    if report is None:
        raise KeyError(report_no)
    return report


def read_cut(db: Connection, cut_no: str) -> CutReport:
    require_identifier(cut_no, 'cut number')
    report = load_cut_report(db, cut_no)
    if report is None:
        raise KeyError(cut_no)
    return report
