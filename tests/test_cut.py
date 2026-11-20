import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from local import close_local, record_local
from receiver import admit, deliver_notice
from receiver.admit import PacketConflict
from report import CutReport, NormalReport, ReportReceipt, issue_cut, issue_normal, read_cut, read_normal
from shift import Calibration, ShiftPair, open_shift, open_station_store
from source import SourceOrigin, close_upstream, dispatch, open_cut, record_upstream
from source.dispatch import CutNotice


class CutCase(unittest.TestCase):
    pair = ShiftPair('headwater', 'U-AM', 'estuary', 'D-PM')
    other_pair = ShiftPair('headwater', 'U-NEXT', 'estuary', 'D-NEXT')

    def setUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.source_path = Path(directory) / 'source.sqlite'
        self.downstream_path = Path(directory) / 'downstream.sqlite'
        self.source = self.open_db(self.source_path, 'upstream')
        self.downstream = self.open_db(self.downstream_path, 'downstream')
        for db, role in ((self.source, 'upstream'), (self.downstream, 'downstream')):
            open_shift(db, self.pair, role, ('intake',), ('outlet',), Calibration(2, 1), Calibration(2, 3))

    def open_db(self, path, role):
        connection = open_station_store(path, role)
        self.addCleanup(connection.close)
        return connection

    def reopen_downstream(self):
        self.downstream.close()
        self.downstream = self.open_db(self.downstream_path, 'downstream')

    def stop_source(self):
        self.source.close()
        self.source_path.unlink()

    def collect(self, raw, entry):
        origin = SourceOrigin(self.pair.upstream_station, self.pair.upstream_shift, 'water-book', entry)
        return record_upstream(self.source, self.pair.upstream_shift, 'intake', raw, origin)

    def send(self, read):
        return dispatch(self.source, self.pair.upstream_shift, read.read_no)

    def prepare_cut(self):
        reads = [self.collect(raw, f'line-{seq}') for seq, raw in enumerate((3, 5, 7), 1)]
        packets = [self.send(reads[0]), self.send(reads[1])]
        notice = open_cut(self.source, self.pair.upstream_shift)
        packets.append(self.send(reads[2]))
        return notice, dict(zip(('U1', 'U2', 'U3'), packets))

    def play(self, packets, notice, events):
        for event in events:
            if event == 'N':
                deliver_notice(self.downstream, self.pair.downstream_shift, notice)
            elif event in packets:
                admit(self.downstream, self.pair.downstream_shift, packets[event])
            else:
                record_local(self.downstream, self.pair.downstream_shift, 'outlet', int(event[1:]))

    def statuses(self, report):
        return [row.status for row in report.upstream_rows]

    def assert_report_header(self, report, notice):
        self.assertIsInstance(report, CutReport)
        self.assertEqual(report.cut_no, notice.cut_no)
        self.assertEqual(report.pair, self.pair)
        self.assertEqual(report.receipt, ReportReceipt(notice.cut_no, f'R/{notice.cut_no}'))
        self.assertEqual([row.packet for row in report.upstream_rows], list(notice.packets))
        self.assertEqual(
            [(row.packet.raw, row.packet.microliters, row.packet.origin.entry)
             for row in report.upstream_rows],
            [(3, 7, 'line-1'), (5, 11, 'line-2')],
        )


class CutTests(CutCase):
    def test_notice_arrives_first(self):
        notice, packets = self.prepare_cut()
        self.play(packets, notice, ['U1', 'L1', 'N', 'U2', 'L2', 'U3'])
        report = issue_cut(self.downstream, 'D-PM')
        self.assert_report_header(report, notice)
        self.assertEqual(self.statuses(report), ['received', 'in_transit'])
        received, pending = report.upstream_rows
        self.assertEqual(received.receipt.admission_seq, 1)
        self.assertIsNone(pending.receipt)
        self.assertEqual([read.raw for read in report.local_reads], [1])
        self.assertEqual(report.local_total_microliters, 5)
        self.assertEqual(read_cut(self.downstream, notice.cut_no), report)

    def test_postcut_packet_arrives_first(self):
        notice, packets = self.prepare_cut()
        self.play(packets, notice, ['U1', 'L1', 'U3', 'L2', 'N', 'U2'])
        report = issue_cut(self.downstream, 'D-PM')
        self.assert_report_header(report, notice)
        self.assertEqual([row.packet.source_seq for row in report.upstream_rows], [1, 2])
        self.assertEqual(self.statuses(report), ['received', 'in_transit'])
        self.assertEqual([read.raw for read in report.local_reads], [1])
        self.assertEqual(report.local_total_microliters, 5)

    def test_locals_before_postcut_packet(self):
        notice, packets = self.prepare_cut()
        self.play(packets, notice, ['U1', 'L1', 'L2', 'U3', 'N', 'U2'])
        report = issue_cut(self.downstream, 'D-PM')
        self.assert_report_header(report, notice)
        self.assertEqual(self.statuses(report), ['received', 'in_transit'])
        self.assertEqual([read.raw for read in report.local_reads], [1, 2])
        self.assertEqual(report.local_total_microliters, 12)

    def test_late_low_seq_dispatch_is_post_cut(self):
        first = self.collect(3, 'line-1')
        second = self.collect(5, 'line-2')
        p2 = self.send(second)
        notice = open_cut(self.source, 'U-AM')
        self.assertEqual([packet.source_seq for packet in notice.packets], [2])
        p1 = self.send(first)
        self.assertEqual(p1.source_seq, 1)
        self.play({'U1': p1, 'U2': p2}, notice, ['L1', 'U1', 'L2', 'N'])
        report = issue_cut(self.downstream, 'D-PM')
        self.assertEqual([row.packet.source_seq for row in report.upstream_rows], [2])
        self.assertEqual(self.statuses(report), ['in_transit'])
        row = report.upstream_rows[0]
        self.assertIsNone(row.receipt)
        self.assertEqual((row.packet.raw, row.packet.origin.entry), (5, 'line-2'))
        self.assertEqual([read.raw for read in report.local_reads], [1])
        self.assertEqual(report.local_total_microliters, 5)

    def test_redispatch_after_cut_keeps_first_dispatch_side(self):
        first = self.collect(3, 'line-1')
        second = self.collect(5, 'line-2')
        p1 = self.send(first)
        notice = open_cut(self.source, 'U-AM')
        p2 = self.send(second)
        self.assertEqual(self.send(first), p1)
        self.play({'U1': p1, 'U2': p2}, notice, ['U1', 'N'])
        report = issue_cut(self.downstream, 'D-PM')
        self.assertEqual([row.packet.source_seq for row in report.upstream_rows], [1])
        self.assertEqual(self.statuses(report), ['received'])
        self.assertEqual(report.upstream_rows[0].packet, p1)

    def test_replay_and_redelivery(self):
        notice, packets = self.prepare_cut()
        self.play(packets, notice, ['U1', 'L1', 'N'])
        first_receipt = admit(self.downstream, 'D-PM', packets['U1'])
        self.assertEqual(admit(self.downstream, 'D-PM', packets['U1']), first_receipt)
        self.assertEqual(deliver_notice(self.downstream, 'D-PM', notice), notice)
        report = issue_cut(self.downstream, 'D-PM')
        self.assertEqual(issue_cut(self.downstream, 'D-PM'), report)
        self.assertEqual(read_cut(self.downstream, notice.cut_no), report)
        self.assertEqual(report.upstream_rows[0].receipt, first_receipt)
        self.assertEqual(self.statuses(report), ['received', 'in_transit'])

    def test_rejections_form_no_cut_point(self):
        notice, packets = self.prepare_cut()
        open_shift(self.downstream, self.other_pair, 'downstream', ('intake',), ('outlet',),
                   Calibration(2, 1), Calibration(2, 3))
        with self.assertRaises(ValueError):
            deliver_notice(self.downstream, 'D-NEXT', notice)
        with self.assertRaises(ValueError):
            admit(self.downstream, 'D-NEXT', packets['U1'])
        with self.assertRaises(KeyError):
            deliver_notice(self.downstream, 'MISSING', notice)
        admit(self.downstream, 'D-PM', packets['U1'])
        with self.assertRaises(PacketConflict):
            admit(self.downstream, 'D-PM', replace(packets['U1'], raw=9))
        forged = replace(packets['U1'], origin=replace(packets['U1'].origin, entry='line-9'))
        with self.assertRaises(PacketConflict):
            deliver_notice(self.downstream, 'D-PM', CutNotice('C/forged', self.pair, (forged,)))
        record_local(self.downstream, 'D-PM', 'outlet', 1)
        deliver_notice(self.downstream, 'D-PM', notice)
        with self.assertRaises(ValueError):
            deliver_notice(self.downstream, 'D-PM', CutNotice('C/other', self.pair, notice.packets))
        self.assertEqual(deliver_notice(self.downstream, 'D-PM', notice), notice)
        report = issue_cut(self.downstream, 'D-PM')
        self.assertEqual(self.statuses(report), ['received', 'in_transit'])
        self.assertEqual([read.raw for read in report.local_reads], [1])
        self.assertEqual(report.local_total_microliters, 5)

    def test_invalid_notice_rejected(self):
        notice, packets = self.prepare_cut()
        bad_microliters = replace(packets['U1'], microliters=8)
        for bad in (
            'not-a-notice',
            replace(notice, cut_no=''),
            replace(notice, packets=(packets['U2'], packets['U1'])),
            replace(notice, packets=(bad_microliters,)),
            CutNotice('C/D-PM', self.other_pair, ()),
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                deliver_notice(self.downstream, 'D-PM', bad)
        self.assertEqual(deliver_notice(self.downstream, 'D-PM', notice), notice)
        report = issue_cut(self.downstream, 'D-PM')
        self.assertEqual(self.statuses(report), ['in_transit', 'in_transit'])
        self.assertEqual(report.local_reads, ())
        self.assertEqual(report.local_total_microliters, 0)

    def test_commit_failure_keeps_original_section(self):
        notice, packets = self.prepare_cut()
        self.play(packets, notice, ['U1', 'L1', 'N'])
        observer = self.open_db(self.downstream_path, 'downstream')
        attempts = []

        def hook(cut_no):
            attempts.append(cut_no)
            with self.assertRaises(KeyError):
                read_cut(observer, cut_no)
            if len(attempts) == 1:
                raise RuntimeError('commit interrupted')

        with self.assertRaisesRegex(RuntimeError, 'commit interrupted'):
            issue_cut(self.downstream, 'D-PM', before_commit=hook)
        with self.assertRaises(KeyError):
            read_cut(observer, notice.cut_no)
        self.play(packets, notice, ['U2', 'L2'])
        report = issue_cut(self.downstream, 'D-PM', before_commit=hook)
        self.assertEqual(attempts, [notice.cut_no, notice.cut_no])
        self.assertEqual(self.statuses(report), ['received', 'in_transit'])
        pending = report.upstream_rows[1]
        self.assertIsNone(pending.receipt)
        self.assertEqual((pending.packet.raw, pending.packet.origin.entry), (5, 'line-2'))
        self.assertEqual([read.raw for read in report.local_reads], [1])
        self.assertEqual(report.local_total_microliters, 5)
        self.assertEqual(report.receipt, ReportReceipt(notice.cut_no, f'R/{notice.cut_no}'))
        self.assertEqual(read_cut(observer, notice.cut_no), report)

    def test_late_arrivals_do_not_change_frozen_cut(self):
        notice, packets = self.prepare_cut()
        self.play(packets, notice, ['U1', 'L1', 'N'])
        report = issue_cut(self.downstream, 'D-PM')
        self.play(packets, notice, ['U2', 'L2', 'U3'])
        self.assertEqual(issue_cut(self.downstream, 'D-PM'), report)
        self.assertEqual(read_cut(self.downstream, notice.cut_no), report)
        self.assertEqual(self.statuses(report), ['received', 'in_transit'])
        self.assertEqual([read.raw for read in report.local_reads], [1])

    def test_normal_report_coexists(self):
        notice, packets = self.prepare_cut()
        self.play(packets, notice, ['U1', 'L1', 'U3', 'L2', 'N', 'U2'])
        cut = issue_cut(self.downstream, 'D-PM')
        manifest = close_upstream(self.source, 'U-AM')
        close_local(self.downstream, 'D-PM')
        normal = issue_normal(self.downstream, 'D-PM', manifest)
        self.assertIsInstance(normal, NormalReport)
        self.assertEqual(normal.body.upstream.total_microliters, 33)
        self.assertEqual(normal.body.downstream.total_microliters, 12)
        self.assertEqual(read_cut(self.downstream, notice.cut_no), cut)
        self.assertEqual(read_normal(self.downstream, normal.report_no), normal)
        self.assertEqual([read.raw for read in read_cut(self.downstream, notice.cut_no).local_reads], [1])

    def test_offline_claim_after_source_stops(self):
        notice, packets = self.prepare_cut()
        self.play(packets, notice, ['U1', 'L1', 'U3', 'L2', 'N', 'U2'])
        report = issue_cut(self.downstream, 'D-PM')
        close_upstream(self.source, 'U-AM')
        close_local(self.downstream, 'D-PM')
        self.stop_source()
        self.reopen_downstream()
        restored = read_cut(self.downstream, notice.cut_no)
        self.assertEqual(restored, report)
        self.assertEqual(restored.receipt, report.receipt)
        self.assertEqual([row.packet.origin.entry for row in restored.upstream_rows], ['line-1', 'line-2'])
        self.assertEqual([read.microliters for read in restored.local_reads], [5])
        self.assertEqual(restored.local_total_microliters, 5)

    def test_open_cut_repeated_and_after_close(self):
        p1 = self.send(self.collect(3, 'line-1'))
        notice = open_cut(self.source, 'U-AM')
        self.send(self.collect(5, 'line-2'))
        self.assertEqual(open_cut(self.source, 'U-AM'), notice)
        self.assertEqual(notice.packets, (p1,))
        self.assertEqual(notice.cut_no, 'C/D-PM')
        close_upstream(self.source, 'U-AM')
        self.assertEqual(open_cut(self.source, 'U-AM'), notice)

    def test_open_cut_requires_open_upstream(self):
        self.send(self.collect(3, 'line-1'))
        close_upstream(self.source, 'U-AM')
        with self.assertRaises(RuntimeError):
            open_cut(self.source, 'U-AM')
        with self.assertRaises(KeyError):
            open_cut(self.source, 'UNKNOWN')

    def test_sign_requires_notice(self):
        self.send(self.collect(3, 'line-1'))
        notice = open_cut(self.source, 'U-AM')
        with self.assertRaises(RuntimeError):
            issue_cut(self.downstream, 'D-PM')
        with self.assertRaises(KeyError):
            read_cut(self.downstream, notice.cut_no)
        with self.assertRaises(KeyError):
            issue_cut(self.downstream, 'UNKNOWN')
        deliver_notice(self.downstream, 'D-PM', notice)
        report = issue_cut(self.downstream, 'D-PM')
        self.assertEqual(self.statuses(report), ['in_transit'])
        self.assertEqual(report.local_reads, ())
        self.assertEqual(report.local_total_microliters, 0)

    def test_empty_cut(self):
        notice = open_cut(self.source, 'U-AM')
        self.assertEqual(notice.packets, ())
        p1 = self.send(self.collect(3, 'line-1'))
        self.play({'U1': p1}, notice, ['L1', 'U1', 'L2', 'N'])
        report = issue_cut(self.downstream, 'D-PM')
        self.assertEqual(report.upstream_rows, ())
        self.assertEqual([read.raw for read in report.local_reads], [1])
        self.assertEqual(report.local_total_microliters, 5)


if __name__ == '__main__':
    unittest.main()
