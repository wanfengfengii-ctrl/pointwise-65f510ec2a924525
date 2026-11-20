from dataclasses import replace

from local import close_local, record_local
from receiver.identity import PacketIdentity
from report import MissingPackets, NormalReport, issue_normal, read_normal
from shift import Calibration
from source import SourceOrigin, close_upstream, dispatch, record_upstream
from source.closeout import UpstreamCloseout
from tests.fixture import ShiftCase


class ReportTests(ShiftCase):
    def test_missing_then_complete(self):
        first, second = self.two_packets()
        self.local_read(1)
        manifest = self.close_both()
        self.receive(first)
        self.assertEqual(self.sign(manifest), MissingPackets((PacketIdentity('U-AM', 2),)))
        self.assert_archive_counts(0, 0)
        self.receive(second)
        report = self.sign(manifest)
        self.assertIsInstance(report, NormalReport)
        self.assertEqual(report.body.expected, (PacketIdentity('U-AM', 1), PacketIdentity('U-AM', 2)))
        self.assertEqual([(row.packet.raw, row.packet.microliters) for row in report.body.upstream.rows],
                         [(3, 7), (5, 11)])
        self.assertEqual(report.body.upstream.total_microliters, 18)
        self.assertEqual(report.body.downstream.total_microliters, 5)
        self.assertEqual(self.sign(manifest), report)
        self.assert_archive_counts(1, 1)

    def test_u2_first(self):
        first, second = self.two_packets()
        self.local_read(1)
        manifest = self.close_both()
        second_receipt, replay = self.receive(second, second)
        self.assertEqual(replay, second_receipt)
        with self.assertRaises(ValueError):
            self.receive(replace(second, raw=99))
        self.assertEqual(self.sign(manifest), MissingPackets((PacketIdentity('U-AM', 1),)))
        self.assert_archive_counts(0, 0)
        self.receive(first)
        report = self.sign(manifest)
        self.assertEqual([row.packet.source_seq for row in report.body.upstream.rows], [1, 2])
        self.assertEqual([row.receipt.admission_seq for row in report.body.upstream.rows], [2, 1])
        self.assertEqual((report.body.upstream.total_microliters, report.body.downstream.total_microliters), (18, 5))

    def test_invalid_closeout(self):
        first, second = self.two_packets()
        self.receive(first, second)
        manifest = close_upstream(self.source, 'U-AM')
        with self.assertRaises(RuntimeError):
            self.sign(manifest)
        close_local(self.downstream, 'D-PM')
        with self.assertRaises(ValueError):
            self.sign(UpstreamCloseout(self.other_pair, manifest.source_seqs))
        with self.assertRaises(ValueError):
            self.sign(replace(manifest, source_seqs=(2, 1)))
        with self.assertRaises(ValueError):
            self.sign(replace(manifest, source_seqs=(True, 2)))
        self.assert_archive_counts(0, 0)
        self.assertEqual(self.sign(manifest).body.upstream.total_microliters, 18)

    def test_empty_ledgers(self):
        manifest = self.close_both()
        self.assertEqual(manifest.source_seqs, ())
        report = self.sign(manifest)
        self.assertEqual((report.body.expected, report.body.upstream.rows, report.body.downstream.rows), ((), (), ()))
        self.assertEqual((report.body.upstream.total_microliters, report.body.downstream.total_microliters), (0, 0))
        self.assertEqual(read_normal(self.downstream, report.report_no), report)

    def test_commit_failure_then_retry(self):
        first, second = self.two_packets()
        self.receive(second, first)
        self.local_read(1)
        manifest = self.close_both()
        observer = self.open_db(self.downstream_path, 'downstream')
        attempts = []

        def fail_once(report_no):
            self.assert_archive_counts(1, 1)
            self.assert_archive_counts(0, 0, observer)
            attempts.append(report_no)
            if len(attempts) == 1:
                raise RuntimeError('commit interrupted')

        with self.assertRaisesRegex(RuntimeError, 'commit interrupted'):
            self.sign(manifest, before_commit=fail_once)
        self.assert_archive_counts(0, 0, observer)
        self.assertEqual(len(attempts), 1)
        with self.assertRaises(KeyError):
            read_normal(observer, attempts[0])
        report = self.sign(manifest, before_commit=fail_once)
        self.assertEqual(attempts, [report.report_no, report.report_no])
        self.assert_archive_counts(1, 1, observer)
        self.assertEqual(read_normal(observer, report.report_no), report)
        self.reopen_downstream()
        self.assertEqual(read_normal(self.downstream, report.report_no).receipt, report.receipt)
        self.assertEqual(self.sign(manifest, before_commit=fail_once), report)
        self.assertEqual(len(attempts), 2)

    def test_offline_readback(self):
        first, second = self.two_packets()
        r2, r1 = self.receive(second, first)
        self.local_read(1)
        manifest = self.close_both()
        report = self.sign(manifest)
        self.assertEqual(self.receive(second), (r2,))
        self.stop_source()
        self.source_path.unlink()
        self.reopen_downstream()
        restored = read_normal(self.downstream, report.report_no)
        self.assertEqual(restored, report)
        self.assertEqual([row.receipt for row in restored.body.upstream.rows], [r1, r2])
        self.assertEqual([(row.packet.raw, row.packet.origin.entry) for row in restored.body.upstream.rows],
                         [(3, 'line-1'), (5, 'line-2')])
        local_read, = restored.body.downstream.rows
        self.assertEqual((local_read.station, local_read.shift, local_read.point, local_read.local_seq, local_read.raw),
                         ('estuary', 'D-PM', 'outlet', 1, 1))
        self.assertEqual(restored.receipt.report_no, report.report_no)

    def test_local_rows(self):
        first, second = self.two_packets()
        self.receive(second, first)
        self.local_read(1)
        self.local_read(2)
        report = self.sign(self.close_both())
        self.stop_source()
        self.reopen_downstream()
        result = read_normal(self.downstream, report.report_no)
        self.assertEqual([(row.local_seq, row.raw, row.microliters) for row in result.body.downstream.rows],
                         [(1, 1, 5), (2, 2, 8)])
        self.assertEqual([row.point for row in result.body.downstream.rows], ['outlet', 'outlet'])
        self.assertEqual(result.body.downstream.total_microliters, 13)

    def test_unknown_number(self):
        report = self.sign(self.close_both())
        with self.assertRaises(KeyError):
            read_normal(self.downstream, 'unknown-report')
        self.assertEqual(read_normal(self.downstream, report.report_no), report)
        self.assert_archive_counts(1, 1)

    def test_zero_and_large_values_survive_archival(self):
        pair = self.other_pair
        for db, role in ((self.source, 'upstream'), (self.downstream, 'downstream')):
            self.open_pair(db, role, pair=pair, upstream_calibration=Calibration(2, 0),
                           downstream_calibration=Calibration(3, 0))
        for raw, entry in ((0, 'zero'), (9007199254740993, 'large')):
            origin = SourceOrigin(pair.upstream_station, pair.upstream_shift, 'water-book', entry)
            read = record_upstream(self.source, pair.upstream_shift, 'intake', raw, origin)
            packet = dispatch(self.source, pair.upstream_shift, read.read_no)
            from receiver import admit
            admit(self.downstream, pair.downstream_shift, packet)
        record_local(self.downstream, pair.downstream_shift, 'outlet', 0)
        close_local(self.downstream, pair.downstream_shift)
        manifest = close_upstream(self.source, pair.upstream_shift)
        report = issue_normal(self.downstream, pair.downstream_shift, manifest)
        self.reopen_downstream()
        archived = read_normal(self.downstream, report.report_no)
        self.assertEqual([row.packet.microliters for row in archived.body.upstream.rows], [0, 18014398509481986])
        self.assertEqual(archived.body.upstream.total_microliters, 18014398509481986)
        self.assertEqual([row.microliters for row in archived.body.downstream.rows], [0])
        self.assertEqual(archived.body.downstream.total_microliters, 0)
