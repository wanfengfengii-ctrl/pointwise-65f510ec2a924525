from dataclasses import replace
from sqlite3 import ProgrammingError

from shift import Calibration, initialize_station_store, open_shift
from shift.store import get_station_status
from source import close_upstream, dispatch, record_upstream
from source.acquire import list_upstream_reads, load_upstream_read
from source.closeout import PendingDispatch, load_upstream_closeout
from source.dispatch import list_dispatched_packets
from tests.fixture import ShiftCase


class DispatchTests(ShiftCase):
    def test_fixed_integer_conversion(self):
        read = self.upstream_read(3, 'line-1')
        packet = self.send(read)
        self.assertEqual(read.read_no, ('headwater', 'U-AM', 1))
        self.assertEqual((packet.source_seq, packet.raw, packet.microliters), (1, 3, 7))
        self.assertEqual(packet.origin, self.origin('line-1'))
        self.assertEqual(packet.point, 'intake')
        self.assertEqual(list_dispatched_packets(self.source, 'U-AM'), (packet,))

    def test_reopen_conflict(self):
        with self.assertRaises(ValueError):
            self.open_pair(self.source, 'upstream', upstream_calibration=Calibration(3, 1))
        self.assertEqual(self.send(self.upstream_read(3, 'line-1')).microliters, 7)

    def test_invalid_input(self):
        for raw in (None, False, 3.0, '3'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                self.upstream_read(raw, 'line-1')
        with self.assertRaises(ValueError):
            record_upstream(self.source, 'U-AM', 'absent', 3, self.origin('line-1'))
        with self.assertRaises(ValueError):
            record_upstream(self.source, 'U-AM', 'intake', 3, replace(self.origin('line-1'), entry=''))
        self.assertEqual(list_upstream_reads(self.source, 'U-AM'), ())
        self.assertEqual(list_dispatched_packets(self.source, 'U-AM'), ())
        self.assertEqual(self.upstream_read(0, 'line-1').source_seq, 1)

    def test_dispatch_replay(self):
        self.upstream_read(3, 'line-1')
        second = self.upstream_read(5, 'line-2')
        original = self.send(second)
        self.reopen_source()
        self.assertEqual(self.send(second), original)
        self.assertEqual(original.source_seq, 2)
        self.assertEqual(list_dispatched_packets(self.source, 'U-AM'), (original,))

    def test_integer_precision(self):
        read = self.upstream_read(9007199254740993, 'large')
        packet = self.send(read)
        self.reopen_source()
        restored = load_upstream_read(self.source, 'U-AM', 1)
        self.assertEqual(restored.raw, 9007199254740993)
        self.assertEqual(restored.microliters, 18014398509481987)
        self.assertEqual(self.send(restored), packet)
        self.assertEqual(packet.microliters, 18014398509481987)

    def test_initialize_reopen(self):
        new_pair = self.other_pair
        custom = self.open_pair(self.source, 'upstream', pair=new_pair, upstream_calibration=Calibration(4, -2))
        origin = replace(self.origin('adjusted'), shift=new_pair.upstream_shift)
        read = record_upstream(self.source, new_pair.upstream_shift, 'intake', 3, origin)
        initialize_station_store(self.source, 'upstream')
        self.reopen_source()
        with self.assertRaises(ValueError):
            initialize_station_store(self.source, 'downstream')
        self.assertEqual(self.open_pair(self.source, 'upstream'), self.source_snapshot)
        self.assertEqual(
            self.open_pair(self.source, 'upstream', pair=new_pair, upstream_calibration=Calibration(4, -2)),
            custom,
        )
        with self.assertRaises(ValueError):
            self.open_pair(self.source, 'upstream', pair=new_pair)
        self.assertEqual(load_upstream_read(self.source, new_pair.upstream_shift, 1), read)
        self.assertEqual(dispatch(self.source, new_pair.upstream_shift, read.read_no).microliters, 10)

    def test_invalid_shift_configuration_is_atomic(self):
        pair = self.other_pair
        for points, calibration in ((('intake', 'intake'), Calibration(2, 1)),
                                    (('intake',), Calibration(True, 1)),
                                    (('intake',), Calibration(2, '1'))):
            with self.subTest(points=points, calibration=calibration), self.assertRaises(ValueError):
                open_shift(self.source, pair, 'upstream', points, ('outlet',), calibration, Calibration(3, 2))
        with self.assertRaises(KeyError):
            get_station_status(self.source, pair.upstream_shift)
        conflict = replace(self.pair, upstream_shift='OTHER')
        with self.assertRaises(ValueError):
            self.open_pair(self.source, 'upstream', pair=conflict)
        self.assertEqual(self.open_pair(self.source, 'upstream', pair=pair).pair, pair)

    def test_pending_dispatch_close(self):
        first = self.upstream_read(3, 'line-1')
        second = self.upstream_read(5, 'line-2')
        first_packet = self.send(first)
        with self.assertRaises(PendingDispatch) as pending:
            close_upstream(self.source, 'U-AM')
        self.assertEqual(pending.exception.source_seqs, (2,))
        self.assertIsNone(load_upstream_closeout(self.source, 'U-AM'))
        self.assertEqual(get_station_status(self.source, 'U-AM'), 'open')
        self.assertEqual(self.send(second).source_seq, 2)
        manifest = close_upstream(self.source, 'U-AM')
        self.assertEqual(manifest.source_seqs, (1, 2))
        self.reopen_source()
        self.assertEqual(close_upstream(self.source, 'U-AM'), manifest)
        self.assertEqual(self.send(first), first_packet)
        with self.assertRaises(RuntimeError):
            self.upstream_read(7, 'late')

    def test_dispatch_requires_the_registered_shift_and_read(self):
        first = self.upstream_read(3, 'line-1')
        self.open_pair(self.source, 'upstream', pair=self.other_pair)
        with self.assertRaises(ValueError):
            dispatch(self.source, self.other_pair.upstream_shift, first.read_no)
        with self.assertRaises(KeyError):
            dispatch(self.source, 'U-AM', ('headwater', 'U-AM', 7))
        self.assertEqual(list_dispatched_packets(self.source, 'U-AM'), ())
        self.assertEqual(self.send(first).source_seq, 1)
