from dataclasses import replace

from local import close_local, record_local
from local.acquire import list_local_reads
from receiver import admit
from receiver.admit import PacketConflict
from receiver.identity import PacketIdentity
from receiver.ledger import list_acceptances
from tests.fixture import ShiftCase


class ReceiveTests(ShiftCase):
    def test_reorder_and_retry(self):
        first, second = self.two_packets()
        r2, r1, replay = self.receive(second, first, second)
        self.assertEqual((r2.identity, r1.identity), (PacketIdentity('U-AM', 2), PacketIdentity('U-AM', 1)))
        self.assertEqual((r2.admission_seq, r1.admission_seq), (1, 2))
        self.assertEqual(replay, r2)
        self.assertEqual([row.packet for row in list_acceptances(self.downstream, 'D-PM')], [second, first])

    def test_late_after_close(self):
        first, second = self.two_packets()
        manifest = self.close_both()
        self.assertEqual(manifest.source_seqs, (1, 2))
        self.reopen_downstream()
        r2, r1, replay = self.receive(second, first, second)
        self.assertEqual((r2.admission_seq, r1.admission_seq), (1, 2))
        self.assertEqual(replay, r2)
        self.assertEqual([row.packet for row in list_acceptances(self.downstream, 'D-PM')], [second, first])
        self.assertEqual(close_local(self.downstream, 'D-PM').reads, ())

    def test_conflict_preserves_original(self):
        first, second = self.two_packets()
        receipt, = self.receive(second)
        self.reopen_downstream()
        for changed in (replace(second, raw=6), replace(second, raw=5.0),
                        replace(second, origin=replace(second.origin, entry='changed'))):
            with self.subTest(changed=changed), self.assertRaises(PacketConflict):
                self.receive(changed)
        self.assertEqual(self.receive(second), (receipt,))
        existing, = list_acceptances(self.downstream, 'D-PM')
        self.assertEqual((existing.packet, existing.receipt), (second, receipt))
        self.assertEqual(self.receive(first)[0].admission_seq, 2)

    def test_invalid_origin(self):
        first, _ = self.two_packets()
        for bad_origin in (replace(first.origin, entry=''), replace(first.origin, shift='WRONG')):
            with self.subTest(origin=bad_origin), self.assertRaises(ValueError):
                self.receive(replace(first, origin=bad_origin))
        with self.assertRaises(ValueError):
            self.receive(replace(first, microliters=8))
        self.assertEqual(list_acceptances(self.downstream, 'D-PM'), ())
        receipt, = self.receive(first)
        self.assertEqual(receipt.admission_seq, 1)
        self.assertEqual(list_acceptances(self.downstream, 'D-PM')[0].packet.origin, self.origin('line-1'))

    def test_wrong_pair(self):
        self.open_pair(self.downstream, 'downstream', pair=self.other_pair)
        first, _ = self.two_packets()
        with self.assertRaises(ValueError):
            admit(self.downstream, self.other_pair.downstream_shift, first)
        with self.assertRaises(KeyError):
            admit(self.downstream, 'MISSING', first)
        self.assertEqual(list_acceptances(self.downstream, self.other_pair.downstream_shift), ())
        self.assertEqual(self.receive(first)[0].admission_seq, 1)
        self.assertEqual(len(list_acceptances(self.downstream, 'D-PM')), 1)
        self.assertEqual(list_acceptances(self.downstream, self.other_pair.downstream_shift), ())

    def test_local_close(self):
        reading = self.local_read(1)
        self.assertEqual((reading.local_seq, reading.raw, reading.microliters), (1, 1, 5))
        closeout = close_local(self.downstream, 'D-PM')
        self.assertEqual(closeout.reads, (reading,))
        with self.assertRaises(RuntimeError):
            self.local_read(2)
        self.assertEqual(list_local_reads(self.downstream, 'D-PM'), (reading,))
        self.assertEqual(close_local(self.downstream, 'D-PM'), closeout)

    def test_invalid_local(self):
        for raw in (None, True, 1.0, '1'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                self.local_read(raw)
        with self.assertRaises(ValueError):
            record_local(self.downstream, 'D-PM', 'intake', 1)
        with self.assertRaises(KeyError):
            record_local(self.downstream, 'UNKNOWN', 'outlet', 1)
        with self.assertRaises(KeyError):
            close_local(self.downstream, 'UNKNOWN')
        self.assertEqual(list_local_reads(self.downstream, 'D-PM'), ())
        self.assertEqual(self.local_read(1).local_seq, 1)

    def test_repeated_point_close(self):
        self.local_read(1)
        self.local_read(2)
        closeout = close_local(self.downstream, 'D-PM')
        self.assertEqual([(row.point, row.local_seq, row.microliters) for row in closeout.reads],
                         [('outlet', 1, 5), ('outlet', 2, 8)])
        self.reopen_downstream()
        self.assertEqual(close_local(self.downstream, 'D-PM'), closeout)
        with self.assertRaises(RuntimeError):
            self.local_read(3)
