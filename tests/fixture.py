import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from local import close_local, record_local
from receiver import admit
from report import issue_normal
from shift import Calibration, ShiftPair, open_shift, open_station_store
from source import SourceOrigin, close_upstream, deliver_in_order, dispatch, record_upstream


class ShiftCase(unittest.TestCase):
    pair = ShiftPair('headwater', 'U-AM', 'estuary', 'D-PM')
    other_pair = ShiftPair('headwater', 'U-NEXT', 'estuary', 'D-NEXT')

    def setUp(self):
        directory = self.enterContext(TemporaryDirectory())
        self.source_path = Path(directory) / 'source.sqlite'
        self.downstream_path = Path(directory) / 'downstream.sqlite'
        self.source = self.open_db(self.source_path, 'upstream')
        self.downstream = self.open_db(self.downstream_path, 'downstream')
        self.source_snapshot = self.open_pair(self.source, 'upstream')
        self.downstream_snapshot = self.open_pair(self.downstream, 'downstream')
        self.delivery_enabled = True

    def open_db(self, path, role):
        connection = open_station_store(path, role)
        self.addCleanup(connection.close)
        return connection

    def reopen_source(self):
        self.source.close()
        self.source = self.open_db(self.source_path, 'upstream')

    def reopen_downstream(self):
        self.downstream.close()
        self.downstream = self.open_db(self.downstream_path, 'downstream')

    def stop_source(self):
        self.source.close()
        self.delivery_enabled = False

    def open_pair(self, db, role, *, pair=None, upstream_calibration=Calibration(2, 1),
                  downstream_calibration=Calibration(3, 2)):
        return open_shift(
            db, pair or self.pair, role, ('intake', 'bank'), ('outlet',),
            upstream_calibration, downstream_calibration,
        )

    def origin(self, entry):
        return SourceOrigin(self.pair.upstream_station, self.pair.upstream_shift, 'water-book', entry)

    def upstream_read(self, raw, entry):
        return record_upstream(self.source, self.pair.upstream_shift, 'intake', raw, self.origin(entry))

    def send(self, read):
        return dispatch(self.source, self.pair.upstream_shift, read.read_no)

    def two_packets(self):
        return self.send(self.upstream_read(3, 'line-1')), self.send(self.upstream_read(5, 'line-2'))

    def receive(self, *packets):
        if not self.delivery_enabled:
            raise RuntimeError('fixture delivery port is closed')
        return deliver_in_order(packets, lambda packet: admit(self.downstream, self.pair.downstream_shift, packet))

    def local_read(self, raw):
        return record_local(self.downstream, self.pair.downstream_shift, 'outlet', raw)

    def close_both(self):
        manifest = close_upstream(self.source, self.pair.upstream_shift)
        close_local(self.downstream, self.pair.downstream_shift)
        return manifest

    def sign(self, manifest, **kwargs):
        return issue_normal(self.downstream, self.pair.downstream_shift, manifest, **kwargs)

    def assert_archive_counts(self, reports, receipts, db=None):
        connection = self.downstream if db is None else db
        self.assertEqual(connection.execute('SELECT COUNT(*) FROM normal_reports').fetchone()[0], reports)
        self.assertEqual(connection.execute('SELECT COUNT(*) FROM report_receipts').fetchone()[0], receipts)
