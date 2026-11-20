import json
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from local import close_local, record_local
from receiver import admit, deliver_notice
from report import MissingPackets, NormalReport, issue_cut, issue_normal, read_cut, read_normal
from shift import Calibration, ShiftPair, open_shift, open_station_store
from source import (
    SourceOrigin,
    close_upstream,
    deliver_in_order,
    deliver_packet,
    dispatch,
    open_cut,
    record_upstream,
)


def collect_and_sign(source, downstream, pair):
    def receive(packet):
        return admit(downstream, pair.downstream_shift, packet)

    packets = []
    for raw, entry in ((3, 'line-1'), (5, 'line-2')):
        origin = SourceOrigin(pair.upstream_station, pair.upstream_shift, 'river-book', entry)
        reading = record_upstream(source, pair.upstream_shift, 'intake', raw, origin)
        packets.append(dispatch(source, pair.upstream_shift, reading.read_no))
    first, second = packets
    receipt, replay = deliver_in_order((second, second), receive)
    assert receipt == replay
    record_local(downstream, pair.downstream_shift, 'outlet', 1)
    manifest = close_upstream(source, pair.upstream_shift)
    close_local(downstream, pair.downstream_shift)
    pending = issue_normal(downstream, pair.downstream_shift, manifest)
    assert isinstance(pending, MissingPackets)
    missing = [item.source_seq for item in pending.missing]
    assert missing == [1]
    print('补齐前缺件', ', '.join(f'U{seq}' for seq in missing))
    deliver_packet(first, receive)
    report = issue_normal(downstream, pair.downstream_shift, manifest)
    assert isinstance(report, NormalReport)
    return missing, report


def cut_handover(source_path, downstream_path, pair):
    with closing(open_station_store(downstream_path, 'downstream')) as downstream:
        with closing(open_station_store(source_path, 'upstream')) as source:
            for db, role in ((source, 'upstream'), (downstream, 'downstream')):
                open_shift(db, pair, role, ('intake',), ('outlet',), Calibration(2, 1), Calibration(2, 3))

            def receive(packet):
                return admit(downstream, pair.downstream_shift, packet)

            reads = []
            for raw, entry in ((3, 'line-1'), (5, 'line-2')):
                origin = SourceOrigin(pair.upstream_station, pair.upstream_shift, 'water-book', entry)
                reads.append(record_upstream(source, pair.upstream_shift, 'intake', raw, origin))
            first, second = (dispatch(source, pair.upstream_shift, read.read_no) for read in reads)
            notice = open_cut(source, pair.upstream_shift)
            assert open_cut(source, pair.upstream_shift) == notice
            origin = SourceOrigin(pair.upstream_station, pair.upstream_shift, 'water-book', 'line-3')
            third_read = record_upstream(source, pair.upstream_shift, 'intake', 7, origin)
            third = dispatch(source, pair.upstream_shift, third_read.read_no)
            deliver_packet(first, receive)
            record_local(downstream, pair.downstream_shift, 'outlet', 1)
            deliver_packet(third, receive)
            record_local(downstream, pair.downstream_shift, 'outlet', 2)
            deliver_notice(downstream, pair.downstream_shift, notice)
            deliver_packet(second, receive)
            report = issue_cut(downstream, pair.downstream_shift)
            manifest = close_upstream(source, pair.upstream_shift)
            close_local(downstream, pair.downstream_shift)
            normal = issue_normal(downstream, pair.downstream_shift, manifest)
            assert isinstance(normal, NormalReport)
    return notice, report, normal


def main():
    pair = ShiftPair('headwater', 'UP-AM', 'estuary', 'DOWN-PM')
    with TemporaryDirectory(prefix='river-comparison-') as directory:
        source_path = Path(directory) / 'upstream.sqlite'
        downstream_path = Path(directory) / 'downstream.sqlite'
        with closing(open_station_store(downstream_path, 'downstream')) as downstream:
            with closing(open_station_store(source_path, 'upstream')) as source:
                for db, role in ((source, 'upstream'), (downstream, 'downstream')):
                    open_shift(db, pair, role, ('intake',), ('outlet',), Calibration(2, 1), Calibration(3, 2))
                missing, report = collect_and_sign(source, downstream, pair)
        with closing(open_station_store(downstream_path, 'downstream')) as reopened:
            archived = read_normal(reopened, report.report_no)
        cut_pair = ShiftPair('headwater', 'UP-EVE', 'estuary', 'DOWN-NIGHT')
        notice, cut_report, cut_normal = cut_handover(source_path, downstream_path, cut_pair)
        with closing(open_station_store(downstream_path, 'downstream')) as reopened:
            restored_cut = read_cut(reopened, notice.cut_no)

    rows = archived.body.upstream.rows
    actual = {'missing': missing, 'source_sequences': [row.packet.source_seq for row in rows]}
    downstream_actual = {
        'totals_ul': [archived.body.upstream.total_microliters, archived.body.downstream.total_microliters],
        'source_book_entries': [row.packet.origin.entry for row in rows],
    }
    expected = {'missing': [1], 'source_sequences': [1, 2]}
    downstream_expected = {'totals_ul': [18, 5], 'source_book_entries': ['line-1', 'line-2']}
    assert archived == report
    assert actual == expected
    assert downstream_actual == downstream_expected
    print(f'停源领取 {archived.report_no}，上游 18 μL，下游 5 μL，回执 {archived.receipt.receipt_no}')
    scenario = {
        'case_id': 'v6.u2_first',
        'input': {'upstream_raw': [3, 5], 'local_raw': [1], 'arrival': [2, 2, 1]},
        'expected': expected,
        'actual': actual,
        'downstream_expected': downstream_expected,
        'downstream_actual': downstream_actual,
        'code_paths': ['receiver/admit.py', 'report/reconcile.py', 'report/summarize.py',
                       'report/publish.py', 'report/readback.py'],
        'oracle': '先收 U2 时 {U1,U2} 去掉 U2 只余 U1。手算 3×2+1=7，5×2+1=11，1×3+2=5。',
    }
    print('POINTWISE_SCENARIO ' + json.dumps(scenario, ensure_ascii=False))

    assert restored_cut == cut_report
    assert restored_cut.receipt == cut_report.receipt
    statuses = [row.status for row in restored_cut.upstream_rows]
    local_raw = [read.raw for read in restored_cut.local_reads]
    cut_expected = {'statuses': ['received', 'in_transit'], 'local_raw': [1], 'local_total_ul': 5}
    cut_actual = {
        'statuses': statuses,
        'local_raw': local_raw,
        'local_total_ul': restored_cut.local_total_microliters,
    }
    assert cut_actual == cut_expected
    print(f'交班 {restored_cut.cut_no}：U1 已收、U2 在途，截点前本站 '
          f'{restored_cut.local_total_microliters} μL')
    print(f'停源领取 {restored_cut.cut_no}，回执 {restored_cut.receipt.receipt_no}；'
          f'整班签报 {cut_normal.report_no} 照常')
    cut_scenario = {
        'case_id': 'v7.cut_postcut_first',
        'input': {'upstream_raw': [3, 5, 7], 'local_raw': [1, 2],
                  'arrival': ['U1', 'L1', 'U3', 'L2', 'N', 'U2']},
        'expected': cut_expected,
        'actual': cut_actual,
        'code_paths': ['source/dispatch.py', 'receiver/admit.py', 'receiver/events.py',
                       'report/publish.py', 'report/readback.py'],
        'oracle': '开单固定 {U1,U2}；U3 切后首纳先于通知形成截点，截点事件本身不入账，'
                  '截点前仅 U1 与 L1 入账。手算 1×2+3=5。',
    }
    print('POINTWISE_SCENARIO ' + json.dumps(cut_scenario, ensure_ascii=False))


if __name__ == '__main__':
    main()
