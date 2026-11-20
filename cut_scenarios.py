from contextlib import closing

from local import close_local, record_local
from local.acquire import list_local_reads
from receiver import admit
from receiver.ledger import list_acceptances
from report import issue_normal, read_normal
from shift import Calibration, ShiftPair, open_shift, open_station_store
from source import SourceOrigin, close_upstream, dispatch, record_upstream
from source.dispatch import list_dispatched_packets


PAIR = ShiftPair("headwater", "U-AM", "estuary", "D-PM")


def open_pair(db, role):
    open_shift(db, PAIR, role, ("intake",), ("outlet",), Calibration(2, 1), Calibration(2, 3))


def collect(db, raw, entry):
    origin = SourceOrigin(PAIR.upstream_station, PAIR.upstream_shift, "water-book", entry)
    return record_upstream(db, PAIR.upstream_shift, "intake", raw, origin)


def send(db, read):
    return dispatch(db, PAIR.upstream_shift, read.read_no)


def v1_view(events):
    with closing(open_station_store(":memory:", "upstream")) as upstream:
        with closing(open_station_store(":memory:", "downstream")) as downstream:
            open_pair(upstream, "upstream")
            open_pair(downstream, "downstream")
            packets = {
                f"U{i}": send(upstream, collect(upstream, raw, f"line-{i}"))
                for i, raw in enumerate((3, 5, 7), 1)
            }
            for event in events:
                if event in packets:
                    admit(downstream, PAIR.downstream_shift, packets[event])
                else:
                    record_local(downstream, PAIR.downstream_shift, "outlet", int(event[1:]))
            accepted = list_acceptances(downstream, PAIR.downstream_shift)
            local_reads = list_local_reads(downstream, PAIR.downstream_shift)
            close_local(downstream, PAIR.downstream_shift)
            report = issue_normal(downstream, PAIR.downstream_shift, close_upstream(upstream, PAIR.upstream_shift))
            assert read_normal(downstream, report.report_no) == report
            return accepted, local_reads, report


def v1_sparse_dispatch():
    with closing(open_station_store(":memory:", "upstream")) as db:
        open_pair(db, "upstream")
        first = collect(db, 3, "line-1")
        second = collect(db, 5, "line-2")
        send(db, second)
        before = list_dispatched_packets(db, PAIR.upstream_shift)
        send(db, first)
        after = list_dispatched_packets(db, PAIR.upstream_shift)
        return tuple(p.source_seq for p in before), tuple(p.source_seq for p in after)


def main():
    histories = (("U1", "L1", "U3", "L2", "U2"), ("U1", "L1", "L2", "U3", "U2"))
    first, second = (v1_view(events) for events in histories)
    assert first == second
    accepted, local_reads, report = first
    admissions = tuple(row.packet.source_seq for row in accepted)
    local_values = tuple(read.microliters for read in local_reads)
    totals = report.body.upstream.total_microliters, report.body.downstream.total_microliters
    assert admissions == (1, 3, 2) and local_values == (5, 7) and totals == (33, 12)
    sparse = v1_sparse_dispatch()
    assert sparse == ((2,), (1, 2))
    print(f"甲 {histories[0]}；乙 {histories[1]}")
    print(f"V1 首纳 {admissions}，本站微升 {local_values}，常规两站合计 {totals}")
    print(f"V1 延发低号的实发账 {sparse[0]} → {sparse[1]}")


if __name__ == "__main__":
    main()
