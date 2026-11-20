from dataclasses import dataclass

from local.closeout import LocalCloseout
from receiver.identity import PacketIdentity
from receiver.ledger import AcceptedPacket
from shift.pairing import require_paired_shifts
from source.closeout import UpstreamCloseout, validate_upstream_closeout


@dataclass(frozen=True, slots=True)
class Coverage:
    expected: tuple[PacketIdentity, ...]
    missing: tuple[PacketIdentity, ...]
    accepted: tuple[AcceptedPacket, ...]


def reconcile(
    upstream: UpstreamCloseout, local: LocalCloseout, acceptances: tuple[AcceptedPacket, ...],
) -> Coverage:
    validate_upstream_closeout(upstream)
    require_paired_shifts(local.pair, upstream.pair)
    expected = tuple(PacketIdentity(upstream.pair.upstream_shift, seq) for seq in upstream.source_seqs)
    by_identity = {
        row.receipt.identity: row for row in acceptances
        if row.packet.pair == upstream.pair and row.receipt.downstream_shift == local.pair.downstream_shift
    }
    missing = tuple(identity for identity in expected if identity not in by_identity)
    matched = tuple(by_identity[identity] for identity in expected if identity in by_identity)
    return Coverage(expected, missing, matched)
