from dataclasses import asdict, dataclass

from .identity import PacketIdentity


@dataclass(frozen=True, slots=True)
class AdmissionReceipt:
    downstream_shift: str
    identity: PacketIdentity
    admission_seq: int


def make_admission_receipt(
    downstream_shift: str, identity: PacketIdentity, admission_seq: int,
) -> AdmissionReceipt:
    return AdmissionReceipt(downstream_shift, identity, admission_seq)


def encode_admission_receipt(receipt: AdmissionReceipt) -> dict:
    return asdict(receipt)


def decode_admission_receipt(data: dict) -> AdmissionReceipt:
    return AdmissionReceipt(data['downstream_shift'], PacketIdentity(**data['identity']), data['admission_seq'])
