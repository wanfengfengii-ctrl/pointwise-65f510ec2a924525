from collections.abc import Callable, Iterable
from typing import TypeVar

from .dispatch import DispatchPacket


Receipt = TypeVar('Receipt')


def deliver_packet(packet: DispatchPacket, receive: Callable[[DispatchPacket], Receipt]) -> Receipt:
    return receive(packet)


def deliver_in_order(
    packets: Iterable[DispatchPacket], receive: Callable[[DispatchPacket], Receipt],
) -> tuple[Receipt, ...]:
    return tuple(deliver_packet(packet, receive) for packet in packets)
