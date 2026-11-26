"""Attribute raw media RPCs to the monitor file attempt that actually sent them."""
from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tgdl.auto_throttle import MonitorAutoThrottle


@dataclass
class _Transfer:
    client: object
    governor: MonitorAutoThrottle
    active: bool = True


_transfer: ContextVar[_Transfer | None] = ContextVar("tgdl_monitor_media_transfer", default=None)


@contextmanager
def monitor_media_requests(client: object, governor: MonitorAutoThrottle | None) -> Iterator[None]:
    # Child chunk tasks / wait_for(anext) inherit this scope. Other downloads
    # sharing the same client (including manual and gallery) do not.
    scope = _Transfer(client, governor) if governor is not None else None
    token = _transfer.set(scope)
    try:
        yield
    finally:
        if scope is not None:
            scope.active = False
        _transfer.reset(token)


@dataclass(frozen=True)
class MediaRequest:
    scope: _Transfer
    generation: int
    sent_at: float
    sender: object
    transport: object
    reconnecting: bool

    def current_governor(self) -> MonitorAutoThrottle | None:
        governor = self.scope.governor
        if self.scope.active and governor.enabled and governor.generation == self.generation:
            return governor
        return None

    def uninterrupted(self) -> bool:
        # Pinned Telethon 1.46 recreates the sender loop at every connection.
        # A successful re-sent future still contains real bytes but its RTT
        # includes reconnect downtime: never credit it toward rate/recovery.
        return (not self.reconnecting and not getattr(self.sender, "_reconnecting", False)
                and self.transport is getattr(self.sender, "_send_loop_handle", None))


def capture_request(client: object, sender: object) -> MediaRequest | None:
    scope = _transfer.get()
    if scope and scope.active and scope.client is client and scope.governor.enabled:
        return MediaRequest(scope, scope.governor.generation, time.monotonic(), sender,
                            getattr(sender, "_send_loop_handle", None),
                            getattr(sender, "_reconnecting", False))
    return None
