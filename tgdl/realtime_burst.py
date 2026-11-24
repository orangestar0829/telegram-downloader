"""Shared runtime policy for opportunistic realtime overflow, not durable work."""
from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping

from tgdl.history import DownloadHistory, PendingMedia
from tgdl.monitor_flow import MonitorPause
from tgdl.slots import SlotCoordinator

FileKey = tuple[int, int]
_MAX_BURST_COMPLETIONS = 3
_ORDINARY_PROTECTION_SECONDS = 5.0


def realtime_candidates(
    channels: Iterable[dict], ready, slots: SlotCoordinator, history: DownloadHistory,
    account: str, workers: int, active: Mapping, *, borrow: bool = False,
) -> Iterator[tuple[dict, PendingMedia]]:
    for item in channels:
        peer = item.get("peer_id")
        if (item.get("mode") != "realtime" or not item.get("enabled", True)
                or peer not in ready or not slots.matches(account, item)):
            continue
        if borrow:
            if not slots.can_borrow_realtime(account, workers, peer):
                continue
        elif not slots.available(account, workers, peer):
            continue
        kinds = {kind.strip() for kind in item.get("types", "photo,video").split(",")
                 if kind.strip()}
        for job in history.pending_for_peer(
            peer, account_id=account, limit=16, kinds=kinds, realtime=True,
        ):
            key = (job.peer_id, job.message_id)
            if key not in active and (
                slots.file_available(key) if borrow
                else slots.available(account, workers, peer, file_key=key)
            ):
                yield item, job


class RealtimeBurst:
    """A short live burst wins; sustained overflow periodically yields a turn.

    Eligibility/permits/leases belong to the caller. These are only scheduler
    hints and never allow an unfinished ordinary transfer to lose its claim.
    """
    def __init__(self):
        self.borrowed: set[FileKey] = set()
        self.preempted: set[FileKey] = set()
        self.protected: dict[FileKey, float] = {}
        self.completed = 0

    @property
    def ordinary_turn(self) -> bool:
        return self.completed >= _MAX_BURST_COMPLETIONS

    def finished(self, key: FileKey) -> None:
        if key in self.borrowed:
            self.borrowed.remove(key)
            self.completed += 1
        self.preempted.discard(key)
        self.protected.pop(key, None)

    def ordinary_started(self, key: FileKey, now: float) -> None:
        self.preempted.discard(key)
        if self.ordinary_turn:
            self.completed = 0
            self.protected[key] = now + _ORDINARY_PROTECTION_SECONDS

    def no_ordinary_demand(self) -> None:
        self.completed = 0

    def withdraw(self, pauses: Mapping[FileKey, MonitorPause], permits: Mapping, retiring) -> None:
        # If overflow is no longer eligible, don't let an in-flight ordinary
        # request park itself. An already yielded permit must be reacquired.
        for key in self.preempted.copy():
            if key in permits and key not in retiring:
                pauses[key].resume()
                self.preempted.discard(key)

    def request(
        self, pauses: Mapping[FileKey, MonitorPause], victims: Iterable[FileKey], now: float,
    ) -> FileKey | None:
        if any(not pauses[key].safely_suspended for key in self.preempted):
            return None
        for key in victims:  # Lowest scheduled priority first.
            pause = pauses[key]
            if (not pause.requested and pause.in_transfer
                    and now >= self.protected.get(key, 0)):
                self.preempted.add(key)
                pause.pause()
                return key
        return None
