"""Fixed GUI file slots. Network connections, scan coroutines and chunks are not slots."""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True)
class SlotPlan:
    # Only read these mappings; publish a fresh plan on each configuration edit.
    owners: dict[int, str]
    limits: dict[str, int]
    streams: dict[str, tuple[dict, ...]]


def make_plan(accounts: Iterable[tuple[str, int, Iterable[Mapping]]]) -> SlotPlan:
    """First configured account in registry order owns the one slot for a Channel."""
    owners: dict[int, str] = {}
    limits: dict[str, int] = {}
    streams: dict[str, tuple[dict, ...]] = {}
    for account, limit, rows in accounts:
        limits[account] = limit
        streams[account] = tuple(dict(row) for row in rows
                                 if row.get("enabled", True) and row.get("mode") == "realtime")
        seen = set()
        for row in streams[account]:
            peer = row.get("peer_id")
            if type(peer) is not int or peer <= 0:
                raise ValueError("实时监控需要从已加入的频道或超级群列表选择目标")
            if peer in seen:
                raise ValueError("此账号已为同一频道设置了实时监听，请修改原有记录。")
            seen.add(peer)
            owners.setdefault(peer, account)
    for account, limit in limits.items():
        reserved = sum(owner == account for owner in owners.values())
        if reserved > limit:
            raise ValueError(f"实时频道需要 {reserved} 个固定名额，但账号 {account} 的文件并发是 {limit}。"
                             "请先减少实时频道或手动调高该账号并发；未保存更改。")
    return SlotPlan(owners, limits, streams)


@dataclass(frozen=True)
class _Permit:
    account: str
    lane: int | None
    key: tuple[int, int] | None
    scheduled: bool
    borrowed_peer: int | None


class SlotCoordinator:
    """Small process-wide admission gate shared by all GUI account runtimes.

    SQLite still owns durable jobs and lease fencing. This gate only governs
    which running file can occupy a configured lane. Reconfiguration never
    resurrects a consumed permit or preempts a file already writing bytes.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._plan = SlotPlan({}, {}, {})
        self._active: dict[object, _Permit] = {}

    def configure(self, plan: SlotPlan):
        with self._lock:
            self._plan = plan

    def streams(self, account_id: str) -> tuple[dict, ...]:
        """Read-only snapshot; duplicate rows on other accounts are standby."""
        with self._lock:
            return tuple(row for row in self._plan.streams.get(account_id, ())
                         if self._plan.owners.get(row["peer_id"]) == account_id)

    def matches(self, account_id: str, item) -> bool:
        # Display name, scheduling interval, etc. do not invalidate the live
        # subscription. Only identity, epoch and the media selection matter.
        fields = ("peer_id", "target", "realtime_epoch", "types", "realtime_backfill", "realtime_since")
        return any(all(row.get(k) == item.get(k) for k in fields)
                   for row in self.streams(account_id))

    def owner(self, peer_id: int) -> str | None:
        with self._lock:
            return self._plan.owners.get(peer_id)

    def owns(self, account_id: str, peer_id: int) -> bool:
        return self.owner(peer_id) == account_id

    def reserved_peers(self) -> frozenset[int]:
        with self._lock:
            return frozenset(self._plan.owners)

    def account_limit(self, account_id: str, fallback: int) -> int:
        with self._lock:
            return self._plan.limits.get(account_id, fallback)

    def ordinary_capacity(self, account_id: str, workers: int) -> int:
        with self._lock:
            limit = min(workers, self._plan.limits.get(account_id, workers))
            return max(0, limit - sum(o == account_id for o in self._plan.owners.values()))

    def _realtime_clear(self, account_id: str, peer_id: int) -> bool:
        # Existing auto downloads from the previous mode/owner must drain.
        return self.owns(account_id, peer_id) and not any(
            (slot.scheduled and slot.key and slot.key[0] == peer_id)
            or (slot.account != account_id and peer_id in (slot.lane, slot.borrowed_peer))
            for slot in self._active.values()
        )

    def can_borrow_realtime(self, account_id: str, workers: int, peer_id: int) -> bool:
        """A running and still-owned dedicated lane may use ordinary capacity."""
        with self._lock:
            return (
                self.ordinary_capacity(account_id, workers) > 0
                and self._realtime_clear(account_id, peer_id)
                and any(slot.account == account_id and slot.lane == peer_id
                        for slot in self._active.values())
            )

    def file_available(self, file_key: tuple[int, int]) -> bool:
        with self._lock:
            return not any(slot.key == file_key for slot in self._active.values())

    def available(self, account_id: str, workers: int, peer_id: int | None = None, *,
                  file_key: tuple[int, int] | None = None, borrow: bool = False) -> bool:
        with self._lock:
            active = self._active.values()
            if file_key is not None and not self.file_available(file_key):
                return False
            limit = min(workers, self._plan.limits.get(account_id, workers))
            if sum(slot.account == account_id for slot in active) >= limit:
                return False
            if borrow:
                if (peer_id is None or file_key is None or file_key[0] != peer_id
                        or not self.can_borrow_realtime(account_id, workers, peer_id)):
                    return False
            if peer_id is None or borrow:
                return (sum(slot.account == account_id and slot.lane is None for slot in active)
                        < self.ordinary_capacity(account_id, workers))
            return self._realtime_clear(account_id, peer_id) and not any(
                slot.lane == peer_id for slot in active
            )

    def acquire(self, account_id: str, workers: int, peer_id: int | None = None, *,
                file_key: tuple[int, int] | None = None, scheduled_monitor: bool = False,
                borrow: bool = False, realtime_item: Mapping | None = None) -> object | None:
        with self._lock:
            if scheduled_monitor and file_key is not None and file_key[0] in self._plan.owners:
                return None
            if realtime_item is not None and (
                realtime_item.get("peer_id") != peer_id or not self.matches(account_id, realtime_item)
            ):
                return None
            if not self.available(account_id, workers, peer_id, file_key=file_key, borrow=borrow):
                return None
            token = object()
            self._active[token] = _Permit(
                account_id, None if borrow else peer_id, file_key, scheduled_monitor,
                peer_id if borrow else None,
            )
            return token

    def release(self, token: object | None) -> None:
        with self._lock:
            self._active.pop(token, None)
