"""Persistent per-channel scan settings (no Telegram credentials stored)."""
from __future__ import annotations

import json
import time
from pathlib import Path
from uuid import uuid4

from tgdl.paths import prepare_data_dir


SCHEDULED = "scheduled"
REALTIME = "realtime"
MONITOR_MODE_LABELS = {SCHEDULED: "定时扫描", REALTIME: "实时监听"}


class MonitorDispatchSummary:
    """Coalesce adjacent successful pause/resume events into readable lines.

    Only actual suspended checkpoints should be recorded as pause events;
    requests that have not yet reached a checkpoint are intentionally silent.
    The caller periodically drains the buffer and forces a drain on shutdown.
    """

    def __init__(self, *, quiet_seconds: float = 0.9, max_seconds: float = 2.5):
        self.quiet_seconds = quiet_seconds
        self.max_seconds = max_seconds
        self._events: dict[tuple[str, str], int] = {}
        self._first_at = 0.0
        self._last_at = 0.0

    def add(self, kind: str, channel_name: str, *, now: float | None = None) -> None:
        if kind not in ("paused", "resumed"):
            raise ValueError("Unknown monitor dispatch event")
        now = time.monotonic() if now is None else now
        if not self._events:
            self._first_at = now
        self._last_at = now
        key = (kind, channel_name)
        self._events[key] = self._events.get(key, 0) + 1

    def drain(self, *, now: float | None = None, force: bool = False) -> list[str]:
        if not self._events:
            return []
        now = time.monotonic() if now is None else now
        if not force and (now - self._last_at < self.quiet_seconds
                          and now - self._first_at < self.max_seconds):
            return []
        lines = []
        for (kind, name), count in self._events.items():
            if kind == "paused":
                lines.append(
                    f"[调度] 高优先级任务优先｜已暂停 {name} 的{count}个下载任务"
                )
            else:
                lines.append(
                    f"[调度] {name}：恢复{count}个下载任务｜从原进度继续"
                )
        self._events.clear()
        return lines


def monitor_path() -> Path:
    return prepare_data_dir() / "accounts" / "default" / "monitored_channels.json"


def next_due_channel(
    items: list[dict], next_at: dict[str, float], now: float, turn: int,
) -> dict | None:
    """Higher numeric priority starts first; slices rotate to avoid starvation."""
    ready = sorted(
        (
            item for item in items
            if item.get("enabled", True) and item.get("mode") != REALTIME
            and next_at.get(item["target"], 0) <= now
        ),
        key=lambda item: -int(item.get("priority", 0)),
    )
    return ready[turn % len(ready)] if ready else None


def choose_monitor_download(
    candidates: list[tuple[dict, object]],
    active_counts: dict[str, int],
    occupied_seconds: dict[str, float],
) -> tuple[dict, object] | None:
    """Fairly assign the next *monitor* download slot.

    First prefer a channel with queued work and no active transfer, then
    consider borrowed capacity. Within the same active-slot count, balance
    completed/in-progress slot occupancy time instead of file counts.
    Numeric priority breaks ties and supplies a bounded 1..4 weight; no
    channel can starve simply because its priority is lower.
    """
    if not candidates:
        return None
    priorities = sorted({int(item.get("priority", 0)) for item, _ in candidates})
    weights = {priority: 1 + min(rank, 3) for rank, priority in enumerate(priorities)}

    def key(pair: tuple[dict, object]):
        item, _job = pair
        target = item["target"]
        priority = int(item.get("priority", 0))
        slots = active_counts.get(target, 0)
        return (
            slots,
            occupied_seconds.get(target, 0.0) / weights[priority],
            -priority,
        )

    return min(candidates, key=key)



def configure_monitor_mode(row: dict, old: dict | None, mode: str, dialogs) -> None:
    """Prepare a mode edit without mutating the existing saved row."""
    row["mode"] = mode
    if mode != REALTIME:
        row.pop("realtime_epoch", None)
        row.pop("realtime_backfill", None)
        row.pop("realtime_since", None)
        return
    from tgdl.account_view import joined_channel_peer

    previous = old or {}
    peer = previous.get("peer_id")
    if not isinstance(peer, int) or isinstance(peer, bool) or peer <= 0:
        peer = joined_channel_peer(dialogs, row["target"])
    row["peer_id"] = peer
    previous_epoch = previous.get("realtime_epoch") if previous.get("mode") == REALTIME else None
    row["realtime_epoch"] = previous_epoch or uuid4().hex
    if not previous_epoch:
        row["realtime_since"] = int(time.time())
    elif "realtime_since" in previous:
        row["realtime_since"] = previous["realtime_since"]
    row["realtime_backfill"] = (
        bool(previous.get("realtime_backfill", False)) if previous_epoch
        else old is not None and previous.get("mode") != REALTIME
    )


def load_channels(path: Path | None = None) -> list[dict]:
    path = path or monitor_path()
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Invalid channel monitor configuration")
    result = []
    for item in data:
        if not isinstance(item, dict):
            continue
        target = str(item.get("target", "")).strip()
        try:
            minutes = int(item.get("minutes", 30))
        except (TypeError, ValueError):
            continue
        try:
            priority = int(item.get("priority", 0))
        except (TypeError, ValueError):
            priority = 0
        try:
            batch_size = int(item.get("batch_size", 100))
        except (TypeError, ValueError):
            batch_size = 100
        if not 1 <= batch_size <= 100_000:
            batch_size = 100
        if target and 1 <= minutes <= 1440:
            result.append({
                "target": target,
                "minutes": minutes,
                "priority": priority,
                "batch_size": batch_size,
                "types": str(item.get("types", "photo,video")),
                "enabled": bool(item.get("enabled", True)),
                "direction": item.get("direction", "newest") if item.get("direction") in ("newest", "oldest") else "newest",
                "name": str(item.get("name", "")).strip(),
                "mode": REALTIME if item.get("mode") == REALTIME else SCHEDULED,
            })
            try:
                raw_peer_id = item.get("peer_id")
                peer_id = 0 if isinstance(raw_peer_id, bool) else int(raw_peer_id or 0)
            except (TypeError, ValueError):
                peer_id = 0
            if peer_id:
                result[-1]["peer_id"] = peer_id
            if result[-1]["mode"] == REALTIME:
                epoch = item.get("realtime_epoch")
                if isinstance(epoch, str) and epoch.strip():
                    result[-1]["realtime_epoch"] = epoch.strip()
                result[-1]["realtime_backfill"] = item.get("realtime_backfill") is True
                since = item.get("realtime_since")
                if type(since) is int and 0 < since < 2**32:
                    result[-1]["realtime_since"] = since
    return result


def save_channels(items: list[dict], path: Path | None = None) -> None:
    path = path or monitor_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)
