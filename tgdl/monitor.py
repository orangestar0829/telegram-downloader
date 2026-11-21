"""Persistent per-channel scan settings (no Telegram credentials stored)."""
from __future__ import annotations

import json
from pathlib import Path

from tgdl.paths import prepare_data_dir


def monitor_path() -> Path:
    return prepare_data_dir() / "monitored_channels.json"


def next_due_channel(
    items: list[dict], next_at: dict[str, float], now: float, turn: int,
) -> dict | None:
    """Higher numeric priority starts first; slices rotate to avoid starvation."""
    ready = sorted(
        (
            item for item in items
            if item.get("enabled", True) and next_at.get(item["target"], 0) <= now
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
            })
    return result


def save_channels(items: list[dict], path: Path | None = None) -> None:
    path = path or monitor_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)
