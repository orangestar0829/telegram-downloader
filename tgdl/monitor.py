"""Persistent per-channel scan settings (no Telegram credentials stored)."""
from __future__ import annotations

import json
from pathlib import Path

from tgdl.paths import prepare_data_dir


def monitor_path() -> Path:
    return prepare_data_dir() / "monitored_channels.json"


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
        minutes = int(item.get("minutes", 30))
        if target and 1 <= minutes <= 1440:
            result.append({
                "target": target,
                "minutes": minutes,
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
