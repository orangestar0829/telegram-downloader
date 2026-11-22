"""Persist only the main GUI window's normal size and position."""

from __future__ import annotations

import json
import re
from pathlib import Path

from tgdl.paths import data_dir

DEFAULT_GEOMETRY = "1080x820"
MIN_WIDTH = 790
MIN_HEIGHT = 620
_GEOMETRY = re.compile(r"^(\d+)x(\d+)([+-]\d+)([+-]\d+)$")


def window_state_path() -> Path:
    return data_dir() / "window_state.json"


def safe_geometry(value: object, screen_width: int, screen_height: int) -> str | None:
    """Validate saved geometry; never restore an invisible/off-screen window."""
    if not isinstance(value, str):
        return None
    matched = _GEOMETRY.fullmatch(value)
    if matched is None:
        return None
    width, height, x, y = map(int, matched.groups())
    if width < MIN_WIDTH or height < MIN_HEIGHT:
        return None
    if width > screen_width or height > screen_height:
        return None
    if x < 0 or y < 0 or x + 100 > screen_width or y + 80 > screen_height:
        return None
    # Preserve the saved size while ensuring the close button/title bar can
    # still be reached after changing resolution or disconnecting a display.
    x = min(x, max(0, screen_width - width))
    y = min(y, max(0, screen_height - height))
    return f"{width}x{height}+{x}+{y}"


def load_window_geometry(screen_width: int, screen_height: int) -> str:
    try:
        payload = json.loads(window_state_path().read_text(encoding="utf-8"))
        restored = safe_geometry(payload.get("geometry"), screen_width, screen_height)
        if restored is not None:
            return restored
    except (OSError, ValueError, AttributeError, TypeError):
        pass
    return DEFAULT_GEOMETRY


def save_window_geometry(geometry: str, screen_width: int, screen_height: int) -> bool:
    checked = safe_geometry(geometry, screen_width, screen_height)
    if checked is None:
        return False
    path = window_state_path()
    temp = path.with_suffix(".tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp.write_text(
            json.dumps({"geometry": checked}, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        temp.replace(path)
        return True
    except OSError:
        return False
