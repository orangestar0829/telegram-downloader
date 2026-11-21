"""Persistent runtime paths for portable and installed distributions."""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

APP_NAME = "TelegramDownloader"


def executable_dir() -> Path:
    return Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent.parent


def user_data_dir() -> Path:
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        return Path(base) / APP_NAME
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
    return Path(base) / APP_NAME


def is_portable() -> bool:
    return (executable_dir() / "portable.flag").is_file()


def data_dir() -> Path:
    if is_portable():
        return executable_dir() / "data"
    if getattr(sys, "frozen", False):
        return user_data_dir()
    return executable_dir()


def prepare_data_dir() -> Path:
    """Safely import legacy data once; never overwrite existing user data."""
    destination = data_dir()
    destination.mkdir(parents=True, exist_ok=True)
    sources = (executable_dir() / "_internal", executable_dir())
    if is_portable():
        sources += (user_data_dir(),)
    for filename in (".env", "tgdl_session.session", "tgdl_download_history.sqlite3"):
        target = destination / filename
        if target.exists():
            continue
        for source in sources:
            candidate = source / filename
            if candidate.is_file():
                shutil.copy2(candidate, target)
                break
    return destination
