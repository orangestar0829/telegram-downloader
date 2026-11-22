"""Persistent runtime paths for portable and installed distributions."""
from __future__ import annotations

import os
import json
import shutil
import sqlite3
from contextlib import closing
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


def migrate_default_session(directory: Path) -> Path:
    """Copy legacy session into accounts/default without losing auth state.

    This happens on startup *before* creating the Telethon client. Keep the
    legacy session untouched as a rollback copy. SQLite backup incorporates
    any valid WAL records and prevents a partially copied target.
    """
    legacy = directory / "tgdl_session.session"
    target = directory / "accounts" / "default" / "tgdl_session.session"
    if target.exists() or not legacy.is_file():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    pending = target.with_suffix(".session.migrating")
    try:
        if pending.exists():
            pending.unlink()
        # SQLite context manager commits but does not close file handles;
        # close explicitly before rename or cleanup on Windows.
        with closing(sqlite3.connect(f"file:{legacy.as_posix()}?mode=ro", uri=True)) as src:
            with closing(sqlite3.connect(pending)) as dest:
                src.backup(dest)
                if dest.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                    raise sqlite3.DatabaseError("Migrated Session failed quick_check")
        pending.replace(target)
    finally:
        if pending.exists():
            pending.unlink()
    return target


def migrate_default_env(directory: Path) -> Path:
    """Copy the legacy .env into accounts/default once, retaining rollback copy.

    The copy is never overwritten on subsequent launches. Use a temporary
    sibling and atomic replace to prevent partially written live settings.
    """
    legacy = directory / ".env"
    target = directory / "accounts" / "default" / ".env"
    if target.exists() or not legacy.is_file():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(".env.migrating")
    try:
        shutil.copy2(legacy, temporary)
        # Reject undecodable or unreadable input rather than publishing a
        # malformed configuration; empty .env files remain valid.
        temporary.read_text(encoding="utf-8")
        temporary.replace(target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return target


def migrate_default_monitors(directory: Path) -> Path:
    """Copy the original account's monitor JSON into accounts/default once.

    Never overwrite an already migrated file; leave the root copy untouched
    for a downgrade. Validate before publishing a new config so an interrupted
    or malformed migration cannot replace a working monitor list.
    """
    legacy = directory / "monitored_channels.json"
    target = directory / "accounts" / "default" / "monitored_channels.json"
    if target.exists() or not legacy.is_file():
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".migrating")
    try:
        shutil.copy2(legacy, temporary)
        data = json.loads(temporary.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError("Invalid legacy channel monitor configuration")
        temporary.replace(target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return target


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
    migrate_default_session(destination)
    migrate_default_monitors(destination)
    migrate_default_env(destination)
    return destination
