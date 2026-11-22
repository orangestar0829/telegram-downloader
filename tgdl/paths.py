"""Persistent runtime paths for portable and installed distributions."""
from __future__ import annotations

import os
import json
import shutil
import sqlite3
import sys
import tempfile
from contextlib import closing
from pathlib import Path

APP_NAME = "TelegramDownloader"
_MONITOR_MIGRATION_ERRORS: dict[Path, str] = {}


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


def _atomic_import(source: Path, target: Path, *, sqlite: bool = False) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{target.name}.", suffix=".migrating", dir=target.parent, delete=False
    ) as pending_file:
        pending = Path(pending_file.name)
    try:
        if sqlite:
            # as_uri encodes literal characters such as '#' and '?'. Close
            # all handles before renaming or deleting on Windows.
            with closing(sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True)) as src:
                with closing(sqlite3.connect(pending)) as dest:
                    src.backup(dest)
                    if dest.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                        raise sqlite3.DatabaseError("Migrated SQLite database failed quick_check")
        else:
            shutil.copy2(source, pending)
        if not target.exists():
            pending.replace(target)
    finally:
        pending.unlink(missing_ok=True)


def migrate_default_session(directory: Path) -> Path:
    """Copy the legacy session into accounts/default, preserving its WAL."""
    legacy = directory / "tgdl_session.session"
    target = directory / "accounts" / "default" / "tgdl_session.session"
    if not target.exists() and legacy.is_file():
        _atomic_import(legacy, target, sqlite=True)
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


def monitor_migration_error(directory: Path) -> str | None:
    """Return a non-fatal import warning for the GUI to surface."""
    return _MONITOR_MIGRATION_ERRORS.get(directory)


def prepare_data_dir() -> Path:
    """Safely import legacy data once; never overwrite existing user data."""
    destination = data_dir()
    destination.mkdir(parents=True, exist_ok=True)
    sources = (executable_dir() / "_internal", executable_dir())
    if is_portable():
        sources += (user_data_dir(),)
    monitor_filename = "monitored_channels.json"
    history_filename = "tgdl_download_history.sqlite3"
    session_filename = "tgdl_session.session"
    default = destination / "accounts" / "default"
    _MONITOR_MIGRATION_ERRORS.pop(destination, None)
    for filename in (".env", session_filename, history_filename, monitor_filename):
        target = destination / filename
        if target.exists() or (filename != history_filename and (default / filename).exists()):
            continue
        try:
            for source in sources:
                # Prefer the active default-account version in each source,
                # not that source's stale root-level rollback copy.
                candidates = (source / filename,) if filename == history_filename else (
                    source / "accounts" / "default" / filename, source / filename,
                )
                candidate = next((path for path in candidates if path.is_file()), None)
                if candidate is not None:
                    _atomic_import(candidate, target, sqlite=filename in (session_filename, history_filename))
                    break
        except OSError as exc:
            if filename != monitor_filename:
                raise
            _MONITOR_MIGRATION_ERRORS[destination] = str(exc)
    migrate_default_session(destination)
    try:
        migrate_default_monitors(destination)
    except (ValueError, OSError) as exc:
        # Invalid legacy JSON should not prevent the entire app from opening.
        # The original file remains available for recovery.
        _MONITOR_MIGRATION_ERRORS[destination] = str(exc)
    migrate_default_env(destination)
    return destination
