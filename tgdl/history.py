"""Durable downloaded-message ledger, independent of local media retention."""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from tgdl.paths import prepare_data_dir

def default_history_path() -> Path:
    """Use the portable data folder, or the installed user's writable data."""
    return prepare_data_dir() / "tgdl_download_history.sqlite3"


class DownloadHistory:
    """SQLite history keyed by Telegram peer ID and message ID.

    A row means the media was successfully saved at least once. Removing the
    file after cloud backup does not remove the row.
    """

    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path) if path is not None else default_history_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        with self._lock, self._connection:
            self._connection.execute("PRAGMA busy_timeout = 30000")
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS downloaded_media (
                    peer_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    original_path TEXT NOT NULL,
                    saved_at TEXT NOT NULL,
                    PRIMARY KEY (peer_id, message_id)
                )"""
            )

    def contains(self, peer_id: int, message_id: int) -> bool:
        with self._lock:
            return self._connection.execute(
                "SELECT 1 FROM downloaded_media WHERE peer_id=? AND message_id=?",
                (peer_id, message_id),
            ).fetchone() is not None

    def mark_saved(
        self, peer_id: int, message_id: int, kind: str, path: Path
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """INSERT OR IGNORE INTO downloaded_media
                   (peer_id, message_id, kind, original_path, saved_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (peer_id, message_id, kind, str(path),
                 datetime.now(timezone.utc).isoformat()),
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()
