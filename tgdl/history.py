"""Durable downloaded-message ledger, independent of local media retention."""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tgdl.paths import prepare_data_dir

def default_history_path() -> Path:
    """Use the portable data folder, or the installed user's writable data."""
    return prepare_data_dir() / "tgdl_download_history.sqlite3"


@dataclass(frozen=True)
class PendingMedia:
    peer_id: int
    message_id: int
    target: str
    title: str
    kind: str
    base: str


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
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS scan_state (
                    peer_id INTEGER PRIMARY KEY,
                    completed_id INTEGER NOT NULL DEFAULT 0,
                    upper_id INTEGER NOT NULL DEFAULT 0,
                    cursor_id INTEGER NOT NULL DEFAULT 0,
                    direction TEXT NOT NULL DEFAULT 'newest',
                    media_types TEXT NOT NULL DEFAULT ''
                )"""
            )
            columns = {row[1] for row in self._connection.execute("PRAGMA table_info(scan_state)")}
            if "media_types" not in columns:
                self._connection.execute(
                    "ALTER TABLE scan_state ADD COLUMN media_types TEXT NOT NULL DEFAULT ''"
                )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS scan_failures (
                    peer_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    PRIMARY KEY(peer_id, message_id)
                )"""
            )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS pending_media (
                    peer_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    target TEXT NOT NULL,
                    title TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    base TEXT NOT NULL,
                    PRIMARY KEY (peer_id, message_id)
                )"""
            )
            self._connection.execute(
                "CREATE INDEX IF NOT EXISTS pending_media_target ON pending_media (target)"
            )

    def scan_state(self, peer_id: int) -> tuple[int, int, int, str]:
        with self._lock:
            row = self._connection.execute(
                "SELECT completed_id, upper_id, cursor_id, direction FROM scan_state WHERE peer_id=?",
                (peer_id,),
            ).fetchone()
            return (int(row[0]), int(row[1]), int(row[2]), str(row[3])) if row else (0, 0, 0, "newest")

    def scan_media_types(self, peer_id: int) -> str:
        with self._lock:
            row = self._connection.execute(
                "SELECT media_types FROM scan_state WHERE peer_id=?", (peer_id,),
            ).fetchone()
            return str(row[0]) if row else ""

    def save_scan_state(
        self, peer_id: int, completed: int, upper: int, cursor: int,
        direction: str, media_types: str = "",
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """INSERT INTO scan_state(
                    peer_id, completed_id, upper_id, cursor_id, direction, media_types
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(peer_id) DO UPDATE SET
                    completed_id=excluded.completed_id, upper_id=excluded.upper_id,
                    cursor_id=excluded.cursor_id, direction=excluded.direction,
                    media_types=excluded.media_types""",
                (peer_id, completed, upper, cursor, direction, media_types),
            )

    def _enqueue(self, item: PendingMedia) -> int:
        cursor = self._connection.execute(
            """INSERT OR IGNORE INTO pending_media
               (peer_id, message_id, target, title, kind, base)
               SELECT ?, ?, ?, ?, ?, ?
               WHERE NOT EXISTS (
                   SELECT 1 FROM downloaded_media
                   WHERE peer_id=? AND message_id=?
               )""",
            (item.peer_id, item.message_id, item.target, item.title,
             item.kind, item.base, item.peer_id, item.message_id),
        )
        return cursor.rowcount

    def enqueue_media(self, items: list[PendingMedia]) -> int:
        with self._lock, self._connection:
            return sum(self._enqueue(item) for item in items)

    def commit_scan_batch(
        self, peer_id: int, items: list[PendingMedia], *,
        completed: int, upper: int, cursor: int, direction: str, media_types: str,
    ) -> int:
        """Enqueue media and advance the scan cursor in one SQLite transaction.

        If interrupted before commit the slice is replayed; if committed,
        pending downloads survive restarts even though the cursor advanced.
        """
        with self._lock, self._connection:
            queued = sum(self._enqueue(item) for item in items)
            self._connection.execute(
                """INSERT INTO scan_state
                   (peer_id, completed_id, upper_id, cursor_id, direction, media_types)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(peer_id) DO UPDATE SET
                     completed_id=excluded.completed_id,
                     upper_id=excluded.upper_id,
                     cursor_id=excluded.cursor_id,
                     direction=excluded.direction,
                     media_types=excluded.media_types""",
                (peer_id, completed, upper, cursor, direction, media_types),
            )
            return queued

    def pending_for_target(self, target: str, limit: int = 16) -> list[PendingMedia]:
        with self._lock:
            return [PendingMedia(*row) for row in self._connection.execute(
                """SELECT peer_id, message_id, target, title, kind, base
                   FROM pending_media WHERE target=? ORDER BY rowid LIMIT ?""",
                (target, limit),
            )]

    def pending_count(self, target: str | None = None) -> int:
        with self._lock:
            if target is None:
                row = self._connection.execute("SELECT COUNT(*) FROM pending_media").fetchone()
            else:
                row = self._connection.execute(
                    "SELECT COUNT(*) FROM pending_media WHERE target=?", (target,),
                ).fetchone()
            return int(row[0])

    def remove_pending(self, peer_id: int, message_id: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "DELETE FROM pending_media WHERE peer_id=? AND message_id=?",
                (peer_id, message_id),
            )

    def mark_failure(self, peer_id: int, message_id: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO scan_failures(peer_id, message_id) VALUES (?, ?)",
                (peer_id, message_id),
            )
            self._connection.execute(
                "DELETE FROM pending_media WHERE peer_id=? AND message_id=?",
                (peer_id, message_id),
            )

    def clear_failure(self, peer_id: int, message_id: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "DELETE FROM scan_failures WHERE peer_id=? AND message_id=?", (peer_id, message_id),
            )

    def failures(self, peer_id: int, limit: int = 100) -> list[int]:
        with self._lock:
            return [int(row[0]) for row in self._connection.execute(
                "SELECT message_id FROM scan_failures WHERE peer_id=? ORDER BY message_id LIMIT ?",
                (peer_id, limit),
            )]

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
            self._connection.execute(
                "DELETE FROM scan_failures WHERE peer_id=? AND message_id=?", (peer_id, message_id),
            )
            self._connection.execute(
                "DELETE FROM pending_media WHERE peer_id=? AND message_id=?", (peer_id, message_id),
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()
