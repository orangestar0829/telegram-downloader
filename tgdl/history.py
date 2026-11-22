"""Durable downloaded-message ledger, independent of local media retention."""

from __future__ import annotations

import sqlite3
import time
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
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS account_scan_state (
                    account_id TEXT NOT NULL, peer_id INTEGER NOT NULL,
                    completed_id INTEGER NOT NULL DEFAULT 0,
                    upper_id INTEGER NOT NULL DEFAULT 0,
                    cursor_id INTEGER NOT NULL DEFAULT 0,
                    direction TEXT NOT NULL DEFAULT 'newest',
                    media_types TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(account_id, peer_id))"""
            )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS download_claims (
                    peer_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                    account_id TEXT NOT NULL, expires_at REAL NOT NULL,
                    PRIMARY KEY(peer_id, message_id))"""
            )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS account_attempt_backoff (
                    account_id TEXT NOT NULL, peer_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL, retry_after REAL NOT NULL,
                    PRIMARY KEY(account_id,peer_id,message_id))"""
            )
            cols = {row[1] for row in self._connection.execute(
                "PRAGMA table_info(downloaded_media)"
            )}
            if "account_id" not in cols:
                self._connection.execute("ALTER TABLE downloaded_media ADD COLUMN account_id TEXT")

    def scan_state(self, peer_id: int, *, account_id: str = "default") -> tuple[int, int, int, str]:
        table = "scan_state" if account_id == "default" else "account_scan_state"
        clause = "peer_id=?" if account_id == "default" else "account_id=? AND peer_id=?"
        args = (peer_id,) if account_id == "default" else (account_id, peer_id)
        with self._lock:
            row = self._connection.execute(
                f"SELECT completed_id, upper_id, cursor_id, direction FROM {table} WHERE {clause}",
                args,
            ).fetchone()
            return (int(row[0]), int(row[1]), int(row[2]), str(row[3])) if row else (0, 0, 0, "newest")

    def scan_media_types(self, peer_id: int, *, account_id: str = "default") -> str:
        table = "scan_state" if account_id == "default" else "account_scan_state"
        clause = "peer_id=?" if account_id == "default" else "account_id=? AND peer_id=?"
        args = (peer_id,) if account_id == "default" else (account_id, peer_id)
        with self._lock:
            row = self._connection.execute(
                f"SELECT media_types FROM {table} WHERE {clause}", args,
            ).fetchone()
            return str(row[0]) if row else ""

    def save_scan_state(
        self, peer_id: int, completed: int, upper: int, cursor: int,
        direction: str, media_types: str = "", *, account_id: str = "default",
    ) -> None:
        if account_id == "default":
            table = "scan_state"
            columns = "peer_id,completed_id,upper_id,cursor_id,direction,media_types"
            vals = (peer_id, completed, upper, cursor, direction, media_types)
            conflict = "peer_id"
        else:
            table = "account_scan_state"
            columns = "account_id,peer_id,completed_id,upper_id,cursor_id,direction,media_types"
            vals = (account_id, peer_id, completed, upper, cursor, direction, media_types)
            conflict = "account_id,peer_id"
        placeholders = ",".join("?" for _ in vals)
        with self._lock, self._connection:
            self._connection.execute(
                f"""INSERT INTO {table}({columns}) VALUES({placeholders})
                    ON CONFLICT({conflict}) DO UPDATE SET
                    completed_id=excluded.completed_id,upper_id=excluded.upper_id,
                    cursor_id=excluded.cursor_id,direction=excluded.direction,
                    media_types=excluded.media_types""", vals,
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
        account_id: str = "default",
    ) -> int:
        """Enqueue media and advance the scan cursor in one SQLite transaction.

        If interrupted before commit the slice is replayed; if committed,
        pending downloads survive restarts even though the cursor advanced.
        """
        with self._lock, self._connection:
            queued = sum(self._enqueue(item) for item in items)
            if account_id == "default":
                table = "scan_state"
                columns = "peer_id,completed_id,upper_id,cursor_id,direction,media_types"
                vals = (peer_id, completed, upper, cursor, direction, media_types)
                conflict = "peer_id"
            else:
                table = "account_scan_state"
                columns = "account_id,peer_id,completed_id,upper_id,cursor_id,direction,media_types"
                vals = (account_id, peer_id, completed, upper, cursor, direction, media_types)
                conflict = "account_id,peer_id"
            placeholders = ",".join("?" for _ in vals)
            self._connection.execute(
                f"""INSERT INTO {table}({columns}) VALUES({placeholders})
                    ON CONFLICT({conflict}) DO UPDATE SET
                    completed_id=excluded.completed_id,upper_id=excluded.upper_id,
                    cursor_id=excluded.cursor_id,direction=excluded.direction,
                    media_types=excluded.media_types""", vals,
            )
            return queued

    def pending_for_target(self, target: str, limit: int = 16, *,
                           account_id: str | None = None,
                           kinds: set[str] | None = None) -> list[PendingMedia]:
        if account_id is None:
            with self._lock:
                return [PendingMedia(*row) for row in self._connection.execute(
                    """SELECT peer_id, message_id, target, title, kind, base
                       FROM pending_media WHERE target=? ORDER BY rowid LIMIT ?""",
                    (target, limit),
                )]
        now = time.time()
        if kinds is not None and not kinds:
            return []
        selected = ",".join(sorted(kinds)) if kinds is not None else ""
        with self._lock:
            return [PendingMedia(*row) for row in self._connection.execute(
                """SELECT p.peer_id,p.message_id,p.target,p.title,p.kind,p.base
                   FROM pending_media p WHERE p.target=?
                   AND (?='' OR p.kind='' OR instr(','||?||',', ','||p.kind||',')>0)
                   AND NOT EXISTS (SELECT 1 FROM downloaded_media d
                       WHERE d.peer_id=p.peer_id AND d.message_id=p.message_id)
                   AND NOT EXISTS (SELECT 1 FROM download_claims c
                       WHERE c.peer_id=p.peer_id AND c.message_id=p.message_id
                         AND c.expires_at>?)
                   AND NOT EXISTS (SELECT 1 FROM account_attempt_backoff b
                       WHERE b.account_id=? AND b.peer_id=p.peer_id
                         AND b.message_id=p.message_id AND b.retry_after>?)
                   ORDER BY p.rowid LIMIT ?""",
                (target, selected, selected, now, account_id, now, limit),
            )]

    def pending_for_peer(self, peer_id: int, *, account_id: str,
                         limit: int = 16, now: float | None = None,
                         kinds: set[str] | None = None) -> list[PendingMedia]:
        """Claimable files by canonical peer, constrained to this monitor's media."""
        now = time.time() if now is None else now
        selected = ",".join(sorted(kinds)) if kinds is not None else ""
        if kinds is not None and not kinds:
            return []
        with self._lock:
            return [PendingMedia(*row) for row in self._connection.execute(
                """SELECT p.peer_id,p.message_id,p.target,p.title,p.kind,p.base
                   FROM pending_media p WHERE p.peer_id=?
                   AND (?='' OR p.kind='' OR instr(','||?||',', ','||p.kind||',')>0)
                   AND NOT EXISTS (SELECT 1 FROM downloaded_media d
                       WHERE d.peer_id=p.peer_id AND d.message_id=p.message_id)
                   AND NOT EXISTS (SELECT 1 FROM download_claims c
                       WHERE c.peer_id=p.peer_id AND c.message_id=p.message_id
                         AND c.expires_at>?)
                   AND NOT EXISTS (SELECT 1 FROM account_attempt_backoff b
                       WHERE b.account_id=? AND b.peer_id=p.peer_id
                         AND b.message_id=p.message_id AND b.retry_after>?)
                   ORDER BY p.rowid LIMIT ?""",
                (peer_id, selected, selected, now, account_id, now, limit),
            )]

    def try_claim(self, account_id: str, peer_id: int, message_id: int, *,
                  ttl: float = 600, now: float | None = None) -> bool:
        """Atomically claim this task, even with concurrent SQLite connections."""
        if ttl <= 0:
            raise ValueError("Lease TTL must be positive")
        now = time.time() if now is None else now
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            result = self._connection.execute(
                """INSERT INTO download_claims(peer_id,message_id,account_id,expires_at)
                   SELECT ?,?,?,? WHERE
                     EXISTS (SELECT 1 FROM pending_media WHERE peer_id=? AND message_id=?)
                     AND NOT EXISTS (SELECT 1 FROM downloaded_media
                                     WHERE peer_id=? AND message_id=?)
                     AND NOT EXISTS (SELECT 1 FROM account_attempt_backoff
                                     WHERE account_id=? AND peer_id=? AND message_id=?
                                       AND retry_after>?)
                   ON CONFLICT(peer_id,message_id) DO UPDATE SET
                     account_id=excluded.account_id,expires_at=excluded.expires_at
                   WHERE download_claims.expires_at<=?""",
                (peer_id, message_id, account_id, now + ttl,
                 peer_id, message_id, peer_id, message_id,
                 account_id, peer_id, message_id, now, now),
            )
            return result.rowcount == 1

    def renew_claim(self, account_id: str, peer_id: int, message_id: int, *,
                    ttl: float = 600, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        with self._lock, self._connection:
            result = self._connection.execute(
                """UPDATE download_claims SET expires_at=?
                   WHERE peer_id=? AND message_id=? AND account_id=?""",
                (now + ttl, peer_id, message_id, account_id),
            )
            return result.rowcount == 1

    def release_claim(self, account_id: str, peer_id: int, message_id: int, *,
                      cooldown: float = 0, now: float | None = None) -> None:
        """Release failed/cancelled work; per-account backoff does not block peers."""
        now = time.time() if now is None else now
        with self._lock, self._connection:
            owned = self._connection.execute(
                "DELETE FROM download_claims WHERE peer_id=? AND message_id=? AND account_id=?",
                (peer_id, message_id, account_id),
            ).rowcount
            if owned and cooldown > 0:
                self._connection.execute(
                    """INSERT INTO account_attempt_backoff(account_id,peer_id,message_id,retry_after)
                       VALUES (?,?,?,?) ON CONFLICT(account_id,peer_id,message_id)
                       DO UPDATE SET retry_after=excluded.retry_after""",
                    (account_id, peer_id, message_id, now + cooldown),
                )

    def active_claims(self) -> list[tuple[int, int, str, float]]:
        with self._lock:
            return [(int(peer), int(mid), str(owner), float(until))
                    for peer, mid, owner, until in self._connection.execute(
                        "SELECT peer_id,message_id,account_id,expires_at FROM download_claims"
                    )]

    def attribute_saved(self, peer_id: int, message_id: int, account_id: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                """UPDATE downloaded_media SET account_id=?
                   WHERE peer_id=? AND message_id=? AND account_id IS NULL""",
                (account_id, peer_id, message_id),
            )

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
            self._connection.execute(
                "DELETE FROM download_claims WHERE peer_id=? AND message_id=?", (peer_id, message_id),
            )
            self._connection.execute(
                "DELETE FROM account_attempt_backoff WHERE peer_id=? AND message_id=?", (peer_id, message_id),
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()
