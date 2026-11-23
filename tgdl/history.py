"""Durable downloaded-message ledger, independent of local media retention."""

from __future__ import annotations

import sqlite3
import time
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tgdl.paths import prepare_data_dir

_DEFAULT_BUSY_TIMEOUT_MS = 30_000
_REALTIME_BUSY_TIMEOUT_MS = 25
_UNREADABLE_RETRY_SECONDS = (3600, 6 * 3600, 24 * 3600)
_UNREADABLE_LIMIT = len(_UNREADABLE_RETRY_SECONDS) + 1

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


@dataclass(frozen=True)
class UnreadableRetry:
    misses: int
    delay_seconds: int | None


class DownloadHistory:
    """SQLite history keyed by Telegram peer ID and message ID.

    A row means the media was successfully saved at least once. Removing the
    file after cloud backup does not remove the row.
    """

    def __init__(self, path: Path | None = None, *,
                 busy_timeout_ms: int = _DEFAULT_BUSY_TIMEOUT_MS) -> None:
        self.path = Path(path) if path is not None else default_history_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._busy_timeout_ms = max(0, int(busy_timeout_ms))
        self._connection = sqlite3.connect(
            self.path, timeout=self._busy_timeout_ms / 1_000, check_same_thread=False,
        )
        try:
            self._init_schema()
        except BaseException:
            self._connection.close()
            raise

    def _init_schema(self) -> None:
        with self._lock, self._connection:
            # Serialize the PRAGMA/ALTER pairs across separate connections.
            self._connection.execute("BEGIN IMMEDIATE")
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
            pending_columns = {
                row[1] for row in self._connection.execute("PRAGMA table_info(pending_media)")
            }
            if "realtime" not in pending_columns:
                self._connection.execute(
                    "ALTER TABLE pending_media ADD COLUMN realtime INTEGER NOT NULL DEFAULT 0"
                )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS realtime_cursor (
                    account_id TEXT NOT NULL, peer_id INTEGER NOT NULL,
                    epoch TEXT NOT NULL, completed_id INTEGER NOT NULL,
                    PRIMARY KEY (account_id, peer_id)
                )"""
            )
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS legacy_basic_pending_media (
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
                    claim_token TEXT,
                    PRIMARY KEY(peer_id, message_id))"""
            )
            claim_columns = {
                row[1] for row in self._connection.execute("PRAGMA table_info(download_claims)")
            }
            if "claim_token" not in claim_columns:
                # Old live leases remain reserved until their original expiry.
                # The absence of a token makes them unusable by a new worker.
                self._connection.execute("ALTER TABLE download_claims ADD COLUMN claim_token TEXT")
            self._connection.execute(
                """CREATE TABLE IF NOT EXISTS account_attempt_backoff (
                    account_id TEXT NOT NULL, peer_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL, retry_after REAL NOT NULL,
                    PRIMARY KEY(account_id,peer_id,message_id))"""
            )
            self._connection.execute(
                f"""CREATE TABLE IF NOT EXISTS account_unreadable_media (
                    account_id TEXT NOT NULL,
                    peer_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    misses INTEGER NOT NULL CHECK (misses BETWEEN 1 AND {_UNREADABLE_LIMIT}),
                    retry_after REAL,
                    CHECK ((misses = {_UNREADABLE_LIMIT} AND retry_after IS NULL)
                        OR (misses < {_UNREADABLE_LIMIT} AND retry_after IS NOT NULL)),
                    PRIMARY KEY (account_id, peer_id, message_id)
                )"""
            )
            self._connection.execute(
                """CREATE INDEX IF NOT EXISTS unreadable_media_message
                   ON account_unreadable_media(peer_id, message_id)"""
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
        if cursor.rowcount or item.peer_id <= 0 or item.target == f"-{item.peer_id}":
            return cursor.rowcount

        # An untyped legacy basic-group row can occupy the positive key of
        # a Channel with the same raw ID. Archive specifically identifiable
        # rows before replacing them; do not guess about ambiguous old IDs.
        archived = self._connection.execute(
            """INSERT OR IGNORE INTO legacy_basic_pending_media
               SELECT peer_id,message_id,target,title,kind,base FROM pending_media
               WHERE peer_id=? AND message_id=? AND target=('-'||peer_id)
               AND NOT EXISTS (SELECT 1 FROM downloaded_media WHERE peer_id=? AND message_id=?)""",
            (item.peer_id, item.message_id, item.peer_id, item.message_id),
        )
        if archived.rowcount:
            self._connection.execute(
                """UPDATE pending_media SET target=?,title=?,kind=?,base=?
                   WHERE peer_id=? AND message_id=?""",
                (item.target, item.title, item.kind, item.base, item.peer_id, item.message_id),
            )
        return archived.rowcount

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

    @contextmanager
    def _realtime_transaction(self) -> Iterator[None]:
        # This connection is shared with the ordinary worker. Limit how long
        # new push work can block its loop on another SQLite writer; existing
        # ordinary operations retain their established timeout.
        with self._lock:
            timeout = min(self._busy_timeout_ms, _REALTIME_BUSY_TIMEOUT_MS)
            self._connection.execute(f"PRAGMA busy_timeout = {timeout}")
            try:
                with self._connection:
                    self._connection.execute("BEGIN IMMEDIATE")
                    yield
            finally:
                self._connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")

    def realtime_cursor(self, account_id: str, peer_id: int, epoch: str) -> int | None:
        with self._lock:
            row = self._connection.execute(
                """SELECT completed_id FROM realtime_cursor
                   WHERE account_id=? AND peer_id=? AND epoch=?""",
                (account_id, peer_id, epoch),
            ).fetchone()
            return int(row[0]) if row else None

    def previous_realtime_boundary(self, account_id: str, peer_id: int) -> int | None:
        """A conservative takeover watermark from another account's coverage."""
        with self._lock:
            value = self._connection.execute(
                """SELECT MIN(completed_id) FROM realtime_cursor
                   WHERE peer_id=? AND account_id<>?""",
                (peer_id, account_id),
            ).fetchone()[0]
            return int(value) if value is not None else None

    def start_realtime(
        self, account_id: str, peer_id: int, epoch: str, baseline: int, *,
        promote_before: int | None = None, kinds: frozenset[str] = frozenset(),
    ) -> int:
        """Record the tail/scheduled watermark and migrate existing cutover work."""
        with self._realtime_transaction():
            cursor = self.realtime_cursor(account_id, peer_id, epoch)
            if cursor is not None:
                return cursor
            self._connection.execute(
                """INSERT INTO realtime_cursor(account_id,peer_id,epoch,completed_id)
                   VALUES (?,?,?,?) ON CONFLICT(account_id,peer_id) DO UPDATE SET
                   epoch=excluded.epoch,completed_id=excluded.completed_id""",
                (account_id, peer_id, epoch, baseline),
            )
            if promote_before is not None and kinds:
                self._connection.execute(
                    """UPDATE pending_media SET realtime=1
                       WHERE peer_id=? AND message_id<=? AND realtime=0
                       AND NOT (peer_id>0 AND target=('-'||peer_id))
                       AND (kind='' OR instr(','||?||',', ','||kind||',')>0)
                       AND NOT EXISTS (SELECT 1 FROM downloaded_media d
                           WHERE d.peer_id=pending_media.peer_id
                             AND d.message_id=pending_media.message_id)""",
                    (peer_id, promote_before, ",".join(sorted(kinds))),
                )
            return baseline

    def commit_realtime(self, account_id: str, peer_id: int, epoch: str,
                        completed: int, items: list[PendingMedia], *,
                        observed: set[int] | None = None) -> tuple[int, int | None]:
        """Atomically enqueue and checkpoint a proven history range or push batch.

        Out-of-order pushes are durable, but cannot move the cursor past a
        missing predecessor. A different epoch cannot write anything, even
        if its asynchronous task is still unwinding.
        """
        with self._realtime_transaction():
            cursor = self.realtime_cursor(account_id, peer_id, epoch)
            if cursor is None or completed <= cursor:
                return 0, cursor
            changed = 0
            for item in items:
                if item.peer_id != peer_id or not 0 < item.message_id <= completed:
                    raise ValueError("Real-time queue item does not match its cursor")
                if item.message_id <= cursor:
                    continue
                self._enqueue(item)
                changed += self._connection.execute(
                    """UPDATE pending_media SET realtime=1,
                           kind=?
                       WHERE peer_id=? AND message_id=? AND realtime=0
                         AND NOT (peer_id>0 AND target=('-'||peer_id))""",
                    (item.kind, peer_id, item.message_id),
                ).rowcount
            advance = completed
            if observed is not None:
                advance = cursor
                while advance < completed and advance + 1 in observed:
                    advance += 1
            if advance > cursor:
                self._connection.execute(
                    """UPDATE realtime_cursor SET completed_id=?
                       WHERE account_id=? AND peer_id=? AND epoch=?""",
                    (advance, account_id, peer_id, epoch),
                )
            return changed, advance

    def pending_for_target(self, target: str, limit: int = 16, *,
                           account_id: str | None = None,
                           kinds: set[str] | None = None,
                           realtime: bool | None = None) -> list[PendingMedia]:
        order = "message_id" if realtime is True else "rowid"
        if account_id is None:
            with self._lock:
                return [PendingMedia(*row) for row in self._connection.execute(
                    f"""SELECT peer_id, message_id, target, title, kind, base
                       FROM pending_media WHERE target=? AND (? IS NULL OR realtime=?)
                       ORDER BY {order} LIMIT ?""",
                    (target, realtime, realtime, limit),
                )]
        # An old positive-ID row whose target explicitly uses the marked
        # basic-chat ID belongs to the old Chat namespace, not a Channel.
        # Leave it in the DB for rollback, but do not dispatch it.
        now = time.time()
        if kinds is not None and not kinds:
            return []
        selected = ",".join(sorted(kinds)) if kinds is not None else ""
        with self._lock:
            return [PendingMedia(*row) for row in self._connection.execute(
                f"""SELECT p.peer_id,p.message_id,p.target,p.title,p.kind,p.base
                   FROM pending_media p WHERE p.target=?
                   AND NOT (p.peer_id>0 AND p.target=('-'||p.peer_id))
                   AND (? IS NULL OR p.realtime=?)
                   AND (?='' OR p.kind='' OR instr(','||?||',', ','||p.kind||',')>0)
                   AND NOT EXISTS (SELECT 1 FROM downloaded_media d
                       WHERE d.peer_id=p.peer_id AND d.message_id=p.message_id)
                   AND NOT EXISTS (SELECT 1 FROM download_claims c
                       WHERE c.peer_id=p.peer_id AND c.message_id=p.message_id
                         AND c.expires_at>?)
                   AND NOT EXISTS (SELECT 1 FROM account_attempt_backoff b
                       WHERE b.account_id=? AND b.peer_id=p.peer_id
                         AND b.message_id=p.message_id AND b.retry_after>?)
                   AND NOT EXISTS (SELECT 1 FROM account_unreadable_media u
                       WHERE u.account_id=? AND u.peer_id=p.peer_id AND u.message_id=p.message_id
                         AND (u.retry_after IS NULL OR u.retry_after>?))
                   ORDER BY p.{order} LIMIT ?""",
                (target, realtime, realtime, selected, selected, now, account_id, now,
                 account_id, now, limit),
            )]

    def pending_for_peer(self, peer_id: int, *, account_id: str,
                         limit: int = 16, now: float | None = None,
                         kinds: set[str] | None = None,
                         realtime: bool | None = None) -> list[PendingMedia]:
        """Claimable files by canonical peer, constrained to this monitor's media."""
        order = "message_id" if realtime is True else "rowid"
        now = time.time() if now is None else now
        selected = ",".join(sorted(kinds)) if kinds is not None else ""
        if kinds is not None and not kinds:
            return []
        with self._lock:
            return [PendingMedia(*row) for row in self._connection.execute(
                f"""SELECT p.peer_id,p.message_id,p.target,p.title,p.kind,p.base
                   FROM pending_media p WHERE p.peer_id=?
                   AND NOT (p.peer_id>0 AND p.target=('-'||p.peer_id))
                   AND (? IS NULL OR p.realtime=?)
                   AND (?='' OR p.kind='' OR instr(','||?||',', ','||p.kind||',')>0)
                   AND NOT EXISTS (SELECT 1 FROM downloaded_media d
                       WHERE d.peer_id=p.peer_id AND d.message_id=p.message_id)
                   AND NOT EXISTS (SELECT 1 FROM download_claims c
                       WHERE c.peer_id=p.peer_id AND c.message_id=p.message_id
                         AND c.expires_at>?)
                   AND NOT EXISTS (SELECT 1 FROM account_attempt_backoff b
                       WHERE b.account_id=? AND b.peer_id=p.peer_id
                         AND b.message_id=p.message_id AND b.retry_after>?)
                   AND NOT EXISTS (SELECT 1 FROM account_unreadable_media u
                       WHERE u.account_id=? AND u.peer_id=p.peer_id AND u.message_id=p.message_id
                         AND (u.retry_after IS NULL OR u.retry_after>?))
                   ORDER BY p.{order} LIMIT ?""",
                (peer_id, realtime, realtime, selected, selected, now, account_id, now,
                 account_id, now, limit),
            )]

    def try_claim(self, account_id: str, peer_id: int, message_id: int, *,
                  ttl: float = 600, now: float | None = None) -> str | None:
        """Return a unique token for this acquisition, not merely for this account."""
        if not 0 < ttl < float("inf"):
            raise ValueError("Lease TTL must be finite and positive")
        token = uuid.uuid4().hex
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            now = time.time() if now is None else now
            result = self._connection.execute(
                """INSERT INTO download_claims(peer_id,message_id,account_id,expires_at,claim_token)
                   SELECT ?,?,?,?,? WHERE
                     EXISTS (SELECT 1 FROM pending_media WHERE peer_id=? AND message_id=?
                             AND NOT (peer_id>0 AND target=('-'||peer_id)))
                     AND NOT EXISTS (SELECT 1 FROM downloaded_media
                                     WHERE peer_id=? AND message_id=?)
                     AND NOT EXISTS (SELECT 1 FROM account_attempt_backoff
                                     WHERE account_id=? AND peer_id=? AND message_id=?
                                       AND retry_after>?)
                     AND NOT EXISTS (SELECT 1 FROM account_unreadable_media
                                     WHERE account_id=? AND peer_id=? AND message_id=?
                                       AND (retry_after IS NULL OR retry_after>?))
                   ON CONFLICT(peer_id,message_id) DO UPDATE SET
                     account_id=excluded.account_id,expires_at=excluded.expires_at,
                     claim_token=excluded.claim_token
                   WHERE download_claims.expires_at<=?""",
                (peer_id, message_id, account_id, now + ttl, token,
                 peer_id, message_id, peer_id, message_id,
                 account_id, peer_id, message_id, now,
                 account_id, peer_id, message_id, now, now),
            )
            return token if result.rowcount == 1 else None

    def renew_claim(self, account_id: str, peer_id: int, message_id: int, *,
                    token: str, ttl: float = 600, now: float | None = None) -> bool:
        if not 0 < ttl < float("inf"):
            raise ValueError("Lease TTL must be finite and positive")
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            now = time.time() if now is None else now
            result = self._connection.execute(
                """UPDATE download_claims SET expires_at=?
                   WHERE peer_id=? AND message_id=? AND account_id=?
                     AND claim_token=? AND expires_at>?""",
                (now + ttl, peer_id, message_id, account_id, token, now),
            )
            return result.rowcount == 1

    def _release_claim_locked(self, account_id: str, peer_id: int, message_id: int,
                              token: str, now: float) -> bool:
        return self._connection.execute(
            """DELETE FROM download_claims
               WHERE peer_id=? AND message_id=? AND account_id=?
                 AND claim_token=? AND expires_at>?""",
            (peer_id, message_id, account_id, token, now),
        ).rowcount == 1

    def release_claim(self, account_id: str, peer_id: int, message_id: int, *,
                      token: str, cooldown: float = 0, now: float | None = None) -> bool:
        """Only a live attempt can clear its lease and set its account's backoff."""
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            now = time.time() if now is None else now
            owned = self._release_claim_locked(account_id, peer_id, message_id, token, now)
            if owned and cooldown > 0:
                self._connection.execute(
                    """INSERT INTO account_attempt_backoff(account_id,peer_id,message_id,retry_after)
                       VALUES (?,?,?,?) ON CONFLICT(account_id,peer_id,message_id)
                       DO UPDATE SET retry_after=excluded.retry_after""",
                    (account_id, peer_id, message_id, now + cooldown),
                )
            return owned

    def finish_unreadable_claim(self, account_id: str, peer_id: int, message_id: int, *,
                                token: str, now: float | None = None) -> UnreadableRetry | None:
        """Count a definitive metadata miss and release only this live attempt.

        An omitted retry time on the fourth miss means this account no longer
        claims the shared job. No queue row is deleted.
        """
        key = (account_id, peer_id, message_id)
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            now = time.time() if now is None else now
            if not self._release_claim_locked(account_id, peer_id, message_id, token, now):
                return None
            previous = self._connection.execute(
                """SELECT misses FROM account_unreadable_media
                   WHERE account_id=? AND peer_id=? AND message_id=?""",
                key,
            ).fetchone()
            misses = min(_UNREADABLE_LIMIT, (previous[0] if previous else 0) + 1)
            delay = _UNREADABLE_RETRY_SECONDS[misses - 1] if misses < _UNREADABLE_LIMIT else None
            self._connection.execute(
                """INSERT INTO account_unreadable_media(account_id,peer_id,message_id,misses,retry_after)
                   VALUES (?,?,?,?,?) ON CONFLICT(account_id,peer_id,message_id)
                   DO UPDATE SET misses=excluded.misses,retry_after=excluded.retry_after""",
                (*key, misses, now + delay if delay is not None else None),
            )
            return UnreadableRetry(misses, delay)

    def record_readable_message(self, account_id: str, peer_id: int, message_id: int, *,
                                token: str, now: float | None = None) -> bool:
        """Reset a prior unreadable streak on a confirmed, dated Telegram read."""
        key = (account_id, peer_id, message_id)
        with self._lock:
            # No write lock needed for ordinary reads without a miss.
            if self._connection.execute(
                """SELECT 1 FROM account_unreadable_media
                   WHERE account_id=? AND peer_id=? AND message_id=?""",
                key,
            ).fetchone() is None:
                return False
            with self._connection:
                self._connection.execute("BEGIN IMMEDIATE")
                now = time.time() if now is None else now
                if not self._owns_claim_locked(account_id, peer_id, message_id, token, now):
                    return False
                return self._connection.execute(
                    """DELETE FROM account_unreadable_media
                       WHERE account_id=? AND peer_id=? AND message_id=?""",
                    key,
                ).rowcount == 1

    def _owns_claim_locked(self, account_id: str, peer_id: int, message_id: int,
                           token: str, now: float) -> bool:
        return self._connection.execute(
            """SELECT 1 FROM download_claims
               WHERE peer_id=? AND message_id=? AND account_id=?
                 AND claim_token=? AND expires_at>?""",
            (peer_id, message_id, account_id, token, now),
        ).fetchone() is not None

    def owns_claim(self, account_id: str, peer_id: int, message_id: int, *,
                   token: str, now: float | None = None) -> bool:
        """Best-effort preflight; final publication always rechecks under a write lock."""
        with self._lock:
            return self._owns_claim_locked(
                account_id, peer_id, message_id, token, time.time() if now is None else now,
            )

    def _has_live_claim_locked(self, peer_id: int, message_id: int, now: float) -> bool:
        return self._connection.execute(
            """SELECT 1 FROM download_claims
               WHERE peer_id=? AND message_id=? AND expires_at>?""",
            (peer_id, message_id, now),
        ).fetchone() is not None

    def has_live_claim(self, peer_id: int, message_id: int) -> bool:
        with self._lock:
            return self._has_live_claim_locked(peer_id, message_id, time.time())

    def active_claims(self) -> list[tuple[int, int, str, float]]:
        with self._lock:
            return [(int(peer), int(mid), str(owner), float(until))
                    for peer, mid, owner, until in self._connection.execute(
                        "SELECT peer_id,message_id,account_id,expires_at FROM download_claims"
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

    def remove_pending(self, peer_id: int, message_id: int, *,
                       now: float | None = None) -> None:
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            now = time.time() if now is None else now
            if not self._has_live_claim_locked(peer_id, message_id, now):
                self._connection.execute(
                    "DELETE FROM pending_media WHERE peer_id=? AND message_id=?",
                    (peer_id, message_id),
                )

    def mark_failure(self, peer_id: int, message_id: int, *,
                     now: float | None = None) -> None:
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            now = time.time() if now is None else now
            if self._has_live_claim_locked(peer_id, message_id, now) or self.contains(peer_id, message_id):
                return
            self._connection.execute(
                "INSERT OR IGNORE INTO scan_failures(peer_id, message_id) VALUES (?, ?)",
                (peer_id, message_id),
            )
            self._connection.execute(
                "DELETE FROM pending_media WHERE peer_id=? AND message_id=?",
                (peer_id, message_id),
            )

    def clear_failure(self, peer_id: int, message_id: int, *,
                      now: float | None = None) -> None:
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            now = time.time() if now is None else now
            if not self._has_live_claim_locked(peer_id, message_id, now):
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

    def _record_saved_locked(
        self, peer_id: int, message_id: int, kind: str, path: Path,
        account_id: str | None,
    ) -> None:
        self._connection.execute(
            """INSERT OR IGNORE INTO downloaded_media
               (peer_id, message_id, kind, original_path, saved_at, account_id)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (peer_id, message_id, kind, str(path),
             datetime.now(timezone.utc).isoformat(), account_id),
        )
        for table in ("scan_failures", "pending_media", "account_attempt_backoff", "account_unreadable_media"):
            self._connection.execute(
                f"DELETE FROM {table} WHERE peer_id=? AND message_id=?", (peer_id, message_id),
            )

    def mark_saved(self, peer_id: int, message_id: int, kind: str, path: Path, *,
                   now: float | None = None) -> bool:
        """Record manual success only when there is no live monitor lease."""
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            now = time.time() if now is None else now
            if self._has_live_claim_locked(peer_id, message_id, now):
                return False
            self._record_saved_locked(peer_id, message_id, kind, path, None)
            self._connection.execute(
                "DELETE FROM download_claims WHERE peer_id=? AND message_id=? AND expires_at<=?",
                (peer_id, message_id, now),
            )
            return True

    def publish_claimed_media(
        self, peer_id: int, message_id: int, account_id: str, claim_token: str,
        kind: str, publish: Callable[[], tuple[Path, bool] | None], *,
        now: float | None = None,
    ) -> tuple[Path, bool] | None:
        """Fence final publication and the attributed DB commit to one attempt.

        The callback does only quick, synchronous filesystem publication or
        validation, after staging/transfer has already finished. If it raises
        or declines (None), the claim and job are left unchanged. If SQLite
        subsequently fails, an already-published complete file can be
        recovered on retry; filesystem publication is not a SQLite operation.
        """
        with self._lock, self._connection:
            self._connection.execute("BEGIN IMMEDIATE")
            now = time.time() if now is None else now
            if not self._owns_claim_locked(account_id, peer_id, message_id, claim_token, now):
                return None
            result = publish()
            if result is None:
                return None
            path, _created = result
            self._record_saved_locked(peer_id, message_id, kind, path, account_id)
            self._connection.execute(
                """DELETE FROM download_claims WHERE peer_id=? AND message_id=?
                   AND account_id=? AND claim_token=?""",
                (peer_id, message_id, account_id, claim_token),
            )
            return result

    def mark_claim_saved(self, account_id: str, peer_id: int, message_id: int,
                         kind: str, path: Path, *, token: str, now: float | None = None) -> bool:
        """Commit specified completed media (no alternate pathname to select)."""
        return self.publish_claimed_media(
            peer_id, message_id, account_id, token, kind, lambda: (path, False), now=now,
        ) is not None

    def close(self) -> None:
        with self._lock:
            self._connection.close()
