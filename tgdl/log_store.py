"""Bounded asynchronous daily diagnostic logging independent from Tk text widgets."""
from __future__ import annotations

import os
import queue
import re
import threading
from datetime import date, datetime, timedelta
from pathlib import Path

_SECRET = re.compile(
    r"""(?i)\b(telegram_api_hash|socks5_password|proxy_password|api_hash|password|"""
    r"""telegram_phone|phone_code_hash|login_token|auth_key|verification_code)"""
    r"""['"]?[ \t]*[:=][ \t]*[^\r\n]*"""
)
_LINK = re.compile(
    r"""(?i)\b(?:(?:https?|tg|socks5h?)://|magnet:\?|"""
    r"""(?:t\.me|telegram\.me|telegram\.dog)/(?:\+|joinchat/))[^\s<>'"]+"""
)


def redact(text: str) -> str:
    return _LINK.sub("[REDACTED LINK]", _SECRET.sub(r"\1=[REDACTED]", text))


class DailyLogStore:
    def __init__(self, directory: Path, *, retain_days: int = 7) -> None:
        if retain_days < 1:
            raise ValueError("retain_days must be positive")
        self.directory = Path(directory)
        self.retain_days = retain_days
        self.queue: queue.Queue[str] = queue.Queue(maxsize=20000)
        self.dropped = 0
        self.failed = threading.Event()
        self._closed = threading.Event()
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._worker, daemon=True, name="tgdl-log-writer")
        self._thread.start()

    def write(self, message: str) -> None:
        if not message:
            return
        with self._lock:
            if self._closed.is_set() or self.failed.is_set():
                return
            try:
                self.queue.put_nowait(redact(message))
            except queue.Full:
                self.dropped += 1

    def _cleanup(self, today: date) -> None:
        earliest = today - timedelta(days=self.retain_days - 1)
        for path in self.directory.glob("runtime-????-??-??.log"):
            try:
                day = date.fromisoformat(path.stem.removeprefix("runtime-"))
                if day < earliest:
                    path.unlink()
            except (ValueError, OSError):
                pass

    def _worker(self) -> None:
        current_day: date | None = None
        out = None
        try:
            self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            while True:
                try:
                    item = self.queue.get(timeout=0.1)
                except queue.Empty:
                    if self._closed.is_set() and self.queue.empty():
                        break
                    continue
                today = datetime.now().date()
                if today != current_day:
                    if out is not None:
                        out.close()
                    self._cleanup(today)
                    out = open(
                        self.directory / f"runtime-{today:%Y-%m-%d}.log",
                        "a", encoding="utf-8", errors="replace",
                        opener=lambda path, flags: os.open(path, flags, 0o600),
                    )
                    current_day = today
                out.write(item)
                out.flush()
        except OSError:
            self.failed.set()
        finally:
            if out is not None:
                try:
                    out.close()
                except OSError:
                    self.failed.set()

    def close(self) -> None:
        with self._lock:
            if self._closed.is_set():
                return
            self._closed.set()
        # A full queue or a stuck filesystem must not prevent the GUI exiting.
        self._thread.join(timeout=1)
