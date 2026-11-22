"""Bounded asynchronous daily diagnostic logging independent from Tk text widgets."""
from __future__ import annotations

import queue
import re
import threading
from datetime import date, datetime, timedelta
from pathlib import Path

_SECRET = re.compile(
    r"(?i)\b(telegram_api_hash|socks5_password|proxy_password|api_hash|password)"
    r"(\s*[:=]\s*)([^\s,;]+)"
)


def redact(text: str) -> str:
    return _SECRET.sub(lambda m: m.group(1) + m.group(2) + "[REDACTED]", text)


class DailyLogStore:
    def __init__(self, directory: Path, *, retain_days: int = 7) -> None:
        if retain_days < 1:
            raise ValueError("retain_days must be positive")
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.retain_days = retain_days
        self.queue: queue.Queue[str | None] = queue.Queue(maxsize=20000)
        self.dropped = 0
        self._closed = False
        self._thread = threading.Thread(target=self._worker, daemon=True, name="tgdl-log-writer")
        self._thread.start()

    def write(self, message: str) -> None:
        if self._closed or not message:
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
            while True:
                item = self.queue.get()
                if item is None:
                    break
                today = datetime.now().date()
                if today != current_day:
                    if out is not None:
                        out.close()
                    self._cleanup(today)
                    out = (self.directory / f"runtime-{today:%Y-%m-%d}.log").open(
                        "a", encoding="utf-8"
                    )
                    current_day = today
                out.write(item)
                out.flush()
        finally:
            if out is not None:
                out.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Drain items before stopping; shutdown is not on the Tk event loop.
        self.queue.put(None)
        self._thread.join(timeout=10)
