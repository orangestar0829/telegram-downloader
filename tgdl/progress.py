"""Small callback-only progress tracker, safe for background asyncio producers."""
from __future__ import annotations

import time
from collections.abc import Callable

ProgressCallback = Callable[[str, str, int, int, str], None]
REFRESH_SECONDS = 0.25


def format_transfer_size(byte_count: int) -> str:
    if byte_count >= 1024 ** 2:
        return f"{byte_count / 1024 ** 2:.1f} MiB"
    if byte_count >= 1024:
        return f"{byte_count / 1024:.1f} KiB"
    return f"{byte_count} B"


def format_transfer_rate(bytes_per_second: float) -> str:
    if bytes_per_second >= 1024 ** 2:
        return f"{bytes_per_second / 1024 ** 2:.2f} MiB/s"
    if bytes_per_second >= 1024:
        return f"{bytes_per_second / 1024:.2f} KiB/s"
    return f"{bytes_per_second:.2f} B/s"


class TransferProgress:
    def __init__(self, key: str, label: str, callback: ProgressCallback | None):
        self.key = key
        self.label = label
        self.callback = callback
        self.total = 0
        self.done = 0
        self.status = ""
        self.last_emit = 0.0

    def emit(self, status: str, done: int | None = None, total: int | None = None,
             *, force: bool = False):
        if done is not None:
            self.done = max(0, int(done))
        if total is not None and total > 0:
            self.total = int(total)
        status_changed = status != self.status
        self.status = status
        now = time.monotonic()
        if self.callback and (force or status_changed or now - self.last_emit >= REFRESH_SECONDS
                              or self.total > 0 and self.done >= self.total):
            self.last_emit = now
            self.callback(self.key, self.label, self.done, self.total, status)

    def advance(self, amount: int):
        self.emit("下载中", done=self.done + amount)
