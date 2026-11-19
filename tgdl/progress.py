"""Small callback-only progress tracker, safe for background asyncio producers."""
from __future__ import annotations

import time
from collections.abc import Callable

ProgressCallback = Callable[[str, str, int, int, str], None]
REFRESH_SECONDS = 0.25


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
