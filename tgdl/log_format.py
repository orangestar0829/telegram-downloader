"""Line-aware timestamp formatter for the GUI's merged stdout/stderr."""
from __future__ import annotations

import threading
from datetime import datetime


class TimestampedLines:
    def __init__(self, clock=None) -> None:
        self._clock = clock or datetime.now
        self._lock = threading.Lock()
        self._line_start = True

    def format(self, text: str, *, prefix: str = "") -> str:
        if not text:
            return ""
        with self._lock:
            result = []
            for part in text.splitlines(keepends=True):
                if self._line_start:
                    result.append(f"[{self._clock():%Y-%m-%d %H:%M:%S}] ")
                    if prefix:
                        result.append(prefix)
                result.append(part)
                self._line_start = part.endswith(("\n", "\r"))
            return "".join(result)
