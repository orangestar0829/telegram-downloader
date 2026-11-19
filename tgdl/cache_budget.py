"""Shared, asynchronous SSD staging reservations for chunked media.

Reservations are per process and per physical volume. Disk free space is
checked again whenever a waiter wakes; no download is redirected to the HDD
on cache pressure.
"""
from __future__ import annotations

import asyncio
import shutil
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

GIB = 1024 ** 3
MAX_CACHE_BYTES = 20 * GIB
MIN_FREE_BYTES = 10 * GIB
SPACE_POLL_SECONDS = 2.0


class CacheFileTooLarge(ValueError):
    """An individual file cannot ever fit within the staging allowance."""


class _CacheBudget:
    def __init__(self, *, capacity: int = MAX_CACHE_BYTES, reserve: int = MIN_FREE_BYTES):
        self.capacity = capacity
        self.reserve = reserve
        self.active_bytes = 0
        self._condition = asyncio.Condition()
        self._waiters: deque[object] = deque()
        self._roots: set[Path] = set()
        self._owned_directories: set[Path] = set()

    def _orphan_bytes(self) -> int:
        """Count leftover temporary files, without double counting this run."""
        total = 0
        for root in self._roots:
            try:
                candidates = list(root.glob("tgdl-*"))
            except OSError:
                continue
            for folder in candidates:
                if not folder.is_dir() or folder.resolve() in self._owned_directories:
                    continue
                try:
                    total += sum(
                        file.stat().st_size for file in folder.rglob("*") if file.is_file()
                    )
                except OSError:
                    # External cleanup might race with scanning these entries.
                    continue
        return total

    async def acquire(self, directory: Path, size: int) -> None:
        if size > self.capacity:
            raise CacheFileTooLarge(
                f"文件大小 {size / GIB:.2f} GiB 超过 SSD 缓存上限 {self.capacity / GIB:.0f} GiB"
            )
        ticket = object()
        announced = False
        async with self._condition:
            self._roots.add(Path(directory).resolve())
            self._waiters.append(ticket)
            try:
                while True:
                    free = shutil.disk_usage(directory).free
                    # Reservations include in-flight staging files. Counting
                    # their already allocated bytes twice is conservative.
                    if (
                        self._waiters[0] is ticket
                        and self.active_bytes + self._orphan_bytes() + size <= self.capacity
                        and free >= self.reserve + self.active_bytes + size
                    ):
                        self.active_bytes += size
                        self._waiters.popleft()
                        self._condition.notify_all()
                        if announced:
                            print(f"[SSD缓存] 空间已释放，开始下载 {size / GIB:.2f} GiB 文件")
                        return
                    if not announced:
                        print(
                            f"[SSD缓存] 等待缓存空间（最大{self.capacity // GIB} GiB，"
                            f"保留{self.reserve // GIB} GiB磁盘可用空间），"
                            f"文件大小{size / GIB:.2f} GiB"
                        )
                        announced = True
                    try:
                        await asyncio.wait_for(self._condition.wait(), SPACE_POLL_SECONDS)
                    except asyncio.TimeoutError:
                        # The SSD may have been filled/freed outside this process.
                        pass
            finally:
                if ticket in self._waiters:
                    self._waiters.remove(ticket)
                    self._condition.notify_all()

    async def release(self, size: int) -> None:
        async with self._condition:
            self.active_bytes -= size
            self._condition.notify_all()


_budgets: dict[str, _CacheBudget] = {}


class _Reservation:
    """Track only this file's staging directory, not other active files."""

    def __init__(self, budget: _CacheBudget):
        self.budget = budget
        self.owned: set[Path] = set()

    def track(self, path: Path) -> None:
        resolved = Path(path).resolve()
        self.owned.add(resolved)
        self.budget._owned_directories.add(resolved)

    def untrack(self) -> None:
        self.budget._owned_directories.difference_update(self.owned)


def _volume_key(directory: Path) -> str:
    # On Windows the root identifies the underlying disk; aliases/symlinks
    # are normalized before taking the root.
    resolved = directory.resolve()
    return resolved.anchor.casefold() if resolved.anchor else str(resolved)


@asynccontextmanager
async def staging_reservation(directory: Path, size: int):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    key = _volume_key(directory)
    budget = _budgets.setdefault(key, _CacheBudget())
    await budget.acquire(directory, size)
    lease = _Reservation(budget)
    try:
        yield lease
    finally:
        lease.untrack()
        await budget.release(size)


@asynccontextmanager
async def optional_staging_reservation(directory: Path | None, size: int):
    """No-op for legacy downloads without SSD staging."""
    if directory is None:
        yield None
    else:
        async with staging_reservation(directory, size) as budget:
            yield budget
