"""Cooperative, in-process monitor transfer pause and aggregate bandwidth control.

The controls run on the Telethon asyncio loop. The GUI publishes validated
numeric settings; it never manipulates asyncio events from the Tk thread.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable

from tgdl.chunked import (
    CHUNK_IDLE_TIMEOUT, CHUNK_RETRIES, ChunkDownloadStalled, REQUEST_BYTES,
)

WINDOW_BYTES = 8 * REQUEST_BYTES


class MonitorPause:
    """Pause at completed byte boundaries without cancelling or deleting files.

    Acknowledgement means every registered transfer lane is at a checkpoint.
    All lanes are resumed before the monitor worker shuts down.
    """

    def __init__(self) -> None:
        self._resume = asyncio.Event()
        self._resume.set()
        self._stopped = asyncio.Event()
        self._lanes = 0
        self._waiting = 0

    @property
    def requested(self) -> bool:
        return not self._resume.is_set()

    @property
    def suspended(self) -> bool:
        return self.requested and self._stopped.is_set()

    def _sync(self) -> None:
        if self.requested and self._waiting >= self._lanes:
            self._stopped.set()
        else:
            self._stopped.clear()

    def register(self) -> None:
        self._lanes += 1
        self._sync()

    def unregister(self) -> None:
        self._lanes -= 1
        self._sync()

    def pause(self) -> None:
        self._resume.clear()
        self._sync()

    def resume(self) -> None:
        self._resume.set()
        self._sync()

    async def wait_suspended(self) -> None:
        await self._stopped.wait()

    async def checkpoint(self) -> None:
        if not self.requested:
            return
        self._waiting += 1
        self._sync()
        try:
            await self._resume.wait()
        finally:
            self._waiting -= 1
            self._sync()


class SharedSpeedLimiter:
    """Aggregate receive rate for all monitored media, in bytes per second.

    A shared token bucket prevents independent file workers each consuming the
    full configured budget. Settings can change during an await; wake-ups are
    bounded to 100 ms. Zero means unlimited. Per-chunk MTProto bursts remain
    possible: this controls average received throughput, not the server cap.
    """

    def __init__(self, bytes_per_second: float = 0.0) -> None:
        self.bytes_per_second = max(0.0, float(bytes_per_second))
        self._tokens = 0.0
        self._updated = time.monotonic()
        self._previous_limit = self.bytes_per_second
        self._lock = asyncio.Lock()

    async def consume(self, amount: int, pause: MonitorPause | None = None) -> None:
        if amount <= 0:
            return
        while True:
            if pause is not None:
                await pause.checkpoint()
            async with self._lock:
                now = time.monotonic()
                limit = max(0.0, self.bytes_per_second)
                if limit == 0:
                    self._previous_limit = limit
                    self._updated = now
                    self._tokens = 0.0
                    return
                burst = max(512 * 1024, limit / 4)
                if self._previous_limit != limit:
                    self._tokens = min(self._tokens, burst)
                    self._previous_limit = limit
                    self._updated = now
                self._tokens = min(burst, self._tokens + (now - self._updated) * limit)
                self._updated = now
                if self._tokens >= amount:
                    self._tokens -= amount
                    return
                delay = (amount - self._tokens) / limit
            await asyncio.sleep(min(0.1, max(0.01, delay)))


@dataclass
class MonitorFlow:
    pause: MonitorPause
    limiter: SharedSpeedLimiter
    chunks: Callable[[], int]


async def monitor_download_document(
    client: object,
    document: object,
    output: Path,
    flow: MonitorFlow,
    *,
    on_bytes: Callable[[int], None] | None = None,
) -> Path:
    """Download a document with independently placed 4MiB windows.

    Eight lane tasks are available, but only the first configured number may
    fetch. Each window is exclusively owned by one task; within the window
    we check pause/rate at each Telegram block. A reduced lane limit returns
    unfinished aligned ranges to a shared queue after the current block, so
    the remaining active lanes can finish them without discarding bytes.
    The shared pause is cooperative and does not destroy partially received
    data. The parent temp directory remains owned by the caller.
    """
    size = int(getattr(document, "size"))
    if size <= 0:
        raise ValueError("Document size must be positive")
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as handle:
        handle.truncate(size)
    next_window = 0
    returned_ranges: deque[tuple[int, int]] = deque()
    in_flight = 0

    async def worker(lane: int) -> None:
        nonlocal next_window, in_flight
        while True:
            if next_window >= size and not returned_ranges and not in_flight:
                return
            if (lane >= flow.chunks()
                    or (next_window >= size and not returned_ranges)):
                # A slower lane may still return an unfinished range when
                # the limit drops. Do not let its replacement exit early.
                await asyncio.sleep(0.1)
                continue
            owns_window = False
            flow.pause.register()
            try:
                await flow.pause.checkpoint()
                if lane >= flow.chunks():
                    continue
                if returned_ranges:
                    start, end = returned_ranges.popleft()
                else:
                    start = next_window
                    if start >= size:
                        continue
                    end = min(size, start + WINDOW_BYTES)
                    next_window = end
                in_flight += 1
                owns_window = True
                position = start
                handed_off = False
                for attempt in range(CHUNK_RETRIES + 1):
                    if position >= end:
                        break
                    stream = client.iter_download(
                        document, offset=position,
                        limit=math.ceil((end - position) / REQUEST_BYTES),
                        chunk_size=REQUEST_BYTES, request_size=REQUEST_BYTES,
                        file_size=size,
                    )
                    try:
                        with output.open("r+b") as handle:
                            handle.seek(position)
                            iterator = aiter(stream)
                            while position < end:
                                await flow.pause.checkpoint()
                                if lane >= flow.chunks():
                                    returned_ranges.appendleft((position, end))
                                    handed_off = True
                                    break
                                try:
                                    block = await asyncio.wait_for(
                                        anext(iterator), timeout=CHUNK_IDLE_TIMEOUT,
                                    )
                                except StopAsyncIteration as exc:
                                    raise ChunkDownloadStalled(
                                        f"Monitor window {start}:{end} ended at {position}"
                                    ) from exc
                                count = min(len(block), end - position)
                                if count <= 0:
                                    raise ChunkDownloadStalled("Telegram returned empty monitor block")
                                handle.write(memoryview(block)[:count])
                                position += count
                                await flow.limiter.consume(count, pause=flow.pause)
                                if on_bytes:
                                    on_bytes(count)
                    except (TimeoutError, OSError, ChunkDownloadStalled) as exc:
                        if attempt >= CHUNK_RETRIES:
                            raise ChunkDownloadStalled(
                                f"Monitor window {start}:{end} stalled at {position}"
                            ) from exc
                        await asyncio.sleep(min(2 ** attempt, 8))
                    finally:
                        close = getattr(stream, "close", None) or getattr(stream, "aclose", None)
                        if close is not None:
                            closing = close()
                            if inspect.isawaitable(closing):
                                await closing
                    if handed_off:
                        break
                if position != end and not handed_off:
                    raise ChunkDownloadStalled(f"Incomplete monitor window {position} != {end}")
            finally:
                if owns_window:
                    in_flight -= 1
                flow.pause.unregister()

    tasks = [asyncio.create_task(worker(i)) for i in range(8)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    if output.stat().st_size != size:
        raise IOError("Monitor document file size mismatch")
    return output
