"""Bounded parallel byte-range downloads for large Telegram documents.

A single video is downloaded in non-overlapping ranges using Telethon's
iter_download, with separate file descriptors per range. FFmpeg is unnecessary.
The caller must use a temporary path and publish only after validation.
"""
from __future__ import annotations

import asyncio
import math
import mimetypes
import re
import time
from pathlib import Path
from typing import Callable

REQUEST_BYTES = 512 * 1024
MIN_CHUNKED_BYTES = 16 * 1024 * 1024
CHUNK_IDLE_TIMEOUT = 45
CHUNK_RETRIES = 2
RANGE_FILE_BUFFER_BYTES = 1024 * 1024


class ChunkDownloadStalled(IOError):
    """Chunk transfer exhausted its idle/retry budget; use regular download."""


def chunkable_document(source: object, parts: int) -> object | None:
    """Choose a large Telegram document; photos and small files use Telethon."""
    if parts <= 1:
        return None
    media = getattr(source, "media", source)
    document = getattr(media, "document", None)
    if document is None and getattr(media, "file_reference", None) is not None:
        document = media
    size = getattr(document, "size", None)
    return document if isinstance(size, int) and size >= MIN_CHUNKED_BYTES else None


def document_suffix(document: object) -> str:
    """Use only a safe extension from Telegram metadata, not an unsafe name."""
    for attr in getattr(document, "attributes", None) or ():
        filename = getattr(attr, "file_name", None)
        if filename:
            extension = Path(str(filename).replace("\\", "/")).suffix
            if re.fullmatch(r"\.[a-zA-Z0-9]{1,10}", extension):
                return extension
    mime = getattr(document, "mime_type", "") or ""
    extension = {
        "video/mp4": ".mp4",
        "video/x-matroska": ".mkv",
        "video/webm": ".webm",
        "video/quicktime": ".mov",
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
    }.get(mime, mimetypes.guess_extension(mime) or ".bin")
    return extension if re.fullmatch(r"\.[a-zA-Z0-9]{1,10}", extension) else ".bin"


def byte_ranges(size: int, parts: int) -> list[tuple[int, int]]:
    """Return disjoint (start, end) ranges aligned to MTProto 512 KiB blocks."""
    if size <= 0:
        raise ValueError("size must be positive")
    if not 1 <= parts <= 8:
        raise ValueError("chunk concurrency must be 1-8")
    blocks = math.ceil(size / REQUEST_BYTES)
    blocks_per_range = math.ceil(blocks / min(parts, blocks))
    span = blocks_per_range * REQUEST_BYTES
    return [(start, min(size, start + span)) for start in range(0, size, span)]


async def download_chunked(
    client: object,
    document: object,
    output: Path,
    *,
    parts: int,
    on_bytes: Callable[[int], None] | None = None,
    idle_timeout: float = CHUNK_IDLE_TIMEOUT,
    retries: int = CHUNK_RETRIES,
    timings: dict[str, float] | None = None,
    diagnostic_label: str = "",
    diagnostic_idle_seconds: float = 10.0,
    diagnostic_repeat_seconds: float = 20.0,
) -> Path:
    """Download exact ranges and reject any truncated part before publishing."""
    size = getattr(document, "size", None)
    if not isinstance(size, int) or size <= 0:
        raise ValueError("Unknown Telegram document size")
    if idle_timeout <= 0 or retries < 0:
        raise ValueError("idle_timeout must be positive and retries nonnegative")
    if diagnostic_idle_seconds <= 0 or diagnostic_repeat_seconds <= 0:
        raise ValueError("diagnostic intervals must be positive")
    ranges = byte_ranges(size, parts)
    output = Path(output)
    prepare_started = time.perf_counter()
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as handle:
        handle.truncate(size)
    if timings is not None:
        timings["prepare_s"] = time.perf_counter() - prepare_started

    completed = 0
    states = [
        {"start": start, "end": end, "last": time.monotonic(), "bytes": 0,
         "attempt": 0, "done": False, "warned": False, "next_log": 0.0}
        for start, end in ranges
    ]
    def report_progress(amount: int) -> None:
        nonlocal completed
        completed += amount
        if on_bytes:
            on_bytes(amount)

    async def fetch_range(index: int, start: int, end: int) -> None:
        state = states[index]
        offset = start
        for attempt in range(retries + 1):
            state["attempt"] = attempt + 1
            stream = client.iter_download(
                document, offset=offset,
                limit=math.ceil((end - offset) / REQUEST_BYTES),
                chunk_size=REQUEST_BYTES, request_size=REQUEST_BYTES,
                file_size=size,
            )
            try:
                # Each range has an independent buffered file handle and seeks
                # only within its own non-overlapping byte range. Keep the
                # original single-file layout; no expensive part merging.
                with output.open("r+b", buffering=RANGE_FILE_BUFFER_BYTES) as handle:
                    handle.seek(offset)
                    iterator = aiter(stream)
                    while offset < end:
                        try:
                            chunk = await asyncio.wait_for(
                                anext(iterator), timeout=idle_timeout
                            )
                        except StopAsyncIteration as exc:
                            raise ChunkDownloadStalled(
                                f"Range {start}:{end} ended at {offset}"
                            ) from exc
                        take = min(len(chunk), end - offset)
                        if take == 0:
                            raise ChunkDownloadStalled("Telegram returned an empty chunk")
                        handle.write(memoryview(chunk)[:take])
                        offset += take
                        now = time.monotonic()
                        if state["warned"]:
                            print(
                                f"[分块诊断] {diagnostic_label} 路{index + 1} 数据恢复，"
                                f"此前等待{now - state['last']:.1f}s"
                            )
                        state["last"] = now
                        state["bytes"] += take
                        state["warned"] = False
                        state["next_log"] = 0.0
                        report_progress(take)
                state["done"] = True
                return
            except (TimeoutError, OSError) as exc:
                print(
                    f"[分块诊断] {diagnostic_label} 路{index + 1} 请求异常："
                    f"{type(exc).__name__}，已收{state['bytes'] / 1048576:.1f} MiB，"
                    f"重试={attempt}/{retries}"
                )
                if attempt >= retries:
                    raise ChunkDownloadStalled(
                        f"Range {start}:{end} stalled at {offset} "
                        f"after {retries + 1} attempts"
                    ) from exc
                delay = min(2 ** attempt, 8)
                print(f"[分块] 区间 {start}:{end} 停顿，{delay}s 后重试 "
                      f"({attempt + 1}/{retries}): {exc}")
                await asyncio.sleep(delay)
            finally:
                close = getattr(stream, "close", None) or getattr(stream, "aclose", None)
                if close is not None:
                    try:
                        await asyncio.wait_for(close(), timeout=idle_timeout)
                    except (TimeoutError, OSError):
                        # A failed network iterator may also fail to close.
                        # The next attempt or outer fallback still owns recovery.
                        pass

    async def monitor_ranges() -> None:
        while True:
            await asyncio.sleep(min(1.0, diagnostic_idle_seconds / 2))
            now = time.monotonic()
            for index, state in enumerate(states):
                if state["done"]:
                    continue
                idle = now - state["last"]
                if idle < diagnostic_idle_seconds or now < state["next_log"]:
                    continue
                print(
                    f"[分块诊断] {diagnostic_label} 路{index + 1}/{len(ranges)} "
                    f"连续{idle:.1f}s未收到数据，累计{state['bytes'] / 1048576:.1f} MiB，"
                    f"尝试次数{state['attempt']}"
                )
                state["warned"] = True
                state["next_log"] = now + diagnostic_repeat_seconds

    transfer_started = time.perf_counter()
    tasks = [asyncio.create_task(fetch_range(i, a, b)) for i, (a, b) in enumerate(ranges)]
    monitor = asyncio.create_task(monitor_ranges())
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
    finally:
        monitor.cancel()
        await asyncio.gather(monitor, return_exceptions=True)

    if output.stat().st_size != size:
        raise IOError(f"File size mismatch: {output.stat().st_size} != {size}")
    if timings is not None:
        timings["transfer_s"] = time.perf_counter() - transfer_started
    return output
