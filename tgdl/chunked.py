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
from pathlib import Path
from typing import Callable

REQUEST_BYTES = 512 * 1024
MIN_CHUNKED_BYTES = 16 * 1024 * 1024


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
) -> Path:
    """Download exact ranges and reject any truncated part before publishing."""
    size = getattr(document, "size", None)
    if not isinstance(size, int) or size <= 0:
        raise ValueError("Unknown Telegram document size")
    ranges = byte_ranges(size, parts)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as handle:
        handle.truncate(size)

    async def fetch_range(start: int, end: int) -> None:
        offset = start
        max_chunks = math.ceil((end - start) / REQUEST_BYTES)
        stream = client.iter_download(
            document,
            offset=start,
            limit=max_chunks,
            chunk_size=REQUEST_BYTES,
            request_size=REQUEST_BYTES,
            file_size=size,
        )
        try:
            with output.open("r+b") as handle:
                handle.seek(start)
                async for chunk in stream:
                    # Prevent a response at a range boundary writing into another.
                    take = min(len(chunk), end - offset)
                    if take <= 0:
                        break
                    handle.write(memoryview(chunk)[:take])
                    offset += take
                    if on_bytes:
                        on_bytes(take)
                    if offset == end:
                        break
        finally:
            close = getattr(stream, "close", None) or getattr(stream, "aclose", None)
            if close is not None:
                await close()
        if offset != end:
            raise IOError(
                f"Incomplete range {start}:{end}; received {offset - start} bytes"
            )

    tasks = [asyncio.create_task(fetch_range(a, b)) for a, b in ranges]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    if output.stat().st_size != size:
        raise IOError(f"File size mismatch: {output.stat().st_size} != {size}")
    return output
