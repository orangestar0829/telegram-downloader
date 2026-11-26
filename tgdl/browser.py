"""Browse a chat's media (thumbnails) and save only the ones you pick.

This backs the gallery view: it lists recent media messages with small
thumbnails (cheap to fetch), and downloads the full-quality file only for the
items the user selects — like scrolling a chat's shared media and tapping the
ones you want.

Kept decoupled from Telethon (duck-typed client) so it can be tested offline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from contextlib import closing, nullcontext
import threading

from tgdl.history import DownloadHistory
from tgdl.slots import SlotCoordinator
from pathlib import Path
from typing import Iterable
from tgdl.progress import ProgressCallback

from tgdl.downloader import (
    DownloadStats,
    download_many,
    classify_message,
    peer_download_folder, history_peer_id,
)


@dataclass
class MediaItem:
    message: object
    id: int
    kind: str
    date: datetime | None
    thumb: bytes | None = None


@dataclass
class SaveResult:
    saved: int = 0
    skipped: int = 0
    failed: int = 0
    files: list[Path] = field(default_factory=list)


async def _fetch_thumb(client, message) -> bytes | None:
    """Best-effort small thumbnail as bytes (never raises)."""
    for thumb in (0, 1):
        try:
            data = await client.download_media(message, file=bytes, thumb=thumb)
            if data:
                return data
        except Exception:
            continue
    return None


async def list_media(
    client,
    entity,
    *,
    limit: int = 60,
    types: Iterable[str] | None = None,
    scan_cap: int = 2000,
    with_thumbs: bool = True,
) -> list[MediaItem]:
    """Return up to ``limit`` media items (newest first) with thumbnails."""
    wanted = set(types) if types else None
    items: list[MediaItem] = []
    scanned = 0
    async for message in client.iter_messages(entity):
        scanned += 1
        if scanned > scan_cap:
            break
        kind = classify_message(message)
        if kind is None:
            continue
        if wanted is not None and kind not in wanted:
            continue
        raw_date = getattr(message, "date", None)
        date = None
        if raw_date is not None:
            date = raw_date if raw_date.tzinfo else raw_date.replace(tzinfo=timezone.utc)
        thumb = await _fetch_thumb(client, message) if with_thumbs else None
        items.append(
            MediaItem(message=message, id=getattr(message, "id", 0), kind=kind, date=date, thumb=thumb)
        )
        if len(items) >= limit:
            break
    return items


async def save_items(
    client,
    items: Iterable[MediaItem],
    *,
    download_dir: Path,
    title: str,
    entity: object,
    skip_existing: bool = True,
    concurrency: int = 3,
    retries: int = 2,
    chunk_concurrency: int = 1,
    temp_download_dir: Path | None = None,
    on_progress: ProgressCallback | None = None,
    gui_slots: SlotCoordinator | None = None,
    stop_event: threading.Event | None = None,
) -> SaveResult:
    """Download the full-quality files for the selected media items."""
    folder = peer_download_folder(download_dir, title, entity)
    photos = folder / "photos"
    videos = folder / "videos"
    photos.mkdir(parents=True, exist_ok=True)
    videos.mkdir(parents=True, exist_ok=True)

    stats = DownloadStats()

    async def jobs():
        for item in items:
            dest = photos if item.kind == "photo" else videos
            date = item.date or datetime.now(timezone.utc)
            base = f"{date.strftime('%Y%m%d')}_{item.id}"
            yield item.message, dest, base

    peer = history_peer_id(entity) if gui_slots is not None else None
    # A GUI gallery save may target the same message as an active subscription.
    # Use the same key and history ledger for these channels. Ordinary gallery
    # browsing in CLI/library callers retains its existing local-file behavior.
    with closing(DownloadHistory()) if gui_slots is not None else nullcontext(None) as history:
        saved_paths = await download_many(
            client, jobs(), stats=stats, concurrency=concurrency,
            retries=retries, skip_existing=skip_existing,
            chunk_concurrency=chunk_concurrency,
            temp_download_dir=temp_download_dir, on_progress=on_progress,
            gallery_history=history, peer_id=peer, gui_slots=gui_slots, stop_event=stop_event,
        )
    result = SaveResult(
        saved=stats.saved, skipped=stats.skipped, failed=stats.failed,
        files=sorted(saved_paths),
    )
    print(f"[gallery] saved={result.saved} skipped={result.skipped} failed={result.failed}")
    return result
