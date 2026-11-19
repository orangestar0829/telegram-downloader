from __future__ import annotations

import asyncio
import re
import tempfile
import time
import threading
from collections.abc import AsyncIterable
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UserAlreadyParticipantError,
    UsernameNotOccupiedError,
)
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import (
    DocumentAttributeFilename,
    Message,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
)
from tqdm import tqdm

from tgdl.chunked import (
    ChunkDownloadStalled, chunkable_document, document_suffix, download_chunked,
)
from tgdl.history import DownloadHistory

PHOTO_TYPES = {"photo"}
VIDEO_TYPES = {"video", "animation"}
ALL_TYPES = PHOTO_TYPES | VIDEO_TYPES

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".heic", ".tif", ".tiff"}
VIDEO_EXTS = {".mp4", ".mkv", ".mov", ".webm", ".avi", ".m4v", ".ts", ".flv"}
ANIM_EXTS = {".gif"}

INVITE_RE = re.compile(
    r"(?:https?://)?t\.me/(?:\+|joinchat/)([A-Za-z0-9_\-]+)",
    re.IGNORECASE,
)
USERNAME_RE = re.compile(
    r"(?:https?://)?t\.me/([A-Za-z0-9_]+)/?$",
    re.IGNORECASE,
)


def document_filename(document: object) -> str:
    for attr in getattr(document, "attributes", None) or []:
        if isinstance(attr, DocumentAttributeFilename):
            return attr.file_name or ""
    return ""


def kind_from_name_and_mime(filename: str, mime: str) -> str | None:
    mime = (mime or "").lower()
    suffix = Path(filename or "").suffix.lower()
    if mime.startswith("image/") or suffix in IMAGE_EXTS:
        if mime == "image/gif" or suffix in ANIM_EXTS:
            return "animation"
        return "photo"
    if mime.startswith("video/") or suffix in VIDEO_EXTS:
        return "video"
    if suffix in ANIM_EXTS:
        return "animation"
    return None


def kind_from_document(document: object) -> str | None:
    if document is None:
        return None
    return kind_from_name_and_mime(
        document_filename(document),
        getattr(document, "mime_type", "") or "",
    )


def classify_message(message: Message) -> str | None:
    if message.sticker or message.voice or message.audio:
        return None

    media = message.media
    if isinstance(media, MessageMediaWebPage):
        web = getattr(media, "webpage", None)
        if web is None:
            return None
        doc_kind = kind_from_document(getattr(web, "document", None))
        if doc_kind:
            return doc_kind
        if getattr(web, "photo", None) is not None:
            return "photo"
        return None

    if isinstance(media, MessageMediaPhoto):
        return "photo"
    if message.video:
        return "video"
    if message.gif:
        return "animation"
    if isinstance(media, MessageMediaDocument) and media.document:
        return kind_from_document(media.document)
    if message.photo:
        return "photo"
    return None


def download_target(message: Message):
    media = message.media
    if isinstance(media, MessageMediaWebPage):
        web = getattr(media, "webpage", None)
        if web is None:
            return message
        document = getattr(web, "document", None)
        if document is not None and kind_from_document(document) in {"video", "animation"}:
            return document
        photo = getattr(web, "photo", None)
        if photo is not None:
            return photo
        if document is not None:
            return document
    return message


def media_origin(message: Message) -> str:
    if isinstance(message.media, MessageMediaWebPage):
        return "link-preview"
    sender = getattr(message, "sender", None)
    if getattr(sender, "bot", False):
        return "bot"
    return "attachment"


def safe_name(text: str) -> str:
    cleaned = re.sub(r'[<>:"/\\|?*]', "_", text).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned[:80] or "channel"


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    for i in range(1, 10_000):
        candidate = path.with_name(f"{stem}_{i}{suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Cannot find a free filename for {path}")


async def resolve_channel(client: TelegramClient, target: str):
    target = target.strip()
    invite = INVITE_RE.search(target)
    if invite:
        invite_hash = invite.group(1)
        try:
            result = await client(ImportChatInviteRequest(invite_hash))
            chats = getattr(result, "chats", None)
            if chats:
                return chats[0]
        except UserAlreadyParticipantError:
            checked = await client(CheckChatInviteRequest(invite_hash))
            chat = getattr(checked, "chat", None)
            if chat is not None:
                return chat
        except Exception:
            pass

    username_match = USERNAME_RE.search(target)
    if username_match:
        target = username_match.group(1)

    if target.startswith("@"):
        target = target[1:]

    try:
        return await client.get_entity(int(target) if _looks_like_id(target) else target)
    except (ChannelPrivateError, UsernameNotOccupiedError, ValueError) as exc:
        raise SystemExit(
            f"Cannot access '{target}'. Join the private channel with this account first, "
            "then pass the numeric id from `python -m tgdl list`."
        ) from exc


def _looks_like_id(value: str) -> bool:
    return value.lstrip("-").isdigit()


class DownloadStats:
    def __init__(self) -> None:
        self.saved = 0
        self.skipped = 0
        self.failed = 0
        self.more_pending = False
        self.matched = 0
        self.first_message_id = 0
        self.last_message_id = 0
        self.scan_upper = 0
        self.scan_completed = 0


async def download_channel(
    client: TelegramClient,
    *,
    target: str,
    download_dir: Path,
    types: set[str],
    limit: int | None,
    since: datetime | None,
    until: datetime | None,
    skip_existing: bool = True,
    dry_run: bool = False,
    concurrency: int = 3,
    retries: int = 2,
    chunk_concurrency: int = 1,
    on_saved: Callable[[str], None] | None = None,
    monitor_direction: str | None = None,
    monitor_batch_size: int = 100,
    stop_event: threading.Event | None = None,
    on_channel_resolved: Callable[[str, int], None] | None = None,
) -> DownloadStats:
    entity = await resolve_channel(client, target)
    title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity.id)
    if on_channel_resolved is not None:
        on_channel_resolved(str(title), int(entity.id))
    def saved_with_source(detail: str) -> None:
        labeled = f"[{title}] {detail}"
        if on_saved is not None:
            on_saved(labeled)
        else:
            print(labeled)
    folder = download_dir / safe_name(str(title))
    photos_dir = folder / "photos"
    videos_dir = folder / "videos"
    if not dry_run:
        photos_dir.mkdir(parents=True, exist_ok=True)
        videos_dir.mkdir(parents=True, exist_ok=True)

    print(f"Channel: {title}")
    if dry_run:
        print("Mode:    list only (no download)")
    else:
        print(f"Save to: {folder}")
    print(f"Types:   {', '.join(sorted(types))}")

    stats = DownloadStats()
    scanned = 0
    monitored = monitor_direction in ("newest", "oldest") and not dry_run
    if monitored and monitor_batch_size < 1:
        raise ValueError("monitor_batch_size must be positive")
    history = DownloadHistory() if not dry_run else None
    peer_id = int(entity.id)
    scan_upper = 0
    scan_completed = 0
    scan_cursor = 0
    interrupted = False
    last_seen = 0
    reached_batch_limit = False
    batch_ids: list[int] = []

    if monitored and history is not None:
        media_types = ",".join(sorted(types))
        scan_completed, scan_upper, scan_cursor, saved_direction = history.scan_state(peer_id)
        if history.scan_media_types(peer_id) not in ("", media_types):
            # Newly enabled media types require a safe one-time full re-scan.
            scan_completed, scan_upper, scan_cursor = 0, 0, 0
        elif history.scan_media_types(peer_id) == "" and scan_completed:
            # Unknown legacy scan filter: replay history rather than skip media.
            scan_completed, scan_upper, scan_cursor = 0, 0, 0
        starting_cycle = not scan_upper
        if scan_upper and saved_direction != monitor_direction:
            # A different direction needs to replay the pending window safely.
            scan_cursor = 0
        if not scan_upper:
            async for latest in client.iter_messages(entity, limit=1):
                scan_upper = int(latest.id)
                break
            if scan_upper <= scan_completed:
                scan_upper = 0
            else:
                history.save_scan_state(peer_id, scan_completed, scan_upper, 0, monitor_direction, media_types)
        stats.scan_upper = scan_upper
        stats.scan_completed = scan_completed
        print(
            f"[监控] {title}: 方向={'旧→新' if monitor_direction == 'oldest' else '新→旧'}"
            f"，已完成检查点={scan_completed}，区间上限={scan_upper}"
        )

        # Retry failures once per scan window, including when nothing new
        # arrived; do not retry the same failures on every 100-item batch.
        for failed_id in history.failures(peer_id, limit=20) if starting_cycle else []:
            if stop_event is not None and stop_event.is_set():
                interrupted = True
                break
            try:
                failed_message = await client.get_messages(entity, ids=failed_id)
                if not isinstance(failed_message, Message) or failed_message.date is None:
                    history.clear_failure(peer_id, failed_id)
                    continue
                failed_kind = classify_message(failed_message)
                if failed_kind not in types:
                    history.clear_failure(peer_id, failed_id)
                    continue
                failed_dir = photos_dir if failed_kind == "photo" else videos_dir
                failed_base = f"{failed_message.date.strftime('%Y%m%d')}_{failed_id}"
                await _download_with_retry(
                    client, failed_message, failed_dir, failed_base, stats,
                    skip_existing=True, retries=retries, chunk_concurrency=chunk_concurrency,
                    history=history, peer_id=peer_id, on_saved=saved_with_source,
                )
            except Exception as exc:
                print(f"[监控] 重试消息 {failed_id} 失败：{exc}")
        if interrupted:
            history.close()
            return stats
        if not scan_upper:
            history.close()
            print(f"[监控] {title}: 没有新增消息")
            return stats

    async def jobs():
        nonlocal scanned, interrupted, last_seen, reached_batch_limit
        if monitored:
            if monitor_direction == "oldest":
                iterator = client.iter_messages(
                    entity, reverse=True,
                    min_id=scan_cursor or scan_completed, max_id=scan_upper + 1,
                )
            else:
                iterator = client.iter_messages(
                    entity, min_id=scan_completed,
                    max_id=scan_cursor or (scan_upper + 1),
                )
        else:
            iterator = client.iter_messages(entity, offset_date=until)
        async for message in iterator:
            if stop_event is not None and stop_event.is_set():
                interrupted = True
                break
            if monitored and scanned >= monitor_batch_size:
                reached_batch_limit = True
                break
            if limit is not None and scanned >= limit:
                break
            if not isinstance(message, Message) or message.date is None:
                continue
            last_seen = int(message.id)
            if not stats.first_message_id:
                stats.first_message_id = last_seen
            stats.last_message_id = last_seen
            msg_date = message.date if message.date.tzinfo else message.date.replace(tzinfo=timezone.utc)
            if since is not None and msg_date < since:
                break
            kind = classify_message(message)
            if kind is None or kind not in types:
                continue
            scanned += 1
            stats.matched += 1
            if monitored:
                batch_ids.append(int(message.id))
            base = f"{msg_date.strftime('%Y%m%d')}_{message.id}"
            if dry_run:
                origin = media_origin(message)
                sender = getattr(message, "sender", None)
                sender_name = getattr(sender, "username", None) or getattr(sender, "first_name", None) or "-"
                preview = (message.raw_text or "").replace("\n", " ")[:60]
                print(f"{base}  {kind:<10}  {origin:<13}  @{sender_name}  {preview}")
                stats.saved += 1
                continue
            dest_dir = photos_dir if kind == "photo" else videos_dir
            yield message, dest_dir, base

    try:
        try:
            await download_many(
                client, jobs(), stats=stats, concurrency=concurrency,
                retries=retries, skip_existing=skip_existing,
                chunk_concurrency=chunk_concurrency,
                history=history, peer_id=peer_id,
                on_saved=saved_with_source,
                stop_event=stop_event,
            )
            if monitored and history is not None:
                # A failed download is durably queued before committing cursor.
                for message_id in batch_ids:
                    if not history.contains(peer_id, message_id):
                        history.mark_failure(peer_id, message_id)
                if last_seen and not interrupted:
                    scan_cursor = last_seen
                if not interrupted and not reached_batch_limit:
                    history.save_scan_state(peer_id, scan_upper, 0, 0, monitor_direction, media_types)
                    print(
                        f"[监控] {title}: 本轮区间扫描完成，检查点={scan_upper}，"
                        f"本批消息ID={stats.first_message_id or '-'}→{stats.last_message_id or '-'}"
                    )
                elif last_seen and not interrupted:
                    history.save_scan_state(peer_id, scan_completed, scan_upper, scan_cursor, monitor_direction, media_types)
                    stats.more_pending = True
                    print(
                        f"[监控] {title}: 批次完成，消息ID={stats.first_message_id}→{stats.last_message_id}，"
                        f"游标={scan_cursor}，匹配媒体={scanned}，等待下一批"
                    )
        except FloodWaitError as exc:
            print(f"\nRate limited. Wait {exc.seconds}s then retry this channel.")
            raise
    finally:
        if history is not None:
            history.close()

    print(
        f"\nDone. saved={stats.saved} skipped={stats.skipped} failed={stats.failed} matched={scanned}"
    )
    return stats


async def _download_one(
    client: TelegramClient,
    message: Message,
    dest_dir: Path,
    base: str,
    *,
    skip_existing: bool,
    stats: DownloadStats,
    chunk_concurrency: int = 1,
    history: DownloadHistory | None = None,
    peer_id: int | None = None,
    on_saved: Callable[[str], None] | None = None,
) -> Path | None:
    if skip_existing:
        if history is not None and peer_id is not None and history.contains(peer_id, message.id):
            stats.skipped += 1
            return None
        existing = next(dest_dir.glob(f"{base}.*"), None) if dest_dir.exists() else None
        if existing is not None:
            if history is not None and peer_id is not None and existing.is_file():
                history.mark_saved(peer_id, message.id, dest_dir.name, existing)
            stats.skipped += 1
            return None
    dest_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    mode = "普通下载"
    # Telethon writes using the original filename. Per-task directories avoid
    # collisions and allow failed/cancelled partial files to be cleaned up.
    with tempfile.TemporaryDirectory(prefix="tgdl-", dir=dest_dir) as temporary:
        with tqdm(unit="B", unit_scale=True, desc=base, leave=False) as bar:
            def progress(current: int, total: int) -> None:
                bar.total = total or bar.total
                bar.n = current
                bar.refresh()

            target = download_target(message)
            document = chunkable_document(target, chunk_concurrency)
            if document is not None and callable(getattr(client, "iter_download", None)):
                try:
                    saved = await download_chunked(
                        client, document, Path(temporary) / ("data" + document_suffix(document)),
                        parts=chunk_concurrency, on_bytes=bar.update,
                    )
                except ChunkDownloadStalled as exc:
                    print(f"[分块] {base} 超时/失败: {exc}; 自动退回普通下载")
                    # Do not publish the preallocated/partial chunked file.
                    saved = await client.download_media(
                        target, file=str(Path(temporary) / ("normal" + document_suffix(document))),
                        progress_callback=progress,
                    )
                else:
                    mode = f"分块{chunk_concurrency}路"
            else:
                saved = await client.download_media(
                    target, file=str(temporary), progress_callback=progress,
                )
        if not saved:
            stats.failed += 1
            return None
        saved_path = Path(saved)
        suffix = saved_path.suffix or ".bin"
        final_path = dest_dir / f"{base}{suffix}"
        if skip_existing and final_path.exists():
            stats.skipped += 1
            return None
        if final_path.exists():
            final_path = unique_path(final_path)
        saved_path.replace(final_path)
        if history is not None and peer_id is not None:
            history.mark_saved(peer_id, message.id, dest_dir.name, final_path)
    stats.saved += 1
    elapsed = max(time.monotonic() - started, 0.001)
    size_mib = final_path.stat().st_size / 1048576
    detail = (f"[下载完成] {final_path.name} | {size_mib:.1f} MiB | "
              f"{elapsed:.1f}秒 | {size_mib / elapsed:.2f} MiB/s | {mode}")
    if on_saved is not None:
        on_saved(detail)
    else:
        print(detail)
    return final_path


async def _download_with_retry(
    client: TelegramClient,
    message: Message,
    dest_dir: Path,
    base: str,
    stats: DownloadStats,
    *,
    skip_existing: bool,
    retries: int,
    chunk_concurrency: int = 1,
    history: DownloadHistory | None = None,
    peer_id: int | None = None,
    on_saved: Callable[[str], None] | None = None,
) -> Path | None:
    for attempt in range(retries + 1):
        try:
            return await _download_one(
                client, message, dest_dir, base,
                skip_existing=skip_existing, stats=stats,
                chunk_concurrency=chunk_concurrency,
                history=history, peer_id=peer_id,
                on_saved=on_saved,
            )
        except FloodWaitError:
            raise
        except Exception as exc:
            if attempt >= retries:
                stats.failed += 1
                print(f"Failed message {getattr(message, 'id', '?')}: {exc}")
                return None
            delay = min(2 ** attempt, 8)
            print(f"Retry message {getattr(message, 'id', '?')} "
                  f"({attempt + 1}/{retries}) in {delay}s: {exc}")
            await asyncio.sleep(delay)
    return None


async def download_many(
    client: TelegramClient,
    jobs: AsyncIterable[tuple[Message, Path, str]],
    *,
    stats: DownloadStats,
    concurrency: int = 3,
    retries: int = 2,
    skip_existing: bool = True,
    chunk_concurrency: int = 1,
    history: DownloadHistory | None = None,
    peer_id: int | None = None,
    on_saved: Callable[[str], None] | None = None,
    stop_event: threading.Event | None = None,
) -> list[Path]:
    """Bounded multi-file concurrency (not parallel chunks of one video)."""
    if not 1 <= concurrency <= 8:
        raise ValueError("concurrency must be between 1 and 8")
    if not 0 <= retries <= 5:
        raise ValueError("retries must be between 0 and 5")
    if not 1 <= chunk_concurrency <= 8:
        raise ValueError("chunk_concurrency must be between 1 and 8")
    pending: set[asyncio.Task[Path | None]] = set()
    saved: list[Path] = []

    async def collect_one() -> None:
        nonlocal pending
        finished, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED
        )
        for task in finished:
            path = task.result()  # propagate FloodWaitError; stop remaining work
            if path is not None:
                saved.append(path)

    try:
        async for message, dest_dir, base in jobs:
            while len(pending) >= concurrency:
                await collect_one()
            if stop_event is not None and stop_event.is_set():
                break
            pending.add(
                asyncio.create_task(
                    _download_with_retry(
                        client, message, dest_dir, base, stats,
                        skip_existing=skip_existing, retries=retries,
                        chunk_concurrency=chunk_concurrency,
                        history=history, peer_id=peer_id,
                        on_saved=on_saved,
                    )
                )
            )
        while pending:
            await collect_one()
    finally:
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    return saved
