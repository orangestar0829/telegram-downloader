from __future__ import annotations

import asyncio
import errno
import os
import re
import sqlite3
import stat
import tempfile
import time
import threading
import unicodedata
from contextlib import closing
from collections.abc import AsyncIterable
from collections.abc import Callable, Set
from datetime import datetime, timezone
from glob import escape as glob_escape
from pathlib import Path

from telethon import TelegramClient, utils
from telethon.errors import (
    ChannelPrivateError,
    FloodPremiumWaitError,
    FloodWaitError,
    UserAlreadyParticipantError,
    UsernameNotOccupiedError,
)
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import (
    Channel,
    Chat,
    ChatEmpty,
    ChatForbidden,
    Document,
    DocumentAttributeFilename,
    Message,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
    PeerChat,
    PeerUser,
    Photo,
    PhotoCachedSize,
    PhotoSize,
    PhotoSizeProgressive,
    PhotoStrippedSize,
    User,
    UserEmpty,
    VideoSize,
)
from tqdm import tqdm

from tgdl.chunked import (
    ChunkDownloadStalled, chunkable_document, document_suffix, download_chunked,
)
from tgdl.monitor_flow import MonitorFlow, monitor_download_document
from tgdl.monitor_rpc import monitor_media_requests
from tgdl.cache_budget import optional_staging_reservation
from tgdl.errors import ChannelAccessError
from tgdl.history import DownloadHistory, PendingMedia
from tgdl.paths import data_dir
from tgdl.slots import SlotCoordinator
from tgdl.progress import ProgressCallback, TransferProgress, format_transfer_size
from tgdl.temp_cleanup import PEER_DOWNLOADS_DIR, is_link_or_reparse_point, own_temporary

PHOTO_TYPES = {"photo"}
VIDEO_TYPES = {"video", "animation"}
ALL_TYPES = PHOTO_TYPES | VIDEO_TYPES
_STAGED_PUBLISH_LOCK = asyncio.Lock()

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


def monitor_media(message: object, types: Set[str]) -> tuple[str, str] | None:
    """Return the same eligible kind and basename for scheduled and push discovery."""
    if not isinstance(message, Message) or message.date is None:
        return None
    kind = classify_message(message)
    if kind is None or kind not in types:
        return None
    return kind, f"{message.date.strftime('%Y%m%d')}_{message.id}"


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


def _media_variant(target: object) -> tuple[int | None, str | None]:
    """Return exact expected bytes and suffix for the payload Telethon will request."""
    media = getattr(target, "media", target)
    document = media if isinstance(media, Document) else getattr(media, "document", None)
    if document is not None:
        size = getattr(document, "size", None)
        return (size if isinstance(size, int) and size > 0 else None, document_suffix(document))

    photo = media if isinstance(media, Photo) else getattr(media, "photo", None)
    if not isinstance(photo, Photo):
        return None, None
    # Use the same selector as Telethon's download_media(thumb=None). An
    # attached VideoSize takes precedence over any JPEG and is served as MP4.
    try:
        selected = TelegramClient._get_thumb(photo.sizes + (photo.video_sizes or []), None)
        if isinstance(selected, VideoSize):
            return (selected.size if selected.size > 0 else None, ".mp4")
        if isinstance(selected, PhotoSizeProgressive):
            size = max(selected.sizes)
        elif isinstance(selected, PhotoCachedSize):
            size = len(selected.bytes)
        elif isinstance(selected, PhotoStrippedSize):
            size = len(utils.stripped_photo_to_jpg(selected.bytes))
        elif isinstance(selected, PhotoSize):
            size = selected.size
        else:
            return None, None
        return (size if size > 0 else None, ".jpg")
    except (AttributeError, IndexError, TypeError, ValueError):
        return None, None


def _regular_media_size(path: Path) -> int | None:
    """Never read linked/reparse-point files as proof of a completed transfer."""
    try:
        entry = path.lstat()
    except (FileNotFoundError, NotADirectoryError):
        return None
    if not stat.S_ISREG(entry.st_mode):
        return None
    if getattr(entry, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
        return None
    return entry.st_size


def _existing_media(dest_dir: Path, base: str, suffix: str, size: int) -> Path | None:
    # Include numbered files previously published when the original name was
    # occupied, but not other extensions or files only sharing a prefix.
    exact = dest_dir / f"{base}{suffix}"
    if _regular_media_size(exact) == size:
        return exact
    pattern = re.compile(rf"{re.escape(base)}(?:_[1-9][0-9]{{0,3}})?{re.escape(suffix)}",
                         re.IGNORECASE)
    return next((path for path in dest_dir.glob(f"{glob_escape(base)}*")
                 if pattern.fullmatch(path.name) and _regular_media_size(path) == size), None)


_NO_HARDLINK = {getattr(errno, name, errno.ENOSYS)
                for name in ("EXDEV", "EPERM", "ENOSYS", "ENOTSUP", "EOPNOTSUPP")}


def _publish_noclobber(source: Path, destination: Path) -> None:
    """Publish a complete adjacent file without replacing occupied paths."""
    try:
        os.link(source, destination)
    except FileExistsError:
        raise
    except (NotImplementedError, OSError) as exc:
        if (isinstance(exc, OSError) and exc.errno not in _NO_HARDLINK
                and getattr(exc, "winerror", None) not in {1, 50, 1314}):
            raise
        if os.name != "nt":
            raise OSError("This destination does not support safe media publication "
                          "without overwriting existing files") from exc
        # Windows rename refuses to replace an existing destination.
        source.rename(destination)


def _publish_media(source: Path, dest_dir: Path, base: str, suffix: str, *,
                   verified_size: int | None) -> tuple[Path, bool]:
    for number in range(10_000):
        stem = f"{base}_{number}" if number else base
        candidate = dest_dir / f"{stem}{suffix}"
        if verified_size is not None and _regular_media_size(candidate) == verified_size:
            return candidate, False
        if os.path.lexists(candidate):
            continue
        try:
            _publish_noclobber(source, candidate)
        except FileExistsError:
            # Another process just occupied it; do not overwrite its output.
            continue
        return candidate, True
    raise OSError(f"Cannot find a free media filename in {dest_dir} for {base}")


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


def download_peer_key(entity: object, *, account_id: str = "default") -> str:
    """Use Telegram's marked ID, and scope non-channel message IDs by account."""
    try:
        peer_id = utils.get_peer_id(entity)
    except TypeError:
        # A minimal resolved entity supplied by an offline client adapter.
        peer_id = int(entity.id)
    if isinstance(entity, (Chat, ChatForbidden, PeerChat, PeerUser, User)):
        return f"{safe_name(account_id)}_{peer_id}"
    return str(peer_id)


_FOLDER_INDEX = ".tgdl-channel-folders.sqlite3"
_PEER_FOLDER_NAME = re.compile(r"(.+?) - (.+)")
_RESERVED_FOLDER = re.compile(r"(?:CON|PRN|AUX|NUL|COM[1-9¹²³]|LPT[1-9¹²³])(?:\.|$)", re.I)


def _folder_label(title: str) -> str:
    label = safe_name(str(title)).rstrip(". ") or "channel"
    if (_RESERVED_FOLDER.match(label)
            or label.casefold() in {PEER_DOWNLOADS_DIR, _FOLDER_INDEX}):
        return f"_{label}"
    return label


def _legacy_label_key(label: str) -> str:
    # Older Windows releases sometimes used full-width equivalents for
    # punctuation. Ignore spacing around parentheses, not arbitrary spaces.
    label = safe_name(unicodedata.normalize("NFKC", label)).rstrip(". ").casefold()
    return re.sub(r"\s*([()])\s*", r"\1", label)


def _real_folders(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(p for p in root.iterdir() if not is_link_or_reparse_point(p) and p.is_dir())


def _legacy_folder_owners(folder: Path) -> set[int]:
    """Read the original destination ledger, not the legacy filename."""
    history = data_dir() / "tgdl_download_history.sqlite3"
    if not history.is_file():
        return set()
    prefixes = {p.as_posix().rstrip("/") + "/" for p in (folder, folder.absolute())}
    expression = "replace(original_path, char(92), '/')"
    with closing(sqlite3.connect(history.absolute().as_uri() + "?mode=ro",
                                 uri=True, timeout=10)) as db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='downloaded_media'").fetchone():
            return set()
        owners = set()
        for prefix in prefixes:
            for owner, original in db.execute(
                f"SELECT peer_id, original_path FROM downloaded_media "
                f"WHERE substr({expression}, 1, ?) = ? COLLATE NOCASE",
                (len(prefix), prefix),
            ):
                relative = original.replace(chr(92), "/")[len(prefix):]
                if relative.partition("/")[0].casefold() in {"photos", "videos"}:
                    owners.add(int(owner))
        return owners


def _select_peer_folder(root: Path, key: str, label: str, owned: set[str], peer: int) -> Path:
    namespace = root / PEER_DOWNLOADS_DIR
    if is_link_or_reparse_point(namespace):
        raise OSError(f"Refusing to use linked peer download namespace: {namespace}")
    peer_folders = _real_folders(namespace)
    prefix = f"{key} - "
    keyed = [p for p in peer_folders if p.name.startswith(prefix)]
    if keyed or "_" not in key:
        # Prefer the old flat directory when the preview already associated
        # this title with this ID (including titles the channel used before).
        # Never auto-claim a title shared by several known peer IDs.
        labels = {label, *(p.name[len(prefix):] for p in keyed)}
        label_keys = {_legacy_label_key(name) for name in labels}
        ambiguous = set()
        for path in peer_folders:
            m = _PEER_FOLDER_NAME.fullmatch(path.name)
            if m and m[1] != key:
                ambiguous.add(_legacy_label_key(m[2]))
        candidates = [
            p for p in _real_folders(root)
            if p.name != PEER_DOWNLOADS_DIR and p.name.casefold() not in owned
            and _legacy_label_key(p.name) in label_keys
        ]
        verified = []
        plausible = []
        for candidate in candidates:
            owners = _legacy_folder_owners(candidate)
            if owners == {peer} and "_" not in key:
                verified.append(candidate)
            elif not owners and keyed and _legacy_label_key(candidate.name) not in ambiguous:
                plausible.append(candidate)
        if len(verified) == 1:
            return verified[0]
        if not verified and len(plausible) == 1:
            return plausible[0]
    if keyed:
        return keyed[0]
    existing_names = {p.name.casefold() for p in root.iterdir()} if root.is_dir() else set()
    if label.casefold() not in owned | existing_names:
        return root / label
    for i in range(10_000):
        name = f"{label} - {key}" + (f"_{i}" if i else "")
        if name.casefold() not in owned | existing_names:
            return root / name
    raise OSError(f"Cannot find a free download folder for {key}")


def _check_download_folder(root: Path, folder: Path) -> None:
    if is_link_or_reparse_point(root) or (
        folder.parent != root and is_link_or_reparse_point(folder.parent)
    ) or any(is_link_or_reparse_point(path) for path in (
        folder, folder / "photos", folder / "videos", folder / "parsed",
    )):
        raise OSError(f"Refusing to use linked download folder: {folder}")


def peer_download_folder(download_dir: Path, title: str, entity: object, *,
                         account_id: str = "default", create: bool = True) -> Path:
    """Persist one folder per peer under a shared download root."""
    root = Path(download_dir)
    key = download_peer_key(entity, account_id=account_id)
    label = _folder_label(title)
    peer = getattr(entity, "id", getattr(entity, "channel_id", None))
    ledger_peer = int(peer) if peer is not None else 0
    index = root / _FOLDER_INDEX
    if is_link_or_reparse_point(root) or is_link_or_reparse_point(index):
        raise OSError(f"Refusing to use linked download directory or index: {root}")
    if not create and not index.is_file():
        folder = _select_peer_folder(root, key, label, set(), ledger_peer)
        _check_download_folder(root, folder)
        return folder

    if create:
        root.mkdir(parents=True, exist_ok=True)
    # BEGIN IMMEDIATE serializes the first assignment even across processes.
    location = str(index) if create else index.absolute().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(location, uri=not create, timeout=10)) as db, db:
        if create:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""CREATE TABLE IF NOT EXISTS folders (
                peer_key TEXT PRIMARY KEY, relative_path TEXT NOT NULL UNIQUE
            )""")
        has_table = create or db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='folders'"
        ).fetchone()
        bindings = dict(db.execute("SELECT peer_key, relative_path FROM folders")) if has_table else {}
        relative = bindings.get(key)
        if relative is None:
            owned = {Path(path).parts[0].casefold() for path in bindings.values() if Path(path).parts}
            folder = _select_peer_folder(root, key, label, owned, ledger_peer)
        else:
            relative = Path(relative)
            parts = relative.parts
            if relative.anchor or not parts or any(
                p in (".", "..") or ":" in p or "\\" in p for p in parts
            ) or (
                len(parts) != 1 and (len(parts) != 2 or parts[0] != PEER_DOWNLOADS_DIR
                                     or not parts[1].startswith(f"{key} - "))
            ):
                raise ValueError(f"Invalid saved download directory for {key}")
            folder = root / relative
        _check_download_folder(root, folder)
        if create:
            folder.mkdir(parents=True, exist_ok=True)
            if relative is None:
                db.execute("INSERT INTO folders VALUES (?, ?)", (key, str(folder.relative_to(root))))
        return folder


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    for i in range(1, 10_000):
        candidate = path.with_name(f"{stem}_{i}{suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Cannot find a free filename for {path}")


def same_volume(source: Path, destination_directory: Path) -> bool:
    """Decide whether publishing can be an atomic rename."""
    if os.name == "nt":
        return source.anchor.casefold() == destination_directory.anchor.casefold()
    return source.stat().st_dev == destination_directory.stat().st_dev


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
        entity = await client.get_entity(int(target) if _looks_like_id(target) else target)
    except (ChannelPrivateError, UsernameNotOccupiedError, ValueError) as exc:
        raise ChannelAccessError(
            f"Cannot access '{target}'. Join the private channel with this account first, "
            "then pass the numeric id from `python -m tgdl list`."
        ) from exc
    if isinstance(entity, (User, UserEmpty, PeerUser)):
        raise ChannelAccessError("Choose a Telegram group or channel, not a user account.")
    return entity


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
        self.queued = 0
        self.scanned_messages = 0


class DownloadLeaseLost(RuntimeError):
    """A monitor transfer no longer owns the attempt that started it."""


class UnsupportedSharedMonitorPeer(ValueError):
    """The shared ledger requires a message counter common to all accounts."""


def history_peer_id(entity) -> int:
    """Separate Telegram Chat IDs from Channel IDs without rewriting old channels."""
    raw_id = int(entity.id)
    return -raw_id if isinstance(entity, (Chat, ChatEmpty, ChatForbidden)) else raw_id


def require_shared_monitor_peer(entity):
    # Only channel entities (broadcast channels and supergroups) have stable
    # message IDs across accounts. Basic groups have account-local message IDs.
    if not isinstance(entity, Channel):
        raise UnsupportedSharedMonitorPeer(
            "额外账号仅支持监控频道或超级群；普通群请用原账号监控"
        )


async def scan_monitor_slice(
    client: TelegramClient, *,
    target: str,
    types: set[str],
    monitor_direction: str = "newest",
    slice_size: int = 20,
    match_limit: int = 20,
    account_id: str = "default",
    stop_event: threading.Event | None = None,
    on_channel_resolved: Callable[[str, int], None] | None = None,
) -> DownloadStats:
    """Scan only a small Telegram message slice, without downloading files.

    SQLite commits the queued media and the scan cursor together, so a crash
    cannot skip discovered media. The 20-message slice yields to other
    monitored channels even when this channel has a 500-media batch quota.
    """
    if monitor_direction not in ("newest", "oldest"):
        raise ValueError("Invalid monitor direction")
    if slice_size < 1 or match_limit < 1:
        raise ValueError("Monitor slice and match limits must be positive")
    entity = await resolve_channel(client, target)
    if account_id != "default":
        require_shared_monitor_peer(entity)
    title = str(getattr(entity, "title", None) or getattr(entity, "username", None) or entity.id)
    peer_id = history_peer_id(entity)
    if on_channel_resolved is not None:
        on_channel_resolved(title, peer_id)
    stats = DownloadStats()
    history = DownloadHistory()
    try:
        completed, upper, cursor, saved_direction = history.scan_state(peer_id, account_id=account_id)
        media_types = ",".join(sorted(types))
        previous_types = history.scan_media_types(peer_id, account_id=account_id)
        if previous_types not in ("", media_types) or (previous_types == "" and completed):
            completed, upper, cursor = 0, 0, 0
        elif upper and saved_direction != monitor_direction:
            cursor = 0

        starting_cycle = not upper
        if starting_cycle:
            # Old failures from the pre-queue monitor also remain retryable.
            failures = [
                PendingMedia(peer_id, mid, target, title, "", "")
                for mid in history.failures(peer_id, limit=20)
            ]
            stats.queued += history.enqueue_media(failures)
            async for latest in client.iter_messages(entity, limit=1):
                upper = int(latest.id)
                break
            if upper <= completed:
                stats.scan_completed = completed
                return stats
            history.save_scan_state(peer_id, completed, upper, 0, monitor_direction, media_types,
                                    account_id=account_id)

        stats.scan_upper = upper
        stats.scan_completed = completed
        if monitor_direction == "oldest":
            iterator = client.iter_messages(
                entity, reverse=True, min_id=cursor or completed,
                max_id=upper + 1, limit=slice_size,
            )
        else:
            iterator = client.iter_messages(
                entity, min_id=completed,
                max_id=cursor or (upper + 1), limit=slice_size,
            )

        discovered: list[PendingMedia] = []
        last_seen = 0
        quota_reached = False
        interrupted = False
        async for message in iterator:
            if stop_event is not None and stop_event.is_set():
                interrupted = True
                break
            stats.scanned_messages += 1
            last_seen = int(message.id)
            if not stats.first_message_id:
                stats.first_message_id = last_seen
            stats.last_message_id = last_seen
            media = monitor_media(message, types)
            if media is None:
                continue
            kind, base = media
            stats.matched += 1
            if history.contains(peer_id, message.id):
                stats.skipped += 1
            else:
                discovered.append(PendingMedia(
                    peer_id, int(message.id), target, title, kind, base,
                ))
            if stats.matched >= match_limit:
                quota_reached = True
                break

        if interrupted:
            # Leave the old cursor intact; all work in this slice is replayable.
            stats.more_pending = True
            return stats
        completed_window = not quota_reached and stats.scanned_messages < slice_size
        stats.queued += history.commit_scan_batch(
            peer_id, discovered,
            completed=upper if completed_window else completed,
            upper=0 if completed_window else upper,
            cursor=0 if completed_window else (last_seen or cursor),
            direction=monitor_direction, media_types=media_types, account_id=account_id,
        )
        stats.more_pending = not completed_window
        if completed_window:
            stats.scan_completed = upper
        return stats
    finally:
        history.close()


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
    temp_download_dir: Path | None = None,
    on_progress: ProgressCallback | None = None,
    gui_slots: SlotCoordinator | None = None,
) -> DownloadStats:
    entity = await resolve_channel(client, target)
    title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(entity.id)
    if on_channel_resolved is not None:
        on_channel_resolved(str(title), history_peer_id(entity))
    def saved_with_source(detail: str) -> None:
        labeled = f"[{title}] {detail}"
        if on_saved is not None:
            on_saved(labeled)
        else:
            print(labeled)
    folder = peer_download_folder(download_dir, title, entity, create=not dry_run)
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
    peer_id = history_peer_id(entity)
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
                    temp_download_dir=temp_download_dir,
                    on_progress=on_progress,
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
                temp_download_dir=temp_download_dir,
                on_progress=on_progress, gui_slots=gui_slots,
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
    temp_download_dir: Path | None = None,
    on_progress: ProgressCallback | None = None,
    monitor_flow: MonitorFlow | None = None,
    account_id: str | None = None,
    claim_token: str | None = None,
    on_claim_committed: Callable[[], None] | None = None,
) -> Path | None:
    has_claim = account_id is not None or claim_token is not None
    if has_claim and (history is None or peer_id is None or not account_id or not claim_token):
        raise ValueError("Monitor downloads require history, peer, account and claim token")

    def require_claim() -> None:
        if has_claim:
            assert history is not None and peer_id is not None and account_id and claim_token
            if not history.owns_claim(account_id, peer_id, message.id, token=claim_token):
                raise DownloadLeaseLost(f"Monitor lease no longer owns message {message.id}")

    def record_claimed(
        publish: Callable[[], tuple[Path, bool] | None],
    ) -> tuple[Path, bool] | None:
        assert history is not None and peer_id is not None and account_id and claim_token
        result = history.publish_claimed_media(
            peer_id, message.id, account_id, claim_token, dest_dir.name, publish,
        )
        if result is not None and on_claim_committed is not None:
            # The renewer must know this worker consumed its own claim before
            # async cache-reservation cleanup gives another task a turn.
            on_claim_committed()
        return result

    require_claim()
    if skip_existing and history is not None and peer_id is not None:
        if history.contains(peer_id, message.id):
            stats.skipped += 1
            return None
    target = download_target(message)
    expected_size, expected_suffix = _media_variant(target)
    if skip_existing and expected_size is not None and expected_suffix is not None:
        existing = _existing_media(dest_dir, base, expected_suffix, expected_size)
        if existing is not None:
            if has_claim:
                checked_existing = False

                def recheck_existing() -> tuple[Path, bool] | None:
                    nonlocal checked_existing
                    checked_existing = True
                    # The candidate may change between discovery and acquiring
                    # the DB write lock. Never record it if no longer valid.
                    if _regular_media_size(existing) != expected_size:
                        return None
                    return existing, False

                reused = record_claimed(recheck_existing)
                if reused is not None:
                    stats.skipped += 1
                    return None
                if not checked_existing:
                    raise DownloadLeaseLost(f"Monitor lease could not record message {message.id}")
                # Candidate changed; download the media instead without
                # changing or releasing the lease.
            else:
                if history is not None and peer_id is not None:
                    history.mark_saved(peer_id, message.id, dest_dir.name, existing)
                stats.skipped += 1
                return None
    dest_dir.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    flood_snapshot = getattr(client, "flood_snapshot", None)
    flood_before = flood_snapshot() if callable(flood_snapshot) else None
    rpc_snapshot = getattr(client, "rpc_snapshot", None)
    rpc_before = rpc_snapshot() if callable(rpc_snapshot) else None
    mode = "普通下载"
    chunk_timings: dict[str, float] = {}
    publish_seconds = 0.0
    document = chunkable_document(
        target, max(2, chunk_concurrency) if monitor_flow is not None else chunk_concurrency
    )
    use_chunks = document is not None and callable(getattr(client, "iter_download", None))
    transfer = TransferProgress(
        f"{peer_id or 0}:{message.id}:{base}",
        f"{dest_dir.parent.name} / {base}",
        on_progress,
    )
    transfer.emit("等待缓存" if use_chunks and temp_download_dir is not None else "准备下载",
                  total=expected_size, force=True)
    temp_parent = dest_dir
    if use_chunks and temp_download_dir is not None:
        candidate = Path(temp_download_dir).expanduser()
        candidate.mkdir(parents=True, exist_ok=True)
        temp_parent = candidate
    # Per-task directories keep failed/cancelled partial files isolated.
    async with optional_staging_reservation(
        temp_parent if use_chunks and temp_download_dir is not None else None,
        int(document.size) if use_chunks else 0,
    ) as cache_lease:
        transfer.emit("准备文件")
        with tempfile.TemporaryDirectory(prefix="tgdl-", dir=temp_parent) as temporary:
            with own_temporary(Path(temporary)):
                if cache_lease is not None:
                    cache_lease.track(Path(temporary))
                media_started = time.monotonic()
                governor = monitor_flow.limiter.auto_throttle if monitor_flow is not None else None
                with (
                    monitor_media_requests(client, governor),
                    tqdm(unit="B", unit_scale=True, desc=base, leave=False) as bar,
                ):
                    def progress(current: int, total: int) -> None:
                        bar.total = total or bar.total
                        bar.n = current
                        bar.refresh()
                        transfer.emit("下载中", done=current, total=total)

                    last_monitored_bytes = 0
                    async def monitored_progress(current: int, total: int) -> None:
                        nonlocal last_monitored_bytes
                        if monitor_flow is not None:
                            await monitor_flow.pause.checkpoint()
                            await monitor_flow.limiter.consume(
                                max(0, current - last_monitored_bytes),
                                pause=monitor_flow.pause,
                            )
                        last_monitored_bytes = current
                        progress(current, total)

                    async def monitored_direct_download(file: str) -> Path | str | None:
                        if monitor_flow is not None:
                            monitor_flow.pause.register()
                        try:
                            if monitor_flow is not None:
                                await monitor_flow.pause.checkpoint()
                            return await client.download_media(
                                target, file=file,
                                progress_callback=monitored_progress if monitor_flow is not None else progress,
                            )
                        finally:
                            if monitor_flow is not None:
                                monitor_flow.pause.unregister()

                    if use_chunks and monitor_flow is not None:
                        chunk_output = Path(temporary) / ("data" + document_suffix(document))
                        try:
                            saved = await monitor_download_document(
                                client, document, chunk_output, monitor_flow,
                                on_bytes=lambda n: (bar.update(n), transfer.advance(n)),
                            )
                            mode = "监控动态分块"
                        except ChunkDownloadStalled as exc:
                            # Preserve the existing fallback if Telegram never
                            # completes a window despite bounded retries.
                            print(f"[监控分块] {base} 请求停顿：{exc}；回退普通下载")
                            chunk_output.unlink(missing_ok=True)
                            saved = await monitored_direct_download(
                                str(Path(temporary) / ("normal" + document_suffix(document)))
                            )
                            mode = "监控普通回退"
                    elif use_chunks:
                        chunk_output = Path(temporary) / ("data" + document_suffix(document))
                        try:
                            saved = await download_chunked(
                                client, document, chunk_output,
                                parts=chunk_concurrency, on_bytes=lambda n: (
                                    bar.update(n), transfer.advance(n)
                                ),
                                timings=chunk_timings,
                                diagnostic_label=f"{dest_dir.parent.name}/{base}",
                            )
                        except ChunkDownloadStalled as exc:
                            chunk_timings.clear()
                            transfer.emit("普通下载回退", done=0, force=True)
                            print(f"[分块] {base} 超时/失败: {exc}; 自动退回普通下载")
                            # The chunked file is preallocated at full size.
                            # Delete it BEFORE normal fallback, so SSD staging
                            # never needs space for two full copies of one media.
                            chunk_output.unlink(missing_ok=True)
                            saved = await monitored_direct_download(
                                str(Path(temporary) / ("normal" + document_suffix(document)))
                            )
                        else:
                            mode = f"分块{chunk_concurrency}路"
                    else:
                        file = Path(temporary)
                        if expected_suffix is not None:
                            file /= "normal" + expected_suffix
                        saved = await monitored_direct_download(str(file))
                media_elapsed = max(time.monotonic() - media_started, 0.001)
                require_claim()
                if not saved:
                    stats.failed += 1
                    transfer.emit("失败", force=True)
                    return None
                saved_path = Path(saved)
                size = _regular_media_size(saved_path)
                # When the variant has no exact size metadata, use the actual
                # direct transfer's authoritative callback total if available.
                transfer_size = expected_size if expected_size is not None else transfer.total or None
                if size is None or size <= 0:
                    raise IOError("Telegram download did not produce a non-empty regular file")
                if transfer_size is not None and size != transfer_size:
                    raise IOError(f"Telegram media size mismatch: {size} != {transfer_size}")
                suffix = saved_path.suffix or ".bin"
                verified_size = (expected_size if skip_existing and expected_suffix
                                 and suffix.lower() == expected_suffix.lower() else None)
                def publish_ready(source: Path) -> tuple[Path, bool]:
                    def publish() -> tuple[Path, bool]:
                        return _publish_media(
                            source, dest_dir, base, suffix, verified_size=verified_size,
                        )
                    if not has_claim:
                        return publish()
                    # The final link/rename and ledger write run without awaits
                    # under the same SQLite lease fence. A stale attempt cannot
                    # publish even if ownership changed while copying.
                    result = record_claimed(publish)
                    if result is None:
                        raise DownloadLeaseLost(f"Monitor lease could not publish message {message.id}")
                    return result

                publish_started = time.monotonic()
                transfer.emit("正在保存", force=True)
                # Publishing within a volume does not copy media bytes.
                if not same_volume(saved_path, dest_dir):
                    # On cross-volume moves, first copy sequentially to a hidden
                    # incomplete destination, and publish only after full verification.
                    # Never interleave large file copies on a mechanical disk:
                    # one sequential publishing pass at a time.
                    async with _STAGED_PUBLISH_LOCK:
                        require_claim()
                        with tempfile.TemporaryDirectory(prefix="tgdl-publish-", dir=dest_dir) as publishing:
                            with own_temporary(Path(publishing)):
                                partial = Path(publishing) / f"data{suffix}"
                                with saved_path.open("rb") as source, partial.open("xb") as sink:
                                    while True:
                                        data = source.read(4 * 1024 * 1024)
                                        if not data:
                                            break
                                        sink.write(data)
                                        await asyncio.sleep(0)
                                if _regular_media_size(partial) != size:
                                    raise IOError("Staged media copy size mismatch")
                                final_path, published = publish_ready(partial)
                else:
                    final_path, published = publish_ready(saved_path)
                publish_seconds = time.monotonic() - publish_started
                if not has_claim and history is not None and peer_id is not None:
                    history.mark_saved(peer_id, message.id, dest_dir.name, final_path)
                if not published:
                    stats.skipped += 1
                    transfer.emit("已跳过", force=True)
                    return None
    stats.saved += 1
    transfer.emit("完成", force=True)
    elapsed = max(time.monotonic() - started, 0.001)
    size_bytes = final_path.stat().st_size
    size_mib = size_bytes / 1048576
    detail = (f"[下载完成] {final_path.name} | {format_transfer_size(size_bytes)} | "
              f"{elapsed:.1f}秒 | 全程均速{size_mib / elapsed:.2f} MiB/s | {mode}"
              f" | 传输阶段{media_elapsed:.1f}秒，均速{size_mib / media_elapsed:.2f} MiB/s")
    if flood_before is not None and callable(flood_snapshot):
        flood_after = flood_snapshot()
        changes = []
        for kind in ("FLOOD_WAIT", "FLOOD_PREMIUM_WAIT"):
            prev = flood_before.get(kind, (0, 0))
            new = flood_after.get(kind, (0, 0))
            count, seconds = new[0] - prev[0], new[1] - prev[1]
            if count:
                changes.append(f"{kind}={count}次/{seconds}s")
        if changes:
            detail += " | 同期全局限流响应：" + "，".join(changes) + "（并发任务可能重叠，非实际总停顿）"
    if rpc_before is not None and callable(rpc_snapshot):
        rpc_after = rpc_snapshot()
        changes = {
            kind: count - rpc_before.get(kind, 0)
            for kind, count in rpc_after.items()
            if count > rpc_before.get(kind, 0)
        }
        if changes:
            total = sum(changes.values())
            classes = "，".join(f"{kind}={count}" for kind, count in sorted(changes.items()))
            detail += (
                f" | 同期全局GetFile RPC异常{total}次（{classes}），"
                f"名义重试等待约{total * 2}s"
                "（并发或内部重试可重叠，非实际阻塞时间）"
            )
    if chunk_timings:
        detail += (
            f" | 文件准备{chunk_timings.get('prepare_s', 0):.2f}秒"
            f" / 分块传输{chunk_timings.get('transfer_s', 0):.2f}秒"
            f" / 最终保存{publish_seconds:.2f}秒"
        )
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
    temp_download_dir: Path | None = None,
    on_progress: ProgressCallback | None = None,
    monitor_flow: MonitorFlow | None = None,
    account_id: str | None = None,
    claim_token: str | None = None,
    on_claim_committed: Callable[[], None] | None = None,
) -> Path | None:
    for attempt in range(retries + 1):
        try:
            if monitor_flow is not None and monitor_flow.pause.requested:
                # A pause requested during the last RPC can outlive a failed
                # media attempt. Register the retry's entry checkpoint so it
                # cannot fetch again after yielding its permit.
                monitor_flow.pause.register()
                try:
                    await monitor_flow.pause.checkpoint()
                finally:
                    monitor_flow.pause.unregister()
            return await _download_one(
                client, message, dest_dir, base,
                skip_existing=skip_existing, stats=stats,
                chunk_concurrency=chunk_concurrency,
                history=history, peer_id=peer_id,
                on_saved=on_saved,
                temp_download_dir=temp_download_dir,
                on_progress=on_progress,
                monitor_flow=monitor_flow,
                account_id=account_id,
                claim_token=claim_token,
                on_claim_committed=on_claim_committed,
            )
        except (asyncio.CancelledError, DownloadLeaseLost):
            if on_progress is not None:
                on_progress(f"{peer_id or 0}:{message.id}:{base}",
                            f"{dest_dir.parent.name} / {base}", 0, 0, "已取消")
            raise
        except Exception as exc:
            if isinstance(exc, FloodWaitError) or (
                monitor_flow is not None and isinstance(exc, FloodPremiumWaitError)
            ):
                # Both raw wait classes can escape Telethon's auto-sleep
                # threshold. Don't turn a monitor wait into a short file retry.
                if on_progress is not None:
                    on_progress(f"{peer_id or 0}:{message.id}:{base}",
                                f"{dest_dir.parent.name} / {base}", 0, 0, "限流等待")
                raise
            if attempt >= retries:
                stats.failed += 1
                if on_progress is not None:
                    on_progress(f"{peer_id or 0}:{message.id}:{base}",
                                f"{dest_dir.parent.name} / {base}", 0, 0, "失败")
                print(f"Failed message {getattr(message, 'id', '?')}: {exc}")
                return None
            delay = min(2 ** attempt, 8)
            if on_progress is not None:
                on_progress(f"{peer_id or 0}:{message.id}:{base}",
                            f"{dest_dir.parent.name} / {base}", 0, 0, "重试等待")
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
    temp_download_dir: Path | None = None,
    on_progress: ProgressCallback | None = None,
    gui_slots: SlotCoordinator | None = None,
    gallery_history: DownloadHistory | None = None,
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

    waiting_for_capacity = False

    async def transfer(message, dest_dir, base):
        nonlocal waiting_for_capacity
        token = None
        ledger = history
        try:
            if gui_slots is not None:
                key = (peer_id, message.id) if peer_id is not None else None
                while True:
                    if stop_event is not None and stop_event.is_set():
                        return None
                    # A cooperatively paused monitor can own the SQLite lease
                    # without occupying a slot. Don't wait on its own file:
                    # the ordinary scheduler yields to manual jobs.
                    coordination_ledger = history if history is not None else gallery_history
                    if (coordination_ledger is not None and peer_id is not None and
                            coordination_ledger.has_live_claim(peer_id, message.id)):
                        stats.skipped += 1
                        return None
                    workers = gui_slots.account_limit("default", concurrency)
                    token = gui_slots.acquire("default", workers, file_key=key)
                    if token is not None:
                        if waiting_for_capacity:
                            print("[手动下载] 普通名额已释放，继续下载。")
                            waiting_for_capacity = False
                        if gallery_history is not None and peer_id in gui_slots.reserved_peers():
                            ledger = gallery_history
                        break
                    if gui_slots.ordinary_capacity("default", workers) == 0 and not waiting_for_capacity:
                        print("[手动下载] 正在等待普通名额：实时频道已占满本账号名额。"
                              "可暂停一个实时频道、提高并发或取消本次手动下载。")
                        waiting_for_capacity = True
                    await asyncio.sleep(0.1)
            path = await _download_with_retry(
                client, message, dest_dir, base, stats,
                skip_existing=skip_existing, retries=retries,
                chunk_concurrency=chunk_concurrency, history=ledger, peer_id=peer_id,
                on_saved=on_saved, temp_download_dir=temp_download_dir, on_progress=on_progress,
            )
            if (path is not None and gallery_history is not None and ledger is None
                    and gui_slots is not None and peer_id in gui_slots.reserved_peers()):
                # Monitoring was enabled while this gallery file was in flight.
                gallery_history.mark_saved(peer_id, message.id, dest_dir.name, path)
            return path
        finally:
            if token is not None:
                gui_slots.release(token)

    try:
        async for message, dest_dir, base in jobs:
            while len(pending) >= concurrency:
                await collect_one()
            if stop_event is not None and stop_event.is_set():
                break
            pending.add(asyncio.create_task(transfer(message, dest_dir, base)))
        while pending:
            await collect_one()
    finally:
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    return saved
