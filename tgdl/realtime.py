"""Receive new messages from joined Telegram channels on an existing session."""
from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice

from telethon import events, utils
from telethon.errors import (
    ChannelInvalidError, ChannelPrivateError, FloodPremiumWaitError, FloodWaitError,
    InviteHashExpiredError, InviteHashInvalidError, UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.tl.types import Channel, ChatInviteAlready, Message, PeerChannel

from tgdl.downloader import INVITE_RE, classify_message
from tgdl.errors import ChannelAccessError
from tgdl.history import DownloadHistory, PendingMedia

_CATCH_UP_BATCH = 32
_GAP_SETTLE_SECONDS = 60
_MAX_PUSH_BUFFER = 256
_PUSH_BATCH_SIZE = 32
_UNAVAILABLE = (
    ChannelInvalidError, ChannelPrivateError, InviteHashExpiredError,
    InviteHashInvalidError, UsernameInvalidError, UsernameNotOccupiedError,
)
_RESOLVE_ERRORS = _UNAVAILABLE + (ValueError, KeyError)
_AUTH_ERRORS = _UNAVAILABLE + (ChannelAccessError,)


def _require_joined(entity) -> Channel:
    if not isinstance(entity, Channel) or entity.left or entity.min:
        raise ChannelAccessError("实时监控仅支持本账号已加入的频道或超级群。")
    if entity.noforwards or entity.restricted:
        raise ChannelAccessError("频道有限制，不支持实时下载。")
    return entity


async def resolve_realtime_peer(client, target: str, *, expected_peer_id: int | None = None) -> Channel:
    """Resolve a joined channel without ever invoking Telegram's join APIs."""
    try:
        # Once configured, the canonical peer is the identity. Resolving it
        # by ID also works after a username change or an invite link expires.
        if expected_peer_id is not None:
            try:
                entity = await client.get_entity(PeerChannel(expected_peer_id))
            except (ValueError, KeyError):
                pass  # Not yet in this session's entity cache: try its alias.
            else:
                if int(getattr(entity, "id", 0)) != expected_peer_id:
                    raise ChannelAccessError("实时监控频道与原频道不一致。")
                return _require_joined(entity)

        target = target.strip()
        parsed, invite = utils.parse_username(target)
        explicit_invite = INVITE_RE.search(target)
        if explicit_invite or invite:
            result = await client(CheckChatInviteRequest(
                explicit_invite.group(1) if explicit_invite else parsed
            ))
            if not isinstance(result, ChatInviteAlready):
                raise ChannelAccessError("请先用该账号加入频道，再启用实时监控。")
            entity = result.chat
        elif target.lstrip("-").isdigit():
            number = int(target)
            entity = await client.get_entity(PeerChannel(number) if number > 0 else number)
        else:
            entity = await client.get_entity(target)
        entity = _require_joined(entity)
        if expected_peer_id is not None and entity.id != expected_peer_id:
            raise ChannelAccessError("实时监控频道与原频道不一致。")
        return entity
    except ChannelAccessError:
        raise
    except _RESOLVE_ERRORS as exc:
        raise ChannelAccessError("无法读取频道，请确认该账号仍在频道内。") from exc


@dataclass(frozen=True)
class _Config:
    peer: int
    target: str
    types: frozenset[str]
    epoch: str
    backfill: bool
    since: int


@dataclass(eq=False)
class _Subscription:
    config: _Config
    item: dict
    ready: bool = False
    generation: int = 0
    highest_event: int = 0
    cursor: int | None = None
    title: str = ""
    entity: Channel | None = None
    subscribed_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc).replace(microsecond=0)
    )
    # Store only the ID, eligible media kind and generated filename base.
    # Non-media/disabled types use None but still count as handled IDs.
    pushes: dict[int, tuple[str, str] | None] = field(default_factory=dict)
    flush_task: asyncio.Task | None = None
    change: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None


class RealtimeMonitor:
    """An account's push listener and bounded, cursor-backed catch-up.

    update() is called on the client's asyncio loop with the admitted channel
    settings; it doesn't access Telegram synchronously. Everything is torn
    down by close() before the owning worker closes its SQLite connection.
    """

    def __init__(self, client, history: DownloadHistory, account_id: str, *,
                 log: Callable[[str], None],
                 is_active: Callable[[], bool] | None = None,
                 is_current: Callable[[dict], bool] | None = None,
                 on_queued: Callable[[], None] | None = None) -> None:
        self.client = client
        self.history = history
        self.account_id = account_id
        self.log = log
        self.is_active = is_active or (lambda: True)
        self.on_queued = on_queued
        self.is_current = is_current or (lambda item: True)
        self._peers: dict[int, _Subscription] = {}
        self._tasks: set[asyncio.Task] = set()
        self._registered = False
        self._closed = False
        self._disconnected = False
        self._loss_task: asyncio.Task | None = None
        self._handler = self._on_message
        self._reconnect_handler = self._on_reconnect

    @property
    def ready_peers(self) -> frozenset[int]:
        return frozenset(
            peer for peer, sub in self._peers.items() if sub.ready and self._current(sub)
        )

    def _register(self) -> None:
        if self._registered:
            return
        self.client.add_event_handler(self._handler, events.NewMessage())
        reconnect = getattr(self.client, "add_reconnect_callback", None)
        if reconnect is not None:
            reconnect(self._reconnect_handler)
        self._registered = True
        disconnected = getattr(self.client, "disconnected", None)
        if disconnected is not None:
            self._loss_task = self._track(self._watch_disconnect(disconnected))

    def _track(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _watch_disconnect(self, disconnected) -> None:
        try:
            await asyncio.shield(disconnected)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass  # Terminal transport error; do not reconnect a shared session here.
        if self._registered and self.is_active():
            self._disconnected = True
            for sub in self._peers.values():
                sub.ready = False
                sub.entity = None
                sub.generation += 1
                if sub.task is not None:
                    sub.task.cancel()
                if sub.flush_task is not None:
                    sub.flush_task.cancel()
            self.log("[实时监控] Telegram 连接已断开，请重新启动下载器，并启动该账号监控。")

    def _unregister(self) -> None:
        if not self._registered:
            return
        self.client.remove_event_handler(self._handler)
        reconnect = getattr(self.client, "remove_reconnect_callback", None)
        if reconnect is not None:
            reconnect(self._reconnect_handler)
        self._registered = False
        if self._loss_task is not None:
            self._loss_task.cancel()
            self._loss_task = None

    def update(self, items: Iterable[dict]) -> None:
        if self._closed:
            return
        wanted: dict[int, tuple[_Config, dict]] = {}
        if self._disconnected and self.client.is_connected():
            # A shared owner has explicitly reconnected the same client.
            # Re-subscribe against the current transport and durable cursor.
            self._unregister()
            self._disconnected = False
        if self.is_active() and not self._disconnected:
            for item in items:
                peer = item.get("peer_id")
                if (not isinstance(peer, int) or peer <= 0 or not item.get("enabled", True)
                        or item.get("mode") != "realtime"):
                    continue
                kinds = frozenset(
                    x.strip() for x in item.get("types", "photo,video").split(",") if x.strip()
                )
                wanted.setdefault(peer, (_Config(
                    peer, item["target"], kinds, str(item.get("realtime_epoch") or ""),
                    bool(item.get("realtime_backfill", False)),
                    item["realtime_since"] if type(item.get("realtime_since")) is int else 0,
                ), dict(item)))
        for peer, sub in list(self._peers.items()):
            if peer not in wanted or wanted[peer][0] != sub.config:
                del self._peers[peer]
                if sub.task is not None:
                    sub.task.cancel()
                if sub.flush_task is not None:
                    sub.flush_task.cancel()
            else:
                sub.item = wanted[peer][1]
                if sub.task is None or sub.task.done():
                    sub.task = self._track(self._run(sub))
        if wanted:
            self._register()  # Install before the initial history fetch.
        else:
            self._unregister()
        for peer, (config, item) in wanted.items():
            if peer not in self._peers:
                sub = self._peers[peer] = _Subscription(config, item)
                sub.task = self._track(self._run(sub))

    async def close(self) -> None:
        self.update([])
        self._closed = True
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def _current(self, sub: _Subscription) -> bool:
        return (self._peers.get(sub.config.peer) is sub and self.is_active()
                and not self._disconnected and self.is_current(sub.item))

    def _on_reconnect(self) -> None:
        for sub in self._peers.values():
            sub.ready = False
            sub.entity = None  # Recheck membership and restrictions.
            sub.generation += 1
            sub.change.set()

    def _message_media(self, sub: _Subscription, message) -> tuple[str, str] | None:
        if (not isinstance(message, Message) or message.date is None
                or message.noforwards or not isinstance(message.peer_id, PeerChannel)
                or message.peer_id.channel_id != sub.config.peer):
            return None
        kind = classify_message(message)
        if kind is None or kind not in sub.config.types:
            return None
        return kind, f"{message.date.strftime('%Y%m%d')}_{message.id}"

    @staticmethod
    def _pending(sub: _Subscription, message_id: int, media: tuple[str, str]) -> PendingMedia:
        kind, base = media
        return PendingMedia(sub.config.peer, message_id, sub.config.target, sub.title, kind, base)

    def _commit(self, sub: _Subscription, completed: int, items: list[PendingMedia], *,
                observed: set[int] | None = None) -> None:
        if not self._current(sub):
            return
        cfg = sub.config
        count, sub.cursor = self.history.commit_realtime(
            self.account_id, cfg.peer, cfg.epoch, completed, items, observed=observed,
        )
        if count and self.on_queued is not None:
            self.on_queued()

    @staticmethod
    def _sqlite_busy(exc: Exception) -> bool:
        if not isinstance(exc, sqlite3.OperationalError):
            return False
        code = getattr(exc, "sqlite_errorcode", None)
        return ((code is not None and code & 0xFF in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED))
                or "locked" in str(exc).lower())

    def _ensure_flusher(self, sub: _Subscription) -> None:
        if (sub.entity is not None and sub.cursor is not None and sub.pushes
                and sub.flush_task is None):
            # create_task runs at the earliest next loop turn and batches
            # messages already buffered by other Telethon dispatch tasks.
            sub.flush_task = self._track(self._drain_pushes(sub))

    async def _drain_pushes(self, sub: _Subscription) -> None:
        retry = 0.05
        try:
            while (self._current(sub) and sub.entity is not None
                   and sub.cursor is not None and sub.pushes):
                mids = tuple(islice(sub.pushes, _PUSH_BATCH_SIZE))
                observed = {mid for mid in mids if mid > sub.cursor}
                if observed:
                    try:
                        items = [
                            self._pending(sub, mid, media) for mid in sorted(observed)
                            if (media := sub.pushes[mid]) is not None
                        ]
                        self._commit(sub, max(observed), items, observed=observed)
                    except Exception as exc:
                        if self._sqlite_busy(exc):
                            await asyncio.sleep(retry)
                            retry = min(1.0, retry * 2)
                            continue  # No progress/eviction while DB is locked.
                        # Telegram considers these updates delivered. Retry
                        # from the durable cursor instead of advancing it.
                        sub.ready = False
                        sub.entity = None
                        sub.generation += 1
                        sub.change.set()
                        self.log(f"[实时入队异常] {sub.title or sub.config.peer}：{type(exc).__name__}")
                        return
                for mid in mids:
                    del sub.pushes[mid]
                retry = 0.05
                if sub.pushes:
                    await asyncio.sleep(0)  # Yield between transactional batches.
        finally:
            sub.flush_task = None
            if sub.cursor is None or sub.highest_event > sub.cursor:
                sub.change.set()

    async def _on_message(self, event) -> None:
        message = event.message
        peer = getattr(message, "peer_id", None)
        if not isinstance(peer, PeerChannel):
            return
        sub = self._peers.get(peer.channel_id)
        if sub is None or not self._current(sub):
            return
        message_id = int(message.id)
        if sub.cursor is not None and message_id <= sub.cursor:
            return
        already_gap = sub.cursor is not None and sub.highest_event > sub.cursor
        sub.highest_event = max(sub.highest_event, message_id)
        # If a burst outruns the buffer, keep the highest observed ID and
        # recover the portion beyond the durable cursor via bounded catch-up.
        if len(sub.pushes) < _MAX_PUSH_BUFFER:
            try:
                sub.pushes[message_id] = self._message_media(sub, message)
            except Exception as exc:
                # Do not mark a post as handled if classification failed.
                # It is still recoverable from the last durable cursor.
                sub.ready = False
                sub.entity = None
                sub.generation += 1
                sub.change.set()
                self.log(f"[实时入队异常] {sub.title or sub.config.peer}：{type(exc).__name__}")
                return
        self._ensure_flusher(sub)
        if not already_gap:
            sub.change.set()

    async def _tail(self, entity: Channel, *, before: datetime | None = None) -> int:
        kwargs = {"offset_date": before} if before is not None else {}
        async for latest in self.client.iter_messages(entity, limit=1, **kwargs):
            return int(latest.id)
        return 0

    async def _read_window(self, sub: _Subscription, entity: Channel, upper: int,
                           generation: int) -> None:
        lower = sub.cursor
        if lower is None or upper <= lower:
            return
        batch: list[PendingMedia] = []
        scanned = 0
        last = lower
        async for message in self.client.iter_messages(
            entity, reverse=True, min_id=lower, max_id=upper + 1,
        ):
            if not self._current(sub) or generation != sub.generation:
                return
            if not last < message.id <= upper:
                raise RuntimeError("Telegram catch-up history was out of range")
            last = message.id
            media = self._message_media(sub, message)
            if media is not None:
                batch.append(self._pending(sub, last, media))
            scanned += 1
            if scanned == _CATCH_UP_BATCH:
                self._commit(sub, last, batch)
                batch = []
                scanned = 0
                await asyncio.sleep(0)
        if self._current(sub) and generation == sub.generation:
            # The authoritative, exhausted range proves gaps (deleted or
            # otherwise unavailable posts) without treating event arrival
            # order as a guarantee.
            self._commit(sub, upper, batch)

    async def _refresh(self, sub: _Subscription, generation: int) -> None:
        cfg = sub.config
        entity = await resolve_realtime_peer(self.client, cfg.target, expected_peer_id=cfg.peer)
        if not self._current(sub) or generation != sub.generation:
            return
        sub.title = str(entity.title or cfg.peer)
        sub.cursor = self.history.realtime_cursor(self.account_id, cfg.peer, cfg.epoch)
        baseline = sub.cursor
        if baseline is None:
            takeover = self.history.previous_realtime_boundary(self.account_id, cfg.peer)
            if cfg.backfill:
                completed, _, _, _ = self.history.scan_state(cfg.peer, account_id=self.account_id)
                previous = self.history.scan_media_types(cfg.peer, account_id=self.account_id)
                previous_kinds = {kind.strip() for kind in previous.split(",") if kind.strip()}
                # Only the completed boundary is safe in either scan direction;
                # a descending scan's in-progress cursor skips lower holes.
                # Unknown or expanded filter coverage requires a safe replay
                # to discover entries the previous scheduled scan may have skipped.
                baseline = completed if previous_kinds and cfg.types <= previous_kinds else 0
                if takeover is not None:
                    baseline = min(baseline, takeover)
            elif takeover is not None:
                # A standby taking over without its own cursor can recover
                # the handover gap without inheriting another account's access.
                baseline = takeover
            else:
                # Telegram timestamps are whole seconds and offset_date is
                # exclusive. The newest post strictly before enrollment is
                # the baseline, not a later tail that could contain a new post
                # whose push has not been dispatched yet.
                deadline = sub.subscribed_at
                if 0 < cfg.since <= int(deadline.timestamp()):
                    deadline = datetime.fromtimestamp(cfg.since, timezone.utc)
                baseline = await self._tail(entity, before=deadline)
                if not self._current(sub) or generation != sub.generation:
                    return
        upper = await self._tail(entity)
        if not self._current(sub) or generation != sub.generation:
            return
        if sub.cursor is None:
            sub.cursor = self.history.start_realtime(
                self.account_id, cfg.peer, cfg.epoch, baseline,
                promote_before=upper if cfg.backfill else None, kinds=cfg.types,
            )
        sub.entity = entity
        self._ensure_flusher(sub)
        await self._read_window(sub, entity, upper, generation)
        if self._current(sub) and generation == sub.generation:
            sub.ready = True

    async def _run(self, sub: _Subscription) -> None:
        applied_generation = -1
        retry = 5.0
        gap_deadline: float | None = None
        loop = asyncio.get_running_loop()
        while self._current(sub):
            try:
                generation = sub.generation
                if applied_generation != generation:
                    await self._refresh(sub, generation)
                    applied_generation = generation
                    retry = 5.0
                    gap_deadline = None
                    if sub.ready:
                        self.log(f"[实时监控] {sub.title} 已就绪")
                    continue
                sub.change.clear()
                if sub.cursor is not None and sub.highest_event > sub.cursor:
                    # Rare ID holes may represent service messages/deletes or
                    # an out-of-order push. Coalesce and repair only upon a
                    # witnessed hole, without a fixed Telegram history poll.
                    if gap_deadline is None:
                        gap_deadline = loop.time() + _GAP_SETTLE_SECONDS
                    try:
                        await asyncio.wait_for(sub.change.wait(), max(0, gap_deadline - loop.time()))
                    except asyncio.TimeoutError:
                        gap_deadline = None
                        if (self._current(sub) and generation == sub.generation
                                and sub.cursor is not None and sub.highest_event > sub.cursor):
                            await self._refresh(sub, generation)
                    continue
                gap_deadline = None
                await sub.change.wait()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                sub.ready = False
                sub.entity = None
                applied_generation = -1
                if isinstance(exc, (FloodWaitError, FloodPremiumWaitError)):
                    delay = max(retry, float(exc.seconds))
                elif isinstance(exc, _AUTH_ERRORS):
                    delay = max(retry, 300.0)
                elif self._sqlite_busy(exc):
                    delay = min(1.0, retry / 20)
                else:
                    delay = retry
                detail = str(exc) if isinstance(exc, ChannelAccessError) else type(exc).__name__
                self.log(f"[实时监控] {sub.title or sub.config.peer}：{detail}，{delay:g}秒后重试")
                retry = min(300.0, retry * 2)
                await asyncio.sleep(delay)
