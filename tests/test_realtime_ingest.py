"""Real-time ingestion with real Telethon message types and a fake transport."""
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.sessions import MemorySession
from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.tl.types import (
    Channel, ChannelForbidden, ChatInviteAlready, ChatPhotoEmpty, Message, MessageMediaPhoto, PeerChannel,
)

from tgdl import realtime
from tgdl.client import DiagnosticTelegramClient
from tgdl.errors import ChannelAccessError
from tgdl.history import DownloadHistory, PendingMedia

PEER = 100
OLD = datetime.now(timezone.utc) - timedelta(hours=1)


def photo(mid, *, peer=PEER, date=OLD, protected=False):
    return Message(id=mid, peer_id=PeerChannel(peer), date=date, message="",
                   media=MessageMediaPhoto(), noforwards=protected)


def item(*, epoch="subscription", backfill=False):
    return dict(peer_id=PEER, target="@photos", enabled=True, mode="realtime",
                types="photo", realtime_epoch=epoch, realtime_backfill=backfill)


class FakeTelegram:
    def __init__(self, messages=()):
        self.posts = {m.id: m for m in messages}
        self.entity = Channel(PEER, "Photos", ChatPhotoEmpty(), OLD, broadcast=True)
        self.invite = ChatInviteAlready(self.entity)
        self.requests = []
        self.calls = []
        self.handlers = []
        self.reconnects = []
        self.disconnected = asyncio.get_running_loop().create_future()
        self.connected = True
        self.before_tail = None
        self.before_window = None

    async def get_entity(self, peer):
        return self.entity

    async def __call__(self, request):
        self.requests.append(request)
        assert isinstance(request, CheckChatInviteRequest)
        return self.invite

    def is_connected(self):
        return self.connected

    def add_event_handler(self, handler, _builder):
        self.handlers.append(handler)

    def remove_event_handler(self, handler):
        self.handlers.remove(handler)

    def add_reconnect_callback(self, callback):
        self.reconnects.append(callback)

    def remove_reconnect_callback(self, callback):
        self.reconnects.remove(callback)

    def reconnect(self):
        for handler in self.reconnects[:]:
            handler()

    async def emit(self, *messages, remember=True):
        for message in messages:
            if remember and message.peer_id.channel_id == PEER:
                self.posts[message.id] = message
            for handler in self.handlers[:]:
                await handler(SimpleNamespace(message=message))

    async def iter_messages(self, entity, **kwargs):
        self.calls.append(kwargs)
        hook = self.before_window if kwargs.get("reverse") else self.before_tail
        if hook is not None:
            await hook(kwargs)
        items = sorted(self.posts.values(), key=lambda m: m.id, reverse=not kwargs.get("reverse"))
        items = [
            m for m in items
            if kwargs.get("min_id", 0) < m.id < kwargs.get("max_id", 2**31)
            and (not kwargs.get("offset_date") or m.date < kwargs["offset_date"])
        ]
        for msg in items[:kwargs.get("limit", len(items))]:
            yield msg


async def wait_for(predicate, timeout=2):
    async def probe():
        while not predicate():
            await asyncio.sleep(.001)
    await asyncio.wait_for(probe(), timeout)


def pending(history, *, account="default", lane=True):
    return [p.message_id for p in history.pending_for_peer(
        PEER, account_id=account, realtime=lane, limit=1000,
    )]


def test_read_only_resolution_checks_membership_and_identity_not_metadata_flags():
    async def run():
        client = FakeTelegram()
        assert await realtime.resolve_realtime_peer(client, "https://t.me/+abc") is client.entity
        assert isinstance(client.requests[0], CheckChatInviteRequest)
        client.invite = SimpleNamespace(chat=client.entity)  # preview is not membership
        with pytest.raises(ChannelAccessError, match="加入"):
            await realtime.resolve_realtime_peer(client, "https://t.me/+abc")
        client.entity.left = True
        with pytest.raises(ChannelAccessError, match="加入"):
            await realtime.resolve_realtime_peer(client, "@photos")
        client.entity.left = False
        client.entity.noforwards = True
        assert await realtime.resolve_realtime_peer(client, "@photos") is client.entity
        assert await realtime.resolve_realtime_peer(client, "@photos", expected_peer_id=PEER) is client.entity
        client.entity.restricted = True
        assert await realtime.resolve_realtime_peer(client, "@photos") is client.entity
        assert await realtime.resolve_realtime_peer(client, "@photos", expected_peer_id=PEER) is client.entity
        client.entity.min = True
        with pytest.raises(ChannelAccessError, match="加入"):
            await realtime.resolve_realtime_peer(client, "@photos")
        client.entity.min = False
        with pytest.raises(ChannelAccessError, match="不一致"):
            await realtime.resolve_realtime_peer(client, "@photos", expected_peer_id=PEER + 1)

        client.entity = ChannelForbidden(PEER, 123, "Photos", broadcast=True)
        with pytest.raises(ChannelAccessError, match="加入"):
            await realtime.resolve_realtime_peer(client, "@photos")

        async def denied(_peer):
            raise ChannelPrivateError(request=None)

        client.get_entity = denied
        with pytest.raises(ChannelAccessError, match="无法读取"):
            await realtime.resolve_realtime_peer(client, "@photos", expected_peer_id=PEER)

    asyncio.run(run())


def test_first_push_next_turn_batches_exact_chunks_without_history_poll(tmp_path, monkeypatch):
    async def run():
        history = DownloadHistory(tmp_path / "state.db")
        client = FakeTelegram([photo(100)])
        rt = realtime.RealtimeMonitor(client, history, "default", log=lambda _: None)
        original = history.commit_realtime
        observed = []

        def commit(*args, **kwargs):
            if kwargs.get("observed") is not None:
                observed.append(len(kwargs["observed"]))
            return original(*args, **kwargs)

        monkeypatch.setattr(history, "commit_realtime", commit)
        # A contiguous stream should not trigger a gap repair request even if
        # we've deliberately made the repair interval very short.
        monkeypatch.setattr(realtime, "_GAP_SETTLE_SECONDS", 0.01)
        rt.update([item()])
        try:
            await wait_for(lambda: PEER in rt.ready_peers)
            assert len(client.calls) == 2
            assert pending(history) == []
            post = photo(101)
            post.message = "caption must not be kept in the push buffer"
            await client.emit(post)
            assert observed == []  # handler merely buffered the notification
            assert rt._peers[PEER].pushes == {101: ("photo", f"{OLD:%Y%m%d}_101")}
            await asyncio.sleep(0)
            assert observed == [1] and pending(history) == [101]
            await client.emit(
                *(photo(mid, protected=mid == 103) for mid in range(102, 168)),
                photo(2, peer=PEER + 1),
            )
            await wait_for(lambda: history.realtime_cursor("default", PEER, "subscription") == 167)
            assert observed == [1, 32, 32, 2]
            assert pending(history) == list(range(101, 168))
            await asyncio.sleep(.02)
            assert len(client.calls) == 2
        finally:
            await rt.close()
            assert not client.handlers and not client.reconnects
            history.close()

    asyncio.run(run())


def test_readable_flagged_channel_catches_up_and_deduplicates_push(tmp_path):
    async def run():
        history = DownloadHistory(tmp_path / "state.db")
        new_post = photo(501, date=datetime.now(timezone.utc), protected=True)
        client = FakeTelegram([photo(500)])
        client.entity.noforwards = True
        client.entity.restricted = True

        async def hide_push_until_after_snapshot(kwargs):
            if not kwargs.get("offset_date"):
                client.posts[501] = new_post  # Push delivery not scheduled yet.

        client.before_tail = hide_push_until_after_snapshot
        rt = realtime.RealtimeMonitor(client, history, "default", log=lambda _: None)
        rt.update([item()])
        try:
            await wait_for(lambda: PEER in rt.ready_peers)
            assert pending(history) == [501]
            assert client.calls[2]["min_id"] == 500
            await client.emit(new_post)
            await asyncio.sleep(0)
            assert pending(history) == [501]
        finally:
            await rt.close()
            history.close()

    asyncio.run(run())


def test_server_denied_history_leaves_flagged_channel_unready_and_enqueues_nothing(tmp_path):
    async def run():
        history = DownloadHistory(tmp_path / "state.db")
        client = FakeTelegram([photo(100, protected=True)])
        client.entity.restricted = client.entity.noforwards = True
        logs = []

        async def deny_history(_kwargs):
            raise ChannelPrivateError(request=None)

        client.before_tail = deny_history
        rt = realtime.RealtimeMonitor(client, history, "default", log=logs.append)
        rt.update([item()])
        try:
            await wait_for(lambda: any("ChannelPrivateError" in line for line in logs))
            assert len(client.calls) == 1                     # Ordinary history path was reached.
            assert not client.requests                       # No join or alternative RPC path.
            assert PEER not in rt.ready_peers
            assert history.realtime_cursor("default", PEER, "subscription") is None
            await client.emit(photo(101, protected=True))    # No admission after denied history.
            assert pending(history) == []
        finally:
            await rt.close()
            history.close()

    asyncio.run(run())


def test_out_of_order_restart_then_reconnect_and_global_dedup(tmp_path):
    async def run():
        path = tmp_path / "state.db"
        client = FakeTelegram([photo(50)])
        history = DownloadHistory(path)
        rt = realtime.RealtimeMonitor(client, history, "default", log=lambda _: None)
        rt.update([item()])
        try:
            await wait_for(lambda: PEER in rt.ready_peers)
            await client.emit(photo(52))
            await wait_for(lambda: pending(history) == [52])
            assert history.realtime_cursor("default", PEER, "subscription") == 50
        finally:
            await rt.close()
            history.close()

        # SQLite survives process restart; discovering 52 again does not create a second row.
        client.posts[51] = photo(51)
        history = DownloadHistory(path)
        rt = realtime.RealtimeMonitor(client, history, "default", log=lambda _: None)
        rt.update([item()])
        try:
            await wait_for(lambda: PEER in rt.ready_peers)
            assert pending(history) == [51, 52]
            assert history.realtime_cursor("default", PEER, "subscription") == 52
            assert history.mark_saved(PEER, 52, "photo", tmp_path / "file.jpg")
            client.posts[53] = photo(53)
            client.reconnect()
            assert PEER not in rt.ready_peers
            await client.emit(photo(54))
            await wait_for(lambda: PEER in rt.ready_peers and pending(history) == [51, 53, 54])
            assert history.contains(PEER, 52)
            assert history.realtime_cursor("default", PEER, "subscription") == 54
        finally:
            await rt.close()
            history.close()

    asyncio.run(run())


@pytest.mark.parametrize(("previous_types", "lower"), [("photo,video", 10), ("video", 0), ("", 0)])
def test_scheduled_conversion_revisits_descending_gaps_and_promotes_cutover_only(
    tmp_path, previous_types, lower,
):
    async def run():
        history = DownloadHistory(tmp_path / "state.db")
        history.save_scan_state(PEER, 10, 25, 20, "newest", previous_types)
        for mid, kind in [(5, "photo"), (7, ""), (8, "video"), (20, "photo"), (26, "photo")]:
            history.enqueue_media([PendingMedia(PEER, mid, "@photos", "Photos", kind, "base")])
        assert history.mark_saved(PEER, 11, "photo", tmp_path / "old.jpg")
        client = FakeTelegram(photo(mid) for mid in range(1, 26) if mid != 8)
        client.posts[8] = Message(id=8, peer_id=PeerChannel(PEER), date=OLD, message="skip")
        started = asyncio.Event()
        resume = asyncio.Event()

        async def block_range(_kwargs):
            started.set()
            await resume.wait()

        client.before_window = block_range
        rt = realtime.RealtimeMonitor(client, history, "default", log=lambda _: None)
        rt.update([item(backfill=True)])
        try:
            await asyncio.wait_for(started.wait(), 2)
            assert PEER not in rt.ready_peers
            assert client.calls[1]["min_id"] == lower
            assert pending(history) == [5, 7, 20]
            await client.emit(photo(27))
            resume.set()
            await wait_for(lambda: PEER in rt.ready_peers and 27 in pending(history))
            desired = set(range(lower + 1, 26)) - {8, 11} | {5, 7, 27}
            assert set(pending(history)) == desired
            assert pending(history, lane=False) == [8, 26]
            assert history.scan_state(PEER) == (10, 25, 20, "newest")
            assert history.pending_count() == len(desired) + 2
        finally:
            await rt.close()
            history.close()

    asyncio.run(run())


def test_standby_takeover_without_cursor_uses_prior_boundary(tmp_path):
    async def run():
        history = DownloadHistory(tmp_path / "state.db")
        history.start_realtime("old-owner", PEER, "old", 18)
        history.start_realtime("other", PEER, "other", 20)
        assert history.mark_saved(PEER, 20, "photo", tmp_path / "previous.jpg")
        client = FakeTelegram(photo(mid) for mid in range(1, 23))
        rt = realtime.RealtimeMonitor(client, history, "standby", log=lambda _: None)
        rt.update([item()])
        try:
            await wait_for(lambda: PEER in rt.ready_peers)
            assert client.calls[1]["min_id"] == 18
            assert pending(history, account="standby") == [19, 21, 22]
            assert history.realtime_cursor("standby", PEER, "subscription") == 22
            assert history.realtime_cursor("old-owner", PEER, "old") == 18
        finally:
            await rt.close()
            history.close()

    asyncio.run(run())


def test_blocked_epoch_does_not_commit_unwinding_window(tmp_path):
    async def run():
        history = DownloadHistory(tmp_path / "state.db")
        client = FakeTelegram([photo(100), photo(101)])
        entered = asyncio.Event()
        release = asyncio.Event()
        current = True

        async def block_window(_kwargs):
            entered.set()
            await release.wait()

        client.before_window = block_window
        rt = realtime.RealtimeMonitor(
            client, history, "default", log=lambda _: None, is_current=lambda _: current,
        )
        rt.update([item(backfill=True)])
        try:
            await asyncio.wait_for(entered.wait(), 2)
            current = False
            release.set()
            await client.emit(photo(102))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert not rt.ready_peers and pending(history) == []
            assert history.realtime_cursor("default", PEER, "subscription") == 0
        finally:
            await rt.close()
            history.close()

    asyncio.run(run())


def test_sqlite_writer_contention_keeps_buffer_and_retries(tmp_path):
    async def run():
        path = tmp_path / "state.db"
        history = DownloadHistory(path)
        client = FakeTelegram([photo(50)])
        rt = realtime.RealtimeMonitor(client, history, "default", log=lambda _: None)
        rt.update([item()])
        blocker = sqlite3.connect(path)
        try:
            await wait_for(lambda: PEER in rt.ready_peers)
            blocker.execute("BEGIN IMMEDIATE")
            await client.emit(photo(51))
            began = asyncio.get_running_loop().time()
            await asyncio.sleep(0)
            assert asyncio.get_running_loop().time() - began < .5
            assert pending(history) == []
            assert history.realtime_cursor("default", PEER, "subscription") == 50
            blocker.rollback()
            await wait_for(lambda: pending(history) == [51])
            assert history.realtime_cursor("default", PEER, "subscription") == 51
        finally:
            blocker.close()
            await rt.close()
            history.close()

    asyncio.run(run())


def test_flood_wait_does_not_advance_cursor_and_terminal_disconnect_is_observed(tmp_path):
    async def run():
        history = DownloadHistory(tmp_path / "state.db")
        client = FakeTelegram([photo(50)])
        logs = []
        rt = realtime.RealtimeMonitor(client, history, "default", log=logs.append)
        rt.update([item()])
        try:
            await wait_for(lambda: PEER in rt.ready_peers)

            async def flood(_kwargs):
                raise FloodWaitError(request=None, capture=120)

            client.before_tail = flood
            client.reconnect()
            await wait_for(lambda: any("120秒" in log for log in logs))
            assert not rt.ready_peers
            assert history.realtime_cursor("default", PEER, "subscription") == 50
            client.connected = False
            client.disconnected.set_result(None)
            await wait_for(lambda: any("连接已断开" in log for log in logs))
            rt.update([item()])
            assert not client.handlers
        finally:
            await rt.close()
            history.close()

    asyncio.run(run())


def test_diagnostic_client_notifies_before_telethon_reconnect_rpc(monkeypatch):
    async def run():
        order = []
        async def upstream(_self):
            order.append("upstream")
        monkeypatch.setattr(TelegramClient, "_handle_auto_reconnect", upstream)
        client = DiagnosticTelegramClient(MemorySession(), 1, "abc")
        observer = lambda: order.append("observer")
        client.add_reconnect_callback(observer)
        await client._sender._auto_reconnect_callback()
        client.remove_reconnect_callback(observer)
        await client._sender._auto_reconnect_callback()
        assert order == ["observer", "upstream", "upstream"]

    asyncio.run(run())


def test_delayed_first_start_uses_saved_activation_time_and_bounded_transactions(
    tmp_path, monkeypatch,
):
    async def run():
        history = DownloadHistory(tmp_path / "state.db")
        enabled = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=10)
        client = FakeTelegram(
            [photo(99)] + [photo(mid, date=enabled) for mid in range(100, 167)]
        )
        rt = realtime.RealtimeMonitor(client, history, "default", log=lambda _: None)
        commits = []
        original = history.commit_realtime

        def commit(account, peer, epoch, completed, items, **kwargs):
            commits.append((completed, len(items)))
            return original(account, peer, epoch, completed, items, **kwargs)

        monkeypatch.setattr(history, "commit_realtime", commit)
        rt.update([dict(item(), realtime_since=int(enabled.timestamp()))])
        try:
            await wait_for(lambda: PEER in rt.ready_peers)
            assert client.calls[0]["offset_date"] == enabled
            assert client.calls[2]["min_id"] == 99
            assert commits == [(131, 32), (163, 32), (166, 3)]
            assert pending(history) == list(range(100, 167))
            assert history.realtime_cursor("default", PEER, "subscription") == 166
        finally:
            await rt.close()
            history.close()

    asyncio.run(run())


def test_buffer_overflow_recovers_via_event_triggered_bounded_history(tmp_path, monkeypatch):
    async def run():
        history = DownloadHistory(tmp_path / "state.db")
        client = FakeTelegram([photo(10)])
        rt = realtime.RealtimeMonitor(client, history, "default", log=lambda _: None)
        monkeypatch.setattr(realtime, "_MAX_PUSH_BUFFER", 5)
        monkeypatch.setattr(realtime, "_GAP_SETTLE_SECONDS", .03)
        rt.update([item()])
        try:
            await wait_for(lambda: PEER in rt.ready_peers)
            await client.emit(*(photo(mid) for mid in range(11, 21)))
            await asyncio.sleep(0)
            assert pending(history) == list(range(11, 16))
            assert history.realtime_cursor("default", PEER, "subscription") == 15
            await wait_for(lambda: history.realtime_cursor("default", PEER, "subscription") == 20)
            assert pending(history) == list(range(11, 21))
            assert [(c.get("min_id"), c.get("max_id")) for c in client.calls] == [
                (None, None), (None, None), (None, None), (15, 21),
            ]
        finally:
            await rt.close()
            history.close()

    asyncio.run(run())
