"""An offline cutover/restart test with the real GUI monitor and download ledger."""
import asyncio
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from telethon import events
from telethon.tl.types import (
    Channel, ChatPhotoEmpty, Message, MessageMediaPhoto, PeerChannel, Photo, PhotoSize,
)

from tgdl import gui
from tgdl.accounts import AccountRegistry
from tgdl.history import DownloadHistory, PendingMedia
from tgdl.monitor import configure_monitor_mode, load_channels, save_channels
from tgdl.realtime_runtime import RealtimeDispatch
from tgdl.slots import SlotCoordinator, make_plan


PEER = 84563
TARGET = "@joined_test"
ACCOUNT = "default"
GROUP = Channel(PEER, "Joined test", ChatPhotoEmpty(), None, broadcast=True)


def payload(mid):
    return f"test-media-{mid}".encode()


def post(mid):
    now = datetime.now(timezone.utc)
    return Message(
        id=mid, peer_id=PeerChannel(PEER), date=now, message="",
        media=MessageMediaPhoto(photo=Photo(
            mid, 1, b"fake", now, [PhotoSize("x", 10, 10, len(payload(mid)))], 1,
        )),
    )


class OfflineTelegram:
    """Fake only Telegram transport; use production ingestion/file publishing."""
    def __init__(self, server, *, hold_catchup=False):
        self.server = server
        self.handlers = set()
        self.reconnect_handlers = set()
        self.disconnected = asyncio.get_running_loop().create_future()
        self.catchup_entered = asyncio.Event()
        self.release_catchup = asyncio.Event()
        if not hold_catchup:
            self.release_catchup.set()
        self.history_windows = []
        self.tail_reads = 0
        self.downloads = []

    def add_event_handler(self, handler, builder):
        assert isinstance(builder, events.NewMessage)
        self.handlers.add(handler)

    def remove_event_handler(self, handler):
        self.handlers.remove(handler)

    def add_reconnect_callback(self, handler):
        self.reconnect_handlers.add(handler)

    def remove_reconnect_callback(self, handler):
        self.reconnect_handlers.remove(handler)

    async def get_entity(self, identity):
        assert isinstance(identity, PeerChannel)
        assert identity.channel_id == PEER
        return GROUP

    async def iter_messages(self, entity, *, limit=None, reverse=False, min_id=0, max_id=0):
        assert entity is GROUP
        if limit == 1:
            self.tail_reads += 1
        else:
            assert reverse and max_id
            self.history_windows.append((min_id, max_id))
            self.catchup_entered.set()
            await self.release_catchup.wait()
        ids = sorted(
            (mid for mid in self.server if min_id < mid and (not max_id or mid < max_id)),
            reverse=not reverse,
        )
        for mid in ids[:limit]:
            yield self.server[mid]

    async def get_messages(self, entity, *, ids):
        assert entity is GROUP
        return self.server.get(ids)

    async def download_media(self, message, *, file, progress_callback):
        self.downloads.append(message.id)
        out = Path(file)
        assert out.suffix == ".jpg"
        data = payload(message.id)
        out.write_bytes(data)
        await progress_callback(len(data), len(data))
        return str(out)

    async def push(self, mid):
        for handler in tuple(self.handlers):
            await handler(SimpleNamespace(message=self.server[mid]))


def make_app(tmp_path, client, rows):
    app = object.__new__(gui._App)
    app.client = client
    app.settings = SimpleNamespace(
        download_dir=tmp_path / "files", temp_download_dir=None,
    )
    app._account_registry = AccountRegistry(tmp_path)
    app._download_future = None
    app._monitor_active = True
    app._monitor_download_stop = threading.Event()
    app._monitor_scan_stop = threading.Event()
    app._realtime_ready = RealtimeDispatch()
    app._monitored = rows
    app._slots = SlotCoordinator()
    app._slots.configure(make_plan([(ACCOUNT, 1, rows)]))
    app._monitor_progress_state = lambda *_: None
    app._update_download_progress = lambda *_: None
    app._log_download = lambda *_: None
    app._log = lambda *_: None

    async def ensure_existing_client():
        return app.client

    app._ensure_client = ensure_existing_client
    return app


async def eventually(condition, seconds=4):
    async def wait_until_true():
        while not condition():
            await asyncio.sleep(0.02)
    await asyncio.wait_for(wait_until_true(), timeout=seconds)


async def stop_monitor(app, worker):
    app._monitor_active = False
    app._monitor_scan_stop.set()
    app._monitor_download_stop.set()
    await asyncio.wait_for(worker, timeout=4)
    assert app._realtime_ready.service is None
    assert not app.client.handlers
    assert not app.client.reconnect_handlers


def test_gui_realtime_cutover_and_restart_use_existing_cursor_and_ledger(tmp_path, monkeypatch):
    db_path = tmp_path / "tgdl_download_history.sqlite3"
    monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(db_path))
    monitor_path = AccountRegistry(tmp_path).monitor_path(ACCOUNT)
    scheduled = {
        "target": TARGET, "peer_id": PEER, "enabled": True, "types": "photo",
        "mode": "scheduled", "minutes": 5,
    }
    save_channels([scheduled], monitor_path)

    async def scenario(history):
        # A scheduled scan completed through 50 but still has older work
        # queued. Posts 51 and 52 have not yet been seen by the downloader.
        history.mark_saved(PEER, 50, "photos", Path("already-backed-up.jpg"))
        history.commit_scan_batch(
            PEER, [PendingMedia(PEER, 49, TARGET, GROUP.title, "photo", "queued_before_switch")],
            completed=50, upper=0, cursor=0, direction="newest", media_types="photo",
        )
        # Apply the same mode conversion as the GUI and persist its epoch.
        previous, = load_channels(monitor_path)
        live = dict(previous)
        configure_monitor_mode(live, previous, "realtime", [])
        save_channels([live], monitor_path)
        epoch = live["realtime_epoch"]
        server = {mid: post(mid) for mid in (1, 49, 50, 51, 52)}
        first = OfflineTelegram(server, hold_catchup=True)
        app = make_app(tmp_path, first, load_channels(monitor_path))
        worker = asyncio.create_task(app._monitor_download_worker(1, 0, 1))
        try:
            await asyncio.wait_for(first.catchup_entered.wait(), timeout=3)
            assert first.history_windows == [(50, 53)]
            assert len(first.handlers) == 1
            # Duplicate 51 in both history and new-message, a newer live 53
            # beyond this history window, and a duplicate live delivery.
            server[53] = post(53)
            await first.push(51)
            await first.push(53)
            await first.push(53)
            await eventually(lambda: any(
                row.message_id == 53 for row in history.pending_for_peer(
                    PEER, account_id=ACCOUNT, realtime=True,
                )
            ))
            assert history.realtime_cursor(ACCOUNT, PEER, epoch) == 51
            assert not first.downloads, "cutover catch-up must finish before dispatch"
            first.release_catchup.set()
            await eventually(lambda: history.contains(PEER, 53))
            assert first.downloads == [49, 51, 52, 53]
            # The durable checkpoint may remain at 52 because 53 arrived
            # outside this authoritative history window. Its media ledger
            # still suppresses a duplicate if restart rereads the post.
            checkpoint = history.realtime_cursor(ACCOUNT, PEER, epoch)
            assert checkpoint in (52, 53)
        finally:
            first.release_catchup.set()
            await stop_monitor(app, worker)

        # Fresh dispatcher/client/coordinator, same persisted configuration
        # and ledger. Add messages while no event handler is listening.
        server[54], server[55] = post(54), post(55)
        second = OfflineTelegram(server)
        restarted = make_app(tmp_path, second, load_channels(monitor_path))
        worker = asyncio.create_task(restarted._monitor_download_worker(1, 0, 1))
        try:
            await eventually(lambda: history.contains(PEER, 55))
            assert second.history_windows == [(checkpoint, 56)]
            assert second.downloads == [54, 55]
            # Once caught up, a new post starts via push, with no additional
            # history request or whole-channel replay.
            server[56] = post(56)
            await second.push(56)
            await second.push(56)
            await eventually(lambda: history.contains(PEER, 56))
            assert second.downloads == [54, 55, 56]
            assert second.tail_reads == 1
            assert second.history_windows == [(checkpoint, 56)]

            # Telethon can auto-reconnect without replacing the client or
            # resolving its disconnected future. Exercise that hook too.
            assert len(second.reconnect_handlers) == 1
            server[57], server[58] = post(57), post(58)
            for reconnect in tuple(second.reconnect_handlers):
                reconnect()
            await second.push(58)
            await eventually(lambda: history.contains(PEER, 58))
            assert second.downloads == [54, 55, 56, 57, 58]
            assert second.history_windows == [(checkpoint, 56), (56, 59)]
        finally:
            await stop_monitor(restarted, worker)

        assert history.realtime_cursor(ACCOUNT, PEER, epoch) == 58
        assert not history.contains(PEER, 1)
        assert history.pending_count() == 0
        assert not history.active_claims()
        files = {p.read_bytes() for p in (tmp_path / "files").rglob("*.jpg")}
        assert files == {payload(mid) for mid in (49, 51, 52, 53, 54, 55, 56, 57, 58)}

    history = DownloadHistory(db_path)
    try:
        asyncio.run(scenario(history))
    finally:
        history.close()
