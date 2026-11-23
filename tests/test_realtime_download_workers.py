"""Runtime tests for fixed lanes, independent of Telegram transport."""
import asyncio
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.errors import ChannelPrivateError
from telethon.tl.types import Channel, ChatPhotoEmpty, Message, MessageMediaPhoto, PeerChannel

from tgdl import downloader, gui, multi_runtime, realtime
from tgdl.accounts import Account, AccountRegistry
from tgdl.errors import ChannelAccessError
from tgdl.history import DownloadHistory, PendingMedia
from tgdl.monitor_flow import SharedSpeedLimiter
from tgdl.slots import SlotCoordinator, make_plan


@pytest.mark.parametrize("account_id", ["default", "secondary"])
def test_workers_never_borrow_realtime_and_schedule_one_live_file_per_peer(
    tmp_path, monkeypatch, account_id,
):
    # Both lanes must use the same payload path, even when the channel and
    # the readable messages both carry Telegram's forward-control flag.
    rows = [
        {"target": "@old", "peer_id": 100, "enabled": True,
         "priority": 99, "types": "photo"},
        {"target": "@live", "peer_id": 200, "enabled": True,
         "priority": -99, "types": "photo", "mode": "realtime"},
    ]
    slots = SlotCoordinator()
    slots.configure(make_plan([(account_id, 2, rows)]))
    settings = SimpleNamespace(
        download_dir=tmp_path, temp_download_dir=None,
        media_speed_kib=0, media_concurrency=2, media_retries=0, chunk_concurrency=1,
    )

    class Client:
        async def get_entity(self, peer):
            assert isinstance(peer, PeerChannel) and peer.channel_id == 200
            return Channel(200, "@live", ChatPhotoEmpty(), None, megagroup=True, noforwards=True, restricted=True)

        async def get_messages(self, entity, ids):
            return Message(id=ids, peer_id=PeerChannel(entity.id), date=datetime.now(timezone.utc),
                           message="", media=MessageMediaPhoto(), noforwards=entity.noforwards)

        async def download_media(self, message, *, file, progress_callback):
            assert message.noforwards is True
            self.started.put_nowait((message.peer_id.channel_id, message.id))
            await self.wait_on[message.id].wait()
            payload = Path(file) / "offline.jpg"
            payload.write_bytes(f"test data for {message.id}".encode())
            return payload

    async def resolve(_client, target):
        return Channel(100 if target == "@old" else 200, target, ChatPhotoEmpty(), None,
                       megagroup=True, noforwards=True, restricted=True)

    async def ensure():
        pass

    path = tmp_path / "tgdl_download_history.sqlite3"
    client = Client()
    if account_id == "default":
        app = object.__new__(gui._App)
        app.client = client
        app.settings = settings
        app._monitored = rows
        app._download_future = None
        app._monitor_download_stop = threading.Event()
        app._monitor_progress_state = lambda *_: None
        app._update_download_progress = lambda *_: None
        app._log_download = lambda *_: None
        app._ensure_client = ensure
        worker = app._monitor_download_worker(2, 0, 1)
        stop = app._monitor_download_stop.set
        monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(path))
        monkeypatch.setattr(gui, "resolve_channel", resolve)
    else:
        app = multi_runtime.AccountMonitorRuntime(
            Account(account_id, "secondary"), AccountRegistry(tmp_path),
            ask=None, on_log=lambda *_: None, on_saved=lambda *_: None, on_progress=lambda *_: None,
        )
        app.client = client
        assert app.start_monitoring()
        app.peer_by_target["@old"] = 100
        # A prior scheduled attempt might have lost access. A validated
        # real-time subscription is not governed by that old blocked flag.
        app._blocked.add("@live")
        monkeypatch.setattr(app, "_channels", lambda: rows)
        monkeypatch.setattr(multi_runtime, "resolve_channel", resolve)
        monkeypatch.setattr(multi_runtime, "read_env_values", lambda _: {
            "media_concurrency": "2", "media_retries": "0",
            "chunk_concurrency": "1", "media_speed_kib": "0",
        })
        worker = app._download_loop(settings)
        stop = app.stop

    app._slots = slots
    app._realtime_ready = {200}
    history = DownloadHistory(path)
    history.enqueue_media([
        PendingMedia(100, 1, "@old", "old", "photo", "1"),
        PendingMedia(100, 2, "@old", "old", "photo", "2"),
        PendingMedia(200, 10, "@live", "live", "photo", "10"),  # old backlog of live peer
    ])

    async def scenario():
        started = asyncio.Queue()
        wait_on = {mid: asyncio.Event() for mid in (1, 2, 10, 11, 12)}

        client.started = started
        client.wait_on = wait_on
        task = asyncio.create_task(worker)
        try:
            old_peer, old_id = await asyncio.wait_for(started.get(), 3)
            assert (old_peer, old_id) == (100, 1)
            # Through each real worker, not just the coordinator API: an old
            # scheduled file prevents a second account from racing the cutover.
            taking_over = [{**rows[0], "mode": "realtime"}]
            slots.configure(make_plan([(account_id, 2, rows), ("other", 1, taking_over)]))
            assert slots.owns("other", old_peer)
            assert not slots.available("other", 1, old_peer)
            slots.configure(make_plan([(account_id, 2, rows)]))
            await asyncio.sleep(0.32)
            assert started.empty(), "the idle live lane cannot process any ordinary backlog"

            # Keep the first ordinary transfer running while two live items arrive.
            history.start_realtime(account_id, 200, "test-epoch", 10)
            assert history.commit_realtime(account_id, 200, "test-epoch", 12, [
                PendingMedia(200, 11, "@live", "live", "photo", "11"),
                PendingMedia(200, 12, "@live", "live", "photo", "12"),
            ]) == (2, 12)
            if account_id == "default":
                app._download_future = asyncio.get_running_loop().create_future()
            assert await asyncio.wait_for(started.get(), 3) == (200, 11)
            await asyncio.sleep(0.32)
            assert started.empty(), "live peer cannot run two files in parallel"
            wait_on[11].set()
            assert await asyncio.wait_for(started.get(), 3) == (200, 12)
            wait_on[12].set()
            if account_id == "default":
                app._download_future.set_result(None)
            # Disabling the real-time reservation makes its one slot
            # ordinary again immediately, without restarting the account.
            rows[1]["mode"] = "scheduled"
            slots.configure(make_plan([(account_id, 2, rows)]))
            next_job = await asyncio.wait_for(started.get(), 3)
            assert next_job in {(100, 1), (100, 2), (200, 10)}
            assert next_job != (old_peer, old_id)
        finally:
            stop()
            for gate in wait_on.values():
                gate.set()
            await asyncio.wait_for(task, 3)

        assert slots.available(account_id, 2), "stopping releases in-flight tokens"
        assert history.contains(100, 1) and history.contains(200, 11) and history.contains(200, 12)
        assert sorted(p.read_bytes() for p in settings.download_dir.rglob("*.jpg")) == [
            f"test data for {mid}".encode() for mid in sorted((1, 11, 12, next_job[1]), key=str)
        ]
        assert history.pending_count() == 1
        assert not history.active_claims()

    try:
        asyncio.run(scenario())
    finally:
        history.close()


@pytest.mark.parametrize("change", ["all_reserved", "remove_row", "move_peer_to_realtime"])
def test_live_edit_requeues_a_cooperatively_paused_file(tmp_path, monkeypatch, change):
    path = tmp_path / "retire.sqlite3"
    rows = [
        {"target": "@low", "peer_id": 100, "enabled": True, "priority": 0, "types": "video"},
        {"target": "@high", "peer_id": 200, "enabled": True, "priority": 100, "types": "video"},
    ]

    class Client:
        async def get_messages(self, _entity, ids):
            return SimpleNamespace(id=ids, date=datetime.now(timezone.utc))

    async def resolve(_client, target):
        return Channel(100 if target == "@low" else 200, target, ChatPhotoEmpty(), None, megagroup=True)

    app = object.__new__(gui._App)
    app.client = Client()
    app.settings = SimpleNamespace(download_dir=tmp_path, temp_download_dir=None)
    app._monitored = rows
    app._download_future = None
    app._monitor_download_stop = threading.Event()
    app._monitor_progress_state = lambda *_: None
    app._update_download_progress = lambda *_: None
    app._log_download = lambda *_: None

    async def ensure():
        pass

    app._ensure_client = ensure
    app._slots = SlotCoordinator()
    app._slots.configure(make_plan([("default", 1, rows)]))
    monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(path))
    monkeypatch.setattr(gui, "resolve_channel", resolve)
    history = DownloadHistory(path)
    history.enqueue_media([PendingMedia(100, 1, "@low", "low", "video", "1")])

    async def scenario():
        low_started, low_cancelled = asyncio.Event(), asyncio.Event()
        high_started, high_release = asyncio.Event(), asyncio.Event()

        async def download(_client, message, _folder, base, _stats, *,
                           peer_id, history, claim_token, on_claim_committed, monitor_flow, **_):
            if message.id == 1:
                pause = monitor_flow.pause
                pause.register()
                low_started.set()
                try:
                    while not pause.requested:
                        await asyncio.sleep(0.01)
                    await pause.checkpoint()
                    pytest.fail("the edit removed the paused download's ordinary lane")
                except asyncio.CancelledError:
                    low_cancelled.set()
                    raise
                finally:
                    pause.unregister()
            high_started.set()
            await high_release.wait()
            assert history.mark_claim_saved(
                "default", peer_id, message.id, "video", Path(base), token=claim_token,
            )
            on_claim_committed()
            return Path(base)

        monkeypatch.setattr(downloader, "_download_with_retry", download)
        task = asyncio.create_task(app._monitor_download_worker(1, 0, 1))
        try:
            await asyncio.wait_for(low_started.wait(), 3)
            history.enqueue_media([PendingMedia(200, 2, "@high", "high", "video", "2")])
            await asyncio.wait_for(high_started.wait(), 3)  # low has reached its checkpoint
            assert any((peer, mid) == (100, 1)
                       for peer, mid, _account, _expires in history.active_claims())
            workers = 1
            if change == "all_reserved":
                rows.append({"target": "@live", "peer_id": 300, "enabled": True, "mode": "realtime"})
            elif change == "remove_row":
                rows.pop(0)
            else:
                rows[0]["mode"] = "realtime"
                workers = 2  # Still has a free ordinary lane, just not for this peer.
                app._monitor_live_concurrency = 2
            app._slots.configure(make_plan([("default", workers, rows)]))

            await asyncio.wait_for(low_cancelled.wait(), 3)
            assert [row.message_id for row in history.pending_for_peer(100, account_id="default")] == [1]
            assert not history.contains(100, 1)
        finally:
            app._monitor_download_stop.set()
            high_release.set()
            await asyncio.wait_for(task, 3)

        assert not history.active_claims()
        assert history.contains(200, 2)

    try:
        asyncio.run(scenario())
    finally:
        history.close()


def test_secondary_realtime_permission_failure_backs_off_without_blocking_peer(tmp_path, monkeypatch):
    account_id = "secondary"
    runtime = multi_runtime.AccountMonitorRuntime(
        Account(account_id, "secondary"), AccountRegistry(tmp_path),
        ask=None, on_log=lambda *_: None, on_saved=lambda *_: None, on_progress=lambda *_: None,
    )
    row = {"target": "@live", "mode": "realtime"}
    job = PendingMedia(200, 11, "@live", "live", "photo", "11")
    history = DownloadHistory(tmp_path / "access.sqlite3")
    history.enqueue_media([job])

    async def temporarily_inaccessible(*_args, **_kwargs):
        raise ChannelAccessError("no longer joined")

    monkeypatch.setattr(realtime, "resolve_realtime_peer", temporarily_inaccessible)
    try:
        token = history.try_claim(account_id, job.peer_id, job.message_id)
        assert token is not None
        asyncio.run(runtime._transfer(
            history, row, job, None, SharedSpeedLimiter(), 0, 1, token,
        ))
        assert not history.active_claims()
        assert "@live" not in runtime._blocked
        assert history.pending_for_peer(200, account_id=account_id) == []
        assert history.pending_for_peer(200, account_id=account_id, now=time.time() + 310) == [job]
    finally:
        history.close()



def test_secondary_realtime_media_refused_by_telegram_is_never_marked_saved(tmp_path):
    account_id = "secondary"
    successes, logs = [], []
    runtime = multi_runtime.AccountMonitorRuntime(
        Account(account_id, "secondary"), AccountRegistry(tmp_path),
        ask=None, on_log=lambda _, text: logs.append(text),
        on_saved=lambda *args: successes.append(args), on_progress=lambda *_: None,
    )

    class Client:
        reads = 0

        async def get_entity(self, peer):
            assert isinstance(peer, PeerChannel) and peer.channel_id == 200
            return Channel(200, "live", ChatPhotoEmpty(), None, megagroup=True, noforwards=True, restricted=True)

        async def get_messages(self, entity, ids):
            return Message(id=ids, peer_id=PeerChannel(entity.id), date=datetime.now(timezone.utc),
                           message="", media=MessageMediaPhoto(), noforwards=True)

        async def download_media(self, _message, *, file, progress_callback):
            self.reads += 1
            (Path(file) / "incomplete.jpg").write_bytes(b"partial before denial")
            raise ChannelPrivateError(request=None)

    runtime.client = Client()
    settings = SimpleNamespace(download_dir=tmp_path, temp_download_dir=None)
    row = {"target": "@live", "mode": "realtime", "types": "photo"}
    job = PendingMedia(200, 11, "@live", "live", "photo", "11")
    history = DownloadHistory(tmp_path / "refused.sqlite3")
    try:
        history.enqueue_media([job])
        token = history.try_claim(account_id, job.peer_id, job.message_id)
        assert token is not None
        asyncio.run(runtime._transfer(
            history, row, job, settings, SharedSpeedLimiter(), 0, 1, token,
        ))
        assert runtime.client.reads == 1  # The normal media request; no alternate path.
        assert not history.contains(job.peer_id, job.message_id)
        assert not successes and not history.active_claims()
        assert not list(settings.download_dir.rglob("*.jpg"))
        assert any("下载失败" in line for line in logs)
        assert history.pending_for_peer(job.peer_id, account_id=account_id) == []
        assert history.pending_for_peer(job.peer_id, account_id=account_id,
                                        now=time.time() + 61) == [job]
    finally:
        history.close()
