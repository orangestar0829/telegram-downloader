"""Strict high-priority drain; paused files precede fresh same-rank jobs."""

import asyncio
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from tgdl import downloader, gui
from tgdl.history import DownloadHistory, PendingMedia


def test_paused_low_waits_until_higher_priority_transfer_completes(
    tmp_path, monkeypatch,
):
    db = tmp_path / "history.sqlite"
    monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(db))

    async def resolve(_client, target):
        return SimpleNamespace(id=123 if target == "high" else 456, title=target)

    monkeypatch.setattr(gui, "resolve_channel", resolve)

    class Client:
        async def get_messages(self, _entity, ids):
            return SimpleNamespace(
                id=ids, date=datetime(2026, 11, 21, tzinfo=timezone.utc)
            )

    app = object.__new__(gui._App)
    app.client = Client()
    app.settings = SimpleNamespace(download_dir=tmp_path, temp_download_dir=tmp_path)
    app._download_future = None
    app._monitor_download_stop = threading.Event()
    app._monitor_live_concurrency = 1
    app._monitor_live_chunks = 1
    app._monitor_active_transfers = 0
    app._monitored = [
        {"target": "high", "priority": 99, "enabled": True},
        {"target": "low", "priority": 0, "enabled": True},
    ]
    app._log_download = lambda _text: None
    app._update_download_progress = lambda *_args: None
    events = []
    app._monitor_progress_state = lambda peer, mid, status: events.append(
        (peer, mid, status)
    )

    async def ensure():
        pass
    app._ensure_client = ensure
    old_history = DownloadHistory(db)
    old_history.enqueue_media([PendingMedia(456, 1, "low", "low", "video", "1")])
    old_history.close()

    async def scenario():
        low_started = asyncio.Event()
        high_started = asyncio.Event()
        low_resumed = asyncio.Event()
        release_high = asyncio.Event()
        release_low = asyncio.Event()
        high_done = asyncio.Event()

        async def download(_client, message, _folder, base, stats, *, peer_id,
                           monitor_flow, history, **_kwargs):
            monitor_flow.pause.register()
            try:
                if peer_id == 456:
                    low_started.set()
                else:
                    high_started.set()
                while not (release_high.is_set() if peer_id == 123 else release_low.is_set()):
                    await monitor_flow.pause.checkpoint()
                    if peer_id == 456 and high_started.is_set():
                        low_resumed.set()
                    await asyncio.sleep(0.015)
            finally:
                monitor_flow.pause.unregister()
            history.mark_saved(peer_id, message.id, "video", Path(base))
            if peer_id == 123:
                high_done.set()
            return Path(base)

        monkeypatch.setattr(downloader, "_download_with_retry", download)
        worker = asyncio.create_task(app._monitor_download_worker(1, 0, 1))
        try:
            await asyncio.wait_for(low_started.wait(), 3)
            pending = DownloadHistory(db)
            pending.enqueue_media([PendingMedia(123, 2, "high", "high", "video", "2")])
            pending.close()
            await asyncio.wait_for(high_started.wait(), 3)
            # Even if idle time has elapsed, a higher-ranked active file
            # prohibits lower-priority resumption until completion.
            await asyncio.sleep(0.7)
            assert not low_resumed.is_set()
            assert not high_done.is_set()
            release_high.set()
            await asyncio.wait_for(high_done.wait(), 3)
            await asyncio.wait_for(low_resumed.wait(), 3)
            assert (456, 1, "已暂停") in events
            assert (456, 1, "恢复中") in events
        finally:
            release_high.set()
            release_low.set()
            app._monitor_download_stop.set()
            await asyncio.wait_for(worker, 4)

    asyncio.run(scenario())


def test_resumed_job_precedes_new_job_at_same_priority(tmp_path, monkeypatch):
    db = tmp_path / "resume_first.sqlite"
    monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(db))

    async def resolve(_client, target):
        return SimpleNamespace(id=123 if target == "high" else 456, title=target)

    monkeypatch.setattr(gui, "resolve_channel", resolve)

    class Client:
        async def get_messages(self, _entity, ids):
            return SimpleNamespace(id=ids, date=datetime(2026, 11, 21, tzinfo=timezone.utc))

    app = object.__new__(gui._App)
    app.client = Client()
    app.settings = SimpleNamespace(download_dir=tmp_path, temp_download_dir=tmp_path)
    app._download_future = None
    app._monitor_download_stop = threading.Event()
    app._monitor_live_concurrency = 1
    app._monitor_live_chunks = 1
    app._monitored = [
        {"target": "high", "priority": 99, "enabled": True},
        {"target": "low", "priority": 0, "enabled": True},
    ]
    app._log_download = lambda _text: None
    app._update_download_progress = lambda *_args: None
    app._monitor_progress_state = lambda *_args: None
    async def ensure(): pass
    app._ensure_client = ensure
    history = DownloadHistory(db)
    history.enqueue_media([PendingMedia(456, 1, "low", "low", "video", "1")])
    history.close()

    async def scenario():
        low_started = asyncio.Event()
        high_started = asyncio.Event()
        high_release = asyncio.Event()
        low1_release = asyncio.Event()
        low1_resumed = asyncio.Event()
        low2_started = asyncio.Event()
        order = []

        async def download(_client, message, _folder, base, stats, *,
                           peer_id, monitor_flow, history, **_kwargs):
            monitor_flow.pause.register()
            try:
                if peer_id == 123:
                    high_started.set()
                    await high_release.wait()
                elif message.id == 1:
                    low_started.set()
                    while not low1_release.is_set():
                        await monitor_flow.pause.checkpoint()
                        if high_started.is_set() and high_release.is_set():
                            low1_resumed.set()
                        await asyncio.sleep(0.015)
                    order.append("resumed-first")
                else:
                    order.append("new-file")
                    low2_started.set()
            finally:
                monitor_flow.pause.unregister()
            history.mark_saved(peer_id, message.id, "video", Path(base))
            return Path(base)

        monkeypatch.setattr(downloader, "_download_with_retry", download)
        worker = asyncio.create_task(app._monitor_download_worker(1, 0, 1))
        try:
            await asyncio.wait_for(low_started.wait(), 3)
            pending = DownloadHistory(db)
            pending.enqueue_media([PendingMedia(123, 2, "high", "high", "video", "2")])
            pending.close()
            await asyncio.wait_for(high_started.wait(), 3)
            pending = DownloadHistory(db)
            pending.enqueue_media([PendingMedia(456, 3, "low", "low", "video", "3")])
            pending.close()
            await asyncio.sleep(0.3)
            assert not low1_resumed.is_set()
            assert not low2_started.is_set()
            high_release.set()
            await asyncio.wait_for(low1_resumed.wait(), 3)
            assert not low2_started.is_set()
            low1_release.set()
            await asyncio.wait_for(low2_started.wait(), 3)
            assert order == ["resumed-first", "new-file"]
        finally:
            high_release.set()
            low1_release.set()
            app._monitor_download_stop.set()
            await asyncio.wait_for(worker, 4)

    asyncio.run(scenario())
