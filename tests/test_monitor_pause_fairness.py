"""Strict high-priority drain; paused files precede fresh same-rank jobs."""

import asyncio
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from tgdl import downloader, gui
from tgdl.history import DownloadHistory, PendingMedia


class _SchedulerSteps:
    """Advance the real monitor worker without relying on platform timers."""

    def __init__(self):
        self._parked = asyncio.Queue()
        self._permits = asyncio.Queue()
        self._draining = False

    def __getattr__(self, name):
        return getattr(asyncio, name)

    async def sleep(self, _seconds):
        if self._draining:
            await asyncio.sleep(0)
            return
        self._parked.put_nowait(None)
        await self._permits.get()

    async def wait_until_parked(self):
        await asyncio.wait_for(self._parked.get(), 3)

    async def advance(self):
        self._permits.put_nowait(None)
        await self.wait_until_parked()

    def drain(self):
        self._draining = True
        self._permits.put_nowait(None)


@pytest.mark.parametrize("use_staging", [False, True])
def test_paused_low_waits_until_higher_priority_transfer_completes(
    tmp_path, monkeypatch, use_staging,
):
    db = tmp_path / "history.sqlite"

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
    app.settings = SimpleNamespace(download_dir=tmp_path, temp_download_dir=tmp_path if use_staging else None)
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
        steps = _SchedulerSteps()
        # Only worker polling is test-controlled. The real pause checkpoints
        # and SQLite claims still run normally.
        monkeypatch.setattr(gui, "asyncio", steps)
        low_started, high_started = asyncio.Event(), asyncio.Event()
        high_seen = asyncio.Event()
        low_done, high_done = asyncio.Event(), asyncio.Event()
        low_at_checkpoint, low_resumed = asyncio.Event(), asyncio.Event()
        release_high, release_low = asyncio.Event(), asyncio.Event()
        low_pause = None

        def worker_history():
            history = DownloadHistory(db)
            query = history.pending_for_target

            def observe_pending(target, *args, **kwargs):
                jobs = query(target, *args, **kwargs)
                if target == "high" and any(job.message_id == 2 for job in jobs):
                    high_seen.set()
                return jobs

            history.pending_for_target = observe_pending
            return history

        monkeypatch.setattr(gui, "DownloadHistory", worker_history)

        async def download(_client, message, _folder, base, stats, *, peer_id,
                           monitor_flow, history, **_kwargs):
            nonlocal low_pause
            pause = monitor_flow.pause
            pause.register()
            try:
                if peer_id == 456:
                    low_pause = pause
                    low_started.set()
                    if not use_staging:
                        await low_at_checkpoint.wait()
                        await pause.checkpoint()
                        low_resumed.set()
                    await release_low.wait()
                else:
                    high_started.set()
                    await release_high.wait()
            finally:
                pause.unregister()
            history.mark_saved(peer_id, message.id, "video", Path(base))
            (low_done if peer_id == 456 else high_done).set()
            return Path(base)

        monkeypatch.setattr(downloader, "_download_with_retry", download)
        worker = asyncio.create_task(app._monitor_download_worker(1, 0, 1))
        phase = "start low"
        try:
            await asyncio.wait_for(low_started.wait(), 3)
            await steps.wait_until_parked()
            pending = DownloadHistory(db)
            pending.enqueue_media([PendingMedia(123, 2, "high", "high", "video", "2")])
            pending.close()
            # Let the monitor examine the eligible high-priority file while
            # low still holds the slot, before checking its decision.
            phase = "evaluate high while low is active"
            await steps.advance()
            assert high_seen.is_set(), "worker did not see the eligible high-priority job"
            assert not high_started.is_set()
            if use_staging:
                assert not low_pause.requested
                assert (456, 1, "已暂停") not in events
                phase = "finish staged low"
                release_low.set()
                await asyncio.wait_for(low_done.wait(), 3)
                phase = "start high after staged low completed"
                await steps.advance()
                await asyncio.wait_for(high_started.wait(), 3)
                phase = "finish high"
                release_high.set()
                await asyncio.wait_for(high_done.wait(), 3)
                return

            assert low_pause.requested
            phase = "acknowledge pause and start high"
            low_at_checkpoint.set()
            await asyncio.wait_for(low_pause.wait_suspended(), 3)
            await steps.advance()
            await asyncio.wait_for(high_started.wait(), 3)
            # Even with a newly available slot, active high-priority work
            # must finish before the scheduler resumes the low-priority file.
            app._monitor_live_concurrency = 2
            phase = "keep low paused even with a spare slot"
            await steps.advance()
            assert low_pause.suspended
            assert not low_resumed.is_set()
            assert not high_done.is_set()
            phase = "finish high and resume low"
            release_high.set()
            await asyncio.wait_for(high_done.wait(), 3)
            await steps.advance()
            await asyncio.wait_for(low_resumed.wait(), 3)
            assert (456, 1, "已暂停") in events
            assert (456, 1, "恢复中") in events
        except asyncio.TimeoutError:
            if worker.done():
                worker.result()
            pytest.fail(
                f"monitor failed to {phase} (staging={use_staging}, "
                f"low_started={low_started.is_set()}, low_done={low_done.is_set()}, "
                f"high_seen={high_seen.is_set()}, high_started={high_started.is_set()}, "
                f"high_done={high_done.is_set()}, progress={events!r})"
            )
        finally:
            low_at_checkpoint.set()
            release_high.set()
            release_low.set()
            app._monitor_download_stop.set()
            steps.drain()
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
    app.settings = SimpleNamespace(download_dir=tmp_path, temp_download_dir=None)
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
