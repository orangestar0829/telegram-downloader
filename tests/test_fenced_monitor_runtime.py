"""Each monitor attempt owns only its individual SQLite lease token."""

import asyncio
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl.types import Channel, ChatPhotoEmpty

from tgdl import downloader, gui, history as history_module, multi_runtime
from tgdl.accounts import Account, AccountRegistry
from tgdl.history import DownloadHistory, PendingMedia


def monitor_worker(account_id, tmp_path, monkeypatch, db):
    class Client:
        async def get_messages(self, _entity, ids):
            return SimpleNamespace(id=ids, date=datetime.now(timezone.utc))

    async def resolve(_client, _target):
        return Channel(123, "Shared", ChatPhotoEmpty(), None, megagroup=True)

    settings = SimpleNamespace(
        download_dir=tmp_path, temp_download_dir=None,
        media_speed_kib=0, media_concurrency=1, media_retries=0, chunk_concurrency=1,
    )
    channels = [
        {"target": "@shared", "peer_id": 123, "enabled": True, "types": "video"},
    ]
    if account_id == "default":
        app = object.__new__(gui._App)
        app.client = Client()
        app.settings = settings
        app._monitored = channels
        app._download_future = None
        app._monitor_download_stop = threading.Event()
        app._monitor_progress_state = lambda *_: None
        app._update_download_progress = lambda *_: None
        app._log_download = lambda *_: None

        async def ready():
            pass

        app._ensure_client = ready
        monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(db))
        monkeypatch.setattr(gui, "resolve_channel", resolve)
        return gui, downloader, app._monitor_download_worker(1, 0, 1), app._monitor_download_stop.set

    runtime = multi_runtime.AccountMonitorRuntime(
        Account(account_id, "Secondary"), AccountRegistry(tmp_path),
        ask=None, on_log=lambda *_: None, on_saved=lambda *_: None, on_progress=lambda *_: None,
    )
    runtime.client = Client()
    runtime.peer_by_target["@shared"] = 123
    monkeypatch.setattr(runtime, "_channels", lambda: channels)
    monkeypatch.setattr(multi_runtime, "resolve_channel", resolve)
    monkeypatch.setattr(multi_runtime, "classify_message", lambda _: "video")
    monkeypatch.setattr(multi_runtime, "read_env_values", lambda _: {
        "media_concurrency": "1", "media_retries": "0",
        "chunk_concurrency": "1", "media_speed_kib": "0",
    })
    return multi_runtime, multi_runtime, runtime._download_loop(settings), runtime.stop


@pytest.mark.parametrize("account_id", ["default", "other"])
@pytest.mark.parametrize("already_committed", [False, True])
def test_monitor_heartbeat_distinguishes_lost_lease_from_committed_cleanup(
    tmp_path, monkeypatch, account_id, already_committed,
):
    db = tmp_path / "tgdl_download_history.sqlite3"
    module, download_module, worker_coro, stop = monitor_worker(account_id, tmp_path, monkeypatch, db)
    clock = SimpleNamespace(now=0)
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(history_module, "time", SimpleNamespace(time=lambda: 1000 + clock.now))
    history = DownloadHistory(db)
    history.enqueue_media([PendingMedia(123, 1, "@shared", "shared", "video", "1")])
    first_started, first_cancelled, first_release, second_saved = [asyncio.Event() for _ in range(4)]
    tokens = {}
    heartbeat = asyncio.Event()
    renew = DownloadHistory.renew_claim

    def observe_renew(self, owner, peer, mid, *, token, **kwargs):
        if mid == 1:
            heartbeat.set()
        return renew(self, owner, peer, mid, token=token, **kwargs)

    monkeypatch.setattr(DownloadHistory, "renew_claim", observe_renew)

    async def download(_client, message, _folder, base, stats, *,
                       peer_id, history, claim_token, on_claim_committed, **_):
        tokens[message.id] = claim_token
        if message.id == 1:
            if already_committed:
                assert history.mark_claim_saved(
                    account_id, peer_id, message.id, "video", Path(base), token=claim_token
                )
                on_claim_committed()
            first_started.set()
            try:
                await first_release.wait()  # emulate an async resource teardown after committing
            except asyncio.CancelledError:
                first_cancelled.set()
                raise
            return Path(base)

        assert history.mark_claim_saved(
            account_id, peer_id, message.id, "video", Path(base), token=claim_token
        )
        on_claim_committed()
        second_saved.set()
        return Path(base)

    monkeypatch.setattr(download_module, "_download_with_retry", download)

    async def scenario():
        worker = asyncio.create_task(worker_coro)
        try:
            await asyncio.wait_for(first_started.wait(), 3)
            clock.now = 601
            if already_committed:
                assert history.contains(123, 1)
                # Observe that the worker has cycled through the heartbeat
                # while the already-committed transfer is still in teardown.
                seen_heartbeat_check = asyncio.Event()
                now_tick = module.time.monotonic

                def monitor_now():
                    seen_heartbeat_check.set()
                    return now_tick()

                monkeypatch.setattr(module.time, "monotonic", monitor_now)
                await asyncio.wait_for(seen_heartbeat_check.wait(), 3)
                assert not heartbeat.is_set()
                assert not first_cancelled.is_set()
                first_release.set()
            else:
                # This simulates another process acquiring the same job with
                # the same account identifier while the old worker was stalled.
                replacement = history.try_claim(account_id, 123, 1)
                assert replacement is not None and replacement != tokens[1]
                await asyncio.wait_for(first_cancelled.wait(), 3)
                assert heartbeat.is_set()
                assert history.owns_claim(account_id, 123, 1, token=replacement)
                assert not history.contains(123, 1)

            history.enqueue_media([PendingMedia(123, 2, "@shared", "shared", "video", "2")])
            await asyncio.wait_for(second_saved.wait(), 3)
            assert not worker.done()
            assert history.contains(123, 2)
            if not already_committed:
                assert history.owns_claim(account_id, 123, 1, token=replacement)
        finally:
            first_release.set()
            stop()
            await asyncio.wait_for(worker, 4)

    try:
        asyncio.run(scenario())
    finally:
        history.close()
