"""Offline coverage of per-account policies in the real monitor workers."""

import asyncio
import threading
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.errors import FloodPremiumWaitError, FloodWaitError
from telethon.tl.types import Channel, ChatPhotoEmpty

from tgdl import downloader, gui, history as history_module, monitor_flow, multi_runtime, realtime
from tgdl.accounts import Account, AccountRegistry
from tgdl.auto_throttle import MonitorAutoThrottle
from tgdl.config import load_settings, read_env_values, update_env
from tgdl.history import DownloadHistory, PendingMedia
from tgdl.slots import SlotCoordinator, make_plan


PRIMARY_CAP = 4096
ORDINARY_JOB = PendingMedia(100, 1, "@old", "old", "video", "1")
LIVE_JOB = PendingMedia(200, 11, "@live", "live", "video", "11")


def setup_workers(account_id, tmp_path, monkeypatch, *, include_live=False):
    registry = AccountRegistry(tmp_path)
    db_path = tmp_path / "tgdl_download_history.sqlite3"
    env_path = registry.env_path(account_id)
    update_env({
        "api_id": "1", "api_hash": "offline", "download_dir": str(tmp_path),
        "media_speed_kib": str(PRIMARY_CAP), "monitor_auto_throttle": "1",
        "media_concurrency": "2", "media_retries": "2", "chunk_concurrency": "1",
    }, env_path)
    rows = [{
        "target": "@old", "peer_id": 100, "enabled": True,
        "types": "video", "priority": 0,
    }]
    if include_live:
        rows.append({
            "target": "@live", "peer_id": 200, "enabled": True,
            "types": "video", "priority": 0, "mode": "realtime",
        })

    async def ensure():
        pass

    async def get_messages(_entity, ids):
        return SimpleNamespace(id=ids, date=datetime.now(timezone.utc))

    def entity(peer, target):
        return Channel(peer, target, ChatPhotoEmpty(), None, megagroup=True)

    async def resolve(_client, target):
        assert target == "@old"
        return entity(100, target)

    async def resolve_live(_client, target, *, expected_peer_id):
        assert target == "@live" and expected_peer_id == 200
        return entity(200, target)

    monkeypatch.setattr(realtime, "resolve_realtime_peer", resolve_live)
    slots = SlotCoordinator()
    slots.configure(make_plan([(account_id, 2, rows)]))
    app = object.__new__(gui._App)
    app._account_registry = registry
    app._monitor_throttle_publications = {}
    app._extra_runtimes = {}
    client = SimpleNamespace(get_messages=get_messages)

    if account_id == "default":
        app._slots = slots
        app._monitor_download_stop = threading.Event()
        app._ensure_client = ensure
        app.client = client
        app._monitored = rows
        app._realtime_ready = {200} if include_live else set()
        app._monitor_live_throttle_settings = (True, PRIMARY_CAP)
        app._download_future = None
        app._log_download = lambda *_: None
        app._update_download_progress = lambda *_: None
        monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(db_path))
        monkeypatch.setattr(gui, "resolve_channel", resolve)

        def launch():
            app._monitor_download_stop.clear()
            app.settings = load_settings(env_path=env_path)
            return asyncio.create_task(app._monitor_download_worker(2, 2, 1))

        return app, app, db_path, env_path, launch, app._monitor_download_stop.set, downloader

    runtime = multi_runtime.AccountMonitorRuntime(
        Account(account_id, "second"), registry, slots=slots, ask=None,
        on_log=lambda *_: None, on_saved=lambda *_: None, on_progress=lambda *_: None,
    )
    app._extra_runtimes[account_id] = runtime
    runtime.client = client
    runtime._realtime_ready = {200} if include_live else set()
    monkeypatch.setattr(runtime, "_channels", lambda: rows)
    monkeypatch.setattr(multi_runtime, "resolve_channel", resolve)
    monkeypatch.setattr(multi_runtime, "classify_message", lambda _: "video")

    def launch():
        runtime.stop_event.clear()
        assert runtime.start_monitoring()
        runtime.peer_by_target["@old"] = 100
        return asyncio.create_task(runtime._download_loop(load_settings(env_path=env_path)))

    return app, runtime, db_path, env_path, launch, runtime.stop, multi_runtime


@pytest.mark.parametrize("account_id", ["default", "secondary"])
def test_worker_shares_cap_across_both_modes_preserves_cuts_and_applies_toggle(
    tmp_path, monkeypatch, account_id,
):
    app, owner, db, env, launch, stop, download_module = setup_workers(
        account_id, tmp_path, monkeypatch, include_live=True,
    )
    clock = SimpleNamespace(now=100.0)
    worker_module = gui if account_id == "default" else multi_runtime
    monkeypatch.setattr(worker_module, "time", SimpleNamespace(monotonic=lambda: clock.now))

    with closing(DownloadHistory(db)) as ledger:
        ledger.enqueue_media([ORDINARY_JOB])
        ledger.start_realtime(account_id, 200, "epoch", 10)
        assert ledger.commit_realtime(account_id, 200, "epoch", 11, [LIVE_JOB]) == (1, 11)

        async def scenario():
            app.loop = SimpleNamespace(loop=asyncio.get_running_loop())
            started = asyncio.Queue()
            held = asyncio.Event()
            polled = asyncio.Event()

            class ClockedGovernor(MonitorAutoThrottle):
                def __init__(self, limiter, *, log):
                    super().__init__(limiter, clock=lambda: clock.now, log=log)

                def configure(self, manual_kib, enabled):
                    super().configure(manual_kib, enabled)
                    polled.set()

            monkeypatch.setattr(monitor_flow, "MonitorAutoThrottle", ClockedGovernor)

            async def download(_client, message, _folder, base, _stats, *,
                               peer_id, history, claim_token, on_claim_committed, monitor_flow, **_):
                started.put_nowait((message.id, monitor_flow.limiter))
                await held.wait()
                assert history.mark_claim_saved(
                    account_id, peer_id, message.id, "video", Path(base), token=claim_token,
                )
                on_claim_committed()
                return Path(base)

            def publish(enabled):
                values = read_env_values(env)
                update_env({**values, "monitor_auto_throttle": str(int(enabled))}, env)
                app._publish_monitor_throttle_settings(account_id, enabled, PRIMARY_CAP)

            def pair(governor):
                clock.now += 10
                governor.on_wait(clock.now - 1, 3, "FLOOD_WAIT")
                clock.now += 10
                governor.on_wait(clock.now - 1, 3, "FLOOD_PREMIUM_WAIT")

            monkeypatch.setattr(download_module, "_download_with_retry", download)
            worker = launch()
            try:
                first = await asyncio.wait_for(started.get(), 3)
                second = await asyncio.wait_for(started.get(), 3)
                limiters = dict((first, second))
                assert set(limiters) == {ORDINARY_JOB.message_id, LIVE_JOB.message_id}
                limiter = limiters[ORDINARY_JOB.message_id]
                assert limiter is limiters[LIVE_JOB.message_id]
                governor = owner._monitor_throttle
                assert governor is limiter.auto_throttle and governor.enabled
                assert limiter.bytes_per_second == PRIMARY_CAP * 1024

                pair(governor)
                cut = PRIMARY_CAP * 3 // 4
                assert limiter.bytes_per_second == cut * 1024
                assert read_env_values(env)["media_speed_kib"] == str(PRIMARY_CAP)
                # Force a refresh (including secondary's 5s .env poll) while
                # both modes still have an active transfer.
                polled.clear()
                await asyncio.wait_for(polled.wait(), 2)
                assert limiter.bytes_per_second == cut * 1024
                assert not worker.done()

                publish(False)
                await asyncio.sleep(0)   # queued cross-thread callback, not a periodic timer
                assert not governor.enabled
                assert limiter.bytes_per_second == PRIMARY_CAP * 1024

                publish(True)
                await asyncio.sleep(0)
                assert governor.enabled
                pair(governor)
                assert governor.effective_kib == cut
            finally:
                held.set()
                stop()
                await asyncio.wait_for(worker, 3)

            assert owner._monitor_throttle is None
            assert not ledger.active_claims()
            assert ledger.contains(100, 1) and ledger.contains(200, 11)

            polled.clear()
            restarted = launch()
            try:
                await asyncio.wait_for(polled.wait(), 2)
                fresh = owner._monitor_throttle
                assert fresh is not governor and fresh.enabled
                assert fresh.effective_kib == PRIMARY_CAP
                clock.now += 20
                fresh.on_wait(clock.now - 1, 3)
                assert fresh.effective_kib == PRIMARY_CAP
            finally:
                stop()
                await asyncio.wait_for(restarted, 3)
            assert owner._monitor_throttle is None

        asyncio.run(scenario())


@pytest.mark.parametrize("account_id", ["default", "secondary"])
@pytest.mark.parametrize("error", [FloodWaitError, FloodPremiumWaitError])
def test_long_media_flood_keeps_claim_in_account_cooldown(
    tmp_path, monkeypatch, account_id, error,
):
    _, _, db, _, launch, stop, _ = setup_workers(account_id, tmp_path, monkeypatch)
    start = 1000
    wait = 120
    monkeypatch.setattr(history_module, "time", SimpleNamespace(time=lambda: start))
    attempts = []

    async def fail(*_args, **kwargs):
        assert kwargs["monitor_flow"] is not None
        attempts.append(kwargs["account_id"])
        stop()  # only allow this transfer; its normal release path still runs
        raise error(request=None, capture=wait)

    # Exercise the real file retry wrapper plus the real worker's catch/release.
    monkeypatch.setattr(downloader, "_download_one", fail)
    with closing(DownloadHistory(db)) as ledger:
        ledger.enqueue_media([ORDINARY_JOB])

        async def scenario():
            await asyncio.wait_for(launch(), 2)

        asyncio.run(scenario())
        assert attempts == [account_id], "a server wait must not become an ordinary short retry"
        assert not ledger.active_claims()
        assert not ledger.pending_for_peer(100, account_id=account_id, now=start + wait - .1)
        assert ledger.pending_for_peer(100, account_id=account_id, now=start + wait + 1) == [
            ORDINARY_JOB
        ]


def test_secondary_restarts_monitor_generation_without_reconnecting(tmp_path, monkeypatch):
    app, owner, db, env, launch, stop, download_module = setup_workers(
        "secondary", tmp_path, monkeypatch,
    )
    client = owner.client
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(multi_runtime, "time", SimpleNamespace(monotonic=lambda: clock.now))

    with closing(DownloadHistory(db)) as ledger:
        ledger.enqueue_media([ORDINARY_JOB])

        async def scenario():
            held = asyncio.Event()
            started = asyncio.Event()
            refreshed = asyncio.Event()
            app.loop = SimpleNamespace(loop=asyncio.get_running_loop())

            class ClockedGovernor(MonitorAutoThrottle):
                def __init__(self, limiter, *, log):
                    super().__init__(limiter, clock=lambda: clock.now, log=log)

                def configure(self, manual_kib, enabled):
                    super().configure(manual_kib, enabled)
                    refreshed.set()

            monkeypatch.setattr(monitor_flow, "MonitorAutoThrottle", ClockedGovernor)

            async def download(_client, message, _folder, base, _stats, *,
                               peer_id, history, claim_token, on_claim_committed, **_):
                started.set()
                await held.wait()
                assert history.mark_claim_saved(
                    "secondary", peer_id, message.id, "video", Path(base), token=claim_token,
                )
                on_claim_committed()
                return Path(base)

            def pair(governor):
                for _ in range(2):
                    clock.now += 10
                    governor.on_wait(clock.now - 1, 3)

            def publish(enabled, manual_kib):
                update_env({
                    **read_env_values(env), "monitor_auto_throttle": str(int(enabled)),
                    "media_speed_kib": str(manual_kib),
                }, env)
                app._publish_monitor_throttle_settings("secondary", enabled, manual_kib)

            async def wait_drained():
                while owner._monitor_throttle is not None:
                    await asyncio.sleep(.01)

            monkeypatch.setattr(download_module, "_download_with_retry", download)
            worker = launch()
            try:
                await asyncio.wait_for(started.wait(), 2)
                original = owner._monitor_throttle
                pair(original)
                assert original.effective_kib == PRIMARY_CAP * 3 // 4

                owner.stop_monitoring()
                assert owner.monitoring_draining
                assert not owner.start_monitoring()  # Original job still owns its governor.
                refreshed.clear()
                await asyncio.wait_for(refreshed.wait(), 2)
                assert owner._monitor_throttle is original
                assert original.effective_kib == PRIMARY_CAP * 3 // 4

                held.set()
                await asyncio.wait_for(wait_drained(), 2)
                assert ledger.contains(100, 1)
                assert not owner.monitoring_draining

                # The same logged-in account can save twice while the policy
                # is absent. Restart must use final saved state immediately,
                # not the settings captured when the account was launched.
                publish(False, 2048)
                publish(True, 8192)
                await asyncio.sleep(0)
                assert owner._monitor_throttle is None
                refreshed.clear()
                assert owner.start_monitoring()
                await asyncio.wait_for(refreshed.wait(), 2)
                restarted = owner._monitor_throttle
                assert restarted is not original and restarted.enabled
                assert restarted.effective_kib == 8192

                pair(restarted)
                assert restarted.effective_kib == 6144
                refreshed.clear()
                # No await between actions: the dispatcher cannot observe a
                # stopped tick and must notice the new stop-event identity.
                owner.stop_monitoring()
                assert owner.start_monitoring()
                await asyncio.wait_for(refreshed.wait(), 2)
                assert owner._monitor_throttle is not restarted
                assert owner._monitor_throttle.effective_kib == 8192
                assert owner.client is client and not worker.done()
            finally:
                held.set()
                stop()
                await asyncio.wait_for(worker, 3)
            assert owner._monitor_throttle is None

        asyncio.run(scenario())
