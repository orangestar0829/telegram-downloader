"""Offline integration of realtime overflow with both account workers."""
import asyncio
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl.types import Channel, ChatPhotoEmpty, Message, MessageMediaPhoto, PeerChannel

from tgdl import gui, multi_runtime
from tgdl.accounts import Account, AccountRegistry
from tgdl.history import DownloadHistory, PendingMedia
from tgdl.monitor_flow import MonitorPause
from tgdl.realtime_burst import RealtimeBurst, realtime_candidates
from tgdl.slots import SlotCoordinator, make_plan


def live_row(peer=200):
    return {"target": "@live", "peer_id": peer, "enabled": True,
            "mode": "realtime", "priority": -999, "types": "photo"}


def test_borrow_candidate_does_not_preempt_for_a_duplicate_manual_file():
    slots = SlotCoordinator()
    row = live_row()
    slots.configure(make_plan([("a", 3, [row])]))
    assert slots.acquire("a", 3, 200, file_key=(200, 10)) is not None
    manual = slots.acquire("a", 3, file_key=(200, 11))
    assert manual is not None
    job = PendingMedia(200, 11, "@live", "live", "photo", "11")
    history = SimpleNamespace(pending_for_peer=lambda *a, **kw: [job])
    assert list(realtime_candidates([row], {200}, slots, history, "a", 3, {}, borrow=True)) == []
    slots.release(manual)
    assert list(realtime_candidates([row], {200}, slots, history, "a", 3, {}, borrow=True)) == [(row, job)]
    assert list(realtime_candidates([row], set(), slots, history, "a", 3, {}, borrow=True)) == []


def test_return_turn_only_after_sustained_burst_and_protects_resumed_file():
    flow = MonitorPause()
    victim = (123, 1)
    pauses = {victim: flow}
    burst = RealtimeBurst()
    assert burst.request(pauses, [victim], 100) is None  # no media lane yet
    flow.register()
    assert burst.request(pauses, [victim], 100) == victim
    assert not flow.safely_suspended  # in-flight media request has not reached a checkpoint
    for i in range(3):
        key = (200, i)
        burst.borrowed.add(key)
        burst.finished(key)
        assert burst.ordinary_turn is (i == 2)
    flow.resume()
    burst.ordinary_started(victim, 100)
    assert burst.request(pauses, [victim], 104) is None
    assert burst.request(pauses, [victim], 106) == victim


@pytest.mark.parametrize("account_id", ["default", "secondary"])
@pytest.mark.parametrize("ordinary", ["interrupt", "staged", "free", "manual", "stop", "sustained"])
def test_realtime_borrows_only_a_safe_ordinary_slot(tmp_path, monkeypatch, account_id, ordinary):
    rows = [{"target": "@old", "peer_id": 100, "enabled": True,
             "priority": 99, "types": "photo"}, live_row()]
    slots = SlotCoordinator()
    slots.configure(make_plan([(account_id, 2, rows)]))
    settings = SimpleNamespace(
        download_dir=tmp_path, temp_download_dir=tmp_path / "stage" if ordinary == "staged" else None,
        media_speed_kib=0, media_concurrency=2, media_retries=0, chunk_concurrency=1,
    )
    path = tmp_path / "tgdl_download_history.sqlite3"
    state_changes = []
    history = DownloadHistory(path)
    interruptible = ordinary in {"interrupt", "stop", "sustained"}
    has_scheduled = interruptible or ordinary == "staged"
    if has_scheduled:
        history.enqueue_media([PendingMedia(100, 1, "@old", "old", "photo", "1")])

    def entity(peer):
        return Channel(peer, str(peer), ChatPhotoEmpty(), None, megagroup=True)

    async def resolve(_client, target):
        assert target == "@old"
        return entity(100)

    async def ensure():
        pass

    class Client:
        async def get_entity(self, peer):
            assert isinstance(peer, PeerChannel) and peer.channel_id == 200
            return entity(200)

        async def get_messages(self, selected, ids):
            return Message(id=ids, peer_id=PeerChannel(selected.id), date=datetime.now(timezone.utc),
                           message="", media=MessageMediaPhoto())

    client = Client()
    if account_id == "default":
        runtime = object.__new__(gui._App)
        runtime.client = client
        runtime.settings = settings
        runtime._monitored = rows
        runtime._download_future = None
        runtime._monitor_download_stop = threading.Event()
        runtime._monitor_progress_state = lambda *args: state_changes.append(args)
        runtime._update_download_progress = lambda *_: None
        runtime._log_download = lambda *_: None
        runtime._ensure_client = ensure
        worker = runtime._monitor_download_worker(2, 0, 1)
        stop = runtime._monitor_download_stop.set
        monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(path))
        monkeypatch.setattr(gui, "resolve_channel", resolve)
    else:
        runtime = multi_runtime.AccountMonitorRuntime(
            Account(account_id, "secondary"), AccountRegistry(tmp_path),
            ask=None, on_log=lambda *_: None, on_saved=lambda *_: None, on_progress=lambda *_: None,
            on_progress_state=lambda *args: state_changes.append(args),
        )
        runtime.client = client
        assert runtime.start_monitoring()
        runtime.peer_by_target["@old"] = 100
        monkeypatch.setattr(runtime, "_channels", lambda: rows)
        monkeypatch.setattr(multi_runtime, "resolve_channel", resolve)
        monkeypatch.setattr(multi_runtime, "read_env_values", lambda _: {
            "media_concurrency": "2", "media_retries": "0",
            "chunk_concurrency": "1", "media_speed_kib": "0",
        })
        worker = runtime._download_loop(settings)
        stop = runtime.stop
    runtime._slots = slots
    runtime._realtime_ready = {200}

    async def exercise():
        starts = asyncio.Queue()
        low_at_checkpoint, low_resumed, low_finish = (asyncio.Event() for _ in range(3))
        last_live = 14 if ordinary == "sustained" else 11
        live_finish = {mid: asyncio.Event() for mid in range(10, last_live + 1)}
        evaluated_victims = asyncio.Queue()
        original_request = RealtimeBurst.request

        def request(policy, pauses, victims, now):
            victims = list(victims)
            key = original_request(policy, pauses, victims, now)
            evaluated_victims.put_nowait((victims, key))
            return key

        async def media(message, *, file, progress_callback):
            peer, mid = message.peer_id.channel_id, message.id
            payload = Path(file) / "fixture.jpg"
            starts.put_nowait((peer, mid))
            if peer == 100:
                payload.write_bytes(b"first")
                await low_at_checkpoint.wait()
                await progress_callback(5, 10)
                low_resumed.set()
                await low_finish.wait()
                with payload.open("ab") as f:
                    f.write(b"-last")
                await progress_callback(10, 10)
            else:
                await live_finish[mid].wait()
                payload.write_bytes(f"live-{mid}".encode())
            return payload

        client.download_media = media
        monkeypatch.setattr(RealtimeBurst, "request", request)
        manual = slots.acquire(account_id, 2, file_key=(300, 1)) if ordinary == "manual" else None
        task = asyncio.create_task(worker)
        try:
            if has_scheduled:
                assert await asyncio.wait_for(starts.get(), 5) == (100, 1)
            history.start_realtime(account_id, 200, "epoch", 0)
            assert history.commit_realtime(account_id, 200, "epoch", last_live, [
                PendingMedia(200, mid, "@live", "live", "photo", str(mid))
                for mid in live_finish
            ]) == (len(live_finish), last_live)
            assert await asyncio.wait_for(starts.get(), 5) == (200, 10)
            if interruptible:
                assert await asyncio.wait_for(evaluated_victims.get(), 5) == ([(100, 1)], (100, 1))
                assert starts.empty()
                assert not slots.available(account_id, 2)  # in-flight RPC still costs its permit
                low_at_checkpoint.set()
            elif ordinary in {"staged", "manual"}:
                assert await asyncio.wait_for(evaluated_victims.get(), 5) == ([], None)
                assert starts.empty()
                if ordinary == "staged":
                    low_at_checkpoint.set()
                    await asyncio.wait_for(low_resumed.wait(), 5)
                    assert starts.empty()
                    low_finish.set()
                else:
                    slots.release(manual)
                    manual = None
            assert await asyncio.wait_for(starts.get(), 5) == (200, 11)
            assert not live_finish[10].is_set()  # dedicated and borrowed really overlap
            if interruptible:
                assert not low_resumed.is_set()
                assert (100, 1, "已暂停") in state_changes
                assert any((peer, mid) == (100, 1) for peer, mid, _, _ in history.active_claims())
                if ordinary == "stop":
                    stop()  # Drain a still-paused worker, even with live files in progress.
                if ordinary == "interrupt" and account_id == "secondary":
                    runtime._blocked.add("@old")  # no new claims, but an active one may finish
                live_finish[11].set()
                if ordinary == "sustained":
                    for mid in (12, 13):  # Keep the dedicated permit busy throughout.
                        assert await asyncio.wait_for(starts.get(), 5) == (200, mid)
                        assert not low_resumed.is_set()
                        live_finish[mid].set()
                await asyncio.wait_for(low_resumed.wait(), 5)
                assert (100, 1, "恢复中") in state_changes
                assert not live_finish[10].is_set()
            assert starts.empty()  # The original ordinary coroutine was not restarted.
        finally:
            stop()
            slots.release(manual)
            low_at_checkpoint.set()
            low_finish.set()
            for gate in live_finish.values():
                gate.set()
            await asyncio.wait_for(task, 5)
        assert not history.active_claims()
        assert history.contains(200, 10) and history.contains(200, 11)
        if ordinary == "sustained":
            assert history.contains(200, 12) and history.contains(200, 13)
            assert not history.contains(200, 14)  # Still eligible, but ordinary got a turn.
        if has_scheduled:
            assert history.contains(100, 1)
            assert [p.read_bytes() for p in tmp_path.rglob("1.jpg")] == [b"first-last"]

    try:
        asyncio.run(exercise())
    finally:
        history.close()
