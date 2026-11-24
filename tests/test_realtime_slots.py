"""Account budgets, handover and GUI manual-file admission (offline)."""
import asyncio
import threading
from types import SimpleNamespace

from tgdl import downloader, monitor
from tgdl.downloader import DownloadStats
from tgdl.history import DownloadHistory, PendingMedia
from tgdl.slots import SlotCoordinator, make_plan


def row(peer):
    return {"mode": "realtime", "enabled": True, "peer_id": peer, "target": str(peer)}


def test_shared_channel_has_one_owner_even_across_handover_and_inflight_drain():
    slots = SlotCoordinator()
    slots.configure(make_plan([("a", 2, [row(10)]), ("b", 1, [row(10)])]))
    assert slots.ordinary_capacity("a", 2) == 1
    assert slots.ordinary_capacity("b", 1) == 1
    assert not slots.owns("b", 10)
    a_live = slots.acquire("a", 2, 10, file_key=(10, 8))
    assert a_live is not None
    assert slots.acquire("b", 1, file_key=(10, 8)) is None  # manual/monitor file fencing
    slots.configure(make_plan([("a", 2, []), ("b", 1, [row(10)])]))
    assert slots.owns("b", 10)
    assert slots.acquire("b", 1, 10, file_key=(10, 9)) is None
    slots.release(a_live)
    assert slots.acquire("b", 1, 10, file_key=(10, 9)) is not None


def test_cutover_waits_for_same_peer_scheduled_transfer_on_another_account():
    slots = SlotCoordinator()
    slots.configure(make_plan([("a", 2, []), ("b", 2, [])]))
    prior = slots.acquire("b", 2, file_key=(40, 9), scheduled_monitor=True)
    assert prior is not None
    slots.configure(make_plan([("a", 2, [row(40)]), ("b", 2, [])]))
    assert not slots.available("a", 2, 40)
    assert slots.acquire("b", 2, file_key=(40, 10), scheduled_monitor=True) is None
    slots.release(prior)
    assert slots.available("a", 2, 40)


def test_reconfigure_does_not_overbook_during_existing_transfers():
    slots = SlotCoordinator()
    slots.configure(make_plan([("a", 2, [])]))
    tokens = [slots.acquire("a", 2), slots.acquire("a", 2)]
    slots.configure(make_plan([("a", 2, [row(1), row(2)])]))
    assert slots.ordinary_capacity("a", 2) == 0
    assert not slots.available("a", 2, 1)
    slots.release(tokens.pop())
    assert slots.acquire("a", 2, 2) is not None
    assert not slots.available("a", 2, 1)
    slots.release(tokens.pop())
    assert slots.available("a", 2, 1)
    assert not slots.available("a", 2)  # never borrow idle peer 1


def test_overflow_uses_only_owned_ordinary_capacity_and_blocks_old_owner_handover():
    slots = SlotCoordinator()
    slots.configure(make_plan([("a", 3, [row(10), row(20)]), ("b", 2, [row(10)])]))
    assert slots.acquire("a", 3, 10, file_key=(10, 2), borrow=True) is None
    live = slots.acquire("a", 3, 10, file_key=(10, 1))
    manual = slots.acquire("a", 3, file_key=(30, 1))
    assert live is not None and manual is not None
    assert slots.acquire("a", 3, 10, file_key=(10, 2), borrow=True) is None
    assert slots.acquire("b", 2, 10, file_key=(10, 2), borrow=True) is None
    slots.release(manual)
    overflow = slots.acquire("a", 3, 10, file_key=(10, 2), borrow=True)
    assert overflow is not None
    assert slots.acquire("a", 3, 10, file_key=(10, 3), borrow=True) is None
    assert slots.acquire("a", 3, file_key=(30, 1)) is None
    assert slots.acquire("a", 3, 20, file_key=(20, 1)) is not None
    slots.release(live)
    replacement = slots.acquire("a", 3, 10, file_key=(10, 3))
    assert replacement is not None  # Its own overflow never occupies its dedicated lane.
    slots.configure(make_plan([("a", 3, [row(20)]), ("b", 2, [row(10)])]))
    slots.release(replacement)
    assert slots.acquire("b", 2, 10, file_key=(10, 4)) is None
    slots.release(overflow)
    assert slots.acquire("b", 2, 10, file_key=(10, 4)) is not None


def test_new_activation_timestamp_survives_exit_before_first_monitor_start(tmp_path, monkeypatch):
    monkeypatch.setattr(monitor.time, "time", lambda: 123456)
    row = {"target": "-1000000000010", "minutes": 5}
    # Canonical ID comes from the selected joined channel's typed dialog.
    dialog = SimpleNamespace(entity_id=-1000000000010, kind="channel", title="test", username=None)
    monitor.configure_monitor_mode(row, None, monitor.REALTIME, [dialog])
    path = tmp_path / "mon.json"
    monitor.save_channels([row], path)
    saved, = monitor.load_channels(path)
    assert saved["realtime_since"] == 123456
    retained = dict(saved, priority=99)
    monkeypatch.setattr(monitor.time, "time", lambda: 123999)
    monitor.configure_monitor_mode(retained, saved, monitor.REALTIME, [])
    assert retained["realtime_since"] == 123456
    monitor.configure_monitor_mode(retained, saved, monitor.SCHEDULED, [])
    assert "realtime_since" not in retained


def test_manual_jobs_stay_within_ordinary_allowance_and_cancel_waiters(tmp_path, monkeypatch):
    async def scenario():
        slots = SlotCoordinator()
        plan = lambda peers: make_plan([("default", 2, [row(p) for p in peers])])
        slots.configure(plan([10]))
        live = slots.acquire("default", 2, 10, file_key=(10, 111))
        entered = []
        first_entered = asyncio.Event()
        resume = asyncio.Event()
        stop = threading.Event()

        async def transfer(client, message, dest, base, stats, **kwargs):
            entered.append(message.id)
            first_entered.set()
            await resume.wait()

        async def jobs():
            for mid in (1, 2, 3):
                yield SimpleNamespace(id=mid), tmp_path, str(mid)

        monkeypatch.setattr(downloader, "_download_with_retry", transfer)
        task = asyncio.create_task(downloader.download_many(
            object(), jobs(), concurrency=2, peer_id=20, gui_slots=slots,
            stats=DownloadStats(), stop_event=stop,
        ))
        await asyncio.wait_for(first_entered.wait(), 1)
        assert entered == [1]
        slots.configure(plan([10, 20]))  # ongoing file drains, no new manual file
        resume.set()
        await asyncio.sleep(.15)
        assert entered == [1]
        assert not task.done()
        stop.set()
        await asyncio.wait_for(task, 1)
        assert entered == [1]
        slots.release(live)
        assert slots.available("default", 2, 10)

    asyncio.run(scenario())


def test_manual_gallery_or_bulk_skips_a_paused_monitors_leased_file(tmp_path, monkeypatch):
    history = DownloadHistory(tmp_path / "history.db")
    job = PendingMedia(20, 1, "@test", "T", "video", "1")
    history.enqueue_media([job])
    token = history.try_claim("default", 20, 1)
    slots = SlotCoordinator()
    slots.configure(make_plan([("default", 2, [row(10)])]))

    async def jobs():
        yield SimpleNamespace(id=1), tmp_path, "1"

    try:
        for kwargs in ({"history": history}, {"gallery_history": history}):
            stats = DownloadStats()
            saved = asyncio.run(downloader.download_many(
                None, jobs(), stats=stats, gui_slots=slots, concurrency=2, peer_id=20, **kwargs,
            ))
            assert saved == [] and stats.skipped == 1
            assert history.owns_claim("default", 20, 1, token=token)
    finally:
        history.close()
