"""Monitor download worker does not block the independent scan coroutine."""
import asyncio
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from tgdl import downloader, gui
from tgdl.history import DownloadHistory, PendingMedia


class Message:
    def __init__(self, mid):
        self.id = mid
        self.date = datetime(2026, 11, 20, tzinfo=timezone.utc)


class Client:
    async def get_messages(self, entity, ids):
        return Message(ids)

    async def iter_messages(self, entity, **kwargs):
        ids = sorted(range(1, 5), reverse=not kwargs.get("reverse", False))
        filtered = [
            mid for mid in ids
            if mid > kwargs.get("min_id", 0)
            and mid < kwargs.get("max_id", 10**12)
        ]
        for mid in filtered[:kwargs.get("limit", len(filtered))]:
            yield Message(mid)


async def resolve(client, target):
    return SimpleNamespace(id=123 if target == "high" else 456, title=target)


def create_app(tmp_path, monkeypatch):
    db = tmp_path / "history.sqlite3"
    monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(db))
    monkeypatch.setattr(gui, "resolve_channel", resolve)
    monkeypatch.setattr(downloader, "DownloadHistory", lambda: DownloadHistory(db))
    monkeypatch.setattr(downloader, "resolve_channel", resolve)
    monkeypatch.setattr(downloader, "Message", Message)
    monkeypatch.setattr(downloader, "classify_message", lambda m: "photo")
    app = object.__new__(gui._App)
    app.client = Client()
    app.settings = SimpleNamespace(download_dir=tmp_path, temp_download_dir=tmp_path)
    app._download_future = None
    app._monitor_download_stop = threading.Event()
    app._monitor_worker_task = None
    app._monitor_active_transfers = 0
    app._monitored = [
        {"target": "high", "priority": 10, "enabled": True},
        {"target": "low", "priority": 0, "enabled": True},
    ]
    app._log_download = lambda text: None
    app._update_download_progress = lambda *args: None

    async def ensure():
        return None

    app._ensure_client = ensure
    return app, db


def test_single_slot_balances_time_instead_of_counting_three_files(tmp_path, monkeypatch):
    app, db = create_app(tmp_path, monkeypatch)
    history = DownloadHistory(db)
    history.enqueue_media([
        PendingMedia(123, mid, "high", "high", "photo", str(mid))
        for mid in range(11, 17)
    ] + [PendingMedia(456, 25, "low", "low", "photo", "25")])
    history.close()
    scheduled = []

    async def fake_download(client, message, folder, base, stats, *, history, peer_id, **kwargs):
        scheduled.append((peer_id, message.id))
        history.mark_saved(peer_id, message.id, "photo", Path(base))
        stats.saved += 1
        if len(scheduled) == 4:
            app._monitor_download_stop.set()
        return Path(base)

    monkeypatch.setattr(downloader, "_download_with_retry", fake_download)
    asyncio.run(asyncio.wait_for(app._monitor_download_worker(1, 0, 1), timeout=5))
    assert [peer for peer, _ in scheduled] == [123, 456, 123, 123]
    hist = DownloadHistory(db)
    assert hist.pending_count() == 3
    hist.close()


def test_monitor_fair_choice_prioritizes_unoccupied_channel():
    from tgdl.monitor import choose_monitor_download

    high = ({"target": "high", "priority": 10}, object())
    low = ({"target": "low", "priority": 0}, object())
    assert choose_monitor_download(
        [high, low], {"high": 1}, {"high": 400, "low": 0}
    ) is low
    assert choose_monitor_download(
        [high, low], {"high": 0, "low": 0}, {"high": 0, "low": 0}
    ) is high
    assert choose_monitor_download(
        [high, low], {"high": 1, "low": 1}, {"high": 400, "low": 2}
    ) is low


def test_monitor_fair_choice_borrows_idle_slot():
    from tgdl.monitor import choose_monitor_download

    high = ({"target": "high", "priority": 10}, object())
    assert choose_monitor_download(
        [high], {"high": 1}, {"high": 900}
    ) is high
    assert choose_monitor_download([], {}, {}) is None


def test_monitor_fair_choice_uses_elapsed_time_not_file_count():
    from tgdl.monitor import choose_monitor_download

    large = ({"target": "large", "priority": 10}, object())
    small = ({"target": "small", "priority": 0}, object())
    # With both channels free, the one that consumed less download time
    # wins even when it has lower numeric priority.
    assert choose_monitor_download(
        [large, small], {}, {"large": 3600, "small": 40}
    ) is small


def test_monitor_fair_choice_prevents_three_large_videos_from_monopolizing():
    from tgdl.monitor import choose_monitor_download

    high = ({"target": "high", "priority": 10}, object())
    low = ({"target": "low", "priority": 0}, object())
    third = ({"target": "third", "priority": -5}, object())
    pending = [high, low, third]
    slots = {}
    order = []
    for _ in range(3):
        picked = choose_monitor_download(pending, slots, {})
        order.append(picked[0]["target"])
        slots[order[-1]] = slots.get(order[-1], 0) + 1
    assert order == ["high", "low", "third"]


def test_monitor_worker_gives_small_media_its_own_slot(tmp_path, monkeypatch):
    app, db = create_app(tmp_path, monkeypatch)
    history = DownloadHistory(db)
    history.enqueue_media([
        PendingMedia(123, 101, "high", "high", "video", "101"),
        PendingMedia(123, 102, "high", "high", "video", "102"),
        PendingMedia(123, 103, "high", "high", "video", "103"),
        PendingMedia(456, 201, "low", "low", "photo", "201"),
    ])
    history.close()

    async def scenario():
        big_video_release = asyncio.Event()
        photo_finished = asyncio.Event()
        started = []
        async def fake_download(client, message, folder, base, stats, *, history, peer_id, **kwargs):
            started.append((peer_id, message.id))
            if peer_id == 123:
                await big_video_release.wait()
            else:
                photo_finished.set()
            history.mark_saved(peer_id, message.id, "photo", Path(base))
            return Path(base)

        monkeypatch.setattr(downloader, "_download_with_retry", fake_download)
        worker = asyncio.create_task(app._monitor_download_worker(3, 0, 1))
        try:
            await asyncio.wait_for(photo_finished.wait(), timeout=3)
            assert (456, 201) in started
            assert len([item for item in started if item[0] == 123]) <= 2
            assert not big_video_release.is_set()
        finally:
            app._monitor_download_stop.set()
            big_video_release.set()
            await asyncio.wait_for(worker, timeout=3)

    asyncio.run(scenario())


def test_monitor_worker_uses_live_file_concurrency_without_cancelling(tmp_path, monkeypatch):
    app, db = create_app(tmp_path, monkeypatch)
    app._monitor_live_concurrency = 2
    history = DownloadHistory(db)
    history.enqueue_media([
        PendingMedia(123, mid, "high", "high", "video", str(mid))
        for mid in range(301, 305)
    ])
    history.close()

    async def scenario():
        started = asyncio.Queue()
        gates = {mid: asyncio.Event() for mid in range(301, 305)}

        async def fake_download(client, message, folder, base, stats, *, history, peer_id, **kwargs):
            started.put_nowait(message.id)
            await gates[message.id].wait()
            history.mark_saved(peer_id, message.id, "video", Path(base))
            return Path(base)

        monkeypatch.setattr(downloader, "_download_with_retry", fake_download)
        worker = asyncio.create_task(app._monitor_download_worker(2, 0, 1))
        try:
            first = await asyncio.wait_for(started.get(), timeout=3)
            second = await asyncio.wait_for(started.get(), timeout=3)
            assert app._monitor_active_transfers == 2
            # Limit changes while existing downloads are running.
            app._monitor_live_concurrency = 1
            gates[first].set()
            await asyncio.sleep(0.55)
            assert started.empty(), "lower limit must not start another file while one is active"
            assert not gates[second].is_set(), "lowering limit must not cancel the other file"

            gates[second].set()
            third = await asyncio.wait_for(started.get(), timeout=3)
            assert app._monitor_active_transfers == 1
            # Raising from 1 to 2 should fill the newly available slot.
            app._monitor_live_concurrency = 2
            fourth = await asyncio.wait_for(started.get(), timeout=3)
            assert {first, second, third, fourth} == set(range(301, 305))
            assert app._monitor_active_transfers == 2
        finally:
            app._monitor_download_stop.set()
            for gate in gates.values():
                gate.set()
            await asyncio.wait_for(worker, timeout=3)
        completed = DownloadHistory(db)
        try:
            assert completed.pending_count() == 0
        finally:
            completed.close()

    asyncio.run(scenario())


def test_live_monitor_concurrency_validates_and_persists(monkeypatch):
    app = object.__new__(gui._App)
    class Var:
        def __init__(self, value):
            self.value = value
        def get(self):
            return self.value

    app.media_concurrency_var = Var("2")
    app._monitor_active = True
    app._monitor_active_transfers = 2
    app._monitor_live_concurrency = 2
    messages = []
    persisted = []
    app._log = messages.append
    monkeypatch.setattr(gui, "read_env_values", lambda: {"media_concurrency": "2", "phone": "saved"})
    monkeypatch.setattr(gui, "update_env", lambda values: persisted.append(dict(values)))
    app.media_concurrency_var.value = "1"
    app._on_monitor_concurrency_changed()
    assert app._monitor_live_concurrency == 1
    assert persisted[-1]["media_concurrency"] == "1"
    assert persisted[-1]["phone"] == "saved"
    assert "正在下载 2 个文件" in messages[-1]

    for invalid in ("", "0", "9", "x"):
        app.media_concurrency_var.value = invalid
        app._on_monitor_concurrency_changed()
    assert app._monitor_live_concurrency == 1
    assert len(persisted) == 1
    app.media_concurrency_var.value = "3"
    app._on_monitor_concurrency_changed()
    assert app._monitor_live_concurrency == 3
    assert len(persisted) == 2


def test_scan_completes_while_video_transfer_is_blocked(tmp_path, monkeypatch):
    app, db = create_app(tmp_path, monkeypatch)
    history = DownloadHistory(db)
    history.enqueue_media([PendingMedia(123, 99, "high", "high", "video", "99")])
    history.close()

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_download(client, message, folder, base, stats, *, history, peer_id, **kwargs):
            started.set()
            await release.wait()
            history.mark_saved(peer_id, message.id, "video", Path(base))
            return Path(base)

        monkeypatch.setattr(downloader, "_download_with_retry", slow_download)
        worker = asyncio.create_task(app._monitor_download_worker(1, 0, 1))
        try:
            await asyncio.wait_for(started.wait(), timeout=3)
            result = await asyncio.wait_for(
                downloader.scan_monitor_slice(
                    app.client, target="low", types={"photo"}, slice_size=2,
                ),
                timeout=2,
            )
            assert result.scanned_messages == 2
            assert result.more_pending
            assert not release.is_set()
        finally:
            app._monitor_download_stop.set()
            release.set()
            await asyncio.wait_for(worker, timeout=3)

    asyncio.run(scenario())
