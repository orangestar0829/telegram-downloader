"""Monitor slices must enqueue durably without blocking on file downloads."""
import asyncio
import threading
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl.types import Channel, Chat, ChatPhotoEmpty

from tgdl import downloader
from tgdl.history import DownloadHistory, PendingMedia


class FakeMessage:
    def __init__(self, message_id):
        self.id = message_id
        self.date = datetime(2026, 11, 20, tzinfo=timezone.utc)


class FakeTelegram:
    def __init__(self, ids):
        self.ids = ids
        self.calls = []

    async def iter_messages(self, entity, **kwargs):
        self.calls.append(kwargs)
        ids = sorted(self.ids, reverse=not kwargs.get("reverse", False))
        filtered = [
            mid for mid in ids
            if mid > kwargs.get("min_id", 0)
            and mid < kwargs.get("max_id", 10**12)
        ]
        for mid in filtered[:kwargs.get("limit", len(filtered))]:
            yield FakeMessage(mid)


@pytest.fixture
def monitor_env(monkeypatch, tmp_path):
    db = tmp_path / "history.sqlite3"
    monkeypatch.setattr(downloader, "DownloadHistory", lambda: DownloadHistory(db))
    monkeypatch.setattr(downloader, "resolve_channel", fake_resolve)
    monkeypatch.setattr(downloader, "Message", FakeMessage)
    monkeypatch.setattr(downloader, "classify_message", lambda m: "photo")
    return db


async def fake_resolve(client, target):
    return SimpleNamespace(id=123, title="照片频道")


@pytest.mark.parametrize("direction,expected", [
    ("newest", [6, 5, 4, 3, 2, 1]),
    ("oldest", [1, 2, 3, 4, 5, 6]),
])
def test_scan_yields_after_slice_and_recovers_from_restart(monitor_env, direction, expected):
    client = FakeTelegram(list(range(1, 7)))

    async def scan():
        return await downloader.scan_monitor_slice(
            client, target="@photos", types={"photo"},
            monitor_direction=direction, slice_size=2, match_limit=500,
        )

    first = asyncio.run(scan())
    assert first.matched == 2
    assert first.more_pending
    # A fresh history connection must see both queued jobs and the cursor.
    history = DownloadHistory(monitor_env)
    assert history.pending_count() == 2
    assert history.scan_state(123)[1] == 6
    history.close()
    results = [first]
    for _ in range(4):
        results.append(asyncio.run(scan()))
        if not results[-1].more_pending:
            break
    history = DownloadHistory(monitor_env)
    assert history.scan_state(123)[0] == 6
    assert [x.message_id for x in history.pending_for_target("@photos", 100)] == expected
    history.close()
    assert client.calls[1]["limit"] == 2


def test_scan_quota_smaller_than_slice(monitor_env):
    client = FakeTelegram(list(range(1, 51)))

    async def scan():
        return await downloader.scan_monitor_slice(
            client, target="@photos", types={"photo"},
            monitor_direction="newest", slice_size=20, match_limit=3,
        )

    one = asyncio.run(scan())
    assert one.matched == 3
    assert one.scanned_messages == 3
    assert one.more_pending
    two = asyncio.run(scan())
    assert two.matched == 3
    hist = DownloadHistory(monitor_env)
    assert [x.message_id for x in hist.pending_for_target("@photos")] == [50, 49, 48, 47, 46, 45]
    hist.close()


def test_queue_write_and_checkpoint_are_atomic(monitor_env):
    history = DownloadHistory(monitor_env)
    item = PendingMedia(123, 5, "@photos", "title", "photo", "20261120_5")
    assert history.commit_scan_batch(
        123, [item], completed=0, upper=10, cursor=5,
        direction="newest", media_types="photo",
    ) == 1
    assert history.commit_scan_batch(
        123, [item], completed=0, upper=10, cursor=5,
        direction="newest", media_types="photo",
    ) == 0
    assert history.pending_count() == 1
    assert history.scan_state(123) == (0, 10, 5, "newest")
    history.mark_saved(123, 5, "photo", Path("saved.jpg"))
    assert history.pending_count() == 0
    assert history.contains(123, 5)
    assert history.enqueue_media([item]) == 0
    history.close()


def test_cancelled_slice_does_not_advance_checkpoint(monitor_env, monkeypatch):
    client = FakeTelegram([1, 2, 3])
    event = threading.Event()
    event.set()
    result = asyncio.run(downloader.scan_monitor_slice(
        client, target="@photos", types={"photo"}, stop_event=event,
    ))
    hist = DownloadHistory(monitor_env)
    assert hist.pending_count() == 0
    assert hist.scan_state(123)[2] == 0
    assert result.more_pending
    hist.close()



def test_basic_group_is_scanned_only_by_original_account(monitor_env, monkeypatch):
    async def basic_group(_client, _target):
        return Chat(123, "Basic group", ChatPhotoEmpty(), 2, None, 1)

    monkeypatch.setattr(downloader, "resolve_channel", basic_group)
    client = FakeTelegram([1, 2, 3])
    with pytest.raises(downloader.UnsupportedSharedMonitorPeer, match="普通群"):
        asyncio.run(downloader.scan_monitor_slice(
            client, target="-123", types={"photo"}, account_id="secondary"
        ))
    assert not client.calls
    history = DownloadHistory(monitor_env)
    assert history.pending_count() == 0
    history.close()

    asyncio.run(downloader.scan_monitor_slice(client, target="-123", types={"photo"}))
    history = DownloadHistory(monitor_env)
    assert history.pending_count() == 3
    assert [job.peer_id for job in history.pending_for_target("-123")] == [-123] * 3
    assert history.scan_state(-123)[0] == 3
    assert history.scan_state(123)[0] == 0
    history.close()


def test_basic_group_does_not_share_history_keys_with_channel(monitor_env, monkeypatch):
    async def resolve(_client, target):
        if target == "-123":
            return Chat(123, "Basic group", ChatPhotoEmpty(), 2, None, 1)
        return Channel(123, "Supergroup", ChatPhotoEmpty(), None, megagroup=True)

    monkeypatch.setattr(downloader, "resolve_channel", resolve)
    client = FakeTelegram([1])
    async def scenario():
        await downloader.scan_monitor_slice(client, target="-123", types={"photo"})
        await downloader.scan_monitor_slice(
            client, target="-100123", types={"photo"}, account_id="secondary"
        )
    asyncio.run(scenario())
    history = DownloadHistory(monitor_env)
    history.mark_saved(-123, 1, "photo", Path("old-basic-group.jpg"))
    assert history.contains(-123, 1)
    assert not history.contains(123, 1)
    assert [job.message_id for job in history.pending_for_peer(123, account_id="secondary")] == [1]
    assert history.try_claim("secondary", 123, 1)
    history.close()


def test_unambiguous_legacy_basic_group_pending_row_cannot_be_claimed(monitor_env):
    history = DownloadHistory(monitor_env)
    history.enqueue_media([PendingMedia(123, 1, "-123", "Basic group", "photo", "old")])
    assert history.pending_count() == 1  # retain old preview data
    assert not history.pending_for_peer(123, account_id="secondary")
    assert not history.pending_for_target("-123", account_id="default")
    assert not history.try_claim("secondary", 123, 1)

    actual_channel_job = PendingMedia(123, 1, "-100123", "Supergroup", "photo", "new")
    assert history.enqueue_media([actual_channel_job]) == 1
    assert history.pending_for_peer(123, account_id="secondary") == [actual_channel_job]
    assert history.try_claim("secondary", 123, 1)
    saved_legacy = history._connection.execute(
        "SELECT target,base FROM legacy_basic_pending_media WHERE peer_id=123 AND message_id=1"
    ).fetchone()
    assert saved_legacy == ("-123", "old")
    history.close()
