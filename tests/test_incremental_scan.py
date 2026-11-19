"""Verify durable, ordered scan windows without using a real Telegram account."""
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from tgdl import downloader
from tgdl.history import DownloadHistory


class FakeMessage:
    def __init__(self, message_id):
        self.id = message_id
        self.date = datetime(2026, 11, 19, tzinfo=timezone.utc)


class FakeTelegram:
    def __init__(self, ids):
        self.ids = ids
        self.calls = []

    async def iter_messages(self, entity, **kwargs):
        self.calls.append(kwargs)
        ids = sorted(self.ids, reverse=not kwargs.get("reverse", False))
        ids = [
            i for i in ids
            if i > kwargs.get("min_id", 0)
            and i < kwargs.get("max_id", 10**12)
        ]
        for i in ids[:kwargs.get("limit", len(ids))]:
            yield FakeMessage(i)

    async def get_messages(self, entity, ids):
        return FakeMessage(ids) if ids in self.ids else None


@pytest.mark.parametrize("direction,expected", [
    ("oldest", [[1, 2], [3, 4], [5], [6, 7]]),
    ("newest", [[5, 4], [3, 2], [1], [7, 6]]),
])
def test_full_then_incremental_and_offline_resume(tmp_path, monkeypatch, direction, expected):
    file_path = tmp_path / "history.sqlite3"
    factory = lambda: DownloadHistory(file_path)
    monkeypatch.setattr(downloader, "DownloadHistory", factory)
    monkeypatch.setattr(downloader, "Message", FakeMessage)
    monkeypatch.setattr(downloader, "classify_message", lambda msg: "video")

    async def resolve(client, target):
        return SimpleNamespace(id=123, title="Test")

    monkeypatch.setattr(downloader, "resolve_channel", resolve)
    downloaded = []

    async def fake_many(client, jobs, *, stats, history, peer_id, **kwargs):
        batch = []
        async for msg, folder, base in jobs:
            batch.append(msg.id)
            if not history.contains(peer_id, msg.id):
                history.mark_saved(peer_id, msg.id, "videos", Path(base))
                stats.saved += 1
            else:
                stats.skipped += 1
        downloaded.append(batch)

    monkeypatch.setattr(downloader, "download_many", fake_many)
    client = FakeTelegram([1, 2, 3, 4, 5])

    async def scan():
        return await downloader.download_channel(
            client, target="test", download_dir=tmp_path,
            types={"video"}, limit=None, since=None, until=None,
            monitor_direction=direction, monitor_batch_size=2,
        )

    asyncio.run(scan())
    second = asyncio.run(scan())
    assert second.first_message_id != 0
    assert second.last_message_id != 0
    assert second.scan_upper == 5
    assert second.matched == 2
    asyncio.run(scan())
    history = DownloadHistory(file_path)
    assert history.scan_state(123) == (5, 0, 0, direction)
    assert history.scan_media_types(123) == "video"
    history.close()
    client.ids = [1, 2, 3, 4, 5, 6, 7]
    asyncio.run(scan())
    assert downloaded == expected
    history = DownloadHistory(file_path)
    assert history.scan_state(123)[0] == 7
    history.close()


def test_new_schema_preserves_download_history_and_failures(tmp_path):
    history = DownloadHistory(tmp_path / "existing.sqlite3")
    history.mark_saved(1, 100, "video", tmp_path / "video.mp4")
    history.mark_failure(1, 101)
    history.save_scan_state(1, 100, 200, 150, "newest")
    history.close()
    history = DownloadHistory(tmp_path / "existing.sqlite3")
    assert history.contains(1, 100)
    assert history.failures(1) == [101]
    assert history.scan_state(1) == (100, 200, 150, "newest")
    history.mark_saved(1, 101, "video", tmp_path / "new.mp4")
    assert history.failures(1) == []
    history.close()


def test_failed_media_retried_when_no_new_messages(tmp_path, monkeypatch):
    file_path = tmp_path / "history.sqlite3"
    monkeypatch.setattr(downloader, "DownloadHistory", lambda: DownloadHistory(file_path))
    monkeypatch.setattr(downloader, "Message", FakeMessage)
    monkeypatch.setattr(downloader, "classify_message", lambda msg: "video")

    async def resolve(client, target):
        return SimpleNamespace(id=123, title="Test")

    monkeypatch.setattr(downloader, "resolve_channel", resolve)
    attempts = []

    async def fake_many(client, jobs, *, history, peer_id, stats, **kwargs):
        async for msg, folder, base in jobs:
            if msg.id == 2:
                stats.failed += 1
            else:
                history.mark_saved(peer_id, msg.id, "videos", Path(base))
                stats.saved += 1

    async def fake_retry(client, msg, folder, base, stats, *, history, peer_id, **kwargs):
        attempts.append(msg.id)
        history.mark_saved(peer_id, msg.id, "videos", Path(base))
        stats.saved += 1
        return Path(base)

    monkeypatch.setattr(downloader, "download_many", fake_many)
    monkeypatch.setattr(downloader, "_download_with_retry", fake_retry)
    client = FakeTelegram([1, 2, 3])

    async def scan():
        return await downloader.download_channel(
            client, target="test", download_dir=tmp_path, types={"video"},
            limit=None, since=None, until=None,
            monitor_direction="oldest", monitor_batch_size=100,
        )

    asyncio.run(scan())
    hist = DownloadHistory(file_path)
    assert hist.scan_state(123)[0] == 3
    assert hist.failures(123) == [2]
    hist.close()
    asyncio.run(scan())
    hist = DownloadHistory(file_path)
    assert attempts == [2]
    assert hist.contains(123, 2)
    assert hist.failures(123) == []
    hist.close()


def test_download_completion_includes_channel_name(tmp_path, monkeypatch):
    monkeypatch.setattr(downloader, "Message", FakeMessage)
    monkeypatch.setattr(downloader, "classify_message", lambda msg: "video")

    async def resolve(client, target):
        return SimpleNamespace(id=321, title="群组名称")

    monkeypatch.setattr(downloader, "resolve_channel", resolve)
    labels = []
    resolved = []

    async def fake_many(client, jobs, *, on_saved, **kwargs):
        async for msg, _folder, _base in jobs:
            on_saved(f"[下载完成] 20261119_{msg.id}.mp4")

    monkeypatch.setattr(downloader, "download_many", fake_many)

    async def run():
        await downloader.download_channel(
            FakeTelegram([12]), target="test", download_dir=tmp_path,
            types={"video"}, limit=None, since=None, until=None,
            dry_run=False, on_saved=labels.append,
            on_channel_resolved=lambda title, peer_id: resolved.append((title, peer_id)),
        )

    asyncio.run(run())
    assert resolved == [("群组名称", 321)]
    assert labels == ["[群组名称] [下载完成] 20261119_12.mp4"]
