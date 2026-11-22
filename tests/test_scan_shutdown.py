import asyncio
import sqlite3
import threading
from pathlib import Path
from types import SimpleNamespace

from tgdl import downloader
from tgdl.history import DownloadHistory


def test_graceful_stop_waits_for_existing_files_but_starts_no_more(monkeypatch, tmp_path):
    stop = threading.Event()
    started = []
    finished = []
    two_started = asyncio.Event()

    async def fake_download(client, message, *args, **kwargs):
        started.append(message.id)
        if len(started) == 2:
            two_started.set()
        await asyncio.sleep(0.02)
        finished.append(message.id)
        return Path(f"{message.id}.mp4")

    monkeypatch.setattr(downloader, "_download_with_retry", fake_download)

    async def jobs():
        for i in range(10):
            yield SimpleNamespace(id=i), tmp_path, str(i)

    async def run():
        task = asyncio.create_task(downloader.download_many(
            None, jobs(), stats=downloader.DownloadStats(),
            concurrency=2, stop_event=stop,
        ))
        await two_started.wait()
        stop.set()
        await task

    asyncio.run(run())
    assert sorted(started) == [0, 1]
    assert sorted(finished) == [0, 1]


def test_legacy_sqlite_downloads_survive_schema_extension(tmp_path):
    path = tmp_path / "old.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute(
        """CREATE TABLE downloaded_media (
            peer_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
            kind TEXT NOT NULL, original_path TEXT NOT NULL,
            saved_at TEXT NOT NULL, PRIMARY KEY(peer_id, message_id)
        )"""
    )
    conn.execute(
        "INSERT INTO downloaded_media VALUES (123, 456, 'video', 'old.mp4', '2026-11-01')"
    )
    conn.commit()
    conn.close()
    history = DownloadHistory(path)
    assert history.contains(123, 456)
    assert history.scan_state(123) == (0, 0, 0, "newest")
    history.save_scan_state(123, 456, 0, 0, "oldest")
    assert history.contains(123, 456)
    history.close()
