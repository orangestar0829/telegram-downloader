"""SQLite download history must outlive deletion of the media files."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

from tgdl.downloader import DownloadStats, _download_one
from tgdl.history import DownloadHistory


class FakeClient:
    def __init__(self):
        self.calls = 0

    async def download_media(self, target, *, file, progress_callback):
        self.calls += 1
        destination = Path(file)
        if destination.is_dir():
            destination = destination / "video.mp4"
        destination.write_bytes(b"saved media")
        return str(destination)


def message(message_id):
    return SimpleNamespace(id=message_id, media=None)


def run(client, msg, folder, history, peer_id=1234):
    return asyncio.run(_download_one(
        client, msg, folder, f"20261119_{msg.id}",
        skip_existing=True, stats=DownloadStats(), history=history, peer_id=peer_id,
    ))


def test_deleted_media_is_not_redownloaded(tmp_path):
    db_path = tmp_path / "ledger" / "history.sqlite3"
    folder = tmp_path / "media"
    client = FakeClient()
    history = DownloadHistory(db_path)
    first = run(client, message(1), folder, history)
    assert first.read_bytes() == b"saved media"
    assert history.contains(1234, 1)
    first.unlink()
    history.close()

    history = DownloadHistory(db_path)
    assert run(client, message(1), folder, history) is None
    assert client.calls == 1
    # Different channel, same message ID is not considered downloaded.
    assert run(client, message(1), folder, history, peer_id=9999) is not None
    history.close()


def test_existing_file_backfills_history(tmp_path):
    history = DownloadHistory(tmp_path / "history.sqlite3")
    folder = tmp_path / "videos"
    folder.mkdir()
    path = folder / "20261119_8.mp4"
    path.write_bytes(b"existing file")
    client = FakeClient()
    known = SimpleNamespace(id=8, media=SimpleNamespace(
        document=SimpleNamespace(size=len(b"existing file"), mime_type="video/mp4"),
    ))
    assert run(client, known, folder, history) is None
    assert history.contains(1234, 8)
    path.unlink()
    assert run(client, message(8), folder, history) is None
    assert client.calls == 0
    history.close()


def test_failed_media_is_not_recorded(tmp_path):
    class FailedClient(FakeClient):
        async def download_media(self, target, *, file, progress_callback):
            raise OSError("network failed")

    history = DownloadHistory(tmp_path / "history.sqlite3")
    try:
        asyncio.run(_download_one(
            FailedClient(), message(12), tmp_path / "videos",
            "20261119_12", skip_existing=True, stats=DownloadStats(),
            history=history, peer_id=1234,
        ))
    except OSError:
        pass
    else:
        raise AssertionError("expected download failure")
    assert not history.contains(1234, 12)
    history.close()
