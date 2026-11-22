import asyncio
from pathlib import Path
from types import SimpleNamespace

from telethon.tl.types import PeerChannel, PeerChat

from tgdl.downloader import DownloadStats, _download_one, peer_download_folder
from tgdl.history import DownloadHistory


def test_same_title_and_message_id_are_kept_separate_from_peer_and_legacy_files(tmp_path):
    legacy = tmp_path / "Shared" / "videos"
    legacy.mkdir(parents=True)
    (legacy / "20260101_1.mp4").write_bytes(b"legacy unverified")
    first = peer_download_folder(tmp_path, "Shared", PeerChannel(100)) / "videos"
    second = peer_download_folder(tmp_path, "Shared", PeerChannel(200)) / "videos"
    first.mkdir()
    (first / "20260101_1.mp4").write_bytes(b"first channel")
    client = SimpleNamespace()

    async def download_media(message, file, progress_callback):
        path = Path(file) / "original.mp4"
        path.write_bytes(b"second channel")
        return path

    client.download_media = download_media
    history = DownloadHistory(tmp_path / "history.sqlite3")
    try:
        stats = DownloadStats()
        saved = asyncio.run(_download_one(
            client, SimpleNamespace(id=1, media=None), second, "20260101_1",
            skip_existing=True, stats=stats, history=history, peer_id=200,
        ))
        assert stats.saved == 1 and stats.skipped == 0
        assert saved.read_bytes() == b"second channel"
        assert (first / saved.name).read_bytes() == b"first channel"
        assert (legacy / saved.name).read_bytes() == b"legacy unverified"
        assert history.contains(200, 1)
    finally:
        history.close()


def test_peer_folder_survives_rename_and_dry_run_does_not_create_folder(tmp_path):
    entity = PeerChannel(300)
    would_save_to = peer_download_folder(tmp_path, "..", entity, create=False)
    assert would_save_to.name.endswith(" - channel")
    assert not would_save_to.parent.exists()
    existing = peer_download_folder(tmp_path, "Old Title", entity)
    assert peer_download_folder(tmp_path, "New Title", entity) == existing
    assert peer_download_folder(tmp_path, "Old Title", PeerChat(300)) != existing
