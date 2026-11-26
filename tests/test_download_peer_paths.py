import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from pathlib import Path
from types import SimpleNamespace

from telethon.tl.types import PeerChannel, PeerChat

from tgdl.downloader import DownloadStats, _download_one, peer_download_folder
from tgdl.history import DownloadHistory


def test_same_title_and_message_id_are_kept_separate(tmp_path, monkeypatch):
    monkeypatch.setattr("tgdl.downloader.data_dir", lambda: tmp_path)
    legacy = tmp_path / "Shared" / "videos"
    legacy.mkdir(parents=True)
    (legacy / "20260101_1.mp4").write_bytes(b"legacy unverified")
    history = DownloadHistory(tmp_path / "tgdl_download_history.sqlite3")
    history.mark_saved(100, 1, "videos", legacy / "20260101_1.mp4")
    # Resolve the wrong same-title channel first: it must not take A's folder.
    second = peer_download_folder(tmp_path, "Shared", PeerChannel(200)) / "videos"
    first = peer_download_folder(tmp_path, "Shared", PeerChannel(100)) / "videos"
    assert first == legacy
    assert first != second
    client = SimpleNamespace()

    async def download_media(message, file, progress_callback):
        path = Path(file) / "original.mp4"
        path.write_bytes(b"second channel")
        return path

    client.download_media = download_media
    try:
        stats = DownloadStats()
        saved = asyncio.run(_download_one(
            client, SimpleNamespace(id=1, media=None), second, "20260101_1",
            skip_existing=True, stats=stats, history=history, peer_id=200,
        ))
        assert stats.saved == 1 and stats.skipped == 0
        assert saved.read_bytes() == b"second channel"
        assert (legacy / saved.name).read_bytes() == b"legacy unverified"
        assert history.contains(200, 1)
    finally:
        history.close()


def test_peer_folder_survives_rename_and_dry_run_does_not_create_folder(tmp_path):
    entity = PeerChannel(300)
    would_save_to = peer_download_folder(tmp_path, "..", entity, create=False)
    assert would_save_to == tmp_path / "channel"
    assert not list(tmp_path.iterdir())
    existing = peer_download_folder(tmp_path, "Old Title", entity)
    assert peer_download_folder(tmp_path, "New Title", entity) == existing
    assert peer_download_folder(tmp_path, "Old Title", PeerChat(300)) != existing


def test_restore_original_upload_directory_for_all_accounts(tmp_path):
    channel = PeerChannel(4430703298)
    old = tmp_path / "夫妻理论交流公社 （复活版）"
    (old / "photos").mkdir(parents=True)
    (old / "photos" / "original.jpg").write_bytes(b"old")
    split = tmp_path / "tgdl-by-peer" / "-1004430703298 - 夫妻理论交流公社（复活版）"
    (split / "photos").mkdir(parents=True)
    (split / "photos" / "preview.jpg").write_bytes(b"preview")
    with ThreadPoolExecutor(max_workers=4) as pool:
        outputs = list(pool.map(
            lambda account: peer_download_folder(tmp_path, "夫妻理论交流公社（复活版）",
                                                 channel, account_id=account),
            ["default", "second", "third", "fourth"],
        ))
    assert outputs == [old] * 4
    assert peer_download_folder(tmp_path, "Renamed", channel, create=False) == old
    assert peer_download_folder(tmp_path, "Renamed", channel) == old
    assert (old / "photos" / "original.jpg").read_bytes() == b"old"
    assert (split / "photos" / "preview.jpg").read_bytes() == b"preview"


def test_upgrade_can_match_legacy_windows_punctuation(tmp_path):
    old = tmp_path / "stripchat ｜ ST站 ｜ 直播录屏"
    old.mkdir()
    (tmp_path / "tgdl-by-peer" / "-1000000000100 - stripchat _ ST站 _ 直播录屏").mkdir(parents=True)
    assert peer_download_folder(tmp_path, "stripchat | ST站 | 直播录屏", PeerChannel(100)) == old


def test_preview_channel_with_no_legacy_folder_keeps_its_existing_path(tmp_path):
    existing = tmp_path / "tgdl-by-peer" / "-1000000000100 - Old Title"
    existing.mkdir(parents=True)
    assert peer_download_folder(tmp_path, "New Title", PeerChannel(100)) == existing


def test_ambiguous_legacy_folder_does_not_merge_known_peers(tmp_path):
    (tmp_path / "Same").mkdir()
    for peer in (100, 200):
        existing = tmp_path / "tgdl-by-peer" / f"-1000000000{peer} - Same"
        existing.mkdir(parents=True)
        if peer == 100:
            first = existing
        else:
            second = existing
    assert peer_download_folder(tmp_path, "Same", PeerChannel(100)) == first
    assert peer_download_folder(tmp_path, "Same", PeerChannel(200)) == second


def test_malicious_linked_media_subfolder_not_used(tmp_path):
    direct = tmp_path / "Shared"
    direct.mkdir()
    (tmp_path / "tgdl-by-peer" / "-1000000000100 - Shared").mkdir(parents=True)
    try:
        (direct / "videos").symlink_to(tmp_path, target_is_directory=True)
    except OSError:
        pytest.skip("Creating symlinks is not permitted")
    with pytest.raises(OSError, match="linked download folder"):
        peer_download_folder(tmp_path, "Shared", PeerChannel(100))


def test_legacy_directory_without_ownership_evidence_is_not_reused(tmp_path, monkeypatch):
    monkeypatch.setattr("tgdl.downloader.data_dir", lambda: tmp_path)
    old = tmp_path / "Shared"
    old.mkdir()
    assert peer_download_folder(tmp_path, "Shared", PeerChannel(100)) != old
