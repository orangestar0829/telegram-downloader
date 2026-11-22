"""Offline tests for size/variant verification and no-clobber publication."""
import asyncio
import errno
import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon import utils
from telethon.tl.types import (
    Document, DocumentAttributeFilename, MessageMediaDocument, MessageMediaPhoto,
    Photo, PhotoCachedSize, PhotoSize, PhotoSizeProgressive, PhotoStrippedSize, VideoSize,
)

from tgdl import downloader
from tgdl.history import DownloadHistory

COMPLETE = b"complete media"
BASE = "20261122_12"
DATE = datetime(2026, 11, 22, tzinfo=timezone.utc)
PEER = 345


class Client:
    def __init__(self, data=COMPLETE, *, during_download=None, reported_total=None):
        self.data = data
        self.calls = 0
        self.during_download = during_download
        self.reported_total = reported_total

    async def download_media(self, target, *, file, progress_callback):
        self.calls += 1
        path = Path(file)
        if path.is_dir():
            path /= "video.mp4"
        path.write_bytes(self.data)
        if self.reported_total is not None:
            progress_callback(len(self.data), self.reported_total)
        if self.during_download:
            self.during_download()
        return str(path)


def message(size=len(COMPLETE), filename="clip.mp4"):
    return SimpleNamespace(id=12, media=MessageMediaDocument(document=Document(
        1, 1, b"ref", DATE, "video/mp4", size, 1, [DocumentAttributeFilename(filename)],
    )))


def download_history(folder, client, msg=None):
    history = DownloadHistory(folder.parent / "history.sqlite3")
    stats = downloader.DownloadStats()
    try:
        path = asyncio.run(downloader._download_one(
            client, msg or message(), folder, BASE, skip_existing=True,
            stats=stats, history=history, peer_id=PEER,
        ))
        return path, stats, history.contains(PEER, 12)
    finally:
        history.close()


@pytest.mark.parametrize("wrong_size", [0, 3, len(COMPLETE) + 1])
@pytest.mark.parametrize("appear_after_scan", [False, True])
def test_bad_collision_keeps_original_and_saves_to_alternate(tmp_path, wrong_size, appear_after_scan):
    folder = tmp_path / "media"
    folder.mkdir()
    occupied = folder / f"{BASE}.mp4"
    old_bytes = b"x" * wrong_size

    def create_collision():
        occupied.write_bytes(old_bytes)

    if not appear_after_scan:
        create_collision()
    client = Client(during_download=create_collision if appear_after_scan else None)
    saved, stats, recorded = download_history(folder, client)
    assert client.calls == 1
    assert stats.saved == 1 and stats.skipped == 0 and recorded
    assert occupied.read_bytes() == old_bytes
    assert saved.name == f"{BASE}_1.mp4"
    assert saved.read_bytes() == COMPLETE


@pytest.mark.parametrize("appear_after_scan", [False, True])
def test_valid_collision_can_be_backfilled(tmp_path, appear_after_scan):
    folder = tmp_path / "media"
    folder.mkdir()
    occupied = folder / f"{BASE}.mp4"
    alternate = occupied if appear_after_scan else folder / f"{BASE}_2.MP4"

    def create_collision():
        alternate.write_bytes(COMPLETE)

    if not appear_after_scan:
        create_collision()
    client = Client(during_download=create_collision if appear_after_scan else None)
    saved, stats, recorded = download_history(folder, client)
    assert client.calls == int(appear_after_scan)
    assert saved is None and stats.skipped == 1 and recorded
    assert alternate.read_bytes() == COMPLETE
    assert list(folder.iterdir()) == [alternate]


def test_original_document_suffix_takes_precedence_over_mime(tmp_path):
    folder = tmp_path / "media"
    folder.mkdir()
    wrong_suffix = folder / f"{BASE}.mp4"
    wrong_suffix.write_bytes(b"x" * len(COMPLETE))
    irrelevant_name = folder / f"{BASE}_extra.MKV"
    irrelevant_name.write_bytes(b"y" * len(COMPLETE))
    client = Client()
    saved, stats, recorded = download_history(folder, client, message(filename="clip.MKV"))
    assert saved.name == f"{BASE}.MKV"
    assert client.calls == 1 and stats.saved == 1 and recorded
    assert saved.read_bytes() == COMPLETE
    assert wrong_suffix.read_bytes() == b"x" * len(COMPLETE)


def test_unknown_size_never_uses_old_file_as_history_proof(tmp_path):
    folder = tmp_path / "media"
    folder.mkdir()
    previous = folder / f"{BASE}.mp4"
    previous.write_bytes(b"x" * len(COMPLETE))
    client = Client(reported_total=len(COMPLETE))
    saved, stats, recorded = download_history(folder, client, SimpleNamespace(id=12, media=None))
    assert client.calls == 1 and stats.saved == 1 and recorded
    assert saved.name == f"{BASE}_1.mp4"
    assert saved.read_bytes() == COMPLETE
    assert previous.read_bytes() == b"x" * len(COMPLETE)


@pytest.mark.parametrize("known_size,data,total", [
    (True, b"", None), (True, b"short", len(b"short")),
    (False, b"", None), (False, b"short", len(COMPLETE)),
])
def test_empty_or_truncated_fetch_is_not_published_or_recorded(tmp_path, known_size, data, total):
    folder = tmp_path / "media"
    history = DownloadHistory(tmp_path / "history.sqlite3")
    stats = downloader.DownloadStats()
    try:
        saved = asyncio.run(downloader._download_with_retry(
            Client(data=data, reported_total=total),
            message() if known_size else SimpleNamespace(id=12, media=None),
            folder, BASE, stats, skip_existing=True, retries=0, history=history, peer_id=PEER,
        ))
        assert saved is None and stats.failed == 1 and stats.saved == 0
        assert not history.contains(PEER, 12)
        assert list(folder.iterdir()) == []
    finally:
        history.close()


def test_prior_sqlite_success_is_not_reevaluated(tmp_path):
    folder = tmp_path / "media"
    folder.mkdir()
    old = folder / f"{BASE}.mp4"
    old.write_bytes(b"")
    history = DownloadHistory(tmp_path / "history.sqlite3")
    history.mark_saved(PEER, 12, "videos", old)
    history.close()

    client = Client()
    saved, stats, recorded = download_history(folder, client)
    assert client.calls == 0 and stats.skipped == 1 and recorded
    assert saved is None and old.read_bytes() == b""


def test_dangling_link_does_not_get_replaced(tmp_path):
    folder = tmp_path / "media"
    folder.mkdir()
    dangling = folder / f"{BASE}.mp4"
    unresolved = tmp_path / "does-not-exist"
    try:
        dangling.symlink_to(unresolved)
    except OSError:
        pytest.skip("Symbolic links are not available in this Windows test environment")
    saved, stats, recorded = download_history(folder, Client())
    assert recorded and stats.saved == 1
    assert dangling.is_symlink() and dangling.readlink() == unresolved
    assert not unresolved.exists()
    assert saved.read_bytes() == COMPLETE


def test_collision_during_cross_volume_copy_preserves_newly_occupied_path(tmp_path, monkeypatch):
    folder = tmp_path / "media"
    occupied = folder / f"{BASE}.mp4"
    monkeypatch.setattr(downloader, "same_volume", lambda source, destination: False)
    real_sleep = asyncio.sleep

    async def occupy_while_copying(seconds):
        if seconds == 0 and not occupied.exists():
            occupied.write_bytes(b"another process wrote this")
        await real_sleep(seconds)

    monkeypatch.setattr(downloader.asyncio, "sleep", occupy_while_copying)
    saved, stats, recorded = download_history(folder, Client())
    assert recorded and stats.saved == 1
    assert occupied.read_bytes() == b"another process wrote this"
    assert saved.read_bytes() == COMPLETE
    assert saved.name == f"{BASE}_1.mp4"


def test_atomic_publication_handles_racing_creator(tmp_path, monkeypatch):
    folder = tmp_path / "media"
    occupied = folder / f"{BASE}.mp4"
    real_link = os.link

    def with_collision(source, destination):
        if destination == occupied:
            occupied.write_bytes(b"racing process")
        return real_link(source, destination)

    monkeypatch.setattr(downloader.os, "link", with_collision)
    saved, stats, recorded = download_history(folder, Client())
    assert recorded and stats.saved == 1
    assert saved.read_bytes() == COMPLETE
    assert occupied.read_bytes() == b"racing process"


def test_no_hardlink_fallback_never_uses_overwriting_posix_rename(tmp_path, monkeypatch):
    source = tmp_path / "verified.mp4"
    destination = tmp_path / "final.mp4"
    source.write_bytes(COMPLETE)

    def unsupported(*args):
        raise OSError(errno.ENOSYS, "No hard links")

    monkeypatch.setattr(downloader.os, "link", unsupported)
    if os.name == "nt":
        downloader._publish_noclobber(source, destination)
        assert destination.read_bytes() == COMPLETE
    else:
        with pytest.raises(OSError, match="does not support safe media publication"):
            downloader._publish_noclobber(source, destination)
        assert source.read_bytes() == COMPLETE
        assert not destination.exists()


@pytest.mark.parametrize("sizes,video,expected", [
    ([PhotoSize("x", 10, 10, 1300)], [], (1300, ".jpg")),
    ([PhotoSizeProgressive("x", 10, 10, [150, 300, 100])], [], (300, ".jpg")),
    ([PhotoCachedSize("x", 10, 10, b"bytes")], [], (5, ".jpg")),
    ([PhotoStrippedSize("i", b"\x01\x02\x03test")], [],
     (len(utils.stripped_photo_to_jpg(b"\x01\x02\x03test")), ".jpg")),
    ([PhotoSize("x", 10, 10, 1300)], [VideoSize("y", 10, 10, 340)], (340, ".mp4")),
    ([PhotoSizeProgressive("x", 10, 10, [])], [], (None, None)),
])
def test_photo_variant_matches_telethon_selection(sizes, video, expected):
    photo = Photo(1, 1, b"ref", DATE, sizes, 1, video_sizes=video)
    assert downloader._media_variant(MessageMediaPhoto(photo=photo)) == expected


def test_real_telethon_uses_the_expected_cached_photo_destination(tmp_path):
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.tl.types import Message, PeerChannel

    client = TelegramClient(StringSession(), 1, "not-a-real-api-hash")
    photo = Photo(1, 1, b"ref", DATE, [PhotoCachedSize("x", 10, 10, COMPLETE)], 1)
    msg = Message(id=12, peer_id=PeerChannel(PEER), date=DATE, message="",
                  media=MessageMediaPhoto(photo=photo))
    saved, stats, recorded = download_history(tmp_path / "photos", client, msg)
    assert stats.saved == 1 and recorded
    assert saved.name == f"{BASE}.jpg"
    assert saved.read_bytes() == COMPLETE


def test_real_telethon_document_respects_explicit_safe_suffix(tmp_path, monkeypatch):
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon.tl.types import Message, PeerChannel

    client = TelegramClient(StringSession(), 1, "not-a-real-api-hash")

    async def download_payload(location, file, *, file_size, **kwargs):
        assert Path(file).name == "normal.mov"
        assert file_size == len(COMPLETE)
        Path(file).write_bytes(COMPLETE)

    monkeypatch.setattr(client, "_download_file", download_payload)
    msg = Message(id=12, peer_id=PeerChannel(PEER), date=DATE, message="",
                  media=message(filename="clip.mov").media)
    saved, stats, recorded = download_history(tmp_path / "videos", client, msg)
    assert stats.saved == 1 and recorded
    assert saved.name == f"{BASE}.mov"
    assert saved.read_bytes() == COMPLETE
