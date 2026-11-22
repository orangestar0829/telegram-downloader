"""Crash-recovery cleanup and live-lock safety tests."""
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from tgdl.single_instance import AlreadyRunning, gui_instance_guard
from tgdl.temp_cleanup import MARKER, inspect_cleanup, is_link_or_reparse_point, own_temporary


def test_crashed_ssd_download_is_cleaned_and_regular_file_preserved(tmp_path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    orphan = ssd / "tgdl-dead"
    orphan.mkdir()
    with own_temporary(orphan):
        (orphan / "data.mp4").write_bytes(b"a" * 120)
    safe = ssd / "finished.mp4"
    safe.write_bytes(b"keep")
    result = inspect_cleanup(ssd, None)
    assert result.deleted == 1
    assert result.bytes_freed >= 120
    assert not orphan.exists()
    assert safe.read_bytes() == b"keep"


def test_active_transfer_cannot_be_cleaned(tmp_path):
    ssd = tmp_path / "ssd"
    ssd.mkdir()
    active = ssd / "tgdl-active"
    active.mkdir()
    with own_temporary(active):
        (active / "data.mp4").write_bytes(b"transferring")
        result = inspect_cleanup(ssd, None)
        assert result.active == 1
        assert result.deleted == 0
        assert (active / "data.mp4").exists()
    result = inspect_cleanup(ssd, None)
    assert result.deleted == 1
    assert not active.exists()


def test_hdd_publish_directory_cleanup_and_no_final_deletion(tmp_path):
    root = tmp_path / "downloads"
    media = root / "chat" / "videos"
    media.mkdir(parents=True)
    orphan = media / "tgdl-publish-crashed"
    orphan.mkdir()
    with own_temporary(orphan):
        (orphan / "data.mp4").write_bytes(b"partial")
    finished = media / "20261119_100.mp4"
    finished.write_bytes(b"complete")
    report = inspect_cleanup(None, root)
    assert report.deleted == 1
    assert not orphan.exists()
    assert finished.read_bytes() == b"complete"


def test_legacy_cache_is_only_deleted_after_manual_opt_in(tmp_path):
    ssd = tmp_path / "cache"
    ssd.mkdir()
    legacy = ssd / "tgdl-ab_c1234"
    legacy.mkdir()
    (legacy / "data.mp4").write_bytes(b"old")
    unknown = ssd / "tgdl-invalid"
    unknown.mkdir()
    (unknown / MARKER).write_text("not our marker", encoding="utf-8")
    first = inspect_cleanup(ssd, None)
    assert first.deleted == 0
    assert first.legacy == [legacy]
    assert legacy.exists() and unknown.exists()
    second = inspect_cleanup(ssd, None, approved_legacy=set(first.legacy))
    assert second.deleted == 1
    assert not legacy.exists()
    # Corrupted ownership marker: never guess whether another task is live.
    assert unknown.exists()


def test_old_cache_confirmation_cannot_delete_real_channels_or_new_candidates(tmp_path):
    root = tmp_path / "downloads"
    channel = root / "tgdl-ab_c1234"
    videos = channel / "videos"
    videos.mkdir(parents=True)
    final = videos / "20261119_100.mp4"
    final.write_bytes(b"irreplaceable")
    cache = videos / "tgdl-ab_c1234"
    cache.mkdir()
    (cache / "data.mp4").write_bytes(b"partial")
    unrelated = videos / "tgdl-12345678"
    unrelated.mkdir()
    (unrelated / "notes.txt").write_bytes(b"keep")
    first = inspect_cleanup(root, root)
    assert first.legacy == [cache]
    later = videos / "tgdl-87654321"
    later.mkdir()
    (later / "normal.mp4").write_bytes(b"new and unapproved")
    result = inspect_cleanup(root, root, approved_legacy={cache})
    assert result.deleted == 1
    assert not cache.exists()
    assert result.legacy == [later]
    assert (unrelated / "notes.txt").exists()
    assert final.read_bytes() == b"irreplaceable"


def test_windows_junction_attribute_is_detected_without_pathlib_is_junction(tmp_path, monkeypatch):
    junction = tmp_path / "junction"
    junction.mkdir()
    real_lstat = Path.lstat

    def fake_lstat(path):
        if path == junction:
            return SimpleNamespace(st_mode=stat.S_IFDIR, st_file_attributes=0x0400)
        return real_lstat(path)

    monkeypatch.setattr(Path, "lstat", fake_lstat)
    assert is_link_or_reparse_point(junction)
    assert not is_link_or_reparse_point(tmp_path)


def test_gui_single_instance_guard_rejects_another_live_instance(tmp_path):
    try:
        with gui_instance_guard(tmp_path):
            with pytest.raises(AlreadyRunning):
                with gui_instance_guard(tmp_path):
                    pass
    except AlreadyRunning:
        # Windows uses a named global-session mutex. A real installed GUI
        # running alongside the tests already proves the guard is occupied.
        pytest.skip("Installed Telegram Downloader GUI is already running")
    with gui_instance_guard(tmp_path):
        pass
