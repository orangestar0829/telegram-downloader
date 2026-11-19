"""Crash-recovery cleanup and live-lock safety tests."""
from pathlib import Path

import pytest

from tgdl.single_instance import AlreadyRunning, gui_instance_guard
from tgdl.temp_cleanup import MARKER, inspect_cleanup, own_temporary


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
    legacy = ssd / "tgdl-old-version"
    legacy.mkdir()
    (legacy / "data.mp4").write_bytes(b"old")
    unknown = ssd / "tgdl-invalid"
    unknown.mkdir()
    (unknown / MARKER).write_text("not our marker", encoding="utf-8")
    first = inspect_cleanup(ssd, None)
    assert first.deleted == 0
    assert len(first.legacy) == 2
    assert legacy.exists() and unknown.exists()
    second = inspect_cleanup(ssd, None, include_legacy=True)
    assert second.deleted == 1
    assert not legacy.exists()
    # Corrupted ownership marker: never guess whether another task is live.
    assert unknown.exists()


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
