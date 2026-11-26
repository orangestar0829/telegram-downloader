"""Real-time progress callbacks and headless GUI model tests."""
import asyncio
import queue
from pathlib import Path
from types import SimpleNamespace

from tgdl import gui
from tgdl.downloader import DownloadStats, _download_one, _download_with_retry
from tgdl.progress import TransferProgress, format_transfer_rate, format_transfer_size


def test_transfer_units_at_boundaries():
    assert format_transfer_size(0) == "0 B"
    assert format_transfer_size(1023) == "1023 B"
    assert format_transfer_size(1024) == "1.0 KiB"
    assert format_transfer_size(37 * 1024) == "37.0 KiB"
    assert format_transfer_size(1024 ** 2) == "1.0 MiB"
    assert format_transfer_rate(512) == "512.00 B/s"
    assert format_transfer_rate(32 * 1024) == "32.00 KiB/s"
    assert format_transfer_rate(1024 ** 2) == "1.00 MiB/s"


def test_progress_tracker_deduplicates_updates_and_includes_terminal():
    events = []
    tracker = TransferProgress("id", "group / 20261119_123", lambda *args: events.append(args))
    tracker.emit("等待缓存", total=100, force=True)
    tracker.emit("下载中", done=20)
    tracker.emit("下载中", done=25)
    tracker.emit("正在保存", done=100, force=True)
    tracker.emit("完成", force=True)
    assert events[0] == ("id", "group / 20261119_123", 0, 100, "等待缓存")
    assert events[-1][-1] == "完成"
    assert events[-1][2:4] == (100, 100)
    assert any(evt[4] == "下载中" for evt in events)


def test_normal_download_reports_actual_transferred_bytes(tmp_path):
    class Client:
        async def download_media(self, target, *, file, progress_callback):
            target_path = Path(file)
            if target_path.is_dir():
                target_path = target_path / "small.mp4"
            progress_callback(2, 37 * 1024)
            progress_callback(37 * 1024, 37 * 1024)
            target_path.write_bytes(b"x" * (37 * 1024))
            return str(target_path)

    events = []
    message = SimpleNamespace(id=15, media=None)
    logs = []
    asyncio.run(_download_one(
        Client(), message, tmp_path / "group" / "videos", "20261119_15",
        skip_existing=True, stats=DownloadStats(), on_saved=logs.append,
        on_progress=lambda *args: events.append(args),
    ))
    assert any(event[2:5] == (37 * 1024, 37 * 1024, "下载中") for event in events)
    assert " | 37.0 KiB | " in logs[0]
    assert events[-1][-1] == "完成"


def test_failed_retry_reports_failure(tmp_path):
    class Client:
        async def download_media(self, target, *, file, progress_callback):
            raise OSError("simulated fail")

    events = []
    result = asyncio.run(_download_with_retry(
        Client(), SimpleNamespace(id=8, media=None), tmp_path / "group" / "videos",
        "20261119_8", DownloadStats(), skip_existing=True, retries=0,
        on_progress=lambda *args: events.append(args),
    ))
    assert result is None
    assert events[-1][-1] == "失败"


class FakeTree:
    def __init__(self):
        self.rows = {}
    def exists(self, key):
        return key in self.rows
    def item(self, key, *, values):
        self.rows[key] = values
    def insert(self, parent, where, *, iid, values):
        self.rows[iid] = values
    def delete(self, key):
        del self.rows[key]


class FakeVar:
    def __init__(self):
        self.value = ""
    def set(self, value):
        self.value = value


def test_gui_progress_updates_multiple_files_and_removes_terminal(monkeypatch):
    app = object.__new__(gui._App)
    app.progress_queue = queue.Queue()
    app._progress_entries = {}
    app.progress_tree = FakeTree()
    app.progress_summary = FakeVar()
    times = [100.0]
    monkeypatch.setattr(gui.time, "monotonic", lambda: times[0])
    app._update_download_progress("a", "group / a", 0, 64 * 1024, "准备下载")
    app._update_download_progress("b", "group / b", 0, 1024 ** 2, "等待缓存")
    app._update_download_progress("c", "group / c", 512, 0, "准备下载")
    app._drain_progress()
    assert len(app.progress_tree.rows) == 3
    assert app.progress_tree.rows["c"][1].endswith(" 512 B")
    times[0] = 101.0
    app._update_download_progress("a", "group / a", 32 * 1024, 64 * 1024, "下载中")
    app._update_download_progress("b", "group / b", 512, 1024 ** 2, "准备下载")
    app._drain_progress()
    assert "32.0 KiB / 64.0 KiB  50%" in app.progress_tree.rows["a"][1]
    assert "512 B / 1.0 MiB" in app.progress_tree.rows["b"][1]
    assert app.progress_tree.rows["a"][2] == "32.00 KiB/s"
    assert "合计速度 32.00 KiB/s" in app.progress_summary.value
    times[0] = 102.0
    app._update_download_progress("a", "group / a", 64 * 1024, 64 * 1024, "完成")
    app._drain_progress()
    assert app.progress_tree.rows["a"][-1] == "完成"
    times[0] = 108.0
    app._drain_progress()
    assert "a" not in app.progress_tree.rows
    assert "b" in app.progress_tree.rows
