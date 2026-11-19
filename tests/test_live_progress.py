"""Real-time progress callbacks and headless GUI model tests."""
import asyncio
import queue
from pathlib import Path
from types import SimpleNamespace

from tgdl import gui
from tgdl.downloader import DownloadStats, _download_one, _download_with_retry
from tgdl.progress import TransferProgress


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
            progress_callback(2, 4)
            progress_callback(4, 4)
            target_path.write_bytes(b"data")
            return str(target_path)

    events = []
    message = SimpleNamespace(id=15, media=None)
    asyncio.run(_download_one(
        Client(), message, tmp_path / "group" / "videos", "20261119_15",
        skip_existing=True, stats=DownloadStats(),
        on_progress=lambda *args: events.append(args),
    ))
    assert any(event[2:5] == (4, 4, "下载中") for event in events)
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
    app._update_download_progress("a", "group / a", 0, 100, "准备下载")
    app._update_download_progress("b", "group / b", 0, 200, "等待缓存")
    app._drain_progress()
    assert len(app.progress_tree.rows) == 2
    times[0] = 101.0
    app._update_download_progress("a", "group / a", 50, 100, "下载中")
    app._drain_progress()
    assert "50%" in app.progress_tree.rows["a"][1]
    assert "2 个文件" in app.progress_summary.value
    times[0] = 102.0
    app._update_download_progress("a", "group / a", 100, 100, "完成")
    app._drain_progress()
    assert app.progress_tree.rows["a"][-1] == "完成"
    times[0] = 108.0
    app._drain_progress()
    assert "a" not in app.progress_tree.rows
    assert "b" in app.progress_tree.rows
