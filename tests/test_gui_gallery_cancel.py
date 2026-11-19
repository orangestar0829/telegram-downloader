"""Headless regression tests: manual gallery saves use the shared cancel path."""
import asyncio
import concurrent.futures
import threading
from types import SimpleNamespace

from tgdl import gui


class FakeButton:
    def __init__(self):
        self.state = "normal"

    def config(self, **kwargs):
        self.state = kwargs.get("state", self.state)


def test_gallery_save_uses_cancel_button_and_awaits_child_cleanup(monkeypatch):
    started = threading.Event()
    cleaned = threading.Event()
    cancelled_messages = []
    loop = gui.AsyncLoop()

    async def fake_save_items(*args, **kwargs):
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            await asyncio.sleep(0)
            cleaned.set()

    async def fake_ensure_client():
        return None

    app = object.__new__(gui._App)
    app.loop = loop
    app._download_future = None
    app._download_task = None
    app._stop_download = threading.Event()
    app._closing = False
    app._gallery_title = "test"
    app.gallery_cells = [(SimpleNamespace(get=lambda: True), object())]
    app._media_options = lambda: (1, 2, 1)
    app._ensure_client = fake_ensure_client
    app.settings = SimpleNamespace(download_dir="C:/test", temp_download_dir=None)
    app.client = object()
    app.save_sel_btn = FakeButton()
    app._update_download_progress = lambda *args: None
    app._log = cancelled_messages.append
    app.root = SimpleNamespace(after=lambda delay, callback: callback())
    monkeypatch.setattr(gui, "save_items", fake_save_items)
    try:
        app.on_save_selected()
        assert started.wait(3)
        assert app.save_sel_btn.state == "disabled"
        app.on_save_selected()  # Another selection must not overwrite current tracking.
        assert any("已有下载任务" in message for message in cancelled_messages)
        app._cancel_current_download()
        assert app._stop_download.is_set()
        assert cleaned.wait(3)
        assert isinstance(app._download_future, concurrent.futures.Future)
        assert app._download_future.done()
        assert app.save_sel_btn.state == "normal"
        assert any("已取消浏览媒体" in message for message in cancelled_messages)
    finally:
        loop.stop()
        loop._thread.join(timeout=3)


def test_cancel_button_reports_idle_without_crashing():
    messages = []
    app = object.__new__(gui._App)
    app._download_future = None
    app._stop_download = threading.Event()
    app._log = messages.append
    app._cancel_current_download()
    assert "没有可取消" in messages[0]


def test_safe_close_sees_manual_gallery_future():
    class PendingFuture:
        def __init__(self):
            self.cancelled = False

        def done(self):
            return False

        def cancel(self):
            self.cancelled = True

    future = PendingFuture()
    app = object.__new__(gui._App)
    app._download_future = future
    app._stop_download = threading.Event()
    app._monitor_active = False
    app._auto_config = None
    app._closing = False
    app.messagebox = SimpleNamespace(askyesnocancel=lambda *args: False)
    app._log = lambda message: None
    app._finish_close = lambda: (_ for _ in ()).throw(AssertionError("closed before cleanup"))
    app._on_close()
    assert future.cancelled
    assert app._closing
