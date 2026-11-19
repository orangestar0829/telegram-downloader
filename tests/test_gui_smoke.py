"""Import-level smoke tests for the GUI module (no display required)."""

from tgdl import gui
from types import SimpleNamespace


def test_gui_exposes_run_gui() -> None:
    assert callable(gui.run_gui)


def test_silent_bar_supports_tqdm_surface() -> None:
    bar = gui._SilentBar(total=100, unit="B")
    # attribute assignment used by the core modules
    bar.total = 10
    bar.n = 5
    bar.update(1)
    bar.refresh()
    bar.set_postfix(peers=3, state="x")
    bar.close()
    with gui._SilentBar() as ctx:
        ctx.update(2)


def test_silence_progress_bars_patches_modules() -> None:
    gui._silence_progress_bars()
    from tgdl import downloader, http_fetch, torrent_fetch

    assert downloader.tqdm is gui._SilentBar
    assert http_fetch.tqdm is gui._SilentBar
    assert torrent_fetch.tqdm is gui._SilentBar


def test_scheduler_skips_overlapping_download_and_can_stop():
    events = []

    class FakeRoot:
        def after(self, interval, callback):
            events.append(("after", interval, callback))
            return "timer1"

        def after_cancel(self, timer):
            events.append(("cancel", timer))

    class Var:
        def __init__(self, value=""):
            self.value = value
        def set(self, value):
            self.value = value

    app = object.__new__(gui._App)
    app.root = FakeRoot()
    app._auto_config = ("channel1", "video", "", 2, 2, 4)
    app._auto_interval_ms = 30 * 60 * 1000
    app._auto_job_id = None
    app._download_future = SimpleNamespace(done=lambda: False)
    app.auto_status_var = Var()
    app.auto_btn = SimpleNamespace(config=lambda **kw: events.append(("button", kw)))
    app._log = lambda message: events.append(("log", message))
    app._start_channel_download = lambda *args: events.append(("download", args))

    app._auto_tick()
    assert events[0] == ("download", app._auto_config)
    assert events[1][0:2] == ("after", 1800000)
    app._stop_auto()
    assert ("cancel", "timer1") in events
    assert app._auto_config is None
    assert app.auto_status_var.value == "定时下载：未启动"
