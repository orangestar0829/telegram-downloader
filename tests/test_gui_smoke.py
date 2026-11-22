"""Import-level smoke tests for the GUI module (no display required)."""

from tgdl import gui
from types import SimpleNamespace


def test_gui_exposes_run_gui() -> None:
    assert callable(gui.run_gui)


def test_vertical_split_initial_position_keeps_both_panes_visible() -> None:
    assert gui._App._initial_split_position(760) == 395
    for height in (420, 500, 680, 900, 1200):
        position = gui._App._initial_split_position(height)
        assert position >= 180
        assert position <= height - 220


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


def test_batch_and_download_settings_are_separate_tabs():
    def widgets():
        created = []
        def make(*args, **kwargs):
            created.append(kwargs)
            return SimpleNamespace(grid=lambda **kw: None, pack=lambda **kw: None,
                                   columnconfigure=lambda *a, **kw: None)
        return SimpleNamespace(Frame=make, Label=make, Checkbutton=make, Entry=make, Button=make), created

    app = object.__new__(gui._App)
    app.root = object()
    for name in ("type_photo", "type_video", "type_anim", "dl_limit", "media_concurrency_var",
                 "media_retries_var", "chunk_concurrency_var", "media_speed_kib_var"):
        setattr(app, name, object())
    app.ttk, batch = widgets()
    app._build_download_tab()
    assert any(w.get("command") == app.on_download for w in batch)
    assert {w.get("variable") or w.get("textvariable") for w in batch} - {None} == {
        app.type_photo, app.type_video, app.type_anim, app.dl_limit,
    }
    assert not any("启动定时" in w.get("text", "") for w in batch)
    assert not hasattr(app, "on_toggle_auto")

    app.ttk, settings = widgets()
    app._build_download_settings_tab()
    assert any(w.get("command") == app._apply_media_settings for w in settings)
    assert {w.get("textvariable") for w in settings} - {None} == {
        app.media_concurrency_var, app.media_retries_var,
        app.chunk_concurrency_var, app.media_speed_kib_var,
    }
