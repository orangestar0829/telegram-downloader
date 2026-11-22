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


def test_split_prefers_full_log_height_when_space_is_available():
    split = gui._App._initial_split_position
    # A roomy window allocates about the natural 15-line log pane, not half.
    assert split(1000, main_height=405, log_height=365) == 630
    # A smaller window preserves the main controls before enlarging logs.
    assert split(685, main_height=405, log_height=365) == 405
    assert split(540, main_height=405, log_height=365) == 320


def test_group_list_scrolls_and_keeps_its_height_when_switching_tabs():
    import pytest
    tk = pytest.importorskip("tkinter")
    from tkinter import scrolledtext, ttk
    try:
        root = tk.Tk()
    except tk.TclError:
        pytest.skip("Tk display unavailable")

    try:
        root.geometry("1100x900")
        app = object.__new__(gui._App)
        app.root, app.tk, app.ttk, app.scrolledtext = root, tk, ttk, scrolledtext
        app.creds_available = True
        app._refresh_main_account_choices = lambda: None
        app._refresh_log_accounts = lambda: None
        app._monitor_refresh = lambda: None
        for name in ("download_dir_var", "manual_var", "status_var", "log_account_filter",
                     "g_limit", "dl_limit", "media_concurrency_var", "media_retries_var",
                     "chunk_concurrency_var", "media_speed_kib_var", "p_limit"):
            setattr(app, name, tk.StringVar(root))
        for name in ("type_photo", "type_video", "type_anim", "p_no_previews",
                     "p_no_bt", "p_dry", "p_catalog_only"):
            setattr(app, name, tk.BooleanVar(root))
        app._build_ui()
        root.update()
        for i in range(30):
            app.tree.insert("", "end", values=(i, "channel", f"Channel {i}"))
        root.update()
        assert int(app.tree.cget("height")) == 10
        assert round(app.tree.yview()[1] * 30) == 10
        app.tree.yview_moveto(1)
        assert app.tree.yview()[1] == 1

        def layout():
            return (app.tree.winfo_height(), app.feature_tabs.winfo_y(),
                    app.feature_tabs.winfo_height(), app.main_split.sashpos(0))

        for height in (900, 620):
            root.geometry(f"1100x{height}")
            root.update()
            before = layout()
            for page in app.feature_tabs.tabs():
                app.feature_tabs.select(page)
                root.update()
                assert layout() == before
            assert app.feature_tabs.winfo_viewable()
            assert app.log_tabs.winfo_viewable()
    finally:
        root.destroy()


def test_monitor_table_requests_five_rows_and_uses_vertical_scrollbar():
    resources = {}

    def widget(*args, **kwargs):
        return SimpleNamespace(pack=lambda **kw: None)

    def tree(*args, **kwargs):
        resources["tree_options"] = kwargs
        return SimpleNamespace(
            pack=lambda **kw: resources.update(tree_pack=kw),
            configure=lambda **kw: resources.update(scroll_config=kw),
            bind=lambda *_: None,
            heading=lambda *_a, **_kw: None,
            column=lambda *_a, **_kw: None,
            yview=lambda *_: None,
        )

    def scrollbar(*args, **kwargs):
        resources["bar_options"] = kwargs
        return SimpleNamespace(pack=lambda **_kw: None, set=lambda *_: None)

    app = object.__new__(gui._App)
    app.root = object()
    app.tk = SimpleNamespace(StringVar=widget, BooleanVar=widget)
    app.ttk = SimpleNamespace(
        Frame=widget, Label=widget, Entry=widget, Combobox=widget, Checkbutton=widget,
        Button=widget, Treeview=tree, Scrollbar=scrollbar,
    )
    app._monitor_refresh = lambda: None
    app._build_monitor_tab()
    assert resources["tree_options"]["height"] == 5
    assert resources["bar_options"]["orient"] == "vertical"
    assert resources["bar_options"]["command"] is app.monitor_tree.yview
    assert resources["scroll_config"]["yscrollcommand"]
    assert resources["tree_pack"]["fill"] == "x"


def test_settings_dialog_is_positioned_before_showing_on_its_parent():
    app = object.__new__(gui._App)
    app.root = SimpleNamespace(
        winfo_rootx=lambda: 300, winfo_rooty=lambda: 180,
        winfo_width=lambda: 1000, winfo_height=lambda: 800,
    )
    calls = []
    window = SimpleNamespace(
        update_idletasks=lambda: calls.append("measure"),
        winfo_reqwidth=lambda: 440, winfo_reqheight=lambda: 340,
        geometry=lambda coordinates: calls.append(coordinates),
        deiconify=lambda: calls.append("show"),
    )
    app._show_centered_settings(window)
    assert calls == ["measure", "+580+410", "show"]
