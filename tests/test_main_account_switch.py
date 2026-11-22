"""Main UI account selector must never mix account views or side effects."""
from queue import Queue
from types import SimpleNamespace

from tgdl.account_identity import account_identity
from tgdl.account_view import account_choices, dialog_rows, progress_owner
from tgdl.accounts import AccountRegistry
from tgdl.config import update_env
from tgdl.gui import _App
from tgdl.monitor import load_channels, save_channels


class Var:
    def __init__(self, value=""):
        self.value = value
    def get(self):
        return self.value
    def set(self, value):
        self.value = value


class Widget:
    def __init__(self):
        self.options = {}
    def configure(self, **kwargs):
        self.options.update(kwargs)
    config = configure


class Tree(Widget):
    def __init__(self):
        super().__init__()
        self.rows = {}
        self.selected = ()
    def selection(self):
        return self.selected
    def selection_remove(self, _items):
        self.selected = ()
    def get_children(self):
        return tuple(self.rows)
    def delete(self, *keys):
        for key in keys:
            self.rows.pop(key, None)
    def exists(self, key):
        return key in self.rows
    def item(self, key, option=None, **kwargs):
        if "values" in kwargs:
            self.rows[key] = tuple(map(str, kwargs["values"]))
        if option == "values":
            return self.rows[key]
        return {"values": self.rows[key]}
    def insert(self, parent, index, *, iid, values):
        self.rows[iid] = tuple(map(str, values))
    def move(self, iid, parent, position):
        assert iid in self.rows


def _make_app(tmp_path):
    registry = AccountRegistry(tmp_path)
    update_env({
        "api_id": "1", "api_hash": "hash-a", "phone": "+111",
        "download_dir": str(tmp_path / "a-download"),
        "proxy_host": "proxy-a", "media_concurrency": "2",
    }, registry.env_path("default"))
    b = registry.add("本地备注 B")
    update_env({
        "api_id": "2", "api_hash": "hash-b", "phone": "+222",
        "download_dir": str(tmp_path / "b-download"),
        "proxy_host": "proxy-b", "media_concurrency": "4",
    }, registry.env_path(b.id))
    a_monitor = [{"target": "-10011", "name": "A 频道", "minutes": 5,
                  "types": "video", "enabled": True}]
    b_monitor = [{"target": "-10022", "name": "B 频道", "minutes": 10,
                  "types": "photo", "enabled": True}]
    save_channels(a_monitor, registry.monitor_path("default"))
    save_channels(b_monitor, registry.monitor_path(b.id))
    runtime_b = SimpleNamespace(
        client=object(), status="已连接：测试 B", active_count=1,
        _next_at={"-10022": 4102444800.0},
    )
    app = _App.__new__(_App)
    app._account_registry = registry
    app._view_account_id = "default"
    app._monitored = load_channels(registry.monitor_path("default"))
    app._view_monitored = []
    app._monitor_next = {"-10011": 4102444800.0}
    app._monitor_active = True
    app._monitor_batch_used = {}
    app._monitor_totals = {}
    app._extra_runtimes = {b.id: runtime_b}
    app._account_dialog_cache = {b.id: [("-10022", "channel", "B 频道", "bchannel")]}
    app._account_dialog_fetches = set()
    app._default_dialog_rows = [("-10011", "channel", "A 频道", "achannel")]
    app._dialog_rows = app._default_dialog_rows
    app._default_status_text = "已连接：测试 A"
    app._default_auto_status_text = "定时下载：未启动"
    app.auto_status_var = Var("定时下载：未启动")
    app._closing = False
    app._gallery_entity = object()
    app._gallery_title = "A old gallery"
    app.gallery_cells = []
    app.main_account_choice = Var()
    app._main_account_selector = Widget()
    app._main_account_choices = {}
    app.log_account_filter = Var("全部账号")
    app._log_filter_options = {"全部账号": None}
    app._log_filter_selector = Widget()
    app.status_var = Var("已连接：测试 A")
    app.manual_var = Var()
    app.dialog_search_var = Var()
    app.tree = Tree()
    app.monitor_tree = Tree()
    app.download_btn = Widget()
    app.load_gallery_btn = Widget()
    app.save_sel_btn = Widget()
    app.parse_btn = Widget()
    app.auto_btn = Widget()
    app.monitor_button = Widget()
    app.login_btn = Widget()
    app._populate_gallery = lambda items: setattr(app, "gallery_cells", items)
    app._filter_dialog_tree = lambda: None
    app._refresh_log_views = lambda: None
    app._log = lambda text: None
    app._log_account = lambda account_id, text: None
    app._refresh_main_account_choices()
    for name in ("api_id", "api_hash", "phone", "download_dir", "temp_download_dir",
                 "proxy_host", "proxy_port", "proxy_username", "proxy_password",
                 "media_concurrency", "media_retries", "chunk_concurrency",
                 "media_speed_kib"):
        setattr(app, name + "_var", Var())
    for name, value in (
        ("monitor_minutes_var", "5"), ("monitor_priority_var", "0"),
        ("monitor_batch_var", "100"), ("monitor_direction_var", "从新到旧"),
    ):
        setattr(app, name, Var(value))
    app.monitor_type_photo = Var(True)
    app.monitor_type_video = Var(False)
    app.monitor_type_anim = Var(False)
    app.messagebox = SimpleNamespace(
        showwarning=lambda *args: (_ for _ in ()).throw(AssertionError(args)),
        showinfo=lambda *args: None,
    )
    return app, b


def test_account_choices_from_real_telegram_profiles_and_cached_dialogs():
    accounts = [SimpleNamespace(id="default", name="本地 A"),
                SimpleNamespace(id="second", name="本地 B")]
    account_identity.update("second", SimpleNamespace(
        first_name="Telegram", last_name="B", username="b_profile"
    ))
    choices = account_choices(accounts)
    assert choices[list(choices)[1]] == "second"
    assert "Telegram B (@b_profile)" in list(choices)[1]
    assert dialog_rows([SimpleNamespace(entity_id=123, kind="supergroup",
                 title="群组", username=None)]) == [("123", "supergroup", "群组", "")]
    assert progress_owner("second:peer:mid", ["second"]) == "second"
    assert progress_owner("peer:mid", ["second"]) == "default"


def test_switch_uses_b_config_channel_list_and_monitor_without_modifying_a(tmp_path):
    app, b = _make_app(tmp_path)
    app._switch_main_account(b.id)
    assert app._selected_view_account() == b.id
    assert app.phone_var.get() == "+222"
    assert app.proxy_host_var.get() == "proxy-b"
    assert app.media_concurrency_var.get() == "4"
    assert app._dialog_rows[0][2] == "B 频道"
    assert app._visible_monitors()[0]["target"] == "-10022"
    assert app._monitored[0]["target"] == "-10011"
    assert app.monitor_tree.rows["0"][0] == "B 频道"
    assert app.monitor_tree.rows["0"][7] == "启用"
    assert app.download_btn.options["state"] == "disabled"
    assert app.parse_btn.options["state"] == "disabled"
    assert app.login_btn.options["text"] == "管理账号登录"
    assert app._log_filter_options[app.log_account_filter.get()] == b.id

    # Edits in main UI are persisted *only* to account B's monitor file.
    app.manual_var.set("-10023")
    app.monitor_minutes_var.set("12")
    app._monitor_add()
    assert {r["target"] for r in load_channels(
        app._account_registry.monitor_path(b.id))} == {"-10022", "-10023"}
    assert [r["target"] for r in load_channels(
        app._account_registry.monitor_path("default"))] == ["-10011"]

    # B startup must never toggle the default worker.
    visited = []
    app._open_accounts = lambda: visited.append("B")
    app._monitor_start_stop()
    assert visited == ["B"]
    assert app._monitor_active is True

    # Switching back restores A's data, not B's.
    app._switch_main_account("default")
    assert app.phone_var.get() == "+111"
    assert app.proxy_host_var.get() == "proxy-a"
    assert app._dialog_rows[0][2] == "A 频道"
    assert app._visible_monitors()[0]["target"] == "-10011"
    assert app.download_btn.options["state"] == "normal"
    assert app._log_filter_options[app.log_account_filter.get()] == "default"


def test_secondary_monitor_changes_in_management_window_are_reflected(tmp_path):
    app, b = _make_app(tmp_path)
    app._switch_main_account(b.id)
    path = app._account_registry.monitor_path(b.id)
    save_channels([{"target": "@changed", "minutes": 7, "types": "video",
                    "enabled": False}], path)
    app._sync_secondary_monitor_settings(b.id)
    assert app._visible_monitors()[0]["target"] == "@changed"
    assert app.monitor_tree.rows["0"][7] == "暂停"
    assert app._monitored[0]["target"] == "-10011"


def test_secondary_manual_action_does_not_start_default_downloader(tmp_path):
    app, b = _make_app(tmp_path)
    app._switch_main_account(b.id)
    called = []
    app._start_channel_download = lambda *args: called.append(args)
    app.on_download()
    assert called == []


def test_progress_tree_displays_only_selected_account(tmp_path):
    app, b = _make_app(tmp_path)
    app._progress_entries = {}
    app._monitor_progress_states = {}
    app.progress_tree = Tree()
    app.progress_summary = Var()
    app.progress_queue = Queue()
    app.progress_queue.put(("peer-a:31", "A video", 100, 200, "下载中"))
    app.progress_queue.put((f"{b.id}:peer-b:41", "B video", 50, 200, "下载中"))

    app._view_account_id = b.id
    app._drain_progress()
    assert set(app.progress_tree.rows) == {f"{b.id}:peer-b:41"}
    assert "B video" in str(app.progress_tree.rows[f"{b.id}:peer-b:41"])
    assert "peer-a:31" in app._progress_entries  # invisible, not cancelled

    app._view_account_id = "default"
    app._drain_progress()
    assert set(app.progress_tree.rows) == {"peer-a:31"}
    assert f"{b.id}:peer-b:41" in app._progress_entries


def test_log_filter_isolates_identical_names_even_after_profile_rename(tmp_path, monkeypatch):
    from queue import Queue
    import tgdl.gui
    from tgdl.account_context import current_account_id
    from tgdl.account_identity import AccountIdentity
    from tgdl.log_format import TimestampedLines

    identity = AccountIdentity()
    monkeypatch.setattr(tgdl.gui, "account_identity", identity)
    monkeypatch.setattr("tgdl.account_view.account_identity", identity)
    app, b = _make_app(tmp_path)
    app.log_queue = Queue()
    app.download_queue = Queue()
    app._timestamp_formatter = TimestampedLines()
    app._download_timestamp_formatter = TimestampedLines()
    app._log_store = SimpleNamespace(write=lambda text: disk_logs.append(text))
    disk_logs = []
    app._log_account = _App._log_account.__get__(app)
    app._log = _App._log.__get__(app)
    profile = SimpleNamespace(first_name="同名", last_name="", username=None)
    identity.update("default", profile)
    identity.update(b.id, profile)
    app._log_account("default", "A worker")
    app._log_account(b.id, "B worker")
    app._log_download_account(b.id, "B transfer")
    writer = tgdl.gui._QueueWriter(app.log_queue, app._timestamp_formatter,
                                  persist=app._log_store.write)
    context_token = current_account_id.set(b.id)
    try:
        writer.write("B diagnostics\n")
    finally:
        current_account_id.reset(context_token)
    a_line, b_line, b_print = (app.log_queue.get_nowait() for _ in range(3))
    b_download = app.download_queue.get_nowait()
    assert all("[账号:同名]" in line.text for line in (a_line, b_line, b_print, b_download))
    assert all(b.id not in text for text in disk_logs)

    app._switch_main_account(b.id)
    assert not app._log_visible(a_line)
    assert all(app._log_visible(line) for line in (b_line, b_print, b_download))
    identity.update(b.id, SimpleNamespace(first_name="改名", last_name="", username=None))
    app._refresh_log_accounts()
    assert app._log_visible(b_line)  # Old readable name, unchanged owner.
    assert not app._log_visible(a_line)
    app._switch_main_account("default")
    assert app._log_visible(a_line)
    assert not any(app._log_visible(line) for line in (b_line, b_print, b_download))


def test_list_refresh_targets_selected_secondary_client(tmp_path):
    app, b = _make_app(tmp_path)
    app._switch_main_account(b.id)
    requested = []
    app._load_secondary_dialogs = lambda account_id, *, force=False: requested.append(
        (account_id, force)
    )
    app.on_list()
    assert requested == [(b.id, True)]


def test_secondary_media_settings_change_only_b_env_and_not_a_live_workers(tmp_path):
    from tgdl.config import read_env_values
    app, b = _make_app(tmp_path)
    app._switch_main_account(b.id)
    app._applied_media_settings = (2, 2, 1, 0)
    app._monitor_live_concurrency = 2
    app.media_concurrency_var.set("6")
    app.media_retries_var.set("1")
    app.chunk_concurrency_var.set("3")
    app.media_speed_kib_var.set("2048")
    app._apply_media_settings()
    a_file = app._account_registry.env_path("default")
    b_file = app._account_registry.env_path(b.id)
    assert read_env_values(a_file)["media_concurrency"] == "2"
    assert read_env_values(b_file)["media_concurrency"] == "6"
    assert read_env_values(b_file)["chunk_concurrency"] == "3"
    assert app._monitor_live_concurrency == 2
    assert app._applied_media_settings == (2, 2, 1, 0)


def test_duplicate_telegram_account_names_have_distinct_display_choices():
    from tgdl.accounts import Account
    fake1, fake2 = "collision-a", "collision-b"
    for account_id in (fake1, fake2):
        account_identity.update(account_id, SimpleNamespace(
            first_name="Same", last_name=None, username=None))
    choices = account_choices([Account(fake1, "相同本地备注"),
                               Account(fake2, "相同本地备注")])
    assert len(choices) == 2
    assert set(choices.values()) == {fake1, fake2}
    assert len(set(choices)) == 2


def test_account_manager_default_dialogs_do_not_show_main_window_b(tmp_path):
    from tgdl.accounts_gui import AccountWindow
    app, b = _make_app(tmp_path)
    app._switch_main_account(b.id)
    assert app._dialog_rows[0][2] == "B 频道"

    manager = AccountWindow.__new__(AccountWindow)
    manager.app = app
    manager.choice = Var("A")
    manager._ids = {"A": "default"}
    manager._dialog_cache = app._account_dialog_cache
    manager.dialog_tree = Tree()
    manager.dialog_keyword = Var("")
    manager.dialog_counter = Var("")
    manager._refresh_dialog_tree()
    assert manager.dialog_tree.rows["-10011"][1] == "channel"
    assert manager.dialog_tree.rows["-10011"][2] == "A 频道"
    assert "-10022" not in manager.dialog_tree.rows

    # 'Refresh default' must not invoke app.on_list(), which would route B.
    app.client = object()
    calls = []
    async def fetch_default():
        calls.append("default client")
    def run_task(coro, button):
        calls.append("scheduled default")
        coro.close()
    app._load_dialogs = fetch_default
    app._run_task = run_task
    app.on_list = lambda: calls.append("WRONG: refresh B")
    manager._ensure_dialogs(force=True)
    assert calls == ["scheduled default"]


def test_main_settings_dialog_merges_edits_after_account_manager_proxy_save(tmp_path):
    from tgdl.config import read_env_values

    app, b = _make_app(tmp_path)
    app._view_account_id = b.id
    app.client = None
    app.root = object()
    path = app._account_registry.env_path(b.id)
    original = read_env_values(path)
    drafts, buttons = [], {}

    def variable(**kwargs):
        var = Var(kwargs["value"])
        drafts.append(var)
        return var

    def button(_parent, *, text, command):
        buttons[text] = command
        return SimpleNamespace(grid=lambda **_kwargs: None)

    def unexpected(*_args):
        raise AssertionError("unexpected warning")

    app.tk = SimpleNamespace(Toplevel=lambda _parent: SimpleNamespace(
        title=lambda _text: None, transient=lambda _parent: None, destroy=lambda: None,
    ), StringVar=variable)
    widget = lambda *_args, **_kwargs: SimpleNamespace(grid=lambda **_kwargs: None)
    app.ttk = SimpleNamespace(Label=widget, Entry=widget, Button=button)
    app.messagebox = SimpleNamespace(showwarning=unexpected, showinfo=lambda *_: None)
    app._log = lambda *_args: None
    app._open_settings()

    # Account manager saves a SOCKS5 host while the main dialog is open.
    update_env({**original, "proxy_host": "manager-proxy", "proxy_port": "9988"}, path)
    drafts[6].set(str(tmp_path / "new-download-dir"))
    app.phone_var.set("+19998887777")  # Independent login draft.
    buttons["保存"]()
    saved = read_env_values(path)
    assert (saved["proxy_host"], saved["proxy_port"], saved["download_dir"]) == (
        "manager-proxy", "9988", str(tmp_path / "new-download-dir")
    )
    assert app.proxy_host_var.get() == "manager-proxy"
    assert app.phone_var.get() == "+19998887777"
