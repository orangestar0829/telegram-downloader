"""Non-GUI behavior checks for embedded account monitor list."""
from types import SimpleNamespace

from tgdl.accounts_gui import AccountWindow


class FakeVar:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class FakeTree:
    def __init__(self):
        self.rows = {}
        self.selected = ("0",)
        self.removed = []
        self.moves = []

    def get_children(self):
        return list(self.rows)

    def exists(self, iid):
        return iid in self.rows

    def insert(self, _parent, position, *, iid, values):
        assert iid not in self.rows
        self.rows[iid] = tuple(map(str, values))

    def delete(self, iid):
        self.removed.append(iid)
        self.rows.pop(iid)

    def item(self, iid, field=None, **kwargs):
        if "values" in kwargs:
            self.rows[iid] = tuple(map(str, kwargs["values"]))
        if field == "values":
            return self.rows[iid]
        return {"values": self.rows[iid]}

    def move(self, iid, parent, position):
        self.moves.append((iid, position))

    def selection(self):
        return self.selected


def test_monitor_refresh_preserves_edit_selection_and_updates_next_scan():
    window = AccountWindow.__new__(AccountWindow)
    window.choice = FakeVar("TG")
    window._ids = {"TG": "account-b"}
    window.tree = FakeTree()
    window._channel_rows = [
        {"target": "-100123", "name": "下载组", "types": "photo,video",
         "minutes": 5, "priority": 99, "batch_size": 500,
         "direction": "oldest", "enabled": True},
    ]
    app = SimpleNamespace(
        _extra_runtimes={"account-b": SimpleNamespace(
            client=object(), _next_at={"-100123": 100000},
        )},
        _monitor_types_label=lambda kinds: "图片、视频",
    )
    window.app = app

    window._refresh_channel_tree()
    original = window.tree.rows["0"]
    assert original[0] == "下载组"
    assert original[1] == "-100123"
    assert original[2] == "图片、视频"
    assert original[6] == "旧→新"
    assert original[7] == "启用"
    assert original[8] != "-"
    window._refresh_channel_tree()
    assert window.tree.rows["0"] == original
    assert window.tree.removed == []
    assert window.tree.selection() == ("0",)


def test_dialog_switch_reuses_cached_results_without_network_call():
    window = AccountWindow.__new__(AccountWindow)
    window.choice = FakeVar("TG")
    window._ids = {"TG": "account-b"}
    window._dialog_cache = {"account-b": [("1", "channel", "频道", "")]}
    window._dialog_loading = set()
    displayed = []
    window._refresh_dialog_tree = lambda: displayed.append(True)
    # No app.loop and no runtime exists. The cached path must work locally.
    window._ensure_dialogs()
    window._ensure_dialogs()
    assert displayed == [True, True]


def test_manager_login_save_keeps_proxy_saved_from_main_window(tmp_path):
    from tgdl.accounts import AccountRegistry
    from tgdl.accounts_gui import _ACCOUNT_FIELDS
    from tgdl.config import read_env_values, update_env

    registry = AccountRegistry(tmp_path)
    path = registry.env_path("account-b")
    update_env({"api_id": "1", "api_hash": "test", "download_dir": str(tmp_path)}, path)
    original = read_env_values(path)
    window = AccountWindow.__new__(AccountWindow)
    window.registry = registry
    window._selected_id = lambda: "account-b"
    window._fields = {key: FakeVar(original[key]) for key, _ in _ACCOUNT_FIELDS}
    window._loaded_fields = {key: var.get() for key, var in window._fields.items()}
    window.app = SimpleNamespace(_log_account=lambda *_: None)

    # The main window saved a proxy while this window remained open; the
    # manager's Login button runs _save_settings before creating its client.
    update_env({**original, "proxy_host": "new-proxy", "proxy_port": "1080"}, path)
    window._fields["media_concurrency"].set("2")
    assert window._save_settings()
    saved = read_env_values(path)
    assert (saved["proxy_host"], saved["proxy_port"], saved["media_concurrency"]) == (
        "new-proxy", "1080", "2"
    )
    assert window._fields["proxy_host"].get() == "new-proxy"
