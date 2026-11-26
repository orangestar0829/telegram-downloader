"""Mode edits persist atomically before touching any running monitor."""
from types import SimpleNamespace

import pytest
from telethon import utils
from telethon.tl.types import PeerChannel

from tests.test_accounts_gui_consistency import FakeTree, FakeVar
from tests.test_monitor_types_ui import app_with_channels
from tgdl import gui, monitor
from tgdl.account_view import monitor_enabled_label
from tgdl.accounts import AccountRegistry
from tgdl.accounts_gui import AccountWindow, _ACCOUNT_FIELDS
from tgdl.config import read_env_values, update_env
from tgdl.monitor import MONITOR_MODE_LABELS, REALTIME, SCHEDULED
from tgdl.slots import SlotCoordinator, make_plan


def _row(n, **changes):
    return {
        "target": str(utils.get_peer_id(PeerChannel(n))),
        "minutes": 5,
        "types": "photo",
        "enabled": True,
        **changes,
    }


def _dialog(n, kind="channel"):
    row = _row(n)
    return (row["target"], kind, f"频道 {n}", f"channel_{n}")


def _bind_plan(app, registry, primary_rows):
    app._slots = SlotCoordinator()

    def plan(account_id=None, *, rows=None, concurrency=None):
        scopes = []
        for account in registry.accounts():
            key = account.id
            cfg = read_env_values(registry.env_path(key))
            limit = concurrency if key == account_id and concurrency else int(cfg["media_concurrency"])
            saved = primary_rows() if key == "default" else monitor.load_channels(registry.monitor_path(key))
            items = rows if key == account_id and rows is not None else saved
            scopes.append((key, limit, items))
        return make_plan(scopes)

    app._slot_plan = plan
    app._refresh_slot_plan = lambda proposed=None: app._slots.configure(proposed or plan())
    app._refresh_slot_plan()


def test_monitor_mode_round_trip_and_epoch_on_conversion(tmp_path):
    path = tmp_path / "monitored_channels.json"
    existing = _row(321)
    default = {**existing, "mode": SCHEDULED}
    assert monitor.next_due_channel([default], {}, 10, 0) == default
    direct = _row(123, target="@channel_123")
    monitor.configure_monitor_mode(direct, None, REALTIME, [_dialog(123)])
    assert direct["peer_id"] == 123
    assert direct["realtime_backfill"] is False

    switched = dict(existing)
    monitor.configure_monitor_mode(switched, existing, REALTIME, [_dialog(321)])
    assert switched["realtime_backfill"] is True
    assert switched["realtime_epoch"] != direct["realtime_epoch"]
    assert monitor.next_due_channel([switched], {}, 10, 0) is None

    monitor.save_channels([direct, switched], path)
    restored_a, restored_b = monitor.load_channels(path)
    assert restored_a["realtime_epoch"] == direct["realtime_epoch"]
    assert restored_b["realtime_epoch"] == switched["realtime_epoch"]
    assert restored_b["realtime_backfill"] is True
    changed = {**restored_b, "types": "video", "enabled": False}
    monitor.configure_monitor_mode(changed, restored_b, REALTIME, [])
    assert changed["realtime_epoch"] == switched["realtime_epoch"]
    monitor.configure_monitor_mode(changed, restored_b, SCHEDULED, [])
    assert "realtime_epoch" not in changed and "realtime_backfill" not in changed


def test_unjoined_ids_and_basic_groups_cannot_be_enrolled_realtime():
    with pytest.raises(ValueError, match="已加入"):
        monitor.configure_monitor_mode(_row(321), None, REALTIME, [_dialog(123)])
    with pytest.raises(ValueError, match="频道或超级群"):
        monitor.configure_monitor_mode(_row(321), None, REALTIME, [_dialog(321, "group")])
    row = _row(321)
    monitor.configure_monitor_mode(row, None, REALTIME, [_dialog(321, "supergroup")])
    assert row["peer_id"] == 321


def test_main_mode_over_capacity_does_not_change_existing_row_or_file(tmp_path, monkeypatch):
    registry = AccountRegistry(tmp_path)
    first = _row(123, mode=REALTIME, realtime_epoch="first", peer_id=123)
    second = _row(321, mode=SCHEDULED)
    app = app_with_channels([first, second])
    warnings = []
    app.messagebox.showwarning = lambda *args: warnings.append(args)
    app._account_registry = registry
    update_env({"media_concurrency": "1"}, registry.env_path("default"))
    app.monitor_mode_var = FakeVar(MONITOR_MODE_LABELS[REALTIME])
    app.monitor_tree.selected = ("1",)
    app._dialog_rows = [_dialog(123), _dialog(321)]
    path = registry.monitor_path("default")
    monitor.save_channels([first, second], path)
    monkeypatch.setattr(gui, "save_channels", lambda items: monitor.save_channels(items, path))
    _bind_plan(app, registry, lambda: app._monitored)

    previous = path.read_bytes()
    app._monitor_update()
    assert "固定名额" in warnings[-1][1]
    assert path.read_bytes() == previous and app._monitored[1] == second
    assert app._slots.owner(321) is None

    app.monitor_tree.selected = ("0",)
    app._monitor_toggle()  # Pausing releases the reservation but keeps the epoch.
    assert app._slots.owner(123) is None
    assert monitor.load_channels(path)[0]["realtime_epoch"] == "first"

    app.monitor_tree.selected = ("1",)
    app._monitor_update()
    assert app._slots.owner(321) == "default"
    assert monitor.load_channels(path)[1]["realtime_backfill"] is True


def test_download_settings_reject_concurrency_below_reserved_lanes(tmp_path):
    registry = AccountRegistry(tmp_path)
    path = registry.env_path("default")
    update_env({"media_concurrency": "2"}, path)
    rows = [
        _row(123, mode=REALTIME, realtime_epoch="a", peer_id=123),
        _row(321, mode=REALTIME, realtime_epoch="b", peer_id=321),
    ]
    app = app_with_channels(rows)
    app._account_registry = registry
    app._applied_media_settings = (2, 2, 1, 0)
    app.media_concurrency_var = FakeVar("1")
    app.media_retries_var = FakeVar("1")
    app.chunk_concurrency_var = FakeVar("1")
    app.media_speed_kib_var = FakeVar("0")
    app.monitor_auto_throttle_var = FakeVar(True)
    warnings = []
    app.messagebox.showwarning = lambda *args: warnings.append(args)
    _bind_plan(app, registry, lambda: app._monitored)

    app._apply_media_settings()
    assert "未生效" in warnings[-1][1]
    assert read_env_values(path)["media_concurrency"] == "2"
    assert read_env_values(path)["monitor_auto_throttle"] == "0"
    assert app._applied_media_settings == (2, 2, 1, 0)
    assert app._slots.owner(321) == "default"


def test_secondary_mode_add_and_settings_preserve_file_on_invalid_budget(tmp_path):
    registry = AccountRegistry(tmp_path)
    account = registry.add("secondary")
    env_path = registry.env_path(account.id)
    update_env({"media_concurrency": "2"}, env_path)
    disk = registry.monitor_path(account.id)
    monitor.save_channels([
        _row(123, mode=REALTIME, realtime_epoch="one", peer_id=123),
        _row(321, mode=REALTIME, realtime_epoch="two", peer_id=321),
    ], disk)
    warnings = []
    window = AccountWindow.__new__(AccountWindow)
    window.app = SimpleNamespace(messagebox=SimpleNamespace(showwarning=lambda *a, **k: warnings.append(a)))
    window.win = object()
    window.registry = registry
    window._selected_id = lambda: account.id
    window._dialog_cache = {account.id: [_dialog(777)]}
    window._channel_rows = monitor.load_channels(disk)
    window.tree = FakeTree()
    window.mode = FakeVar(MONITOR_MODE_LABELS[REALTIME])
    window.direction = FakeVar("从新到旧")
    window.enabled = FakeVar(True)
    window.type_photo, window.type_video, window.type_anim = FakeVar(True), FakeVar(False), FakeVar(False)
    values = read_env_values(env_path)
    window._fields = {key: FakeVar(values[key]) for key, _ in _ACCOUNT_FIELDS}
    window._loaded_fields = dict(values)
    window._fields["media_concurrency"].set("1")
    _bind_plan(window.app, registry, list)

    before = disk.read_bytes()
    window._upsert_channel(_row(777)["target"], "第三个")
    assert "固定名额" in warnings[-1][1]
    assert disk.read_bytes() == before
    assert not window._save_settings()
    assert read_env_values(env_path)["media_concurrency"] == "2"


def test_duplicate_account_rows_show_shared_reservation_not_a_second_lane():
    row = _row(123, mode=REALTIME, realtime_epoch="same", peer_id=123)
    slots = SlotCoordinator()
    slots.configure(make_plan([("default", 2, [row]), ("second", 2, [row])]))
    assert monitor_enabled_label(row, "default", slots) == "启用"
    assert monitor_enabled_label(row, "second", slots) == "共用名额"
    assert monitor_enabled_label({**row, "enabled": False}, "second", slots) == "暂停"


def test_removing_owner_revalidates_backup_capacity_before_save(tmp_path, monkeypatch):
    registry = AccountRegistry(tmp_path)
    backup = registry.add("backup")
    update_env({"media_concurrency": "1"}, registry.env_path(backup.id))
    owner_row = _row(123, mode=REALTIME, realtime_epoch="owner", peer_id=123)
    backup_rows = [
        _row(123, mode=REALTIME, realtime_epoch="backup", peer_id=123),
        _row(321, mode=REALTIME, realtime_epoch="other", peer_id=321),
    ]
    path = registry.monitor_path("default")
    monitor.save_channels([owner_row], path)
    monitor.save_channels(backup_rows, registry.monitor_path(backup.id))
    monkeypatch.setattr(gui, "save_channels", lambda items: monitor.save_channels(items, path))
    app = app_with_channels([owner_row])
    app._account_registry = registry
    warnings = []
    app.messagebox.showwarning = lambda *args: warnings.append(args)
    app.monitor_tree.selection_remove = lambda *_: None
    _bind_plan(app, registry, lambda: app._monitored)

    app._monitor_remove()
    assert "固定名额" in warnings[-1][1]
    assert app._monitored == [owner_row]
    assert monitor.load_channels(path)[0]["peer_id"] == 123
    assert app._slots.owner(123) == "default"

    update_env({"media_concurrency": "2"}, registry.env_path(backup.id))
    app._monitor_remove()
    assert monitor.load_channels(path) == []
    assert app._slots.owner(123) == backup.id


def test_realtime_alias_cannot_duplicate_canonical_peer_in_same_account(tmp_path, monkeypatch):
    registry = AccountRegistry(tmp_path)
    first = _row(123, mode=REALTIME, realtime_epoch="first", peer_id=123)
    alias = _row(123, target="@channel_123", mode=SCHEDULED)
    app = app_with_channels([first, alias])
    warnings = []
    app.messagebox.showwarning = lambda *args: warnings.append(args)
    app._account_registry = registry
    app.monitor_mode_var = FakeVar(MONITOR_MODE_LABELS[REALTIME])
    app.monitor_tree.selected = ("1",)
    app._dialog_rows = [_dialog(123)]
    path = registry.monitor_path("default")
    monitor.save_channels([first, alias], path)
    monkeypatch.setattr(gui, "save_channels", lambda items: monitor.save_channels(items, path))
    _bind_plan(app, registry, lambda: app._monitored)

    original = path.read_bytes()
    app._monitor_update()
    assert warnings and "同一频道" in warnings[-1][1]
    assert path.read_bytes() == original and app._monitored[1] == alias
