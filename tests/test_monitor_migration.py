"""Safe, idempotent migration of original account monitor settings."""
import json

import pytest

from tgdl.accounts import AccountRegistry
from tgdl.monitor import load_channels, monitor_path, save_channels
from tgdl import paths


def test_migrate_legacy_default_monitors_retains_fields_and_other_accounts(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setattr(paths, "data_dir", lambda: root)
    legacy = root / "monitored_channels.json"
    old = [
        {"target": "-100123", "name": "图片频道", "minutes": 5, "priority": 15,
         "batch_size": 500, "types": "photo", "enabled": True, "direction": "newest",
         "peer_id": 123},
        {"target": "-100456", "name": "视频频道", "minutes": 10, "priority": -2,
         "batch_size": 30, "types": "video", "enabled": False, "direction": "oldest"},
    ]
    save_channels(old, legacy)
    source_bytes = legacy.read_bytes()
    other = root / "accounts" / "b-account" / "monitored_channels.json"
    save_channels([{"target": "-100999", "minutes": 5, "types": "video"}], other)
    other_bytes = other.read_bytes()
    history = root / "tgdl_download_history.sqlite3"
    history.write_bytes(b"unchanged-history")

    paths.prepare_data_dir()
    default = root / "accounts" / "default" / "monitored_channels.json"
    assert default.read_bytes() == source_bytes
    assert legacy.read_bytes() == source_bytes
    assert other.read_bytes() == other_bytes
    assert history.read_bytes() == b"unchanged-history"
    assert load_channels(default) == load_channels(legacy)
    assert AccountRegistry(root).monitor_path("default") == default
    # All default helper readers/writers resolve the migrated path.
    monkeypatch.setattr("tgdl.monitor.prepare_data_dir", lambda: root)
    assert monitor_path() == default
    assert load_channels() == load_channels(legacy)
    assert json.loads(default.read_text(encoding="utf-8")) == old


def test_repeat_boot_never_overwrites_updated_default_monitor_settings(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setattr(paths, "data_dir", lambda: root)
    old = root / "monitored_channels.json"
    save_channels([{"target": "@old", "minutes": 5}], old)
    paths.prepare_data_dir()
    new = root / "accounts" / "default" / "monitored_channels.json"
    save_channels([{"target": "@new", "minutes": 7}], new)
    edited = new.read_bytes()
    paths.prepare_data_dir()
    assert new.read_bytes() == edited
    assert load_channels(old)[0]["target"] == "@old"


def test_bad_legacy_json_is_not_published_and_is_preserved(tmp_path):
    old = tmp_path / "monitored_channels.json"
    old.write_text('{"unexpected": "object"}', encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid legacy"):
        paths.migrate_default_monitors(tmp_path)
    assert old.read_text(encoding="utf-8") == '{"unexpected": "object"}'
    folder = tmp_path / "accounts" / "default"
    assert not (folder / "monitored_channels.json").exists()
    assert not (folder / "monitored_channels.json.migrating").exists()


def test_missing_old_file_keeps_new_account_empty(tmp_path):
    assert not paths.migrate_default_monitors(tmp_path).exists()
    assert not (tmp_path / "monitored_channels.json").exists()


def test_legacy_file_is_sufficient_for_rollback(tmp_path):
    old = tmp_path / "monitored_channels.json"
    save_channels([{"target": "@legacy", "minutes": 7, "priority": 9}], old)
    paths.migrate_default_monitors(tmp_path)
    new = tmp_path / "accounts" / "default" / "monitored_channels.json"
    save_channels([{"target": "@changed", "minutes": 6}], new)
    assert load_channels(old)[0]["target"] == "@legacy"
    assert load_channels(new)[0]["target"] == "@changed"
    # Restoring the legacy file to an older installation restores its monitors.
    assert load_channels(old)[0]["priority"] == 9
