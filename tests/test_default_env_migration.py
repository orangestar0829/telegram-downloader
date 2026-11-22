"""Migration/isolated account configuration regression tests."""
import os

import pytest

from tgdl import config, paths
from tgdl.accounts import AccountRegistry
from tgdl.config import load_settings, read_env_values, update_env


def test_default_env_auto_copy_and_repeat_boot_not_overwrite(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setattr(paths, "data_dir", lambda: root)
    legacy = root / ".env"
    legacy.write_text(
        "# old credentials\nTELEGRAM_API_ID=1234\nTELEGRAM_API_HASH=old-hash\n"
        "SOCKS5_HOST=old-proxy\nMEDIA_CONCURRENCY=2\nCUSTOM_FLAG=keep\n",
        encoding="utf-8",
    )
    old = legacy.read_bytes()
    assert paths.prepare_data_dir() == root
    modern = root / "accounts" / "default" / ".env"
    assert modern.read_bytes() == old
    assert legacy.read_bytes() == old
    assert AccountRegistry(root).env_path("default") == modern
    update_env({
        "api_id": "5678", "api_hash": "new-hash",
        "phone": "+1888", "download_dir": "./media", "proxy_host": "new-proxy",
        "proxy_port": "1080", "media_concurrency": "5",
    }, modern)
    new_bytes = modern.read_bytes()
    paths.prepare_data_dir()
    assert modern.read_bytes() == new_bytes
    assert legacy.read_bytes() == old
    assert "CUSTOM_FLAG=keep" in modern.read_text(encoding="utf-8")


def test_existing_account_env_never_overwritten_or_cross_loaded(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setattr(paths, "data_dir", lambda: root)
    legacy = root / ".env"
    legacy.write_text("TELEGRAM_API_ID=44\nTELEGRAM_API_HASH=stale\n"
                      "SOCKS5_HOST=stale-proxy\n", encoding="utf-8")
    a = root / "accounts" / "default" / ".env"
    b = root / "accounts" / "b-account" / ".env"
    update_env({"api_id": "1", "api_hash": "a-hash", "phone": "+111",
                "download_dir": "./downloads", "proxy_host": "proxy-a",
                "media_concurrency": "2"}, a)
    update_env({"api_id": "2", "api_hash": "b-hash", "phone": "+222",
                "download_dir": "./downloads", "proxy_host": "proxy-b",
                "media_concurrency": "4"}, b)
    monkeypatch.setattr(config, "ENV_PATH", a)
    monkeypatch.setattr(config, "DATA_DIR", root)
    monkeypatch.setattr(config, "_bundled_defaults", lambda: None)
    monkeypatch.setenv("TELEGRAM_API_ID", "9999")
    monkeypatch.setenv("TELEGRAM_API_HASH", "leaked-env")
    monkeypatch.setenv("TELEGRAM_PHONE", "+999")
    monkeypatch.setenv("SOCKS5_HOST", "wrong-proxy")
    original_process_env = os.environ["TELEGRAM_API_ID"]
    paths.prepare_data_dir()
    settings_a = load_settings()
    settings_b = load_settings(env_path=b)
    assert (settings_a.api_id, settings_a.api_hash, settings_a.phone,
            settings_a.proxy_host) == (1, "a-hash", "+111", "proxy-a")
    assert (settings_b.api_id, settings_b.api_hash, settings_b.phone,
            settings_b.proxy_host) == (2, "b-hash", "+222", "proxy-b")
    assert config.resolve_api_credentials() == ("1", "a-hash")
    assert os.environ["TELEGRAM_API_ID"] == original_process_env
    assert read_env_values(a)["api_id"] == "1"
    assert read_env_values(b)["api_id"] == "2"
    assert legacy.read_text(encoding="utf-8").startswith("TELEGRAM_API_ID=44")


def test_b_account_missing_credentials_does_not_read_default(tmp_path, monkeypatch):
    a = tmp_path / "accounts" / "default" / ".env"
    b = tmp_path / "accounts" / "b" / ".env"
    update_env({"api_id": "9", "api_hash": "secret", "download_dir": "./download"}, a)
    update_env({"api_id": "", "api_hash": "", "download_dir": "./download"}, b)
    monkeypatch.setattr(config, "ENV_PATH", a)
    monkeypatch.setattr(config, "_bundled_defaults", lambda: None)
    with pytest.raises(SystemExit, match="Missing TELEGRAM_API_ID"):
        load_settings(env_path=b)


def test_empty_legacy_or_no_env_and_recovery(tmp_path):
    missing = tmp_path / "missing"
    assert not paths.migrate_default_env(missing).exists()
    root = tmp_path / "data"
    root.mkdir()
    old = root / ".env"
    old.write_bytes(b"")
    new = paths.migrate_default_env(root)
    assert new.is_file() and new.read_bytes() == b""
    new.write_text("TELEGRAM_API_ID=77\n", encoding="utf-8")
    assert paths.migrate_default_env(root).read_text(encoding="utf-8") == "TELEGRAM_API_ID=77\n"
    assert old.read_bytes() == b""


def test_invalid_utf8_preserves_legacy_and_does_not_publish(tmp_path):
    old = tmp_path / ".env"
    old.write_bytes(b"API=\xff")
    with pytest.raises(UnicodeDecodeError):
        paths.migrate_default_env(tmp_path)
    assert old.read_bytes() == b"API=\xff"
    assert not (tmp_path / "accounts" / "default" / ".env").exists()
    assert not (tmp_path / "accounts" / "default" / ".env.migrating").exists()


def test_default_gui_path_and_cli_session_are_account_local(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "ENV_PATH", tmp_path / "accounts" / "default" / ".env")
    monkeypatch.setattr(config, "SESSION_PATH", tmp_path / "accounts" / "default" / "tgdl_session")
    update_env({"api_id": "123", "api_hash": "hash", "download_dir": "./media"},
               config.ENV_PATH)
    settings = load_settings()
    assert settings.session_path == config.SESSION_PATH
    assert settings.download_dir == tmp_path / "media"
