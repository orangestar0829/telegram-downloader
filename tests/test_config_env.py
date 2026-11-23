from pathlib import Path

from tgdl import config


def test_update_and_read_env_roundtrip(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    monkeypatch.setattr(config, "ENV_PATH", env_path)

    config.update_env(
        {
            "api_id": "12345",
            "api_hash": "deadbeef",
            "phone": "+15551234567",
            "download_dir": "./downloads",
        }
    )
    assert env_path.exists()

    values = config.read_env_values()
    assert values["api_id"] == "12345"
    assert values["api_hash"] == "deadbeef"
    assert values["phone"] == "+15551234567"
    assert values["download_dir"] == "./downloads"


def test_update_env_preserves_comments(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("# my comment\nTELEGRAM_API_ID=old\nCUSTOM=keep\n", encoding="utf-8")
    monkeypatch.setattr(config, "ENV_PATH", env_path)

    config.update_env({"api_id": "new", "api_hash": "h", "phone": "", "download_dir": "./d"})
    text = env_path.read_text(encoding="utf-8")
    assert "# my comment" in text
    assert "CUSTOM=keep" in text
    assert "TELEGRAM_API_ID=new" in text
    assert "TELEGRAM_API_ID=old" not in text


def test_resolve_credentials_from_env_dict() -> None:
    assert config.resolve_api_credentials(
        {"TELEGRAM_API_ID": "111", "TELEGRAM_API_HASH": "abc"}
    ) == ("111", "abc")


def test_resolve_credentials_none_when_missing(monkeypatch) -> None:
    monkeypatch.setattr(config, "_bundled_defaults", lambda: None)
    assert config.resolve_api_credentials({}) is None


def test_resolve_credentials_bundled_fallback(monkeypatch) -> None:
    monkeypatch.setattr(config, "_bundled_defaults", lambda: ("999", "deadbeef"))
    assert config.resolve_api_credentials({}) == ("999", "deadbeef")


def test_default_download_dir_name() -> None:
    assert config.default_download_dir().name == "TelegramDownloader"


def test_account_values_round_trip_literally_without_touching_other_account(tmp_path, monkeypatch):
    from tgdl.accounts import AccountRegistry

    registry = AccountRegistry(tmp_path)
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setenv("TGD_TEST_NAME", "inherited-not-literal")
    a = registry.env_path("default")
    b = registry.env_path("b-account")
    defaults = {"api_id": "1234", "api_hash": "fake-hash", "phone": ""}
    config.update_env({**defaults, "download_dir": "A", "proxy_password": "first"}, a)
    a_before = a.read_bytes()
    password = """ pw #42 "quote" 'single' ${TGD_TEST_NAME}\r\nsecond\t """ + "\\"
    directory = "D:\\Movies #A\\new\\"
    staging = "cache ${TGD_TEST_NAME} # staging"
    desired = {
        **defaults, "download_dir": directory, "temp_download_dir": staging,
        "proxy_host": "proxy-b", "proxy_username": " name ",
        "proxy_password": password,
    }
    config.update_env(desired, b)

    values = config.read_env_values(b)
    for key, value in desired.items():
        assert values[key] == value
    settings = config.load_settings(env_path=b, session_path=registry.session_path("b-account"))
    expected = Path(directory)
    assert settings.download_dir == (expected if expected.is_absolute() else tmp_path / expected)
    assert settings.temp_download_dir == tmp_path / staging
    assert settings.proxy_username == " name "
    assert settings.proxy_password == password
    assert settings.session_path == registry.session_path("b-account")
    assert a.read_bytes() == a_before


def test_dotenv_rewrite_preserves_bom_crlf_export_comments_and_unknown_multiline(tmp_path):
    env_path = tmp_path / ".env"
    initial = (
        "\ufeff# comment\r\n"
        "export TELEGRAM_API_ID = old  # initial API ID\r\n"
        "export CUSTOM_VALUE='multi\r\nSOCKS5_PASSWORD=not-a-setting\r\nline' # unknown comment\r\n"
        "TELEGRAM_API_HASH=test # leave this alone\r\n"
        "SOCKS5_PASSWORD = \"old\r\nsecret\" # password comment\r\n"
        "SOCKS5_PASSWORD=unused # duplicate comment\r\n"
        "DOWNLOAD_DIR=./keep\r\n"
        "OTHER_WITHOUT_EQ # do not change"
    )
    env_path.write_bytes(initial.encode())
    config.update_env({
        "api_id": "1234", "api_hash": "test", "download_dir": "./keep",
        "proxy_password": "new # password",
    }, env_path)
    data = env_path.read_bytes().decode()
    assert data.startswith("\ufeff# comment\r\nexport TELEGRAM_API_ID = 1234  # initial API ID\r\n")
    assert "export CUSTOM_VALUE='multi\r\nSOCKS5_PASSWORD=not-a-setting\r\nline' # unknown comment\r\n" in data
    assert "TELEGRAM_API_HASH=test # leave this alone\r\n" in data
    assert 'SOCKS5_PASSWORD = "new # password" # password comment\r\n' in data
    assert 'SOCKS5_PASSWORD="new # password" # duplicate comment\r\n' in data
    assert "OTHER_WITHOUT_EQ # do not change\r\n" in data
    assert config.read_env_values(env_path)["proxy_password"] == "new # password"
    old = env_path.read_bytes()
    config.update_env(config.read_env_values(env_path), env_path)
    same = env_path.read_bytes()
    config.update_env(config.read_env_values(env_path), env_path)
    assert env_path.read_bytes() == same
    assert old in same  # optional fields can be added without rewriting old lines


def test_invalid_setting_leaves_destination_unmodified(tmp_path, monkeypatch):
    import pytest
    from tgdl.errors import TGDLInputError

    env_path = tmp_path / ".env"
    original = b"# old\r\nTELEGRAM_API_ID=1234\r\n"
    env_path.write_bytes(original)
    with pytest.raises(TGDLInputError, match="SOCKS5_PASSWORD"):
        config.update_env({"proxy_password": "not\x00valid"}, env_path)
    assert env_path.read_bytes() == original

    def denied_replace(_source, _dest):
        raise OSError("replace denied")

    monkeypatch.setattr(Path, "replace", denied_replace)
    with pytest.raises(OSError, match="replace denied"):
        config.update_env({"api_id": "5678"}, env_path)
    assert env_path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [env_path]


def test_monitor_auto_throttle_is_opt_in_and_never_inherited(tmp_path, monkeypatch):
    from tgdl.accounts import AccountRegistry

    registry = AccountRegistry(tmp_path)
    primary = registry.env_path("default")
    credentials = {"api_id": "1234", "api_hash": "test-only"}
    config.update_env(credentials, primary)
    monkeypatch.setenv("MONITOR_AUTO_THROTTLE", "1")
    assert config.read_env_values(primary)["monitor_auto_throttle"] == "0"
    assert not config.load_settings(env_path=primary).monitor_auto_throttle

    config.update_env({**credentials, "monitor_auto_throttle": "1"}, primary)
    secondary = registry.env_path(registry.add("Second").id)
    assert config.load_settings(env_path=primary).monitor_auto_throttle
    assert not config.load_settings(env_path=secondary).monitor_auto_throttle

    config.update_env({**credentials, "monitor_auto_throttle": "YES"}, secondary)
    config.update_env({**credentials, "download_dir": "./other"}, secondary)
    assert config.read_env_values(secondary)["monitor_auto_throttle"] == "YES"
    assert config.load_settings(env_path=secondary).monitor_auto_throttle

    config.update_env({**credentials, "monitor_auto_throttle": "typo"}, secondary)
    assert not config.load_settings(env_path=secondary).monitor_auto_throttle
    assert config.load_settings(env_path=primary).monitor_auto_throttle
