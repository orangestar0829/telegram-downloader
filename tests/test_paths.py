from pathlib import Path
import sqlite3


def make_legacy_session(path: Path, value: str) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE sessions (dc_id INTEGER, auth_key TEXT)')
        conn.execute('INSERT INTO sessions VALUES (2, ?)', (value,))


from tgdl import paths


def test_portable_uses_executable_sibling_data_and_imports_legacy(tmp_path, monkeypatch):
    app = tmp_path / "TelegramDownloader"
    app.mkdir()
    (app / "portable.flag").touch()
    (app / "_internal").mkdir()
    make_legacy_session(app / "_internal" / "tgdl_session.session", "session")
    (app / ".env").write_text("TELEGRAM_API_ID=1\n", encoding="utf-8")
    old_user = tmp_path / "AppData"
    old_user.mkdir()
    (old_user / "tgdl_download_history.sqlite3").write_bytes(b"old-db")
    monkeypatch.setattr(paths, "executable_dir", lambda: app)
    monkeypatch.setattr(paths, "user_data_dir", lambda: old_user)
    assert paths.is_portable()
    data = paths.prepare_data_dir()
    assert data == app / "data"
    assert (data / "tgdl_session.session").is_file()
    with sqlite3.connect(data / "accounts" / "default" / "tgdl_session.session") as conn:
        assert conn.execute("SELECT auth_key FROM sessions").fetchone() == ("session",)
    assert (data / ".env").read_text(encoding="utf-8") == "TELEGRAM_API_ID=1\n"
    assert (data / "tgdl_download_history.sqlite3").read_bytes() == b"old-db"
    (data / ".env").write_text("new", encoding="utf-8")
    paths.prepare_data_dir()
    assert (data / ".env").read_text(encoding="utf-8") == "new"
    # Repeated boot never overwrites the newly authorized default account.
    with sqlite3.connect(data / "accounts" / "default" / "tgdl_session.session") as conn:
        conn.execute("UPDATE sessions SET auth_key='new-token'")
        conn.commit()
    paths.prepare_data_dir()
    with sqlite3.connect(data / "accounts" / "default" / "tgdl_session.session") as conn:
        assert conn.execute("SELECT auth_key FROM sessions").fetchone() == ("new-token",)


def test_installer_uses_writable_user_data(tmp_path, monkeypatch):
    monkeypatch.setattr(paths.sys, "frozen", True, raising=False)
    app = tmp_path / "installed"
    app.mkdir()
    user = tmp_path / "UserData"
    (app / "_internal").mkdir()
    make_legacy_session(app / "_internal" / "tgdl_session.session", "legacy")
    monkeypatch.setattr(paths, "executable_dir", lambda: app)
    monkeypatch.setattr(paths, "user_data_dir", lambda: user)
    assert not paths.is_portable()
    assert paths.prepare_data_dir() == user
    with sqlite3.connect(user / "accounts" / "default" / "tgdl_session.session") as conn:
        assert conn.execute("SELECT auth_key FROM sessions").fetchone() == ("legacy",)
