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
    make_legacy_session(old_user / "tgdl_download_history.sqlite3", "old-db")
    monkeypatch.setattr(paths, "executable_dir", lambda: app)
    monkeypatch.setattr(paths, "user_data_dir", lambda: old_user)
    assert paths.is_portable()
    data = paths.prepare_data_dir()
    assert data == app / "data"
    assert (data / "tgdl_session.session").is_file()
    with sqlite3.connect(data / "accounts" / "default" / "tgdl_session.session") as conn:
        assert conn.execute("SELECT auth_key FROM sessions").fetchone() == ("session",)
    assert (data / ".env").read_text(encoding="utf-8") == "TELEGRAM_API_ID=1\n"
    assert (data / "accounts" / "default" / ".env").read_text(encoding="utf-8") == "TELEGRAM_API_ID=1\n"
    with sqlite3.connect(data / "tgdl_download_history.sqlite3") as conn:
        assert conn.execute("SELECT auth_key FROM sessions").fetchone() == ("old-db",)
    (data / ".env").write_text("new", encoding="utf-8")
    paths.prepare_data_dir()
    assert (data / ".env").read_text(encoding="utf-8") == "new"
    assert (data / "accounts" / "default" / ".env").read_text(encoding="utf-8") == "TELEGRAM_API_ID=1\n"
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
    assert (user / "accounts" / "default" / "tgdl_session.session").is_file()
    with sqlite3.connect(user / "accounts" / "default" / "tgdl_session.session") as conn:
        assert conn.execute("SELECT auth_key FROM sessions").fetchone() == ("legacy",)



def test_portable_import_copies_committed_wal_and_legacy_monitor_list(tmp_path, monkeypatch):
    from contextlib import ExitStack, closing
    import json

    app = tmp_path / "portable"
    app.mkdir()
    (app / "portable.flag").touch()
    installed = tmp_path / "installed"
    installed.mkdir()
    monitors = [{"target": "@old", "minutes": 5}]
    (installed / "monitored_channels.json").write_text(json.dumps(monitors), encoding="utf-8")
    monkeypatch.setattr(paths, "executable_dir", lambda: app)
    monkeypatch.setattr(paths, "user_data_dir", lambda: installed)
    filenames = ("tgdl_session.session", "tgdl_download_history.sqlite3")

    with ExitStack() as stack:
        for filename in filenames:
            source = stack.enter_context(closing(sqlite3.connect(installed / filename)))
            source.execute("PRAGMA journal_mode=WAL")
            source.execute("PRAGMA wal_autocheckpoint=0")
            source.execute("CREATE TABLE payload (value TEXT)")
            source.commit()
            source.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            source.execute("INSERT INTO payload VALUES ('wal row')")
            source.commit()
        # Keep source connections open so the new row remains in the WAL.
        assert all((installed / (filename + "-wal")).stat().st_size > 0 for filename in filenames)
        destination = paths.prepare_data_dir()
        default = destination / "accounts" / "default"
        for copy in (default / filenames[0], destination / filenames[1]):
            with closing(sqlite3.connect(copy)) as imported:
                assert imported.execute("SELECT value FROM payload").fetchall() == [("wal row",)]
        assert json.loads((default / "monitored_channels.json").read_text()) == monitors


def test_portable_import_prefers_active_installed_default_over_rollback(tmp_path, monkeypatch):
    from contextlib import closing

    app = tmp_path / "portable"
    app.mkdir()
    (app / "portable.flag").touch()
    installed = tmp_path / "installed"
    active = installed / "accounts" / "default"
    active.mkdir(parents=True)
    make_legacy_session(installed / "tgdl_session.session", "old-session")
    make_legacy_session(active / "tgdl_session.session", "current-session")
    monkeypatch.setattr(paths, "executable_dir", lambda: app)
    monkeypatch.setattr(paths, "user_data_dir", lambda: installed)

    destination = paths.prepare_data_dir() / "accounts" / "default"
    with closing(sqlite3.connect(destination / "tgdl_session.session")) as imported:
        assert imported.execute("SELECT auth_key FROM sessions").fetchone() == ("current-session",)
