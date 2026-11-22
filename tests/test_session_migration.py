"""Legacy Telethon SQLite Session must survive migration without destructive move."""
import sqlite3

import pytest

from tgdl.paths import migrate_default_session


def _make_session(path, key):
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE sessions (dc_id INTEGER, auth_key TEXT)")
        db.execute("INSERT INTO sessions VALUES (2, ?)", (key,))


def _auth_key(path):
    with sqlite3.connect(path) as db:
        return db.execute("SELECT auth_key FROM sessions").fetchone()[0]


def test_default_session_migrates_once_and_legacy_remains(tmp_path):
    old = tmp_path / "tgdl_session.session"
    _make_session(old, "original-tg-auth")
    new = migrate_default_session(tmp_path)
    assert new == tmp_path / "accounts" / "default" / "tgdl_session.session"
    assert _auth_key(new) == "original-tg-auth"
    assert _auth_key(old) == "original-tg-auth"
    with sqlite3.connect(new) as db:
        db.execute("UPDATE sessions SET auth_key='newer-auth'")
    assert migrate_default_session(tmp_path) == new
    assert _auth_key(new) == "newer-auth"
    assert _auth_key(old) == "original-tg-auth"


def test_migration_refuses_invalid_legacy_session_without_destroying_it(tmp_path):
    old = tmp_path / "tgdl_session.session"
    old.write_bytes(b"invalid sqlite session")
    with pytest.raises(sqlite3.DatabaseError):
        migrate_default_session(tmp_path)
    assert old.read_bytes() == b"invalid sqlite session"
    assert not (tmp_path / "accounts" / "default" / "tgdl_session.session").exists()
    assert not (tmp_path / "accounts" / "default" / "tgdl_session.session.migrating").exists()


def test_session_migration_no_legacy_is_noop(tmp_path):
    new = migrate_default_session(tmp_path)
    assert not new.exists()
