"""Multi-account database migration, ownership and session separation regressions."""
import sqlite3
from pathlib import Path

from tgdl.accounts import AccountRegistry, DEFAULT_ACCOUNT
from tgdl.config import load_settings, read_env_values, update_env
from tgdl.history import DownloadHistory, PendingMedia


def test_second_account_uses_isolated_env_session_and_monitor_paths(tmp_path):
    registry = AccountRegistry(tmp_path)
    update_env({
        "api_id": "12345", "api_hash": "testhash", "phone": "+123",
        "download_dir": str(tmp_path / "download"),
        "proxy_host": "one.example", "proxy_port": "1080",
        "proxy_username": "alice", "proxy_password": "one-secret",
        "media_concurrency": "3", "media_retries": "2",
        "chunk_concurrency": "2", "media_speed_kib": "2048",
    }, registry.env_path(DEFAULT_ACCOUNT))
    original = registry.env_path(DEFAULT_ACCOUNT).read_bytes()
    assert len(registry.accounts()) == 1
    second = registry.add("账号 B")
    assert len(registry.accounts()) == 2
    assert registry.env_path(DEFAULT_ACCOUNT).read_bytes() == original
    assert registry.session_path(DEFAULT_ACCOUNT) == tmp_path / "accounts" / "default" / "tgdl_session"
    assert registry.session_path(second.id) != registry.session_path(DEFAULT_ACCOUNT)
    assert registry.monitor_path(second.id) != registry.monitor_path(DEFAULT_ACCOUNT)
    assert not registry.session_path(second.id).with_suffix(".session").exists()
    env = read_env_values(registry.env_path(second.id))
    assert env["proxy_host"] == ""
    assert env["media_concurrency"] == "1"
    update_env(dict(env, proxy_host="two.example", proxy_port="8888",
                    media_speed_kib="1024", phone="+456"), registry.env_path(second.id))
    settings = load_settings(env_path=registry.env_path(second.id),
                             session_path=registry.session_path(second.id))
    assert settings.phone == "+456"
    assert settings.proxy_host == "two.example"
    assert settings.proxy_port == 8888
    assert settings.media_concurrency == 1
    assert settings.media_speed_kib == 1024
    assert settings.session_path == registry.session_path(second.id)
    assert registry.env_path(DEFAULT_ACCOUNT).read_bytes() == original
    assert registry.accounts()[1].id == second.id


def test_account_path_cannot_traverse(tmp_path):
    registry = AccountRegistry(tmp_path)
    for bad in ("../secrets", "..", "a/b", "A B", "x\\y", ""):
        try:
            registry.env_path(bad)
        except ValueError:
            pass
        else:
            raise AssertionError("accepted invalid account ID")


def test_existing_history_survives_migration_with_independent_scan_state(tmp_path):
    path = tmp_path / "history.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE downloaded_media(
        peer_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
        kind TEXT NOT NULL, original_path TEXT NOT NULL, saved_at TEXT NOT NULL,
        PRIMARY KEY(peer_id,message_id))""")
    conn.execute("INSERT INTO downloaded_media VALUES(123,10,'video','old.mp4','2026-11-20')")
    conn.execute("""CREATE TABLE scan_state(
        peer_id INTEGER PRIMARY KEY, completed_id INTEGER DEFAULT 0,
        upper_id INTEGER DEFAULT 0, cursor_id INTEGER DEFAULT 0,
        direction TEXT DEFAULT 'newest', media_types TEXT DEFAULT '')""")
    conn.execute("INSERT INTO scan_state VALUES(123,9,18,12,'newest','video')")
    conn.commit()
    conn.close()
    with_old = DownloadHistory(path)
    assert with_old.contains(123, 10)
    assert with_old.scan_state(123) == (9, 18, 12, "newest")
    assert with_old.scan_state(123, account_id="secondary") == (0, 0, 0, "newest")
    with_old.save_scan_state(123, 0, 18, 6, "oldest", "photo", account_id="secondary")
    assert with_old.scan_state(123) == (9, 18, 12, "newest")
    assert with_old.scan_state(123, account_id="secondary") == (0, 18, 6, "oldest")
    with_old.close()
    again = DownloadHistory(path)
    assert again.contains(123, 10)
    assert again.scan_state(123, account_id="secondary") == (0, 18, 6, "oldest")
    again.close()


def test_claim_lease_prevents_duplicate_media_and_recovers_after_failure(tmp_path):
    path = tmp_path / "history.sqlite3"
    a = DownloadHistory(path)
    b = DownloadHistory(path)
    item = PendingMedia(123, 50, "@first-alias", "test", "video", "50")
    assert a.enqueue_media([item]) == 1
    assert [x.message_id for x in b.pending_for_peer(123, account_id="B")] == [50]
    assert a.try_claim("A", 123, 50, ttl=60, now=100)
    assert not b.try_claim("B", 123, 50, ttl=60, now=110)
    assert b.pending_for_peer(123, account_id="B", now=110) == []
    assert a.renew_claim("A", 123, 50, ttl=60, now=140)
    assert not b.try_claim("B", 123, 50, ttl=60, now=150)
    a.release_claim("A", 123, 50, cooldown=200, now=150)
    assert not a.try_claim("A", 123, 50, ttl=60, now=160)
    assert b.try_claim("B", 123, 50, ttl=60, now=160)
    b.mark_saved(123, 50, "video", Path("completed.mp4"))
    b.attribute_saved(123, 50, "B")
    assert a.pending_count() == 0
    assert not a.try_claim("A", 123, 50, ttl=60, now=500)
    assert b.active_claims() == []
    assert b._connection.execute("SELECT account_id FROM downloaded_media").fetchone()[0] == "B"
    a.close()
    b.close()


def test_expired_lease_is_reclaimed_after_restart(tmp_path):
    path = tmp_path / "history.sqlite3"
    first = DownloadHistory(path)
    first.enqueue_media([PendingMedia(17, 8, "@chat", "chat", "photo", "8")])
    assert first.try_claim("A", 17, 8, ttl=10, now=100)
    first.close()
    after_restart = DownloadHistory(path)
    assert after_restart.try_claim("B", 17, 8, ttl=10, now=110)
    assert after_restart.active_claims()[0][2] == "B"
    after_restart.close()


def test_account_monitor_media_type_selection_excludes_other_types(tmp_path):
    hist = DownloadHistory(tmp_path / "history.sqlite3")
    hist.enqueue_media([
        PendingMedia(123, 1, "@channel", "channel", "video", "1"),
        PendingMedia(123, 2, "@channel", "channel", "photo", "2"),
        PendingMedia(123, 3, "@channel", "channel", "animation", "3"),
        PendingMedia(123, 4, "@channel", "channel", "", "4"),
    ])
    assert [x.message_id for x in hist.pending_for_peer(
        123, account_id="B", kinds={"photo"}
    )] == [2, 4]
    assert [x.message_id for x in hist.pending_for_target(
        "@channel", account_id="B", kinds={"video"}
    )] == [1, 4]
    assert hist.pending_for_peer(123, account_id="B", kinds=set()) == []
    hist.close()
