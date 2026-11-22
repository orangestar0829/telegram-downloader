"""Multi-account database migration, ownership and session separation regressions."""
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
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
    token_a = a.try_claim("A", 123, 50, ttl=60, now=100)
    assert token_a
    assert not b.try_claim("B", 123, 50, ttl=60, now=110)
    assert b.pending_for_peer(123, account_id="B", now=110) == []
    assert a.renew_claim("A", 123, 50, token=token_a, ttl=60, now=140)
    assert not b.try_claim("B", 123, 50, ttl=60, now=150)
    assert a.release_claim("A", 123, 50, token=token_a, cooldown=200, now=150)
    assert not a.try_claim("A", 123, 50, ttl=60, now=160)
    token_b = b.try_claim("B", 123, 50, ttl=60, now=160)
    assert token_b
    assert b.mark_claim_saved("B", 123, 50, "video", Path("completed.mp4"), token=token_b, now=161)
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


@pytest.mark.parametrize("replacement_account", ["A", "B"])
def test_stale_attempt_cannot_publish_renew_release_or_penalize_replacement(tmp_path, replacement_account):
    path = tmp_path / "history.sqlite3"
    old = DownloadHistory(path)
    new = DownloadHistory(path)
    old.enqueue_media([PendingMedia(17, 8, "@chat", "chat", "photo", "8")])
    first = old.try_claim("A", 17, 8, ttl=10, now=100)
    assert first
    assert not old.owns_claim("A", 17, 8, token=first, now=110)
    assert not old.renew_claim("A", 17, 8, token=first, now=110)
    other = new.try_claim(replacement_account, 17, 8, ttl=10, now=110)
    assert other and first != other
    publications = []

    def stale_publish():
        publications.append("STALE")
        return Path("stale.jpg"), True

    assert not old.renew_claim("A", 17, 8, token=first, now=111)
    assert not old.release_claim("A", 17, 8, token=first, cooldown=600, now=111)
    assert old.publish_claimed_media(17, 8, "A", first, "photo", stale_publish, now=111) is None
    assert publications == []
    assert old.pending_count() == 1
    assert not old.contains(17, 8)
    assert new.active_claims() == [(17, 8, replacement_account, 120.0)]
    assert new.owns_claim(replacement_account, 17, 8, token=other, now=111)

    assert new.release_claim(replacement_account, 17, 8, token=other, now=112)
    fresh = old.try_claim("A", 17, 8, now=113)
    assert fresh  # stale cooldown did not pollute even an attempt on A
    assert old.mark_claim_saved("A", 17, 8, "photo", Path("winner.jpg"), token=fresh, now=114)
    assert new.pending_count() == 0
    assert new.active_claims() == []
    assert new._connection.execute(
        "SELECT original_path,account_id FROM downloaded_media"
    ).fetchone() == ("winner.jpg", "A")
    old.close()
    new.close()


def test_manual_paths_never_mutate_a_live_monitor_task(tmp_path):
    path = tmp_path / "history.sqlite3"
    manual = DownloadHistory(path)
    monitor = DownloadHistory(path)
    manual.mark_failure(17, 8, now=99)
    item = PendingMedia(17, 8, "@channel", "channel", "photo", "8")
    manual.enqueue_media([item])
    prior = monitor.try_claim("other", 17, 8, now=100)
    assert prior
    assert monitor.release_claim("other", 17, 8, token=prior, cooldown=30, now=101)
    live = monitor.try_claim("active", 17, 8, ttl=10, now=102)
    assert live
    assert not manual.mark_saved(17, 8, "photo", Path("manual.jpg"), now=103)
    manual.clear_failure(17, 8, now=103)
    manual.remove_pending(17, 8, now=103)
    manual.mark_failure(17, 8, now=104)
    assert not manual.contains(17, 8)
    assert manual.pending_count() == 1
    assert manual.failures(17) == [8]
    assert manual._connection.execute(
        "SELECT account_id,retry_after FROM account_attempt_backoff"
    ).fetchall() == [("other", 131.0)]
    assert monitor.owns_claim("active", 17, 8, token=live, now=104)

    chosen = Path("actual-name.jpg")
    assert monitor.publish_claimed_media(
        17, 8, "active", live, "photo", lambda: (chosen, True), now=105,
    ) == (chosen, True)
    assert manual.pending_count() == 0
    assert not manual.failures(17)
    assert manual.active_claims() == []
    assert manual._connection.execute(
        "SELECT original_path,account_id FROM downloaded_media"
    ).fetchone() == (str(chosen), "active")

    manual.enqueue_media([PendingMedia(17, 9, "@channel", "channel", "photo", "9")])
    expiring = monitor.try_claim("active", 17, 9, ttl=5, now=110)
    assert expiring
    assert manual.mark_saved(17, 9, "photo", Path("manual.jpg"), now=115)
    assert not monitor.mark_claim_saved(
        "active", 17, 9, "photo", Path("late.jpg"), token=expiring, now=115,
    )
    assert manual._connection.execute(
        "SELECT original_path,account_id FROM downloaded_media WHERE message_id=9"
    ).fetchone() == ("manual.jpg", None)
    manual.close()
    monitor.close()


def test_publication_failure_leaves_queue_and_lease_intact(tmp_path):
    history = DownloadHistory(tmp_path / "history.sqlite3")
    history.enqueue_media([PendingMedia(17, 8, "@channel", "channel", "photo", "8")])
    live = history.try_claim("A", 17, 8, ttl=10, now=100)
    assert live

    def denied_publish():
        raise OSError("cannot publish test file")

    with pytest.raises(OSError, match="cannot publish test file"):
        history.publish_claimed_media(17, 8, "A", live, "photo", denied_publish, now=102)
    assert history.publish_claimed_media(17, 8, "A", live, "photo", lambda: None, now=103) is None
    assert history.owns_claim("A", 17, 8, token=live, now=104)
    assert history.pending_count() == 1
    assert not history.contains(17, 8)
    history.close()


def test_fence_uses_time_after_waiting_for_competing_sqlite_writer(tmp_path, monkeypatch):
    path = tmp_path / "history.sqlite3"
    locker = DownloadHistory(path)
    finisher = DownloadHistory(path)
    locker.enqueue_media([PendingMedia(17, 8, "@channel", "channel", "photo", "8")])
    token = locker.try_claim("A", 17, 8, ttl=10, now=100)
    assert token
    fake_clock = SimpleNamespace(now=108)
    monkeypatch.setattr("tgdl.history.time", SimpleNamespace(time=lambda: fake_clock.now))
    began = threading.Event()

    def trace(sql):
        if sql == "BEGIN IMMEDIATE":
            began.set()

    finisher._connection.set_trace_callback(trace)
    published = []
    locker._connection.execute("BEGIN IMMEDIATE")
    with ThreadPoolExecutor(max_workers=1) as pool:
        try:
            finish = pool.submit(
                finisher.publish_claimed_media, 17, 8, "A", token, "photo",
                lambda: (published.append("posted") or Path("late.jpg"), True),
            )
            assert began.wait(5)
            fake_clock.now = 110
        finally:
            locker._connection.commit()
        assert finish.result(timeout=5) is None
    assert published == []
    assert locker.try_claim("B", 17, 8)
    locker.close()
    finisher.close()


def test_legacy_migration_preserves_existing_tokenless_lease_and_serializes_startup(tmp_path):
    path = tmp_path / "history.sqlite3"
    connection = sqlite3.connect(path)
    with connection:
        connection.executescript("""
            CREATE TABLE download_claims (
                peer_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                account_id TEXT NOT NULL, expires_at REAL NOT NULL,
                PRIMARY KEY(peer_id, message_id)
            );
            INSERT INTO download_claims VALUES (17, 8, 'legacy-account', 110);
            CREATE TABLE downloaded_media (
                peer_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
                kind TEXT NOT NULL, original_path TEXT NOT NULL, saved_at TEXT NOT NULL,
                PRIMARY KEY(peer_id, message_id)
            );
            INSERT INTO downloaded_media VALUES (17, 3, 'photo', 'old.jpg', 'time');
        """)
    connection.close()
    start = threading.Barrier(2)

    def load():
        start.wait(timeout=5)
        return DownloadHistory(path)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.submit(load), pool.submit(load)
        one, two = first.result(timeout=10), second.result(timeout=10)
    assert one.contains(17, 3)
    one.enqueue_media([PendingMedia(17, 8, "@channel", "channel", "photo", "8")])
    assert one.active_claims() == [(17, 8, "legacy-account", 110.0)]
    assert not one.try_claim("new-account", 17, 8, now=109)
    assert not one.mark_saved(17, 8, "photo", Path("manual.jpg"), now=109)
    assert two.try_claim("new-account", 17, 8, now=110)
    one.close()
    two.close()


def test_default_account_remark_survives_restart_without_overwriting_session_or_registry(tmp_path):
    registry = AccountRegistry(tmp_path)
    extra = registry.add("extra")
    assert registry.accounts()[0].name == ""
    paths = [registry.session_path(account_id).with_suffix(".session")
             for account_id in ("default", extra.id)]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"mock-unchanged-session")
    registry_before = registry.registry_file.read_bytes()
    registry.rename("default", "  我的主账号  ")

    reopened = AccountRegistry(tmp_path)
    assert [(row.id, row.name) for row in reopened.accounts()] == [
        ("default", "我的主账号"), (extra.id, "extra"),
    ]
    assert registry.registry_file.read_bytes() == registry_before
    assert all(path.read_bytes() == b"mock-unchanged-session" for path in paths)
    reopened.rename("default", "  ")
    assert AccountRegistry(tmp_path).accounts()[0].name == ""


def test_invalid_default_account_remark_does_not_block_accounts(tmp_path):
    registry = AccountRegistry(tmp_path)
    extra = registry.add("extra")
    remark = tmp_path / "accounts" / "default" / "remark.json"
    remark.parent.mkdir(parents=True, exist_ok=True)
    remark.write_text("{invalid", encoding="utf-8")

    assert [(row.id, row.name) for row in registry.accounts()] == [
        ("default", ""), (extra.id, "extra"),
    ]
    assert remark.read_text(encoding="utf-8") == "{invalid"


def test_secondary_remark_is_optional_on_disk_and_can_be_replaced(tmp_path):
    import json
    from tgdl.accounts import AccountRegistry

    registry = AccountRegistry(tmp_path)
    account = registry.add()
    assert registry.accounts()[-1] == account
    assert account.name == ""
    raw = json.loads(registry.registry_file.read_text(encoding="utf-8"))
    assert raw[0]["name"]  # An earlier preview still sees a valid account row.
    registry.rename(account.id, "  Secondary  ")
    assert registry.accounts()[-1].name == "Secondary"
    registry.rename(account.id, "")
    assert AccountRegistry(tmp_path).accounts()[-1] == account
