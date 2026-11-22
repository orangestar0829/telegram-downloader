from datetime import date, timedelta
import os
import stat

from tgdl.log_store import DailyLogStore, redact


def test_log_store_persists_and_masks_sensitive_values(tmp_path):
    store = DailyLogStore(tmp_path)
    store.write("[账号:abc] SOCKS5_PASSWORD=secret TELEGRAM_API_HASH=hash123\n")
    store.write("[账号:default] 下载完成\n")
    store.close()
    logs = list(tmp_path.glob("runtime-*.log"))
    assert len(logs) == 1
    data = logs[0].read_text(encoding="utf-8")
    assert "secret" not in data and "hash123" not in data
    assert "[账号:abc]" in data and "下载完成" in data
    assert "REDACTED" in data


def test_log_store_removes_only_expired_daily_logs(tmp_path):
    old = tmp_path / f"runtime-{date.today() - timedelta(days=9):%Y-%m-%d}.log"
    old.write_text("old", encoding="utf-8")
    recent = tmp_path / f"runtime-{date.today() - timedelta(days=2):%Y-%m-%d}.log"
    recent.write_text("recent", encoding="utf-8")
    unrelated = tmp_path / "custom.log"
    unrelated.write_text("keep", encoding="utf-8")
    store = DailyLogStore(tmp_path, retain_days=7)
    store.write("hello\n")
    store.close()
    assert not old.exists()
    assert recent.exists()
    assert unrelated.exists()


def test_redact_does_not_hide_unrelated_values():
    assert "password=hunter2" not in redact("password=hunter2")
    assert "priority=10" in redact("priority=10")


def test_log_store_masks_private_urls_and_multitoken_credentials(tmp_path):
    store = DailyLogStore(tmp_path)
    store.write("频道 t.me/+privateinvite\n")
    store.write("直链 https://example.test/private/path?signature=secret\n")
    store.write("tg://login?token=qrsecret\n")
    store.write("SOCKS5_PASSWORD='words with spaces'\n")
    store.close()
    log, = tmp_path.glob("runtime-*.log")
    saved = log.read_text(encoding="utf-8")
    assert not any(secret in saved for secret in
                   ("privateinvite", "/private/path", "signature", "qrsecret", "words with spaces"))
    assert "[REDACTED LINK]" in saved
    if os.name != "nt":
        assert stat.S_IMODE(log.stat().st_mode) & 0o077 == 0


def test_log_store_failure_stops_writes_and_close_does_not_require_space(tmp_path, monkeypatch):
    class FullDisk:
        def write(self, _):
            raise OSError("disk full")

        def close(self):
            pass

    monkeypatch.setattr("tgdl.log_store.open", lambda *a, **kw: FullDisk(), raising=False)
    store = DailyLogStore(tmp_path)
    store.write("before failure")
    assert store.failed.wait(timeout=2)
    store._thread.join(timeout=2)
    for _ in range(store.queue.maxsize):
        store.queue.put_nowait("pending")
    store.write("after failure")
    store.close()
    assert not store._thread.is_alive()
