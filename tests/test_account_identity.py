from types import SimpleNamespace
from queue import Queue

from tgdl.account_identity import AccountIdentity, account_identity, telegram_display_name
from tgdl.account_context import current_account_id
from tgdl.gui import _QueueWriter
from tgdl.log_format import TimestampedLines


def test_telegram_display_name_uses_profile_not_local_alias():
    profile = SimpleNamespace(
        first_name="Alice", last_name="Wang", username="alice_w",
        id=123456, phone="111111",
    )
    assert telegram_display_name(profile) == "Alice Wang (@alice_w)"
    identity = AccountIdentity()
    identity.register("stable-secret-id", "账号 B")
    assert identity.label("stable-secret-id") == "账号 B"
    assert identity.update("stable-secret-id", profile) == "Alice Wang (@alice_w)"
    assert identity.label("stable-secret-id") == "Alice Wang (@alice_w)"
    assert "111111" not in identity.label("stable-secret-id")
    assert "stable-secret-id" not in identity.label("stable-secret-id")


def test_profile_name_cannot_inject_new_log_header():
    profile = SimpleNamespace(
        first_name="Alice\n[账号:forged]", last_name=None, username=None
    )
    label = telegram_display_name(profile)
    assert "\n" not in label
    assert "[" not in label and "]" not in label


def test_all_prints_are_marked_even_for_original_account():
    sink = Queue()
    account_identity.register("acc-identity-test", "B")
    account_identity.update("acc-identity-test",
                            SimpleNamespace(first_name="Bob", last_name="Li", username="bob"))
    writer = _QueueWriter(sink, TimestampedLines())
    token = current_account_id.set("acc-identity-test")
    try:
        writer.write("Connected\n")
    finally:
        current_account_id.reset(token)
    assert "[账号:Bob Li (@bob)]" in sink.get_nowait().text
    writer.write("Main log\n")
    assert "[账号:原账号（未登录）]" in sink.get_nowait().text


def test_explicit_runtime_and_download_logs_keep_tg_profile_name():
    from tgdl.gui import _App
    app = _App.__new__(_App)
    app.log_queue = Queue()
    app.download_queue = Queue()
    app._timestamp_formatter = TimestampedLines()
    app._download_timestamp_formatter = TimestampedLines()
    app._log_store = None
    app._log_account("acc-identity-test", "[频道扫描] 开始\n")
    app._log_download_account("acc-identity-test", "已保存媒体文件")
    runtime = app.log_queue.get_nowait()
    download = app.download_queue.get_nowait()
    assert "[账号:Bob Li (@bob)]" in runtime.text
    assert "[账号:Bob Li (@bob)]" in download.text
    assert "acc-identity-test" not in runtime.text + download.text
    assert runtime.owner_id == download.owner_id == "acc-identity-test"
