import logging

from tgdl.telethon_diagnostics import diagnostic_message, install_telethon_diagnostics


def record(name, message, args=()):
    return logging.LogRecord(name, logging.INFO, "", 1, message, args, None)


def test_flood_wait_and_connection_events_have_safe_summaries():
    flood = record(
        "telethon.client.users",
        "Sleeping%s for %ds (%s) on %s flood wait",
        ("", 12, "0:00:12", "GetFileRequest"),
    )
    assert "FloodWait 12s" in diagnostic_message(flood)
    assert "GetFileRequest" in diagnostic_message(flood)
    reconnect = record("telethon.network.mtprotosender", "Closing current connection to begin reconnect...")
    assert "自动重连开始" in diagnostic_message(reconnect)
    closed = record("telethon.network.connection.connection", "Server closed the connection: %s", ("EOF",))
    assert "连接被关闭" in diagnostic_message(closed)
    assert diagnostic_message(record("telethon.client.users", "Secret token: abc")) is None


def test_log_hook_captures_info_and_restores_logger_level():
    logger = logging.getLogger("telethon.client.users")
    old = logger.level
    messages = []
    uninstall = install_telethon_diagnostics(messages.append)
    try:
        logger.info("Sleeping%s for %ds (%s) on %s flood wait", "", 5, "0:00:05", "GetFileRequest")
        assert any("FloodWait 5s" in msg for msg in messages)
    finally:
        uninstall()
    assert logger.level == old
