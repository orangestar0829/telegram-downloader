"""Read-only Telethon diagnostic logging without modifying retry policies."""
from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable

_LOGGERS = (
    "telethon.client.users",
    "telethon.network.mtprotosender",
    "telethon.network.connection.connection",
)


def diagnostic_message(record: logging.LogRecord) -> str | None:
    message = record.getMessage()
    if record.name.endswith("client.users"):
        if message.startswith("Sleeping") and "flood wait" in message:
            matched = re.search(r"for (\d+)s .* on (\w+) flood wait", message)
            if matched:
                return (
                    f"[网络诊断] Telethon 自动等待 {matched.group(1)}s，"
                    f"方法={matched.group(2)}；此处日志不区分普通/Premium，"
                    "具体类型请看服务端原始响应"
                )
        # GetFileRequest RPC retry errors are emitted from the original
        # server response future with their concrete exception class.
        # Telethon's generic log cannot identify the request safely.
        if message.startswith("Telegram is having internal issues"):
            return None
    elif record.name.endswith("mtprotosender"):
        if message.startswith("Closing current connection to begin reconnect"):
            return "[网络诊断] MTProto 自动重连开始"
        if message.startswith("Failed reconnection attempt"):
            return "[网络诊断] MTProto 自动重连失败，正在重试"
        if message.startswith("Automatic reconnection failed"):
            return "[网络诊断] MTProto 自动重连最终失败"
        if message.startswith("Connection to ") and message.endswith(" complete!"):
            return "[网络诊断] MTProto 连接建立完成"
    elif record.name.endswith("connection.connection"):
        if message.startswith("Server closed the connection"):
            return "[网络诊断] Telegram TCP 连接被关闭，等待恢复"
    return None


class TelethonDiagnostics(logging.Handler):
    def __init__(self, output: Callable[[str], None]):
        super().__init__(level=logging.INFO)
        self.output = output
        self.last_emitted: dict[str, float] = {}

    def emit(self, record: logging.LogRecord) -> None:
        line = diagnostic_message(record)
        if line:
            now = time.monotonic()
            if now - self.last_emitted.get(line, -float("inf")) < 5:
                return
            self.last_emitted[line] = now
            self.output(line + "\n")


def install_telethon_diagnostics(output: Callable[[str], None]):
    """Return an uninstaller. Only selected Telethon loggers change level."""
    handler = TelethonDiagnostics(output)
    originals = []
    for name in _LOGGERS:
        logger = logging.getLogger(name)
        originals.append((logger, logger.level))
        if logger.getEffectiveLevel() > logging.INFO:
            logger.setLevel(logging.INFO)
        logger.addHandler(handler)

    def uninstall():
        for logger, previous in originals:
            logger.removeHandler(handler)
            logger.setLevel(previous)
        handler.close()

    return uninstall
