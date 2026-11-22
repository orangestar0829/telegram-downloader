"""Thread-safe mapping of stable internal account IDs to Telegram profile names.

Never log phone numbers, login tokens or numeric internal account IDs. The
in-memory map is populated from get_me() after a successful login.
"""
from __future__ import annotations

import re
import threading


def telegram_display_name(user: object) -> str:
    parts = " ".join(str(value).strip() for value in (
        getattr(user, "first_name", None), getattr(user, "last_name", None)
    ) if value and str(value).strip())
    username = str(getattr(user, "username", None) or "").strip().lstrip("@")
    display = f"{parts} (@{username})" if parts and username else parts or (f"@{username}" if username else "Telegram 账号")
    # Prevent profile names from forging log lines or misleading account tags.
    return re.sub(r"[\r\n\[\]]", " ", display).strip()[:100]


class AccountIdentity:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._labels: dict[str, str] = {"default": "原账号（未登录）"}
        self._authenticated: set[str] = set()

    def register(self, account_id: str, fallback: str) -> None:
        with self._lock:
            if account_id not in self._labels:
                safe = re.sub(r"[\r\n\[\]]", " ", str(fallback)).strip()[:100]
                self._labels[account_id] = safe or "未登录账号"

    def rename_fallback(self, account_id: str, name: str) -> None:
        """Use a new local remark only before the Telegram identity is known."""
        with self._lock:
            if account_id not in self._authenticated:
                safe = re.sub(r"[\r\n\[\]]", " ", str(name)).strip()[:100]
                self._labels[account_id] = safe or "未登录账号"

    def update(self, account_id: str, user: object) -> str:
        display = telegram_display_name(user)
        with self._lock:
            self._labels[account_id] = display
            self._authenticated.add(account_id)
        return display

    def label(self, account_id: str) -> str:
        with self._lock:
            return self._labels.get(account_id, "未登录账号")


account_identity = AccountIdentity()
