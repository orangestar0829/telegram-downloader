"""Presentation-only routing helpers for the main account selector.

Actual Telegram clients and monitor workers always retain their independent
stable account IDs. No network requests are made by these helpers.
"""
from __future__ import annotations

from typing import Iterable

from tgdl.account_identity import account_identity


def account_choices(accounts: Iterable) -> dict[str, str]:
    """Human-readable, unique selector labels with stable IDs as values."""
    choices: dict[str, str] = {}
    for account in accounts:
        identity = account_identity.label(account.id)
        remark = account.name
        label = f"{identity}（{remark}）" if remark and remark != identity else identity
        base = label
        suffix = 2
        while label in choices:
            label = f"{base} · 账号{suffix}"
            suffix += 1
        choices[label] = account.id
    return choices


def dialog_rows(items: Iterable) -> list[tuple[str, str, str, str]]:
    """Normalize Telegram DialogInfo and legacy cached tuples into UI rows."""
    rows = []
    for item in items:
        if hasattr(item, "entity_id"):
            rows.append((
                str(item.entity_id),
                str(item.kind),
                str(item.title),
                str(getattr(item, "username", None) or ""),
            ))
        else:
            rows.append((
                str(item[0]), str(item[1]), str(item[2]),
                str(item[3]) if len(item) > 3 and item[3] else "",
            ))
    return rows


def progress_owner(key: str, secondary_ids: Iterable[str]) -> str:
    """Only secondary transfers have an account ID prefixed to progress keys."""
    prefix = str(key).split(":", 1)[0]
    return prefix if prefix in secondary_ids else "default"
