"""Presentation-only routing helpers for the main account selector.

Actual Telegram clients and monitor workers always retain their independent
stable account IDs. No network requests are made by these helpers.
"""
from __future__ import annotations

from typing import Iterable
from urllib.parse import urlparse

from telethon import utils
from telethon.tl.types import PeerChannel

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


def joined_channel_peer(dialogs: Iterable, target: str) -> int:
    """Resolve a monitor editor selection from this account's joined dialogs only."""
    raw = str(target).strip()
    alias = raw.lstrip("@").casefold()
    try:
        parsed = urlparse(raw if "://" in raw else "//" + raw)
        if parsed.hostname in {"t.me", "telegram.me", "telegram.dog"}:
            alias = parsed.path.strip("/").casefold()
    except ValueError:
        pass
    for marked_id, kind, _title, username in dialog_rows(dialogs):
        if kind not in {"channel", "supergroup"}:
            continue
        try:
            peer, entity_type = utils.resolve_id(int(marked_id))
        except ValueError:
            continue
        if entity_type is not PeerChannel or peer <= 0:
            continue
        if raw in (marked_id, str(peer)) or (username and alias == username.casefold()):
            return peer
    raise ValueError("实时监听仅支持本账号已加入的频道或超级群。请刷新频道列表后选择该频道。")


def monitor_enabled_label(row: dict, account_id: str, slots) -> str:
    if not row.get("enabled", True):
        return "暂停"
    if slots is not None and row.get("mode") == "realtime" and row.get("peer_id"):
        owner = slots.owner(row["peer_id"])
        if owner is not None and owner != account_id:
            return "共用名额"
    return "启用"


def progress_owner(key: str, secondary_ids: Iterable[str]) -> str:
    """Only secondary transfers have an account ID prefixed to progress keys."""
    prefix = str(key).split(":", 1)[0]
    return prefix if prefix in secondary_ids else "default"
