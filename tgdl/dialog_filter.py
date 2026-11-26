"""Local, case-insensitive filtering of already-loaded Telegram group/channel names.

Does not perform a Telegram message search or fetch network data.
"""
from __future__ import annotations

from typing import Iterable


def filter_dialogs(rows: Iterable, keyword: str) -> list:
    needle = keyword.strip().casefold()
    if not needle:
        return list(rows)

    results = []
    for row in rows:
        # DialogInfo uses named fields; main UI keeps (id, kind, title, username).
        if hasattr(row, "title"):
            searchable = (row.title, getattr(row, "username", "") or "")
        else:
            searchable = (row[2], row[3] if len(row) > 3 else "")
        if any(needle in str(value or "").casefold() for value in searchable):
            results.append(row)
    return results
