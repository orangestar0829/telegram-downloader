# 2026-11-22 — Default monitored channel config moved under accounts/

## Context
The multi-account preview already stored each secondary account's channel
monitors in `data/accounts/<id>/monitored_channels.json`, while the original
account A still used `data/monitored_channels.json`. This confused backups
and the multi-account GUI; users requested the exact same monitor structure.

## Decision
Make `data/accounts/default/monitored_channels.json` the authoritative
monitor file for the original account. On startup, copy the legacy root file
**once** using an adjacent temporary file, parse the JSON array, then
atomically publish. Preserve the legacy file for rollback. Never overwrite
the new file once present, even if the old root file changes. The global
SQLite downloaded media, pending tasks, and scan_state remain untouched;
default account .env also remains at the root.

Both `tgdl.monitor.monitor_path()` (main window/CLI) and
`AccountRegistry.monitor_path("default")` (account manager) now resolve to
the same new location. Secondary accounts keep their existing locations.

## Rejected alternatives
- Destructive move/delete of legacy monitors: loses rollback path.
- Reading root monitors on every launch after migration: would override edits
  made by the new GUI and cause inconsistent channel settings between views.
- Migrating scan_state into account-local SQLite: unnecessary data rewrite;
  the existing account already retains its original global scan cursor.

## Validation
A disposable-directory dry-run preceded implementation (five scenarios).
Automated tests cover preserving all old monitor fields, idempotence,
no-legacy and malformed-JSON handling, rollback, untouched secondary account
configs and unchanged global download history. The real Windows preview and
Telegram account behavior still require hands-on validation before PR merge.
