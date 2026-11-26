# 2026-11-22 — Uniform account sessions, identity logging and embedded monitor UI

## Observed UX / storage problem

PR #4 initially left the original account's Telethon Session in
`data/tgdl_session.session` while additional accounts used
`data/accounts/<id>/tgdl_session.session`. Users requested every Session
under `accounts/` to simplify backup/maintenance. Secondary-account
monitoring also differed from the main monitor UI and opened a separate
"view joined channels" popup, reloading dialogs on every use. Diagnostic
logs displayed opaque local account IDs such as `4c81943bf07740ed`.

## Decisions

- Original account's Telethon SQLite session is copied exactly once from
  `data/tgdl_session.session` (including prior legacy imports) into
  `data/accounts/default/tgdl_session.session` with the SQLite online
  backup API. Verify `PRAGMA quick_check`, close the SQLite file handles
  before atomic rename on Windows, and **retain** the old root session for
  rollback; future starts never overwrite the new Session.
- Keep original account's `.env`, `monitored_channels.json` and global
  `tgdl_download_history.sqlite3` at the old location to avoid changing
  unrelated persistent state.
- Log lines show a readable Telegram profile name (first/last names with
  optional `@username`), derived from account-specific `get_me()`.
  Only in-memory routing/SQLite use stable internal account identifiers.
  Before authorization, show a human-readable provisional label. Keep alias
  history so GUI log filters can still match earlier text after login.
- Secondary-account settings use the same headings, order, localized media
  type checkboxes, scan direction selector, editable batch/priority settings,
  and next-scan column as the original monitor view.
- Embed the per-account joined-channel picker in the AccountWindow above
  the monitor list. Populate once per authorized account, cache in the GUI
  process, and only re-fetch on the user's explicit Refresh; dialog keyword
  filtering remains local, never message content search.
- Keep incremental monitored channel file compatibility; there is no
  change to the history success ledger/deduplication keys.

## Alternatives rejected

- Move/delete original `tgdl_session.session` immediately: risks destroying
  an authorized session on a failed copy and breaks rollback.
- Copy an SQLite session with `shutil.copy2` while a WAL may be present:
  may omit uncheckpointed session metadata. Use SQLite backup instead.
- Use local account nicknames or internal IDs in log text after login:
  inconsistent with user's expected real Telegram identity.
- Re-open the Telegram dialogs picker every time: slow and unnecessary.
- Rebuild monitor Treeview each second: can reset unsaved form edits and
  selections. Update changed rows in place.

## Validation

Offline regression tests cover old Session preservation/new path, idempotent
migration and rollback, account name formatting, and existing downloads. Live
Windows UI interactions and a real Telegram account/session migration still
need user validation before PR #4 becomes ready for merge.
