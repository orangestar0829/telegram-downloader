# 2026-11-22 — Multi-account queue and persistence decisions

## Context

Existing PR #3 download worker is based on a single Telethon client and
`downloaded_media` deduplication only; it cannot safely run with two clients,
because a shared queued message could start two downloads at the same time.

## Decisions

- One Tk GUI with independently running Telethon clients, each with its own
  Session and SOCKS5/rate-limit settings. No separate full GUI processes:
  independently launched processes would race file paths and DB state.
- Preserve the original account's data paths and `scan_state` table. New
  accounts use `data/accounts/<id>` and `account_scan_state`.
- Keep `downloaded_media` and `pending_media` globally keyed by
  (canonical Telegram peer ID, message ID). The account that happens to scan
  first supplies the pending record; others match by peer ID, **not the
  account-specific channel username or URL alias**.
- SQLite `download_claims` gives exclusive account-owned leases, renewed
  every 30 seconds, with expiry recovery after crash. Failures preserve the
  global queue and apply per-account backoff; a failed account cannot remove
  another account's work.
- Account channel monitor switch governs BOTH scanning and eligibility to
  download. Rejected a download-only mode because the account may not have
  access to the channel. Resolve media with the downloader account's client,
  not the scanner's borrowed Telegram file reference.
- Keep old manual bulk path unchanged initially. The multi-account guarantee
  applies to monitored work; warn against combining old manual bulk downloads
  with secondary monitors for the same channel.
- Persist all account-tagged diagnostic output with seven-day rotation and a
  bounded GUI text history. User is testing via PR-specific prerelease assets;
  no stable `v*` tag is created by this feature.

## Rejected alternatives

- Duplicating program instances: separate processes can re-download and
  overwrite the same media without a shared claim mechanism.
- Relocating account A's SQLite/session files: error-prone for portable
  installs and risks invalidating saved history.
- Sharing a Telethon Session between accounts: unauthorized/malformed session
  ownership and concurrent SQLite use.
- A single global download limiter: users explicitly requested each account
  retain its own speed cap, file slots and retries.
- One global scan cursor per channel: hides messages if the accounts have
  different scan directions, media filters or permissions.

## Risks and follow-up

No real Telegram login/media test has been run against users' sessions.
The preview needs two-account real-world validation, permission-loss checks,
and Windows installer/portable smoke verification before the PR is marked ready.
Backup instructions and exact SQLite schema are in
`docs/sqlite-multi-account-migration.md`.
