# Multi-account, multi-proxy downloader — design baseline

Status: PR #4 preview implementation in progress; real Telegram integration testing pending. Base: main after PR #3.

## Agreed requirements

- One GUI and one process hosting multiple independently authenticated Telegram clients, each with its own Telethon session file.
- Each account independently configures its proxy, download rate limit, media-file concurrency, retries, monitoring channels, scan interval, numeric priority (default 0; larger first), media types, batch limit and scan cursor.
- Monitoring is an account/channel switch. An account that has not enabled a channel must **never** be assigned that channel's download work; do not introduce an unmonitored download-only mode.
- If two accounts monitor the same accessible channel, they download **different** media tasks concurrently. Avoid duplicate transfers and duplicate final paths by using shared, atomic task claims and globally shared completed history.
- Switching the selected account in the GUI only changes the settings and monitor view, not running clients/downloads. Provide an all-accounts runtime and download overview.
- Keep existing single-account settings, authentication state, media records and current scheduling behavior working during migration. Do not silently clear/reset cursors or redownload historical media.
- Persist runtime diagnostics to rotating daily files (initial proposal: seven days), including account identity, but not passwords, secrets, or session material. GUI filters all accounts / selected account; download records remain a separate view.
- Distinguish persistent completed-download ledger from ephemeral GUI text. The existing SQLite history file is the migration source of truth.

## Architecture outline

- AccountRegistry: owns per-account runtime/session/config; always stop and close one account cleanly.
- AccountScanner: per-account per-channel scan cursor and per-account channel preferences.
- SharedTaskCoordinator: globally unique media identity (canonical Telegram peer/channel ID plus message ID, with media-type/part qualification if required), transactional pending -> leased -> success/retry transition, lease expiry/recovery after crash.
- AccountDownloader: processes only tasks matching that account's enabled monitored channel; resolves message/media via **its own** Telethon client and access hash rather than reusing another account's entities.
- DownloadHistory: migrate existing downloaded_media, scan_state, scan_failures and pending_media without losing records; add account keys where needed for scan state/config, not to the global success deduplication key.
- Logging: central thread-safe structured event sink (account_id, channel, type, timestamp, message, severity), bounded GUI view, rotating on-disk log, redact credentials.

## Core invariants / concurrency

1. At most one active lease for the same logical download across all accounts; atomic transaction required.
2. Every claim rechecks account-to-channel monitor eligibility and channel access before attempting transfer. Access failure disables or pauses only that account/channel, never global work.
3. Success is only committed after an actual validated final file save; old success rows remain authoritative even when local media was cleaned up.
4. No cross-account reuse of Telegram access_hash or media file references. Account B re-fetches messages with B's client.
5. Each account's speed limiter and download slot budget is independent; no new global concurrency budget as part of the first pass.
6. Per-account scan ordering preserves fairness and numeric priority, avoiding monopolization by a large batch.
7. Migration must preserve the existing account as default account, including portable data/ .env / tgdl_session.session / monitored_channels.json and SQLite history, with backup and repeatable idempotent schema migration.

## Implementation plan / PR checkpoints

1. Inventory all current settings, runtime entrypoints, UI bindings, sessions and on-disk schemas. Add migration/compatibility tests before modifying storage.
2. Introduce account-scoped config/session and schema migration. Verify the original account still works as before.
3. Extract per-account scanner/worker state and shared atomic download coordinator; test simultaneous scans, same-channel claims, failures/restart, permission-denied and fairness.
4. Add GUI account management/switching, all-account view, channel settings and independent proxy/rate limit display.
5. Add persistent rotating diagnostics, bounded log rendering and account-specific filtering. Capture completion/account metadata without changing successful-download deduplication.
6. Full regression suite, Windows package CI, two-account real-world preview validation, then request review.

PR #4 now includes account-specific settings, SQLite claims and per-account scan cursors, a secondary-account monitor runtime, an account-management window and rotating logs. Offline tests validate concurrency and migration. Live Telegram, GUI smoke and Windows packaging validation remain required before review/merge.
