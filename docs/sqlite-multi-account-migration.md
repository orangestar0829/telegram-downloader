# PR #4: SQLite migration and rollback

The application automatically performs an **additive, idempotent** schema
migration when it first opens the download history. You do not need to run SQL.

## Safety / backup

1. Exit the old TelegramDownloader; do not use two versions with the same database at once.
2. Back up the whole portable `data/` directory, or for an installed build
   `%LOCALAPPDATA%\TelegramDownloader\` (including `.env`, Session,
   `monitored_channels.json`, `tgdl_download_history.sqlite3`).
3. Extract PR #4's portable preview package to a NEW directory. Copy the
   backed-up `data/` into it before the first launch.
4. If rollback is needed, stop the preview, restore the original data folder
   from the backup and start the old version.

## What changes

The existing global success ledger `downloaded_media`, pending queue
`pending_media`, failure table `scan_failures` and original account's
`scan_state` table are preserved. For new accounts:

```sql
CREATE TABLE IF NOT EXISTS account_scan_state (
  account_id TEXT NOT NULL, peer_id INTEGER NOT NULL,
  completed_id INTEGER NOT NULL DEFAULT 0, upper_id INTEGER NOT NULL DEFAULT 0,
  cursor_id INTEGER NOT NULL DEFAULT 0, direction TEXT NOT NULL DEFAULT 'newest',
  media_types TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(account_id, peer_id)
);
CREATE TABLE IF NOT EXISTS download_claims (
  peer_id INTEGER NOT NULL, message_id INTEGER NOT NULL,
  account_id TEXT NOT NULL, expires_at REAL NOT NULL,
  PRIMARY KEY(peer_id, message_id)
);
CREATE TABLE IF NOT EXISTS account_attempt_backoff (
  account_id TEXT NOT NULL, peer_id INTEGER NOT NULL,
  message_id INTEGER NOT NULL, retry_after REAL NOT NULL,
  PRIMARY KEY(account_id, peer_id, message_id)
);
```

Additionally, `downloaded_media` receives one nullable column `account_id TEXT`.
Old rows remain `NULL`; they are **not** deleted or recreated.

### Manual migration (optional)

After backing up, run the three CREATE TABLE statements above. Check
`PRAGMA table_info(downloaded_media);`. **Only if `account_id` is absent**, run:

```sql
ALTER TABLE downloaded_media ADD COLUMN account_id TEXT;
```

Do not run ALTER TABLE repeatedly: SQLite rejects duplicate column names.
The GUI's automatic migration already checks this safely.

## New files

- `data/accounts.json`: account IDs and display names, not passwords
- `data/accounts/<id>/.env`: each additional account's private settings
- `data/accounts/<id>/tgdl_session.session`: each account's private session
- `data/accounts/<id>/monitored_channels.json`: independent channel monitors
- `data/logs/runtime-YYYY-MM-DD.log`: rotating diagnostic logs (seven days)

The original account still reads the existing `data/.env`,
`data/tgdl_session.session` and `data/monitored_channels.json`.
