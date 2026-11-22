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
5. The original account's monitor configuration is migrated automatically into `data/accounts/default/monitored_channels.json`. Before first startup, check you have a backup of the legacy root `monitored_channels.json`; the migrated preview retains the original file for rollback.

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

`download_claims` also gains a nullable token column automatically. Each
new claim gets a fresh token, even when the same account reclaims a job.
A pre-upgrade lease has no token: it retains its original expiry and a new
worker cannot renew or complete it. Its pending job becomes eligible after
expiry if no success was recorded. The upgrade does not clear pending jobs
or rewrite historical successes. Do not run an older executable against the
same live database: it does not check lease tokens.

Telegram basic groups use account-local message IDs. Only the original account
monitors them, now under negative peer keys; channel and supergroup keys keep
their prior positive values. Untyped legacy history keys are not rewritten,
because the old tables did not store the entity type. The original account
can rescan old basic-group entries. An old pending row known to be from a
basic group (its stored target is exactly its marked negative peer ID) is not
dispatched under a positive key. If a real channel later needs that key, the
old row is kept in `legacy_basic_pending_media` before the queue slot is reused.

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
- `data/accounts/default/.env`: original account's API/phone/proxy/download settings. Automatically copied from legacy `data/.env` on the first start using a temporary file and atomic replace; the original file is left intact for rollback. Existing new file always takes precedence.
- `data/accounts/default/tgdl_session.session`: original account's session,
  automatically copied from the legacy `data/tgdl_session.session` by a
  SQLite backup and verified before first use (old file is **not deleted**).
- `data/accounts/default/monitored_channels.json`: original account's monitor configuration. On first startup the old `data/monitored_channels.json` is copied through a temporary JSON-validated file and atomically published. Existing new configs are never overwritten; the old file remains intact for rollback.
- `data/accounts/<id>/tgdl_session.session`: each added account's private session.
- `data/accounts/<id>/.env`: each account's private settings. Settings and credentials are loaded independently, never from another account or global process environment.
- `data/accounts/<id>/monitored_channels.json`: independent channel monitors.
- `data/logs/runtime-YYYY-MM-DD.log`: rotating diagnostic logs (seven days).

**Important migration rule:** close the previous program normally and back
up the whole `data/` folder first. At startup, if the new default Session
file already exists it will NOT be overwritten, even if the legacy file is
newer. A temporary SQLite backup incorporates WAL state; the legacy file
remains available for rollback. The original account keeps its existing
global `data/tgdl_download_history.sqlite3` (the history is not moved). The active original account's `.env` and monitor configuration are now both in `data/accounts/default/`; their root-level copies are legacy backups only.

All diagnostic and download-event logs now show the actual Telegram profile
name after login (e.g. `[账号:Alice (@alice)]`); the stable account ID
remains internal. Before login, logs use a readable temporary account label.
If two Telegram accounts have the same displayed name, the local account
selector can still disambiguate them by their local account remarks.
