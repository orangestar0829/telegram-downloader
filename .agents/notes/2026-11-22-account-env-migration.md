# 2026-11-22 — Complete account-local .env layout

## Why
The original account still read and wrote data/.env after sessions and
monitored_channels.json had moved to data/accounts/default/. Multiple accounts
should consistently own one .env, one session and one monitor file under
data/accounts/<stable-id>/. Leaving the original .env at the root risks
confusing backups, upgrades and active GUI credential changes.

## Decision
The authoritative default-account configuration is
data/accounts/default/.env. On startup, prepare_data_dir copies the legacy
data/.env once, through an adjacent temporary file and atomic rename. The
legacy file remains a rollback backup. Repeated startup never overwrites the
new file. Other accounts continue using their existing account-specific .env.

The main GUI and CLI already call the default read_env_values/update_env
helpers, so changing the default path reroutes their edits. load_settings()
now reads explicit per-account env values without mutating os.environ. The
default Telethon session path is accounts/default/tgdl_session so CLI uses
the same migrated login as the GUI. An account missing API credentials can
use packaged API defaults, but cannot inherit another account's or stale
root credentials. Global downloaded-media history/scan cursors remain unchanged.

## Alternatives rejected
- Move/delete root data/.env: unsafe for rollback.
- Fall back to root data/.env whenever new file is missing: would reload
  stale settings after a user intentionally clears/reconfigures a new account.
- Call load_dotenv(override=True) for account switching: would leak account
  values between clients in a single process.
- Move shared download history into accounts: would break global deduplication.

## Validation
The disposable migration tests cover old-only, first boot, no old env,
an invalid UTF-8 file, idempotence, per-account credential/proxy isolation,
stale root and process environment values and stable old-file rollback.
All other media download and monitor tests must continue to pass.
