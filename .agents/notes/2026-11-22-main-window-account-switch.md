# 2026-11-22 — Account-aware main window switching

## Context
In PR #4, secondary Telegram accounts used independent Telethon clients,
sessions and monitor workers, but the original main window was hardwired to
the original account A. Users asked to choose which Telegram profile the
main window displays, including its joined groups/channels, monitoring
configuration and runtime information.

## Decision
Add a main-window account selector keyed by stable internal account ID and
labeled with the actual Telegram display name (plus local remark for
disambiguation). Switching changes presentation only: it never changes an
in-flight worker's client or stops a secondary account. The selected
account controls the displayed profile/status, account-local env fields,
joined channel list, keyword filter, monitor list/next scan state,
runtime/download-log filter and active transfer progress. Joined-channel
data are cached per account and fetched via that account's client only
when needed or manually refreshed.

Account A's existing scan/downloader implementation remains independent
of selection. Main-window monitoring edits for B are persisted using B's
own accounts/<id>/monitored_channels.json; its background worker reloads
config and its next-scan state remains account-local. Cross-window changes
use config file timestamps and local UI refresh, never Telegram re-fetch.
Secondary media limit/concurrency edits write only B's .env and are picked
up by B's runtime; they never change A's live monitor settings.

**Safety boundary:** the pre-existing main-window manual bulk download,
gallery selection, scheduled manual download, and advanced link parser
are still backed by A's legacy non-lease-aware download engine. In the B
account view they are disabled/guarded rather than invoking the wrong
Telegram identity or competing with B's leased monitor transfers. B login,
QR authorization and monitor start/stop continue through the account
manager, automatically focusing the chosen account. This feature adds
account *view* switching and monitor editing, not a new cross-account
manual downloader.

## Alternatives rejected
- Simply change the title/account label while keeping main manual actions:
  would silently access A's client while displaying B's group list.
- Replace the original self.client with the selected secondary client's
  instance: would mutate the identity of A's in-flight scans/downloads and
  break cancellation/log attribution.
- Re-fetch Telegram dialogs on every account switch: extra network calls,
  delays and flood-risk; cache account-specific data instead.
- Move B monitoring workers into the main UI: unnecessary concurrency and
  global-state refactor; keep each background worker separate.

## Verification
Offline tests exercise profile-based selector labels, per-account cached
joined channels, monitor settings and path isolation, B-only saved media
settings, B manual-action guard, account-specific log filtering, progress
visibility and restoration of account A. Full regression tests include
legacy monitor worker behavior. A Windows live GUI test with two Telegram
sessions is still required before PR #4 is ready to merge.
