# 2026-11-22 — Secondary account QR authorization fallback

## Problem
The B account's same phone number can receive a Telegram login code when
logging in via AyuGram, while Telethon returns `SentCodeTypeApp` and the
account's authorized Telegram App does not receive the service message.
The request response proves delivery mode selection, not actual delivery.
Retries may trigger Telegram server-side flood protections.

## Decision
Keep the Telethon phone/code/2FA login path, but add an explicit **QR login**
button to secondary account management. Use Telethon `qr_login()` and
`QRLogin.wait()` with expiration-driven token refresh, optional 2FA, and
cancellation. In-memory QR generation only, using Pillow and qrcode;
no QR URI/token/authorization data written to disk or logs. Authenticate
into account B's own existing session path and keep account A unchanged.

## Rejected approaches
- Forcing SMS using Telethon's deprecated `force_sms`: not supported.
- Reusing/exporting AyuGram session files: unsupported and secret-bearing.
- Sending repeated login code requests: may rate-limit and does not address
  Telegram's choice of app-code delivery.
- Creating a QR via arbitrary external QR conversion site: leaks the
  time-sensitive authorization token.

## Verification
Offline tests cover QR success, refresh, 2FA, cancellation and in-memory
rendering. Require a real B account phone/QR login test on user equipment
before marking PR #4 ready for merge.
