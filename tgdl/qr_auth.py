"""QR login for separately authenticated Telegram sessions.

Never log URLs or binary login tokens. Generate each QR in memory and use the
currently-authorized Telegram mobile app -> Settings -> Devices -> Link Device.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from telethon.errors import SessionPasswordNeededError


async def login_with_qr(
    client,
    *,
    show_qr: Callable[[str, datetime], None],
    ask_password: Callable[[], Awaitable[str]],
    should_stop: Callable[[], bool] = lambda: False,
    max_refreshes: int = 20,
) -> None:
    """Connect, display ephemeral QR URLs, await approval, handle 2FA.

    The QR URL grants access to a Telegram session, so it must never be logged,
    persisted or opened externally. The UI callback is responsible for
    presenting it as an in-memory image.
    """
    if not client.is_connected():
        await client.connect()
    if await client.get_me() is not None:
        return

    qr = await client.qr_login()
    for refresh in range(max_refreshes):
        if should_stop():
            raise asyncio.CancelledError()
        show_qr(qr.url, qr.expires)
        seconds_left = (qr.expires - datetime.now(timezone.utc)).total_seconds()
        if seconds_left <= 0:
            if refresh < max_refreshes - 1:
                await qr.recreate()
            continue
        task = asyncio.create_task(qr.wait(timeout=seconds_left))
        try:
            while not task.done():
                if should_stop():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise asyncio.CancelledError()
                await asyncio.sleep(0.3)
            try:
                await task
                return
            except SessionPasswordNeededError:
                # QR was approved, but the account has two-step verification.
                await client.sign_in(password=await ask_password())
                return
            except asyncio.TimeoutError:
                if refresh < max_refreshes - 1:
                    await qr.recreate()
                continue
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    raise TimeoutError("Telegram 扫码授权超时，请重新打开扫码登录")


def qr_image(url: str, *, pixels: int = 320):
    """Render an in-memory PIL image; never write session tokens to disk."""
    import qrcode

    qr = qrcode.QRCode(
        version=None, error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10, border=3
    )
    qr.add_data(url)
    qr.make(fit=True)
    image = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    image.thumbnail((pixels, pixels))
    return image
