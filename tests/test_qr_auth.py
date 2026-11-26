"""QR authorization: expiration, two-factor verification, cancellation and secrets."""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from telethon.errors import SessionPasswordNeededError

from tgdl.qr_auth import login_with_qr, qr_image


async def unused_password():
    raise AssertionError("unexpected password prompt")


class FakeQR:
    def __init__(self, modes):
        self.modes = iter(modes)
        self.url = "tg://login?token=PRIVATE"
        self.expires = datetime.now(timezone.utc) + timedelta(seconds=30)
        self.recreated = 0

    async def wait(self, timeout=None):
        mode = next(self.modes)
        if mode == "expired":
            raise asyncio.TimeoutError
        if mode == "2fa":
            raise SessionPasswordNeededError(request=None)
        if mode == "hang":
            await asyncio.sleep(120)
        return SimpleNamespace(id=7)

    async def recreate(self):
        self.recreated += 1
        self.url = "tg://login?token=NEW_PRIVATE"
        self.expires = datetime.now(timezone.utc) + timedelta(seconds=30)


class FakeClient:
    def __init__(self, modes):
        self.connected = False
        self.qr = FakeQR(modes)
        self.passwords = []
        self.qr_requests = 0

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connected = True

    async def get_me(self):
        return None

    async def qr_login(self):
        self.qr_requests += 1
        return self.qr

    async def sign_in(self, *, password):
        self.passwords.append(password)


def test_qr_approval_does_not_send_phone_code():
    fake = FakeClient(["ok"])
    displayed = []
    asyncio.run(login_with_qr(
        fake, show_qr=lambda url, expires: displayed.append(url),
        ask_password=unused_password,
    ))
    assert fake.qr_requests == 1
    assert displayed == ["tg://login?token=PRIVATE"]
    assert fake.passwords == []


def test_qr_expiration_refreshes_in_place():
    fake = FakeClient(["expired", "ok"])
    displayed = []
    asyncio.run(login_with_qr(
        fake, show_qr=lambda url, expires: displayed.append(url),
        ask_password=unused_password,
    ))
    assert fake.qr.recreated == 1
    assert displayed == ["tg://login?token=PRIVATE", "tg://login?token=NEW_PRIVATE"]


def test_qr_2fa_requests_password_only_after_scan():
    fake = FakeClient(["2fa"])
    async def ask_password():
        await asyncio.sleep(0)
        return "password_for_test"

    asyncio.run(login_with_qr(
        fake, show_qr=lambda *_: None, ask_password=ask_password
    ))
    assert fake.passwords == ["password_for_test"]


def test_qr_cancel_releases_wait_task():
    fake = FakeClient(["hang"])
    state = {"stop": False}

    async def scenario():
        async def trigger():
            await asyncio.sleep(0.45)
            state["stop"] = True
        asyncio.create_task(trigger())
        with pytest.raises(asyncio.CancelledError):
            await login_with_qr(
                fake, show_qr=lambda *_: None,
                ask_password=unused_password,
                should_stop=lambda: state["stop"],
            )

    asyncio.run(scenario())


def test_qr_render_is_memory_only():
    img = qr_image("tg://login?token=TEST_PRIVATE", pixels=260)
    assert img.size[0] <= 260
    assert img.size[1] <= 260
