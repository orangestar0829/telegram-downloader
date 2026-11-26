"""One coordinated, account-pinned authorization for QR and phone."""

import asyncio
from types import SimpleNamespace

import pytest

from tgdl import gui, login_dialog
from tgdl.accounts import Account, AccountRegistry
from tgdl.config import read_env_values, update_env
from tgdl.login_dialog import TelegramLoginFlow, masked_phone


class PhoneClient:
    def __init__(self):
        self.connected = False
        self.user = None
        self.password = None
        self.sent = []

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connected = True

    async def get_me(self):
        return self.user

    async def send_code_request(self, phone):
        self.sent.append(phone)
        return SimpleNamespace(type=type("SentCodeTypeApp", (), {})())

    async def start(self, *, phone, code_callback, password):
        await self.send_code_request(phone)
        assert await code_callback() == "123456"
        self.password = await password()
        self.user = SimpleNamespace(id=2, phone=phone.lstrip("+"))


def test_qr_to_phone_reuses_one_flow_without_overlapping_attempts(monkeypatch):
    async def scenario():
        updates = asyncio.Queue()
        remembered = []
        client = PhoneClient()
        flow = TelegramLoginFlow(lambda gen, update: updates.put_nowait((gen, update)),
                                 remember_phone=remembered.append)
        qr_finished = asyncio.Event()

        async def qr(_client, *, show_qr, ask_password):
            show_qr("tg://login?token=QR_PRIVATE", None)
            try:
                await asyncio.Event().wait()
            finally:
                qr_finished.set()

        monkeypatch.setattr(login_dialog, "login_with_qr", qr)

        async def next_kind(kind):
            while True:
                generation, update = await asyncio.wait_for(updates.get(), 2)
                if generation == flow.generation and update.kind == kind:
                    return update

        attempt = asyncio.create_task(flow.authenticate(client, "+1 (987) 654-3210"))
        try:
            assert not (await next_kind("qr")).qr_url  # Auto-start QR by default.
            assert (await next_kind("qr")).qr_url.startswith("tg://login")
            assert client.sent == []
            flow.choose("phone")
            phone = await next_kind("phone")
            assert qr_finished.is_set()
            assert phone.saved_phone == "+•••• 3210"
            assert "19876543210" not in repr(phone)
            assert client.sent == []  # Do not send codes until explicitly submitted.
            assert flow.answer(phone.prompt, None)
            code = await next_kind("code")
            assert code.prompt != phone.prompt
            assert "Telegram 应用内" in code.message
            assert flow.answer(code.prompt, "123456")
            password = await next_kind("password")
            assert flow.answer(password.prompt, "  2FA secret  ")
            assert (await asyncio.wait_for(attempt, 2)).id == 2
            assert client.password == "  2FA secret  "
            assert client.sent == ["+19876543210"]
            assert remembered == []
        finally:
            if not attempt.done():
                flow.cancel()
                await asyncio.gather(attempt, return_exceptions=True)

    asyncio.run(scenario())


@pytest.mark.parametrize("remember", [False, True])
def test_manually_entered_phone_is_validated_and_only_saved_after_success(remember):
    async def scenario():
        updates = asyncio.Queue()
        remembered = []
        client = PhoneClient()
        flow = TelegramLoginFlow(lambda _gen, update: updates.put_nowait(update),
                                 remember_phone=remembered.append)
        flow.choose("phone")

        async def next_kind(kind):
            while True:
                update = await asyncio.wait_for(updates.get(), 2)
                if update.kind == kind:
                    return update

        attempt = asyncio.create_task(flow.authenticate(client, None))
        try:
            phone = await next_kind("phone")
            assert phone.saved_phone == ""
            assert flow.answer(phone.prompt, "wrong number", True)
            phone = await next_kind("phone")
            assert "格式无效" in phone.message
            assert client.sent == []
            assert flow.answer(phone.prompt, "+1 (987) 654-3210", remember)
            code = await next_kind("code")
            assert remembered == []
            assert flow.answer(code.prompt, "123456")
            password = await next_kind("password")
            assert flow.answer(password.prompt, "secret")
            assert (await asyncio.wait_for(attempt, 2)).id == 2
            assert remembered == (["+19876543210"] if remember else [])
        finally:
            if not attempt.done():
                flow.cancel()
                await asyncio.gather(attempt, return_exceptions=True)

    asyncio.run(scenario())


def test_a_method_switch_invalidates_the_old_prompt_answer_immediately():
    async def scenario():
        events = asyncio.Queue()
        flow = TelegramLoginFlow(lambda _generation, update: events.put_nowait(update))
        flow.choose("phone")
        attempt = asyncio.create_task(flow.authenticate(PhoneClient(), None))
        phone = await asyncio.wait_for(events.get(), 2)
        flow.choose("qr")
        assert not flow.answer(phone.prompt, "+19876543210", True)
        flow.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(attempt, 2)

    asyncio.run(scenario())


def test_queued_phone_answer_cannot_send_after_immediately_switching_method():
    async def scenario():
        events = asyncio.Queue()
        client = PhoneClient()
        flow = TelegramLoginFlow(lambda _generation, update: events.put_nowait(update))
        flow.choose("phone")
        attempt = asyncio.create_task(flow.authenticate(client, None))
        phone = await asyncio.wait_for(events.get(), 2)
        # Deliver an answer and switch before the async login task resumes.
        assert flow.answer(phone.prompt, "+19876543210", True)
        flow.choose("qr")
        flow.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(attempt, 2)
        assert client.sent == []

    asyncio.run(scenario())


@pytest.mark.parametrize("latest", ["phone", None])
def test_stale_auth_result_cannot_win_over_a_concurrent_switch_or_cancel(monkeypatch, latest):
    remembered = []
    flow = TelegramLoginFlow(lambda *_: None, remember_phone=remembered.append)

    async def race(_client, _generation, mode, _phone):
        if mode == "qr":
            flow.choose(latest)
            return "obsolete", "+11111111"
        return "latest", None

    monkeypatch.setattr(flow, "_attempt", race)
    if latest is None:
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(flow.authenticate(None, None))
    else:
        assert asyncio.run(flow.authenticate(None, None)) == "latest"
    assert remembered == []


def test_short_invalid_numbers_are_never_echoed_in_mask():
    assert masked_phone("+1234") == "+••••"


def test_same_login_entry_points_are_pinned_to_originating_account(tmp_path, monkeypatch):
    registry = AccountRegistry(tmp_path)
    account = Account("other", "Secondary")
    monkeypatch.setattr(registry, "accounts", lambda: [Account("default", "Original"), account])
    for account_id in ("default", "other"):
        update_env({"api_id": "123", "api_hash": "fake_hash"}, registry.env_path(account_id))

    app = object.__new__(gui._App)
    app._closing = False
    app._login_dialog = None
    app._default_login_future = None
    app._extra_runtimes, app._extra_futures = {}, {}
    app._view_account_id = "other"
    app._account_registry = registry
    app.client = None
    app._log_account = app._log_download_account = app._update_download_progress = lambda *_: None
    dialogs, submitted = [], []

    class Dialog:
        def __init__(self, _app, name, *, on_close, remember_phone):
            self.win = SimpleNamespace(lift=lambda: None)
            self.flow = SimpleNamespace(authenticate=object())
            self.closed = lambda: on_close(self)
            self.remember_phone = remember_phone
            dialogs.append(self)

    class Future:
        def add_done_callback(self, _callback):
            pass

    def submit(coro):
        submitted.append(coro.__qualname__)
        coro.close()
        return Future()

    app.loop = SimpleNamespace(submit=submit)
    monkeypatch.setattr(gui, "TelegramLoginDialog", Dialog)

    app.on_login()
    secondary_dialog = dialogs[-1]
    app._view_account_id = "default"
    app.on_login()  # The existing modal, not the new selection, owns this request.
    assert dialogs == [secondary_dialog]
    assert app._extra_runtimes["other"].authenticate is secondary_dialog.flow.authenticate
    secondary_dialog.remember_phone("+19876543210")
    assert read_env_values(registry.env_path("default"))["phone"] == ""
    assert read_env_values(registry.env_path("other"))["phone"] == "+19876543210"
    secondary_dialog.closed()

    app.on_login()
    assert len(dialogs) == 2
    assert submitted[-1].endswith("start_default")
