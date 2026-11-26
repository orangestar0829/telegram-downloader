"""GUI login prompts suspend one account, not the shared asyncio thread."""

import asyncio
import queue
from types import SimpleNamespace

import pytest

from tgdl import gui
from tgdl.account_identity import AccountIdentity
from tgdl.accounts import Account, AccountRegistry
from tgdl.errors import TGDLInputError
from tgdl import multi_runtime


def test_secondary_code_prompt_does_not_block_other_account(tmp_path, monkeypatch):
    monkeypatch.setattr(multi_runtime, "account_identity", AccountIdentity())
    app = object.__new__(gui._App)
    app._closing = False
    app._prompt_lock = asyncio.Lock()
    ui_work = queue.Queue()
    app.root = SimpleNamespace(after=lambda _delay, callback: ui_work.put(callback))
    shown = []
    app._show_login_prompt = lambda _prompt, _secret, submit: shown.append(submit)
    registry = AccountRegistry(tmp_path)
    runtime = multi_runtime.AccountMonitorRuntime(
        Account("other", "Secondary"), registry, ask=app._ask,
        on_log=lambda *_: None, on_saved=lambda *_: None, on_progress=lambda *_: None,
    )
    client = SimpleNamespace()
    async def send_code_request(*_args, **_kw):
        return SimpleNamespace()
    async def start(*, phone, code_callback, password):
        value = await phone() if callable(phone) else phone
        assert value == "+12223334444"
        await client.send_code_request()
        assert await code_callback() == "fake code"
    async def get_me():
        return SimpleNamespace(id=1, first_name="Secondary")
    async def disconnect():
        pass
    async def done_worker(*_args):
        runtime.stop_event.set()
    client.start = start
    client.get_me = get_me
    client.disconnect = disconnect
    client.send_code_request = send_code_request
    monkeypatch.setattr(multi_runtime, "load_settings",
                        lambda **_: SimpleNamespace(phone="+12223334444"))
    monkeypatch.setattr(multi_runtime, "build_client", lambda settings: client)
    monkeypatch.setattr(runtime, "_scan_loop", done_worker)
    monkeypatch.setattr(runtime, "_download_loop", done_worker)

    loop = gui.AsyncLoop()
    pending_dialog = None
    try:
        login = loop.submit(runtime.run())
        pending_dialog = ui_work.get(timeout=2)
        other_account = loop.submit(asyncio.sleep(0, result="still running"))
        assert other_account.result(2) == "still running"
        assert not login.done()
    finally:
        if pending_dialog is not None:
            pending_dialog()
            if shown:
                shown[0]("fake code")
            else:
                runtime.stop()
        else:
            runtime.stop()
        try:
            login.result(2)
        finally:
            loop.stop()
            loop._thread.join(2)


def test_default_client_is_published_only_after_login_once(monkeypatch):
    monkeypatch.setattr(gui, "account_identity", AccountIdentity())
    app = object.__new__(gui._App)
    app.client = None
    app._client_lock = asyncio.Lock()
    app._closing = False
    app.root = SimpleNamespace(after=lambda *_: None)
    app._set_status = lambda *_: None
    started, release = asyncio.Event(), asyncio.Event()
    attempts = []

    async def authenticate(_client, _phone):
        attempts.append(None)
        started.set()
        await release.wait()
        return SimpleNamespace(first_name="A", last_name=None, username=None)

    client = SimpleNamespace()
    monkeypatch.setattr(gui, "load_settings", lambda: SimpleNamespace(phone="+12223334444"))
    monkeypatch.setattr(gui, "build_client", lambda _: client)

    async def scenario():
        first = asyncio.create_task(app._ensure_client(authenticate=authenticate))
        await started.wait()
        second = asyncio.create_task(app._ensure_client())
        await asyncio.sleep(0)
        assert app.client is None
        assert not second.done()
        release.set()
        assert await asyncio.gather(first, second) == [client, client]

    asyncio.run(scenario())
    assert len(attempts) == 1


def test_cancel_login_dismisses_its_dialog_before_opening_another():
    app = object.__new__(gui._App)
    app._closing = False
    app._prompt_lock = asyncio.Lock()
    ui_work = queue.Queue()
    app.root = SimpleNamespace(after=lambda _delay, callback: ui_work.put(callback))
    dialogs = []
    events = []

    class Dialog:
        def __init__(self, prompt, submit):
            assert all(d.closed for d in dialogs)
            self.closed = False
            self.prompt = prompt
            self.submit = submit
            dialogs.append(self)
            events.append("opened " + prompt)

        def winfo_exists(self):
            return not self.closed

        def destroy(self):
            self.closed = True
            events.append("closed " + self.prompt)

    app._show_login_prompt = lambda prompt, _secret, submit: Dialog(prompt, submit)

    async def scenario():
        first = asyncio.create_task(app._ask("account A"))
        second = asyncio.create_task(app._ask("account B", secret=True))
        await asyncio.sleep(0)
        ui_work.get_nowait()()  # Open A.
        first.cancel()
        await asyncio.gather(first, return_exceptions=True)
        ui_work.get_nowait()()  # Destroy A.
        ui_work.get_nowait()()  # Open B.
        dialogs[-1].submit("  B password  ")
        assert await second == "  B password  "

    asyncio.run(scenario())
    assert events == ["opened account A", "closed account A", "opened account B", "closed account B"]


@pytest.mark.parametrize("saved", ["not a phone", ""])
def test_original_rejects_invalid_saved_or_prompted_phone(saved):
    app = object.__new__(gui._App)
    reached_start = []

    async def ask(*_args):
        return "not a phone"

    async def start(*, phone, **_kwargs):
        reached_start.append(True)
        assert callable(phone)
        await phone()

    app._ask = ask
    with pytest.raises(TGDLInputError, match="手机号格式无效"):
        asyncio.run(app.client_start(SimpleNamespace(start=start), SimpleNamespace(phone=saved)))
    assert bool(reached_start) == (not saved)


@pytest.mark.parametrize("saved", ["not a phone", ""])
def test_secondary_rejects_invalid_saved_or_prompted_phone(saved, tmp_path, monkeypatch):
    reached_client, disconnected, logs = [], [], []

    class Client:
        async def send_code_request(self, *_args):
            raise AssertionError("invalid phone must not request a code")

        async def start(self, *, phone, **_kwargs):
            assert callable(phone)
            await phone()

        async def disconnect(self):
            disconnected.append(True)

    async def ask(*_args):
        return "not a phone"

    def build(_):
        reached_client.append(True)
        return Client()

    monkeypatch.setattr(multi_runtime, "load_settings", lambda **_: SimpleNamespace(phone=saved))
    monkeypatch.setattr(multi_runtime, "build_client", build)
    runtime = multi_runtime.AccountMonitorRuntime(
        Account("other", "Secondary"), AccountRegistry(tmp_path), ask=ask,
        on_log=lambda _account, line: logs.append(line),
        on_saved=lambda *_: None, on_progress=lambda *_: None,
    )
    asyncio.run(runtime.run())
    assert any("手机号格式无效" in line for line in logs)
    assert not any("not a phone" in line for line in logs)
    assert bool(reached_client) == bool(disconnected) == (not saved)


def test_valid_phone_normalizes_before_start():
    app = object.__new__(gui._App)
    phone = "(+1) 222-333-4444"

    async def start(*, phone, **_kwargs):
        assert phone == "+12223334444"

    asyncio.run(app.client_start(SimpleNamespace(start=start), SimpleNamespace(phone=phone)))
