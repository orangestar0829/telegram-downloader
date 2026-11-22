"""Restoring B's session never starts an interactive login or its monitor."""

import asyncio
from types import SimpleNamespace

import pytest
from telethon.errors import AuthKeyDuplicatedError, AuthKeyNotFound, SessionRevokedError

from tgdl import multi_runtime
from tgdl.account_identity import AccountIdentity
from tgdl.accounts import Account, AccountRegistry
from tgdl.config import update_env


def fail_interactive(*_args, **_kwargs):
    raise AssertionError("silent session restore must not invoke interactive login")


def create_runtime(tmp_path, monkeypatch, *, phone="", notify=None):
    registry = AccountRegistry(tmp_path)
    account = Account("secondary", "Secondary")
    update_env({
        "api_id": "123", "api_hash": "test", "phone": phone,
        "proxy_host": "example-b.invalid", "proxy_port": "9291",
        "download_dir": str(tmp_path / "media"),
    }, registry.env_path(account.id))
    logs = []
    monkeypatch.setattr(multi_runtime, "account_identity", AccountIdentity())
    monkeypatch.setattr(multi_runtime, "login_with_qr", fail_interactive)
    runtime = multi_runtime.AccountMonitorRuntime(
        account, registry, ask=fail_interactive, on_qr=fail_interactive,
        authenticate=fail_interactive, on_log=lambda _account, line: logs.append(line),
        on_saved=fail_interactive, on_progress=fail_interactive,
        restore_session_only=True, on_authenticated=notify,
    )
    return runtime, registry.session_path(account.id).with_suffix(".session"), logs


class Client:
    def __init__(self, *, user=None, auth_key=True, connect_error=None, get_me_error=None):
        self.session = SimpleNamespace(auth_key=object() if auth_key else None)
        self.user = user
        self.connect_error = connect_error
        self.get_me_error = get_me_error
        self.events = []

    def is_connected(self):
        return False

    async def connect(self):
        self.events.append("connect")
        if self.connect_error:
            raise self.connect_error

    async def get_me(self):
        self.events.append("get_me")
        if self.get_me_error:
            raise self.get_me_error
        return self.user

    async def disconnect(self):
        self.events.append("disconnect")

    start = sign_in = send_code_request = qr_login = fail_interactive


@pytest.mark.parametrize(("payload", "status"), [(None, "未登录"), (b"", "登录已失效")])
def test_absent_or_empty_session_does_not_even_construct_a_client(
    tmp_path, monkeypatch, payload, status,
):
    runtime, path, logs = create_runtime(tmp_path, monkeypatch)
    if payload is not None:
        path.write_bytes(payload)
    monkeypatch.setattr(multi_runtime, "build_client", fail_interactive)
    asyncio.run(runtime.run())
    assert runtime.status == status
    assert status in logs[-1]
    if payload is None:
        assert not path.exists()
    else:
        assert path.read_bytes() == payload


def test_restored_session_stays_connected_with_monitoring_idle(tmp_path, monkeypatch):
    notified = asyncio.Event()
    runtime, path, logs = create_runtime(
        tmp_path, monkeypatch, phone="+1 (222) 333-4444", notify=notified.set,
    )
    path.write_bytes(b"saved secondary session")
    client = Client(user=SimpleNamespace(id=2, first_name="B", phone="12223334444"))

    def build(settings):
        # Telethon appends exactly .session to this pinned session name.
        assert settings.session_path.with_suffix(".session") == path
        assert (settings.proxy_host, settings.proxy_port) == ("example-b.invalid", 9291)
        return client

    monkeypatch.setattr(multi_runtime, "build_client", build)
    monkeypatch.setattr(multi_runtime, "scan_monitor_slice", fail_interactive)

    async def scenario():
        task = asyncio.create_task(runtime.run())
        try:
            await asyncio.wait_for(notified.wait(), 2)
            assert runtime.status.startswith("已连接：")
            assert runtime.client is client
            assert not runtime.monitoring
            assert not task.done()
            assert client.events == ["connect", "get_me"]
        finally:
            runtime.stop(cancel=True)
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())
    assert client.events == ["connect", "get_me", "disconnect"]
    assert path.read_bytes() == b"saved secondary session"


@pytest.mark.parametrize(
    ("client", "phone", "status", "events"),
    [
        (lambda: Client(auth_key=False), "", "登录已失效", ["disconnect"]),
        (Client, "", "登录已失效", ["connect", "get_me", "disconnect"]),
        (lambda: Client(get_me_error=SessionRevokedError(request=None)), "",
         "登录已失效", ["connect", "get_me", "disconnect"]),
        (lambda: Client(connect_error=AuthKeyDuplicatedError(request=None)), "",
         "登录已失效", ["connect", "disconnect"]),
        (lambda: Client(connect_error=AuthKeyNotFound()), "",
         "恢复登录失败", ["connect", "disconnect"]),
        (lambda: Client(connect_error=OSError("private_proxy_details")), "",
         "恢复登录失败", ["connect", "disconnect"]),
        (lambda: Client(user=SimpleNamespace(phone="11112223333")), "+12223334444",
         "会话与账号不匹配", ["connect", "get_me", "disconnect"]),
    ],
)
def test_restore_classifies_failure_without_interactive_fallback(
    tmp_path, monkeypatch, client, phone, status, events,
):
    runtime, path, logs = create_runtime(
        tmp_path, monkeypatch, phone=phone, notify=fail_interactive,
    )
    path.write_bytes(b"saved secondary session")
    fake = client()
    monkeypatch.setattr(multi_runtime, "build_client", lambda _settings: fake)
    asyncio.run(runtime.run())
    assert runtime.status == status
    assert fake.events == events
    assert runtime.client is None
    assert not runtime.monitoring
    assert path.read_bytes() == b"saved secondary session"
    assert all("private_proxy_details" not in line and "11112223333" not in line for line in logs)


def test_shutdown_interrupts_silent_connection_without_touching_session(tmp_path, monkeypatch):
    started = asyncio.Event()
    runtime, path, _logs = create_runtime(tmp_path, monkeypatch, notify=fail_interactive)
    path.write_bytes(b"saved secondary session")

    class Connecting(Client):
        async def connect(self):
            self.events.append("connect")
            started.set()
            await asyncio.Event().wait()

    client = Connecting()
    monkeypatch.setattr(multi_runtime, "build_client", lambda _settings: client)

    async def scenario():
        task = asyncio.create_task(runtime.run())
        await asyncio.wait_for(started.wait(), 2)
        assert runtime.status == "正在恢复登录"
        runtime.stop()
        await asyncio.wait_for(task, 2)

    asyncio.run(scenario())
    assert runtime.status == "已停止"
    assert client.events == ["connect", "disconnect"]
    assert path.read_bytes() == b"saved secondary session"
