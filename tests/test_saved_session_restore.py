import asyncio
from types import SimpleNamespace

import pytest

from tgdl import gui, multi_runtime
from tgdl.accounts import AccountRegistry
from tgdl.account_identity import AccountIdentity
from tgdl.config import load_settings, update_env


class _QueuedLoop:
    def __init__(self):
        self.calls = []

    def submit(self, coro):
        pending = SimpleNamespace(done=lambda: False)
        self.calls.append(coro)
        return pending


def make_app(tmp_path):
    registry = AccountRegistry(tmp_path)
    extra = registry.add("B")
    app = object.__new__(gui._App)
    app._account_registry = registry
    app._extra_runtimes = {}
    app._extra_futures = {}
    app._default_login_future = None
    app._default_login_task = None
    app._closing = False
    app._view_account_id = extra.id
    app.client = None
    app.loop = _QueuedLoop()
    app._log_account = lambda *_: None
    app._log_download_account = lambda *_: None
    app._update_download_progress = lambda *_: None
    app._ask = lambda *_: pytest.fail("Saved-session restore must not prompt")
    app.root = SimpleNamespace(after=lambda _delay, f: f())
    return app, extra.id


def write_stub_session(app, account_id):
    path = app._saved_session_file(account_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"stub used only with a fake client")
    return path


def test_no_saved_session_starts_nothing_creates_nothing(tmp_path):
    app, extra = make_app(tmp_path)
    for account_id in ("default", extra):
        assert not app._restore_saved_session(account_id)
        assert not app._saved_session_file(account_id).exists()
    assert app.loop.calls == []


def test_secondary_restore_is_single_account_and_no_interactive_login(tmp_path, monkeypatch):
    app, extra = make_app(tmp_path)
    write_stub_session(app, extra)
    connected = []
    app._load_secondary_dialogs = connected.append

    class Runtime:
        def __init__(self, account, registry, **kw):
            assert account.id == extra
            assert registry is app._account_registry
            assert kw["restore_session_only"] is True
            assert "authenticate" not in kw
            self.on_authenticated = kw["on_authenticated"]
            self.client = None
            self.status = "正在恢复登录"
            self.monitoring = False

        async def run(self):
            self.client = object()
            self.status = "已连接：B"
            self.on_authenticated()

    monkeypatch.setattr(multi_runtime, "AccountMonitorRuntime", Runtime)
    assert app._restore_saved_session(extra)
    assert app._restore_saved_session(extra)
    assert len(app.loop.calls) == 1
    app._view_account_id = "default"
    asyncio.run(app.loop.calls.pop())
    assert connected == []  # No list fetch for a background account.
    assert not app._extra_runtimes[extra].monitoring
    assert app.client is None


@pytest.mark.parametrize("which, expected", [
    ("usable", "已登录"),
    ("expired", "登录已失效"),
    ("keyless", "登录已失效"),
    ("offline", "恢复登录失败"),
])
def test_default_restore_is_passive_and_distinguishes_expiry(tmp_path, monkeypatch, which, expected):
    monkeypatch.setattr(gui, "account_identity", AccountIdentity())
    app, _extra = make_app(tmp_path)
    path = write_stub_session(app, "default")
    app._view_account_id = "default"
    app._client_lock = asyncio.Lock()
    app._client_starting_task = None
    loaded, statuses, calls = [], [], []
    app._set_status = statuses.append
    app._refresh_log_accounts = lambda: None

    async def load():
        loaded.append(True)

    class Client:
        session = SimpleNamespace(auth_key=None if which == "keyless" else object())

        def is_connected(self):
            return False

        async def connect(self):
            calls.append("connect")
            if which == "offline":
                raise OSError("offline")

        async def get_me(self):
            calls.append("get_me")
            return (SimpleNamespace(first_name="A", last_name=None, username=None)
                    if which == "usable" else None)

        async def disconnect(self):
            calls.append("disconnect")

    app._load_dialogs = load
    update_env({
        "api_id": "1001", "api_hash": "primary-hash",
        "proxy_host": "example-a.invalid", "proxy_port": "9290",
    }, app._account_registry.env_path("default"))

    def read_settings(*, env_path, session_path):
        assert env_path == app._account_registry.env_path("default")
        assert session_path == app._account_registry.session_path("default")
        return load_settings(env_path=env_path, session_path=session_path)

    def build(settings):
        assert (settings.proxy_host, settings.proxy_port) == ("example-a.invalid", 9290)
        return Client()

    monkeypatch.setattr(gui, "load_settings", read_settings)
    monkeypatch.setattr(gui, "build_client", build)
    assert app._restore_saved_session("default")
    assert app._restore_saved_session("default")
    assert len(app.loop.calls) == 1
    asyncio.run(app.loop.calls.pop())
    assert statuses[-1].startswith(expected)
    assert bool(loaded) == (which == "usable")
    assert ("disconnect" in calls) == (which != "usable")
    assert ("connect" in calls) == (which != "keyless")
    assert path.read_bytes() == b"stub used only with a fake client"

def test_startup_validates_all_account_ids_not_just_selected(tmp_path):
    app, extra = make_app(tmp_path)
    extra2 = app._account_registry.add("C")
    observed = []
    app._restore_saved_session = observed.append
    app._check_saved_sessions_at_startup()
    assert observed == ["default", extra, extra2.id]
    app._closing = True
    app._check_saved_sessions_at_startup()
    assert len(observed) == 3


@pytest.mark.parametrize("status, color, hint", [
    ("已登录:A", "#13794d", ""),
    ("已连接：B｜监控未启动", "#13794d", ""),
    ("登录已失效", "#b62d32", "请重新登录"),
    ("会话与账号不匹配", "#b62d32", "请重新登录"),
    ("恢复登录失败", "#a05a14", "请检查此账号的网络、代理或设置"),
    ("未登录", "#546174", ""),
])
def test_session_status_is_color_coded_with_precise_hint(status, color, hint):
    app = object.__new__(gui._App)
    lines, colors = [], []
    app.status_var = SimpleNamespace(set=lines.append)
    app._account_status_label = SimpleNamespace(configure=lambda **kw: colors.append(kw))
    app._paint_account_status(status)
    assert colors == [{"foreground": color}]
    assert lines == [status + ("｜" + hint if hint else "")]
