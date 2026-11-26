"""CLI commands authenticate once with the configured phone, or prompt for it."""

import asyncio
from types import SimpleNamespace

import pytest

from tgdl import cli
from tgdl.errors import TGDLInputError


@pytest.mark.parametrize(
    ("configured", "expected"),
    [(None, {}), ("(+1) 222-333-4444", {"phone": "+12223334444"})],
)
def test_cli_login_starts_once_without_telethon_context(monkeypatch, configured, expected):
    events = []

    class Client:
        async def __aenter__(self):
            raise AssertionError("Telethon's context would start before our phone is supplied")

        async def start(self, **kwargs):
            events.append(("start", kwargs))

        async def get_me(self):
            return SimpleNamespace(id=1, first_name="Test", last_name=None, username=None)

        async def disconnect(self):
            events.append(("disconnect",))

    monkeypatch.setattr(cli, "load_settings", lambda: SimpleNamespace(phone=configured))
    monkeypatch.setattr(cli, "build_client", lambda _: Client())

    asyncio.run(cli.cmd_login())
    assert events == [("start", expected), ("disconnect",)]


def test_cli_disconnects_on_invalid_phone_without_starting(monkeypatch):
    events = []

    class Client:
        async def start(self, **_kwargs):
            raise AssertionError("an invalid configured phone must not reach Telethon")

        async def disconnect(self):
            events.append("disconnected")

    monkeypatch.setattr(cli, "load_settings", lambda: SimpleNamespace(phone="not a phone"))
    monkeypatch.setattr(cli, "build_client", lambda _: Client())

    with pytest.raises(TGDLInputError, match="手机号格式无效"):
        asyncio.run(cli.cmd_login())
    assert events == ["disconnected"]


def test_cli_disconnects_if_command_fails_after_login(monkeypatch):
    events = []

    class Client:
        async def start(self):
            events.append("started")

        async def get_me(self):
            return SimpleNamespace(id=1, first_name="Test", last_name=None, username=None)

        async def disconnect(self):
            events.append("disconnected")

    async def failing_list(_):
        raise RuntimeError("list error")

    monkeypatch.setattr(cli, "load_settings", lambda: SimpleNamespace(phone=None))
    monkeypatch.setattr(cli, "build_client", lambda _: Client())
    monkeypatch.setattr(cli, "list_media_dialogs", failing_list)

    with pytest.raises(RuntimeError, match="list error"):
        asyncio.run(cli.cmd_list())
    assert events == ["started", "disconnected"]
