"""Input failures must not escape an account task as process exits."""

import asyncio
import threading
from types import SimpleNamespace

import pytest

from tgdl import cli
from tgdl.accounts import Account, AccountRegistry
from tgdl.config import update_env
from tgdl.errors import TGDLInputError
from tgdl.gui import AsyncLoop
from tgdl.monitor import save_channels
from tgdl.multi_runtime import AccountMonitorRuntime


def test_invalid_secondary_channel_does_not_stop_shared_loop(tmp_path):
    registry = AccountRegistry(tmp_path)
    account = Account("other", "Secondary")
    save_channels([
        {"target": "@missing", "minutes": 1, "types": "photo"},
    ], registry.monitor_path(account.id))
    logged = threading.Event()
    runtime = AccountMonitorRuntime(
        account, registry, ask=lambda *_: None,
        on_log=lambda *_: logged.set(),
        on_saved=lambda *_: None, on_progress=lambda *_: None,
    )

    async def get_entity(_):
        raise ValueError("no matching channel")

    runtime.client = SimpleNamespace(get_entity=get_entity)
    loop = AsyncLoop()
    try:
        scan = loop.submit(runtime._scan_loop())
        assert logged.wait(2)
        assert loop.submit(asyncio.sleep(0, result="other account still running")).result(2)
        assert not scan.done()
        runtime.stop_event.set()
        scan.result(2)
    finally:
        loop.stop()
        loop._thread.join(2)


def test_invalid_secondary_api_credentials_are_handled(monkeypatch, tmp_path):
    registry = AccountRegistry(tmp_path)
    account = Account("other", "Secondary")
    update_env({"api_id": "not-an-integer", "api_hash": "test"},
               registry.env_path(account.id))
    logs = []
    runtime = AccountMonitorRuntime(
        account, registry, ask=lambda *_: None,
        on_log=lambda _, line: logs.append(line),
        on_saved=lambda *_: None, on_progress=lambda *_: None,
    )
    asyncio.run(runtime.run())
    assert any("TELEGRAM_API_ID must be a number" in line for line in logs)


def test_cli_translates_expected_input_error_to_readable_exit(monkeypatch):
    monkeypatch.setattr("sys.argv", ["tgdl", "download", "-c", "@example"])

    async def reject(_):
        raise TGDLInputError("channel is not accessible")

    monkeypatch.setattr(cli, "cmd_download", reject)
    with pytest.raises(SystemExit, match="^channel is not accessible$"):
        cli.main()


def test_manual_user_target_cannot_share_a_channel_history_key():
    from telethon.tl.types import User
    from tgdl.downloader import resolve_channel
    from tgdl.errors import ChannelAccessError

    async def get_entity(_):
        return User(id=123)

    with pytest.raises(ChannelAccessError, match="not a user account"):
        asyncio.run(resolve_channel(SimpleNamespace(get_entity=get_entity), "@individual"))
