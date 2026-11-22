"""Closing the GUI must wait for real secondary transfer cleanup."""

import asyncio
import queue
import sys
import threading
from types import SimpleNamespace

from tgdl import gui, multi_runtime
from tgdl.account_identity import AccountIdentity
from tgdl.accounts import Account, AccountRegistry


def test_cancel_and_exit_waits_for_secondary_transfer_cleanup(tmp_path, monkeypatch):
    monkeypatch.setattr(multi_runtime, "account_identity", AccountIdentity())
    app = object.__new__(gui._App)
    app.loop = gui.AsyncLoop()
    app._closing = False
    app.client = None
    app._download_future = app._download_task = None
    app._monitor_scan_future = app._monitor_download_future = None
    app._monitor_total_transfers = 0
    app._monitor_active = False
    app._auto_config = None
    app._stop_download = threading.Event()
    app._stdout, app._stderr = sys.stdout, sys.stderr
    app._telethon_diag_uninstall = lambda: None
    app._log = lambda *_: None
    app.messagebox = SimpleNamespace(askyesnocancel=lambda *_: False)
    ui_work = queue.Queue()
    destroyed = threading.Event()
    app.root = SimpleNamespace(
        state=lambda: "zoomed", after=lambda _delay, callback: ui_work.put(callback),
        destroy=destroyed.set,
    )

    client_disconnected = threading.Event()

    class FakeClient:
        async def send_code_request(self, *_args, **_kwargs):
            raise AssertionError("the existing session is already authorized")

        async def start(self, **_kwargs):
            pass

        async def get_me(self):
            return SimpleNamespace(id=1, first_name="Secondary")

        async def disconnect(self):
            client_disconnected.set()

    monkeypatch.setattr(multi_runtime, "load_settings", lambda **_: SimpleNamespace(phone=""))
    monkeypatch.setattr(multi_runtime, "build_client", lambda _: FakeClient())
    runtime = multi_runtime.AccountMonitorRuntime(
        Account("other", "Secondary"), AccountRegistry(tmp_path), ask=None,
        on_log=lambda *_: None, on_saved=lambda *_: None, on_progress=lambda *_: None,
    )
    app._extra_runtimes = {runtime.account.id: runtime}
    transfer_started, cleanup_started = threading.Event(), threading.Event()
    cleanup_release = asyncio.Event()

    async def downloading(_settings):
        try:
            runtime.active_count = 1
            transfer_started.set()
            await asyncio.sleep(60)
        finally:
            cleanup_started.set()
            await cleanup_release.wait()
            runtime.active_count = 0

    monkeypatch.setattr(runtime, "_download_loop", downloading)
    future = app.loop.submit(runtime.run())
    app._extra_futures = {runtime.account.id: future}
    try:
        assert transfer_started.wait(2)
        app._on_close()
        assert cleanup_started.wait(2)
        assert not future.done()
        assert not client_disconnected.is_set()
        assert ui_work.empty()
        app.loop.loop.call_soon_threadsafe(cleanup_release.set)
        ui_work.get(timeout=2)()  # Executes the final Tk destroy callback.
        assert future.done()
        assert client_disconnected.is_set()
        assert destroyed.is_set()
    finally:
        if app.loop._thread.is_alive():
            app.loop.loop.call_soon_threadsafe(cleanup_release.set)
            app.loop.stop()
        app.loop._thread.join(2)


def test_stopping_secondary_account_cancels_pending_login(tmp_path, monkeypatch):
    login_started = asyncio.Event()
    disconnected = asyncio.Event()

    class FakeClient:
        async def send_code_request(self, *_args, **_kwargs):
            raise AssertionError("unexpected login flow")

        async def start(self, *, code_callback, **_kwargs):
            await code_callback()

        async def disconnect(self):
            disconnected.set()

    async def await_code(*_args):
        login_started.set()
        await asyncio.sleep(60)

    monkeypatch.setattr(multi_runtime, "load_settings", lambda **_: SimpleNamespace(phone=""))
    monkeypatch.setattr(multi_runtime, "build_client", lambda _: FakeClient())
    runtime = multi_runtime.AccountMonitorRuntime(
        Account("other", "Secondary"), AccountRegistry(tmp_path), ask=await_code,
        on_log=lambda *_: None, on_saved=lambda *_: None, on_progress=lambda *_: None,
    )

    async def scenario():
        task = asyncio.create_task(runtime.run())
        await asyncio.wait_for(login_started.wait(), 2)
        runtime.stop()
        await asyncio.wait_for(task, 2)
        assert disconnected.is_set()

    asyncio.run(scenario())
