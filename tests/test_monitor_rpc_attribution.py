"""Real DiagnosticTelegramClient sender hooks, no MTProto connection or credentials."""
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon import TelegramClient
from telethon.errors import FloodPremiumWaitError, FloodWaitError, RpcCallFailError
from telethon.tl.functions.upload import GetCdnFileRequest, GetFileRequest

from tgdl import chunked, downloader, monitor_flow
from tgdl.client import DiagnosticTelegramClient
from tgdl.monitor_flow import MonitorFlow, MonitorPause, SharedSpeedLimiter
from tgdl.monitor_rpc import monitor_media_requests
from tests.test_download_integrity import message


class Recorder:
    enabled = True
    generation = 1

    def __init__(self):
        self.waits = []
        self.sizes = []

    def on_wait(self, sent, seconds, kind):
        self.waits.append((kind, seconds))

    def on_success(self, sent, amount):
        self.sizes.append(amount)


class Sender:
    def send(self, request, ordered=False):
        loop = asyncio.get_running_loop()
        self.result = ([loop.create_future() for _ in request]
                       if isinstance(request, (list, tuple)) else loop.create_future())
        self.ordered = ordered
        return self.result


def offline():
    client = object.__new__(DiagnosticTelegramClient)
    client._tgdl_flood_counts = {"FLOOD_WAIT": [0, 0], "FLOOD_PREMIUM_WAIT": [0, 0]}
    client._tgdl_rpc_counts = {}
    sender = Sender()
    client._tgdl_observe_sender(sender)
    return client, sender


def test_borrowed_sender_preserves_batch_identity_and_gates_outcomes(monkeypatch):
    async def run():
        client, main = offline()
        borrowed = Sender()

        async def borrow(self, dc_id):
            return borrowed

        monkeypatch.setattr(TelegramClient, "_borrow_exported_sender", borrow)
        sender = await client._borrow_exported_sender(4)
        assert sender is borrowed
        policy = Recorder()
        req = object.__new__(GetFileRequest)
        with monitor_media_requests(client, policy):
            batch = sender.send([req, req, object(), req, req, req], ordered=True)
            assert batch is sender.result and sender.ordered
            flood = FloodPremiumWaitError(req, capture=8)
            ordinary = RpcCallFailError(req)
            batch[0].set_result(SimpleNamespace(bytes=b"OK"))
            batch[1].set_exception(flood)
            batch[2].set_exception(FloodWaitError(None, capture=30))  # not GetFile
            batch[3].set_exception(ordinary)
            batch[4].cancel()
            batch[5].set_result(SimpleNamespace(bytes=b""))
            await asyncio.sleep(0)
            assert batch[1].exception() is flood and batch[3].exception() is ordinary
            assert isinstance(batch[2].exception(), FloodWaitError)
            assert policy.waits == [("FLOOD_PREMIUM_WAIT", 8)]
            assert policy.sizes == [2]

            old = main.send(req)
            policy.enabled = False
            policy.generation += 1
            off = main.send(req)
            policy.enabled = True
            policy.generation += 1
            old.set_exception(FloodWaitError(req, capture=10))
            off.set_result(SimpleNamespace(bytes=b"during disable"))
            across_reconnect = main.send(req)
            main._send_loop_handle = object()  # Telethon did an automatic reconnect
            across_reconnect.set_result(SimpleNamespace(bytes=b"slow resend"))
            retired = main.send(req)
            await asyncio.sleep(0)
        retired.set_exception(FloodWaitError(req, capture=30))
        await asyncio.sleep(0)
        assert policy.waits == [("FLOOD_PREMIUM_WAIT", 8)]
        assert policy.sizes == [2]
        assert client.flood_snapshot() == {"FLOOD_WAIT": (2, 40), "FLOOD_PREMIUM_WAIT": (1, 8)}
        assert client.rpc_snapshot() == {"RpcCallFailError": 1}

    asyncio.run(run())


@pytest.mark.parametrize("chunk_monitor", [False, True])
def test_real_file_paths_isolate_concurrent_manual_and_other_account(tmp_path, monkeypatch, chunk_monitor):
    async def run():
        client, sender = offline()
        other, other_sender = offline()
        req = object.__new__(GetFileRequest)
        policy = Recorder()
        limiter = SharedSpeedLimiter()
        limiter.auto_throttle = policy
        flow = MonitorFlow(MonitorPause(), limiter, lambda: 2)
        # Exercise monitor chunk tasks and wait_for(anext), not just a direct call.
        if chunk_monitor:
            monkeypatch.setattr(chunked, "MIN_CHUNKED_BYTES", 4)
            monkeypatch.setattr(monitor_flow, "REQUEST_BYTES", 2)
            monkeypatch.setattr(monitor_flow, "WINDOW_BYTES", 2)
        entered = asyncio.Event()
        manual_done = asyncio.Event()

        async def rpc(s, result):
            f = s.send(req)
            if isinstance(result, Exception):
                f.set_exception(result)
            else:
                f.set_result(SimpleNamespace(bytes=result))
            try:
                await f
            except (FloodWaitError, FloodPremiumWaitError):
                pass  # model Telethon handling/retrying internally
            await asyncio.sleep(0)  # drain observation before leaving media scope

        async def monitor_block():
            entered.set()
            await manual_done.wait()
            await rpc(sender, FloodWaitError(req, capture=5))
            await rpc(sender, b"xx")
            # Even a sender used from this task must own the same client.
            await rpc(other_sender, FloodWaitError(req, capture=7))

        async def download_media(target, *, file, progress_callback):
            if target.id == 1:
                await monitor_block()
            else:
                await entered.wait()
                await rpc(sender, FloodPremiumWaitError(req, capture=13))
                await rpc(sender, b"manual")
                manual_done.set()
            Path(file).write_bytes(b"xxxx")
            return file

        def iter_download(*args, **kwargs):
            async def stream():
                await monitor_block()
                yield b"xx"
            return stream()

        client.download_media = download_media
        client.iter_download = iter_download

        async def transfer(mid, flow):
            msg = message(4)
            msg.id = mid
            return await downloader._download_one(
                client, msg, tmp_path, str(mid), monitor_flow=flow, skip_existing=False,
                stats=downloader.DownloadStats(),
            )

        paths = await asyncio.wait_for(asyncio.gather(transfer(1, flow), transfer(2, None)), 2)
        assert all(path.read_bytes() == b"xxxx" for path in paths)
        expected = 2 if chunk_monitor else 1
        assert policy.waits == [("FLOOD_WAIT", 5)] * expected
        assert policy.sizes == [2] * expected
        assert client.flood_snapshot()["FLOOD_PREMIUM_WAIT"] == (1, 13)
        assert other.flood_snapshot()["FLOOD_WAIT"] == (expected, 7 * expected)

    asyncio.run(run())


def test_cdn_media_counts_only_successful_bodies_never_cdn_floods(monkeypatch):
    async def run():
        account, main = offline()
        cdn, child = offline()

        async def get_cdn(self, redirect):
            return cdn

        monkeypatch.setattr(TelegramClient, "_get_cdn_client", get_cdn)
        assert await account._get_cdn_client(object()) is cdn
        req = object.__new__(GetCdnFileRequest)
        policy = Recorder()
        with monitor_media_requests(account, policy):
            for sender in (main, child):
                ok = sender.send(req)
                ok.set_result(SimpleNamespace(bytes=b"cdn"))
                failure = sender.send(req)
                error = FloodWaitError(req, capture=7)
                failure.set_exception(error)
                await asyncio.sleep(0)
                assert failure.exception() is error
        outside = child.send(req)
        outside.set_result(SimpleNamespace(bytes=b"manual"))
        await asyncio.sleep(0)
        assert policy.sizes == [3, 3] and policy.waits == []
        assert account.flood_snapshot()["FLOOD_WAIT"] == cdn.flood_snapshot()["FLOOD_WAIT"] == (0, 0)

    asyncio.run(run())
