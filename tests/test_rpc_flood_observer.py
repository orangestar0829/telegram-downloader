"""Record original RPC errors without changing the Telethon future result."""
import asyncio

from telethon.errors import FloodPremiumWaitError, FloodWaitError
from telethon.tl.functions.upload import GetFileRequest
from tgdl.client import DiagnosticTelegramClient


class FakeSender:
    def __init__(self):
        self.pending = []

    def send(self, request, ordered=False):
        future = asyncio.get_running_loop().create_future()
        self.pending.append(future)
        return future


def test_flood_types_are_counted_separately(capsys):
    async def run():
        # Skip live credentials/network; inspect the sender-future hook only.
        client = object.__new__(DiagnosticTelegramClient)
        client._tgdl_flood_counts = {"FLOOD_WAIT": [0, 0], "FLOOD_PREMIUM_WAIT": [0, 0]}
        sender = FakeSender()
        client._tgdl_observe_sender(sender)
        req = object.__new__(GetFileRequest)
        for kind, duration in ((FloodWaitError, 4), (FloodPremiumWaitError, 10)):
            future = sender.send(req)
            future.set_exception(kind(request=req, capture=duration))
            await asyncio.sleep(0)
            assert isinstance(future.exception(), kind)
        assert client.flood_snapshot() == {
            "FLOOD_WAIT": (1, 4),
            "FLOOD_PREMIUM_WAIT": (1, 10),
        }
        # Reinstalling the observer cannot double count the same request.
        client._tgdl_observe_sender(sender)

    asyncio.run(run())
    output = capsys.readouterr().out
    assert "原始响应 FLOOD_WAIT 4s" in output
    assert "原始响应 FLOOD_PREMIUM_WAIT 10s" in output


def test_ordinary_getfile_result_and_other_requests_not_counted():
    async def run():
        client = object.__new__(DiagnosticTelegramClient)
        client._tgdl_flood_counts = {"FLOOD_WAIT": [0, 0], "FLOOD_PREMIUM_WAIT": [0, 0]}
        sender = FakeSender()
        client._tgdl_observe_sender(sender)
        future = sender.send(object())
        future.set_result("ok")
        await asyncio.sleep(0)
        assert future.result() == "ok"
        assert client.flood_snapshot() == {
            "FLOOD_WAIT": (0, 0),
            "FLOOD_PREMIUM_WAIT": (0, 0),
        }

    asyncio.run(run())
