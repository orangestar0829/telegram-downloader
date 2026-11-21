"""Cooperative pause, aggregate speed limits, and live chunk lanes."""

import asyncio
import time
from types import SimpleNamespace

from tgdl import monitor_flow


def test_pause_acknowledges_only_after_registered_lane_stops():
    async def scenario():
        pause = monitor_flow.MonitorPause()
        started = asyncio.Event()
        transferred = []

        async def transfer():
            pause.register()
            try:
                started.set()
                for i in range(4):
                    await pause.checkpoint()
                    transferred.append(i)
                    await asyncio.sleep(0.04)
            finally:
                pause.unregister()

        worker = asyncio.create_task(transfer())
        await started.wait()
        pause.pause()
        await asyncio.wait_for(pause.wait_suspended(), 2)
        before = len(transferred)
        assert pause.suspended
        await asyncio.sleep(0.15)
        assert len(transferred) == before
        pause.resume()
        await asyncio.wait_for(worker, 3)
        assert transferred == [0, 1, 2, 3]

    asyncio.run(scenario())


def test_aggregate_speed_limit_is_shared():
    async def scenario():
        limiter = monitor_flow.SharedSpeedLimiter(1024 * 1024)
        start = time.monotonic()
        await asyncio.gather(
            limiter.consume(512 * 1024),
            limiter.consume(512 * 1024),
        )
        assert time.monotonic() - start >= 0.88
        limiter.bytes_per_second = 0
        await asyncio.wait_for(limiter.consume(512 * 1024), 0.5)

    asyncio.run(scenario())


def test_speed_limit_change_wakes_existing_download_quickly():
    async def scenario():
        limiter = monitor_flow.SharedSpeedLimiter(1024)
        task = asyncio.create_task(limiter.consume(monitor_flow.REQUEST_BYTES))
        await asyncio.sleep(0.1)
        assert not task.done()
        limiter.bytes_per_second = 0
        await asyncio.wait_for(task, 0.5)

    asyncio.run(scenario())


def test_document_dynamic_lanes_and_pause(tmp_path, monkeypatch):
    monkeypatch.setattr(monitor_flow, "WINDOW_BYTES", monitor_flow.REQUEST_BYTES * 2)
    size = monitor_flow.REQUEST_BYTES * 8
    document = SimpleNamespace(size=size)

    class Client:
        def __init__(self):
            self.starts = []

        def iter_download(self, doc, *, offset, limit, **kwargs):
            self.starts.append(offset)

            async def blocks():
                for number in range(limit):
                    await asyncio.sleep(0.02)
                    yield bytes([(offset // monitor_flow.REQUEST_BYTES + number) % 256]) * monitor_flow.REQUEST_BYTES
            return blocks()

    async def scenario():
        client = Client()
        pause = monitor_flow.MonitorPause()
        limiter = monitor_flow.SharedSpeedLimiter(0)
        active_lanes = 1
        flow = monitor_flow.MonitorFlow(pause, limiter, lambda: active_lanes)
        completed = []
        download = asyncio.create_task(monitor_flow.monitor_download_document(
            client, document, tmp_path / "download.bin", flow,
            on_bytes=completed.append,
        ))
        await asyncio.sleep(0.03)
        active_lanes = 2
        for _ in range(30):
            if len(client.starts) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(client.starts) >= 2, "increasing lanes should start another window"
        pause.pause()
        await asyncio.wait_for(pause.wait_suspended(), 3)
        before = len(completed)
        await asyncio.sleep(0.1)
        assert len(completed) == before, "paused workers must stop writing"
        pause.resume()
        path = await asyncio.wait_for(download, 4)
        assert path.stat().st_size == size
        assert sum(completed) == size
        content = path.read_bytes()
        for index in range(8):
            start = index * monitor_flow.REQUEST_BYTES
            assert content[start:start + 8] == bytes([index]) * 8

    asyncio.run(scenario())


def test_reducing_lanes_hands_unfinished_range_to_active_lane(tmp_path, monkeypatch):
    monkeypatch.setattr(monitor_flow, "WINDOW_BYTES", monitor_flow.REQUEST_BYTES * 4)
    size = monitor_flow.REQUEST_BYTES * 8
    first_window = monitor_flow.WINDOW_BYTES
    document = SimpleNamespace(size=size)

    class Client:
        def __init__(self):
            self.starts = []

        def iter_download(self, doc, *, offset, limit, **kwargs):
            self.starts.append(offset)

            async def blocks():
                for i in range(limit):
                    await asyncio.sleep(0.05)
                    index = offset // monitor_flow.REQUEST_BYTES + i
                    yield bytes([index]) * monitor_flow.REQUEST_BYTES

            return blocks()

    async def scenario():
        client = Client()
        lanes = 2
        flow = monitor_flow.MonitorFlow(
            monitor_flow.MonitorPause(),
            monitor_flow.SharedSpeedLimiter(0),
            lambda: lanes,
        )
        written = []
        task = asyncio.create_task(monitor_flow.monitor_download_document(
            client, document, tmp_path / "lanes.bin", flow,
            on_bytes=written.append,
        ))
        for _ in range(30):
            if len(client.starts) >= 2:
                break
            await asyncio.sleep(0.01)
        assert {0, first_window}.issubset(set(client.starts))
        lanes = 1
        path = await asyncio.wait_for(task, 5)
        assert any(first_window < offset < size for offset in client.starts), (
            "the first lane must receive the remaining part of a yielded window"
        )
        assert sum(written) == size
        content = path.read_bytes()
        for i in range(8):
            start = i * monitor_flow.REQUEST_BYTES
            assert content[start:start + 16] == bytes([i]) * 16

    asyncio.run(scenario())


def test_window_timeout_retries_from_last_committed_offset(tmp_path, monkeypatch):
    monkeypatch.setattr(monitor_flow, "WINDOW_BYTES", monitor_flow.REQUEST_BYTES * 2)
    monkeypatch.setattr(monitor_flow, "CHUNK_IDLE_TIMEOUT", 0.04)
    monkeypatch.setattr(monitor_flow, "CHUNK_RETRIES", 1)
    document = SimpleNamespace(size=2 * monitor_flow.REQUEST_BYTES)

    class Client:
        def __init__(self):
            self.starts = []

        def iter_download(self, doc, *, offset, limit, **kwargs):
            self.starts.append(offset)

            async def blocks():
                if len(self.starts) == 1:
                    yield b"A" * monitor_flow.REQUEST_BYTES
                    await asyncio.sleep(0.1)  # Fail after one safely written block.
                else:
                    yield b"B" * monitor_flow.REQUEST_BYTES
            return blocks()

    async def scenario():
        client = Client()
        flow = monitor_flow.MonitorFlow(
            monitor_flow.MonitorPause(),
            monitor_flow.SharedSpeedLimiter(0),
            lambda: 1,
        )
        written = []
        path = await asyncio.wait_for(
            monitor_flow.monitor_download_document(
                client, document, tmp_path / "retry.bin", flow,
                on_bytes=written.append,
            ),
            timeout=4,
        )
        assert client.starts == [0, monitor_flow.REQUEST_BYTES]
        assert sum(written) == document.size
        content = path.read_bytes()
        assert content[:8] == b"A" * 8
        assert content[-8:] == b"B" * 8

    asyncio.run(scenario())
