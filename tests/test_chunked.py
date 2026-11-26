"""Offline tests for byte-range assembly without a Telegram login."""
import asyncio
import pytest
from tgdl.chunked import (
    ChunkDownloadStalled, REQUEST_BYTES, byte_ranges, download_chunked,
    chunkable_document,
)

class Document:
    mime_type = "video/mp4"
    file_reference = b"abc"
    def __init__(self, size):
        self.size = size

class Stream:
    def __init__(self, data, start, n, fail=False):
        self.data, self.pos, self.n, self.fail = data, start, n, fail
        self.closed = False
    def __aiter__(self):
        return self
    async def __anext__(self):
        if self.fail:
            raise IOError("network failure")
        if self.pos >= len(self.data) or self.n <= 0:
            raise StopAsyncIteration
        start = self.pos
        self.pos += REQUEST_BYTES
        self.n -= 1
        await asyncio.sleep(0)
        return self.data[start:self.pos]
    async def close(self):
        self.closed = True

class FakeClient:
    def __init__(self, data, fail_offset=None):
        self.data, self.fail_offset = data, fail_offset
        self.streams = []
    def iter_download(self, document, *, offset, limit, chunk_size, request_size, file_size):
        assert chunk_size == request_size == REQUEST_BYTES
        assert document.size == file_size
        stream = Stream(self.data, offset, limit, offset == self.fail_offset)
        self.streams.append(stream)
        return stream

def test_ranges():
    for size in (1, REQUEST_BYTES - 1, REQUEST_BYTES + 1, 7 * REQUEST_BYTES + 11):
        for parts in (1, 2, 4, 8):
            chunks = byte_ranges(size, parts)
            assert chunks[0][0] == 0 and chunks[-1][1] == size
            assert all(a < b for a, b in chunks)
            assert all(end == nxt[0] for (_, end), nxt in zip(chunks, chunks[1:]))
            assert all(a % REQUEST_BYTES == 0 for a, _ in chunks)

def test_exact_parallel_assembly(tmp_path):
    data = bytes(range(256)) * (REQUEST_BYTES // 256 * 5) + b"tail"
    client = FakeClient(data)
    dest = tmp_path / "video.mp4"
    timings = {}
    asyncio.run(download_chunked(client, Document(len(data)), dest, parts=4, timings=timings))
    assert dest.read_bytes() == data
    assert timings["prepare_s"] >= 0
    assert timings["transfer_s"] >= 0
    assert len(client.streams) >= 2
    assert all(stream.closed for stream in client.streams)


def test_idle_diagnostic_reports_pause_and_recovery(tmp_path, capsys):
    class DelayedStream(Stream):
        async def __anext__(self):
            await asyncio.sleep(0.045)
            return await super().__anext__()

    class DelayedClient(FakeClient):
        def iter_download(self, document, *, offset, limit, chunk_size, request_size, file_size):
            stream = DelayedStream(self.data, offset, limit)
            self.streams.append(stream)
            return stream

    data = b"a" * REQUEST_BYTES
    output = tmp_path / "delayed.mp4"
    asyncio.run(download_chunked(
        DelayedClient(data), Document(len(data)), output,
        parts=1, idle_timeout=0.5, diagnostic_label="test/11",
        diagnostic_idle_seconds=0.01, diagnostic_repeat_seconds=0.2,
    ))
    report = capsys.readouterr().out
    assert "连续" in report and "未收到数据" in report
    assert "数据恢复" in report
    assert output.read_bytes() == data


def test_idle_diagnostic_watcher_stops_when_cancelled(tmp_path):
    class NeverStream(Stream):
        async def __anext__(self):
            await asyncio.sleep(60)
            return await super().__anext__()

    class NeverClient(FakeClient):
        def iter_download(self, document, *, offset, limit, chunk_size, request_size, file_size):
            stream = NeverStream(self.data, offset, limit)
            self.streams.append(stream)
            return stream

    async def run():
        task = asyncio.create_task(download_chunked(
            NeverClient(b"x" * REQUEST_BYTES), Document(REQUEST_BYTES),
            tmp_path / "cancel.mp4", parts=1, idle_timeout=50,
            diagnostic_idle_seconds=0.01,
        ))
        await asyncio.sleep(0.03)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not any(
            "monitor_ranges" in repr(t.get_coro()) for t in asyncio.all_tasks()
            if t is not asyncio.current_task()
        )

    asyncio.run(run())


def test_chunked_completion_includes_io_stage_timings(tmp_path):
    from tgdl import downloader
    from types import SimpleNamespace

    data = b"x" * (16 * 1024 * 1024 + 3)
    document = Document(len(data))
    client = FakeClient(data)
    message = SimpleNamespace(id=42, media=SimpleNamespace(document=document))
    stats = downloader.DownloadStats()
    logs = []
    events = []
    target = asyncio.run(downloader._download_one(
        client, message, tmp_path, "20261119_42",
        skip_existing=True, stats=stats, chunk_concurrency=4,
        on_saved=logs.append,
        on_progress=lambda *values: events.append(values),
    ))
    assert target is not None and target.read_bytes() == data
    assert not list(tmp_path.glob("tgdl-*"))
    assert stats.saved == 1
    assert "分块4路" in logs[0]
    assert "文件准备" in logs[0] and "分块传输" in logs[0] and "最终保存" in logs[0]
    assert any(event[4] == "下载中" and event[2] > 0 for event in events)
    assert any(event[4] == "正在保存" for event in events)
    assert events[-1][4] == "完成"
    assert events[-1][2] == len(data)
    assert events[-1][3] == len(data)


def test_ssd_staging_then_atomic_publish_to_other_volume(tmp_path, monkeypatch):
    from tgdl import cache_budget, downloader
    from types import SimpleNamespace

    data = b"z" * (16 * 1024 * 1024 + 17)
    client = FakeClient(data)
    message = SimpleNamespace(
        id=99, media=SimpleNamespace(document=Document(len(data)))
    )
    output = tmp_path / "hdd" / "videos"
    staging = tmp_path / "ssd" / "staging"
    monkeypatch.setattr(downloader, "same_volume", lambda src, dst: False)
    monkeypatch.setattr(cache_budget.shutil, "disk_usage",
                        lambda path: SimpleNamespace(free=40 * cache_budget.GIB))
    logs = []
    result = asyncio.run(downloader._download_one(
        client, message, output, "20261119_99",
        skip_existing=True, stats=downloader.DownloadStats(),
        chunk_concurrency=4, on_saved=logs.append,
        temp_download_dir=staging,
    ))
    assert result is not None and result.read_bytes() == data
    assert "最终保存" in logs[0]
    assert not list(staging.glob("tgdl-*"))
    assert not list(output.glob(".tgdl-publishing-*"))


def test_staging_without_enough_space_waits_and_can_be_cancelled(tmp_path, monkeypatch):
    from tgdl import downloader
    from types import SimpleNamespace

    data = b"x" * (16 * 1024 * 1024)
    client = FakeClient(data)
    message = SimpleNamespace(id=17, media=SimpleNamespace(document=Document(len(data))))
    staging = tmp_path / "ssd"
    monkeypatch.setattr(
        __import__("tgdl.cache_budget", fromlist=["shutil"]).shutil, "disk_usage",
        lambda path: SimpleNamespace(free=0),
    )
    output = tmp_path / "hdd"
    async def check_wait():
        task = asyncio.create_task(downloader._download_one(
            client, message, output, "20261119_17",
            skip_existing=True, stats=downloader.DownloadStats(),
            chunk_concurrency=4, temp_download_dir=staging,
        ))
        await asyncio.sleep(0.05)
        assert not task.done()
        assert not list(output.glob("tgdl-*"))
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(check_wait())
    assert not list(staging.glob("tgdl-*"))

def test_truncated_download_rejected(tmp_path):
    data = b"x" * (REQUEST_BYTES * 4)
    client = FakeClient(data[:-100])
    with pytest.raises(ChunkDownloadStalled, match="stalled"):
        asyncio.run(download_chunked(client, Document(len(data)), tmp_path / "bad.bin", parts=4, retries=0))

def test_worker_exception_propagates(tmp_path):
    data = b"x" * (REQUEST_BYTES * 4)
    client = FakeClient(data, fail_offset=REQUEST_BYTES)
    with pytest.raises(ChunkDownloadStalled, match="stalled"):
        asyncio.run(download_chunked(client, Document(len(data)), tmp_path / "bad.bin", parts=4, retries=0))
    assert all(stream.closed for stream in client.streams)

def test_small_media_fallback():
    assert chunkable_document(Document(1024), 4) is None


def test_stalled_parallel_download_falls_back_to_regular(tmp_path, monkeypatch):
    from tgdl import downloader

    from tgdl import chunked

    complete = b"complete-regular-download"
    monkeypatch.setattr(chunked, "MIN_CHUNKED_BYTES", len(complete))

    class Media:
        document = Document(len(complete))

    class Message:
        media = Media()
        id = 123

    class Client:
        def iter_download(self, *args, **kwargs):
            raise AssertionError("Stubbed download_chunked must be used")

        async def download_media(self, message, *, file, progress_callback):
            path = __import__("pathlib").Path(file)
            assert not (path.parent / "data.mp4").exists()
            path.write_bytes(complete)
            return str(path)

    async def fail_chunks(*args, **kwargs):
        args[2].write_bytes(b"partial preallocated chunk")
        raise ChunkDownloadStalled("simulated chunk timeout")

    monkeypatch.setattr(downloader, "download_chunked", fail_chunks)
    stats = downloader.DownloadStats()
    path = asyncio.run(downloader._download_one(
        Client(), Message(), tmp_path, "20261119_123",
        skip_existing=True, stats=stats, chunk_concurrency=4,
    ))
    assert path is not None
    assert path.name == "20261119_123.mp4"
    assert path.read_bytes() == complete
    assert stats.saved == 1 and stats.failed == 0
    assert not list(tmp_path.glob("tgdl-*"))


def test_stalled_stream_has_bounded_timeout_and_retries(tmp_path):
    class HungStream(Stream):
        async def __anext__(self):
            await asyncio.sleep(5)
            return b"x"

    class HungClient(FakeClient):
        def iter_download(self, document, *, offset, limit, chunk_size, request_size, file_size):
            stream = HungStream(self.data, offset, limit)
            self.streams.append(stream)
            return stream

    client = HungClient(b"x" * (REQUEST_BYTES * 2))
    with pytest.raises(ChunkDownloadStalled, match="stalled"):
        asyncio.run(download_chunked(
            client, Document(REQUEST_BYTES * 2), tmp_path / "stalled.bin",
            parts=2, idle_timeout=0.01, retries=1,
        ))
    assert len(client.streams) == 4
    assert all(stream.closed for stream in client.streams)


def test_resume_range_after_transient_failure(tmp_path):
    class FlakyStream(Stream):
        async def __anext__(self):
            if self.pos >= REQUEST_BYTES and not self.closed:
                raise OSError("connection reset")
            return await super().__anext__()

    class FlakyClient(FakeClient):
        def iter_download(self, document, *, offset, limit, chunk_size, request_size, file_size):
            stream = FlakyStream(self.data, offset, limit) if offset == 0 else Stream(
                self.data, offset, limit
            )
            self.streams.append(stream)
            return stream

    data = b"a" * (REQUEST_BYTES * 3)
    client = FlakyClient(data)
    target = tmp_path / "recovered.mp4"
    asyncio.run(download_chunked(
        client, Document(len(data)), target, parts=1,
        idle_timeout=0.1, retries=1,
    ))
    assert target.read_bytes() == data
