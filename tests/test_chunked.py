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


def test_chunked_completion_includes_io_stage_timings(tmp_path):
    from tgdl import downloader
    from types import SimpleNamespace

    data = b"x" * (16 * 1024 * 1024 + 3)
    document = Document(len(data))
    client = FakeClient(data)
    message = SimpleNamespace(id=42, media=SimpleNamespace(document=document))
    stats = downloader.DownloadStats()
    logs = []
    target = asyncio.run(downloader._download_one(
        client, message, tmp_path, "20261119_42",
        skip_existing=True, stats=stats, chunk_concurrency=4,
        on_saved=logs.append,
    ))
    assert target is not None and target.read_bytes() == data
    assert not list(tmp_path.glob("tgdl-*"))
    assert stats.saved == 1
    assert "分块4路" in logs[0]
    assert "文件准备" in logs[0] and "分块传输" in logs[0] and "最终保存" in logs[0]

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

    class Media:
        document = Document(32 * 1024 * 1024)

    class Message:
        media = Media()
        id = 123

    class Client:
        def iter_download(self, *args, **kwargs):
            raise AssertionError("Stubbed download_chunked must be used")

        async def download_media(self, message, *, file, progress_callback):
            path = __import__("pathlib").Path(file)
            path.write_bytes(b"complete-regular-download")
            return str(path)

    async def fail_chunks(*args, **kwargs):
        raise ChunkDownloadStalled("simulated chunk timeout")

    monkeypatch.setattr(downloader, "download_chunked", fail_chunks)
    stats = downloader.DownloadStats()
    path = asyncio.run(downloader._download_one(
        Client(), Message(), tmp_path, "20261119_123",
        skip_existing=True, stats=stats, chunk_concurrency=4,
    ))
    assert path is not None
    assert path.name == "20261119_123.mp4"
    assert path.read_bytes() == b"complete-regular-download"
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
