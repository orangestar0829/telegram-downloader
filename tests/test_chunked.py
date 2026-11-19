"""Offline tests for byte-range assembly without a Telegram login."""
import asyncio
import pytest
from tgdl.chunked import REQUEST_BYTES, byte_ranges, download_chunked, chunkable_document

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
    asyncio.run(download_chunked(client, Document(len(data)), dest, parts=4))
    assert dest.read_bytes() == data
    assert len(client.streams) >= 2
    assert all(stream.closed for stream in client.streams)

def test_truncated_download_rejected(tmp_path):
    data = b"x" * (REQUEST_BYTES * 4)
    client = FakeClient(data[:-100])
    with pytest.raises(IOError, match="Incomplete range"):
        asyncio.run(download_chunked(client, Document(len(data)), tmp_path / "bad.bin", parts=4))

def test_worker_exception_propagates(tmp_path):
    data = b"x" * (REQUEST_BYTES * 4)
    client = FakeClient(data, fail_offset=REQUEST_BYTES)
    with pytest.raises(IOError, match="network failure"):
        asyncio.run(download_chunked(client, Document(len(data)), tmp_path / "bad.bin", parts=4))
    assert all(stream.closed for stream in client.streams)

def test_small_media_fallback():
    assert chunkable_document(Document(1024), 4) is None
