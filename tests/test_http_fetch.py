import asyncio
from types import SimpleNamespace

import aiohttp
import pytest

from tgdl import http_fetch


class Response:
    status = 200
    content_type = "video/mp4"
    url = "https://example.invalid/video.mp4"
    headers = {}

    def __init__(self, chunks, length):
        self.content = SimpleNamespace(iter_chunked=lambda _: chunks())
        self.content_length = length

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        pass


async def fetch(tmp_path, chunks, length):
    session = SimpleNamespace(get=lambda *a, **kw: Response(chunks, length))
    return await http_fetch.download_http_asset(
        session, Response.url, tmp_path / "media", tmp_path / "torrents", "message",
    )


def test_interrupted_http_download_leaves_no_final_or_partial_file(tmp_path):
    async def chunks():
        yield b"abc"
        raise ConnectionError("connection lost")

    with pytest.raises(ConnectionError):
        asyncio.run(fetch(tmp_path, chunks, 6))
    assert list((tmp_path / "media").iterdir()) == []


def test_http_cancel_before_publication_cleans_staging(tmp_path):
    async def scenario():
        downloaded = asyncio.Event()

        async def chunks():
            yield b"abc"
            downloaded.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(fetch(tmp_path, chunks, 6))
        await asyncio.wait_for(downloaded.wait(), timeout=2)
        assert not (tmp_path / "media" / "message.mp4").exists()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert list((tmp_path / "media").iterdir()) == []

    asyncio.run(scenario())


def test_http_validates_known_length_before_atomic_publish(tmp_path):
    async def chunks():
        yield b"abc"

    with pytest.raises(aiohttp.ClientPayloadError):
        asyncio.run(fetch(tmp_path, chunks, 6))
    assert list((tmp_path / "media").iterdir()) == []
    result, path = asyncio.run(fetch(tmp_path, chunks, 3))
    assert result == "media"
    assert path.name == "message.mp4"
    assert path.read_bytes() == b"abc"


def test_http_byte_limit_uses_streamed_bytes_not_buffered_file_size(tmp_path, monkeypatch):
    async def chunks():
        yield b"abc"
        yield b"def"

    monkeypatch.setattr(http_fetch, "MAX_BYTES", 4)
    assert asyncio.run(fetch(tmp_path, chunks, None)) == ("too-large", None)
    assert list((tmp_path / "media").iterdir()) == []


def test_http_media_never_retains_executable_extension(tmp_path, monkeypatch):
    async def chunks():
        yield b"abc"

    monkeypatch.setattr(Response, "headers", {
        "Content-Disposition": 'attachment; filename="video.exe"',
    })
    kind, path = asyncio.run(fetch(tmp_path, chunks, 3))
    assert kind == "media"
    assert path.suffix == ".mp4"
