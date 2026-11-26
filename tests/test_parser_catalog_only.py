import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from telethon.tl.types import Message

from tgdl import parser


def _fake_message():
    message = Mock(spec=Message)
    message.id = 23
    message.date = datetime(2026, 11, 22, tzinfo=timezone.utc)
    message.raw_text = "private sample caption"
    message.web_preview = None
    message.media = None
    return message


def _setup(monkeypatch):
    async def resolve(*_args):
        return SimpleNamespace(id=42, title="channel")

    class Client:
        async def iter_messages(self, *_args, **_kwargs):
            yield _fake_message()

    monkeypatch.setattr(parser, "resolve_channel", resolve)
    monkeypatch.setattr(parser, "classify_message", lambda _message: "photo")
    monkeypatch.setattr(parser, "urls_from_message", lambda _message: [
        "https://t.me/test/42", "https://example.com/test.mp4",
    ])
    return Client()


def test_catalog_only_never_downloads_linked_posts_previews_or_http(tmp_path, monkeypatch):
    client = _setup(monkeypatch)

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("catalog-only must not download anything")

    for name in ("_download_one", "download_http_asset", "download_torrent", "_try_telegram_post"):
        monkeypatch.setattr(parser, name, forbidden)
    stats = asyncio.run(parser.parse_channel(
        client, target="channel", download_dir=tmp_path, limit=None,
        since=None, until=None, catalog_only=True,
    ))
    assert stats.failed == 0
    assert stats.catalogued == 2
    path = next(tmp_path.rglob("links.jsonl"))
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert all(not record["downloaded"] for record in records)
    assert sorted(p.name for p in path.parent.rglob("*") if p.is_file()) == ["links.jsonl", "links.md"]


def test_gui_dry_run_does_not_print_captions_or_urls(tmp_path, monkeypatch, capsys):
    client = _setup(monkeypatch)
    asyncio.run(parser.parse_channel(
        client, target="channel", download_dir=tmp_path, limit=None,
        since=None, until=None, dry_run=True, show_sensitive_details=False,
    ))
    output = capsys.readouterr().out
    assert "private sample caption" not in output
    assert "https://" not in output
    assert "preview/photo" in output
    assert not list(tmp_path.rglob("links.jsonl"))
