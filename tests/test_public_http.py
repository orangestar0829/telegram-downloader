import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from tgdl.public_http import ExternalLinkBlocked, PublicAddressResolver, public_response


def test_external_link_checks_redirect_before_requesting_next_hop():
    requested = []

    class Session:
        @asynccontextmanager
        async def get(self, url, **kwargs):
            assert kwargs["allow_redirects"] is False
            requested.append(url)
            yield SimpleNamespace(status=302, headers={
                "Location": "http://127.0.0.1/",
            })

    async def run():
        with pytest.raises(ExternalLinkBlocked):
            async with public_response(Session(), "https://example.com/start"):
                pytest.fail("A forbidden redirect must never yield a response")

    asyncio.run(run())
    assert requested == ["https://example.com/start"]


def test_external_connector_rejects_private_dns_answers(monkeypatch):
    async def run():
        resolver = PublicAddressResolver()

        async def answer(*args):
            return [{"host": "10.0.0.10"}, {"host": "1.1.1.1"}]

        monkeypatch.setattr(resolver._delegate, "resolve", answer)
        try:
            with pytest.raises(ExternalLinkBlocked):
                await resolver.resolve("example.com")
        finally:
            await resolver.close()

    asyncio.run(run())
