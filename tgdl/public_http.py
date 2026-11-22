"""Limit automatic HTTP fetches of message links to public destinations."""
from __future__ import annotations

import ipaddress
import socket
from contextlib import asynccontextmanager

from aiohttp import ClientSession, TCPConnector
from aiohttp.abc import AbstractResolver
from aiohttp.resolver import ThreadedResolver
from yarl import URL


class ExternalLinkBlocked(ValueError):
    """The link is not appropriate for an unattended network request."""


def _is_public(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return address.is_global and not address.is_multicast and not address.is_reserved


def validate_public_url(value: str | URL) -> URL:
    try:
        url = URL(value)
        host = url.host
    except ValueError as exc:
        raise ExternalLinkBlocked("Invalid external HTTP link") from exc
    if url.scheme not in {"http", "https"} or not host or url.user is not None:
        raise ExternalLinkBlocked("External links must use HTTP(S) without credentials")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return url
    if not _is_public(address):
        raise ExternalLinkBlocked("External link points to a non-public address")
    return url


class PublicAddressResolver(AbstractResolver):
    """Validate the exact address records passed to the socket connector."""

    def __init__(self):
        self._delegate = ThreadedResolver()

    async def resolve(self, host: str, port: int = 0, family: int = socket.AF_INET):
        rows = await self._delegate.resolve(host, port, family)
        if not rows or any(not _is_public(ipaddress.ip_address(row["host"])) for row in rows):
            raise ExternalLinkBlocked("External link resolved to a non-public address")
        return rows

    async def close(self) -> None:
        await self._delegate.close()


@asynccontextmanager
async def public_session(**kwargs):
    resolver = PublicAddressResolver()
    try:
        async with ClientSession(connector=TCPConnector(resolver=resolver), **kwargs) as session:
            yield session
    finally:
        await resolver.close()

@asynccontextmanager
async def public_response(session, url: str, *, max_redirects: int = 5, **kwargs):
    current = validate_public_url(url)
    for count in range(max_redirects + 1):
        async with session.get(str(current), allow_redirects=False, **kwargs) as response:
            if response.status not in {301, 302, 303, 307, 308} or "Location" not in response.headers:
                yield response
                return
            if count == max_redirects:
                break
            current = validate_public_url(current.join(URL(response.headers["Location"])))
    raise ExternalLinkBlocked("External link exceeded the redirect limit")
