from __future__ import annotations

from telethon import TelegramClient
from telethon.errors import FloodPremiumWaitError, FloodWaitError
from telethon.tl.functions.upload import GetFileRequest

from tgdl.config import SESSION_PATH, Settings


class DiagnosticTelegramClient(TelegramClient):
    """Observe actual RPC exceptions before Telethon handles/retries them.

    The send result and error are returned to Telethon unchanged. Borrowed
    media DC senders are instrumented independently of the primary sender.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tgdl_flood_counts = {"FLOOD_WAIT": [0, 0], "FLOOD_PREMIUM_WAIT": [0, 0]}
        self._tgdl_observe_sender(self._sender)

    def flood_snapshot(self) -> dict[str, tuple[int, int]]:
        return {name: tuple(values) for name, values in self._tgdl_flood_counts.items()}

    def _tgdl_observe_sender(self, sender) -> None:
        if getattr(sender, "_tgdl_observed", False):
            return
        send_original = sender.send

        def observing_send(request, ordered=False):
            result = send_original(request, ordered=ordered)
            requests = list(request) if isinstance(request, (tuple, list)) else [request]
            futures = result if isinstance(result, list) else [result]
            for req, future in zip(requests, futures):
                if isinstance(req, GetFileRequest):
                    future.add_done_callback(self._tgdl_record_getfile_result)
            return result

        sender.send = observing_send
        sender._tgdl_observed = True

    def _tgdl_record_getfile_result(self, future) -> None:
        if future.cancelled():
            return
        exc = future.exception()
        if not isinstance(exc, (FloodWaitError, FloodPremiumWaitError)):
            return
        kind = "FLOOD_PREMIUM_WAIT" if isinstance(exc, FloodPremiumWaitError) else "FLOOD_WAIT"
        count, seconds = self._tgdl_flood_counts[kind]
        self._tgdl_flood_counts[kind] = [count + 1, seconds + max(0, exc.seconds)]
        print(f"[网络诊断] 服务端原始响应 {kind} {exc.seconds}s (GetFileRequest) | "
              f"累计{count + 1}次，要求等待累计{seconds + max(0, exc.seconds)}s")

    async def _borrow_exported_sender(self, dc_id):
        sender = await super()._borrow_exported_sender(dc_id)
        self._tgdl_observe_sender(sender)
        return sender


def build_client(settings: Settings) -> TelegramClient:
    # Telethon 1.x accepts a SOCKS5 proxy tuple. Use python-socks for async sockets.
    proxy = None
    if settings.proxy_host:
        proxy = (
            "socks5",
            settings.proxy_host,
            settings.proxy_port or 1080,
            True,
            settings.proxy_username,
            settings.proxy_password,
        )
    return DiagnosticTelegramClient(
        str(SESSION_PATH),
        settings.api_id,
        settings.api_hash,
        proxy=proxy,
        device_model="TG Media Downloader",
        app_version="1.0.0",
        system_version="Windows",
    )


async def ensure_login(client: TelegramClient, phone: str | None) -> None:
    await client.start(phone=phone)
    me = await client.get_me()
    if me is None:
        raise SystemExit("Login failed.")
    name = " ".join(part for part in (me.first_name, me.last_name) if part)
    username = f"@{me.username}" if me.username else ""
    print(f"Logged in as {name} {username} (id={me.id})".strip())
