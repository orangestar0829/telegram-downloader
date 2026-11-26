from __future__ import annotations

from functools import partial

from telethon import TelegramClient
from telethon.errors import FloodPremiumWaitError, FloodWaitError
from telethon import errors
from telethon.tl.functions.upload import GetCdnFileRequest, GetFileRequest
from telethon.utils import parse_phone

from tgdl.monitor_rpc import MediaRequest, capture_request
from tgdl.config import SESSION_PATH, Settings
from tgdl.errors import TGDLInputError


class DiagnosticTelegramClient(TelegramClient):
    """Observe actual RPC exceptions before Telethon handles/retries them.

    The send result and error are returned to Telethon unchanged. Borrowed
    media DC senders are instrumented independently of the primary sender.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tgdl_flood_counts = {"FLOOD_WAIT": [0, 0], "FLOOD_PREMIUM_WAIT": [0, 0]}
        self._tgdl_rpc_counts: dict[str, int] = {}
        self._tgdl_reconnect_callbacks = set()
        self._tgdl_observe_sender(self._sender)

    def add_reconnect_callback(self, callback) -> None:
        """Subscribe to successful reconnects of the main Telegram transport."""
        self._tgdl_reconnect_callbacks.add(callback)

    def remove_reconnect_callback(self, callback) -> None:
        self._tgdl_reconnect_callbacks.discard(callback)

    async def _handle_auto_reconnect(self):
        # Telethon 1.46's public is_connected() remains true during automatic
        # reconnect; it has no public reconnect event. Hook the same callback
        # it registers with its main sender, preserving its normal handling.
        for callback in tuple(self._tgdl_reconnect_callbacks):
            try:
                callback()
            except Exception:
                self._log[__name__].exception("Telegram reconnect observer failed")
        await super()._handle_auto_reconnect()

    def flood_snapshot(self) -> dict[str, tuple[int, int]]:
        return {name: tuple(values) for name, values in self._tgdl_flood_counts.items()}

    def rpc_snapshot(self) -> dict[str, int]:
        return dict(self._tgdl_rpc_counts)

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
                    captured = capture_request(self, sender)
                    callback = self._tgdl_record_getfile_result
                    if captured is not None:
                        callback = partial(callback, captured=captured)
                    future.add_done_callback(callback)
                elif isinstance(req, GetCdnFileRequest):
                    # Successful CDN media also proves healthy downloading.
                    # Its errors are NOT GetFile incidents or diagnostic counts.
                    owner = getattr(self, "_tgdl_cdn_parent", self)
                    captured = capture_request(owner, sender)
                    if captured is not None:
                        future.add_done_callback(partial(self._tgdl_record_cdn_bytes, captured))
            return result

        sender.send = observing_send
        sender._tgdl_observed = True

    @staticmethod
    def _tgdl_record_cdn_bytes(captured: MediaRequest, future) -> None:
        if not future.cancelled() and future.exception() is None:
            DiagnosticTelegramClient._tgdl_record_media_bytes(captured, future)

    @staticmethod
    def _tgdl_record_media_bytes(captured: MediaRequest, future) -> None:
        governor = captured.current_governor()
        if governor is not None and captured.uninterrupted():
            payload = getattr(future.result(), "bytes", None)
            if isinstance(payload, bytes) and payload:
                governor.on_success(captured.sent_at, len(payload))

    def _tgdl_record_getfile_result(self, future, *, captured: MediaRequest | None = None) -> None:
        if future.cancelled():
            return
        exc = future.exception()
        if exc is None:
            if captured is not None:
                self._tgdl_record_media_bytes(captured, future)
            return
        if isinstance(exc, (
            errors.ServerError, errors.RpcCallFailError,
            errors.RpcMcgetFailError, errors.InterdcCallErrorError,
            errors.TimedOutError, errors.InterdcCallRichErrorError,
        )):
            kind = type(exc).__name__
            count = self._tgdl_rpc_counts.get(kind, 0) + 1
            self._tgdl_rpc_counts[kind] = count
            print(
                f"[网络诊断] GetFileRequest RPC异常 {kind}，累计{count}次；"
                "Telethon 将按原策略等待约2s后重试（非实际阻塞计时）"
            )
            return
        if not isinstance(exc, (FloodWaitError, FloodPremiumWaitError)):
            return
        kind = "FLOOD_PREMIUM_WAIT" if isinstance(exc, FloodPremiumWaitError) else "FLOOD_WAIT"
        count, seconds = self._tgdl_flood_counts[kind]
        self._tgdl_flood_counts[kind] = [count + 1, seconds + max(0, exc.seconds)]
        print(f"[网络诊断] 服务端原始响应 {kind} {exc.seconds}s (GetFileRequest) | "
              f"累计{count + 1}次，要求等待累计{seconds + max(0, exc.seconds)}s")

        if captured is not None and (governor := captured.current_governor()) is not None:
            governor.on_wait(captured.sent_at, exc.seconds, kind=kind)

    async def _get_cdn_client(self, cdn_redirect):
        client = await super()._get_cdn_client(cdn_redirect)
        # Telethon may delegate the file to a child with a separate sender.
        # Only its successful CDN bytes may belong to this account's scope.
        client._tgdl_cdn_parent = self
        return client

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
        str(settings.session_path or SESSION_PATH),
        settings.api_id,
        settings.api_hash,
        proxy=proxy,
        device_model="TG Media Downloader",
        app_version="1.0.0",
        system_version="Windows",
    )


def validate_login_phone(raw: str) -> str:
    """Validate before Telethon retries an invalid callable phone forever."""
    digits = parse_phone(raw)
    if not digits or not digits.isascii() or not digits.isdigit():
        raise TGDLInputError("手机号格式无效，请使用带国际区号的号码，例如 +8613800138000。")
    return "+" + digits


async def ensure_login(client: TelegramClient, phone: str | None) -> None:
    if phone:
        await client.start(phone=validate_login_phone(phone))
    else:
        await client.start()  # Keep Telethon's interactive default phone prompt.
    me = await client.get_me()
    if me is None:
        raise TGDLInputError("Login failed.")
    name = " ".join(part for part in (me.first_name, me.last_name) if part)
    username = f"@{me.username}" if me.username else ""
    print(f"Logged in as {name} {username} (id={me.id})".strip())
