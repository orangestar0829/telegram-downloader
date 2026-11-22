"""Independent secondary-account monitoring and downloading on one asyncio loop.

The original account keeps its legacy UI/worker. Each added account owns
its Telethon session, proxy, limiter, scanner and file slots, but all accounts
coordinate pending files through DownloadHistory's SQLite claims.
"""
from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from telethon.errors import FloodWaitError

from tgdl.accounts import Account, AccountRegistry
from tgdl.account_context import current_account_id
from tgdl.account_identity import account_identity
from tgdl.client import build_client, validate_login_phone
from tgdl.config import load_settings, read_env_values
from tgdl.downloader import (
    DownloadStats, _download_with_retry, classify_message, resolve_channel,
    history_peer_id, peer_download_folder, require_shared_monitor_peer, scan_monitor_slice,
    UnsupportedSharedMonitorPeer,
)
from tgdl.errors import ChannelAccessError
from tgdl.history import DownloadHistory, PendingMedia
from tgdl.monitor import choose_monitor_download, load_channels, next_due_channel
from tgdl.monitor_flow import MonitorFlow, MonitorPause, SharedSpeedLimiter
from tgdl.qr_auth import login_with_qr



# The Telegram server chooses the code delivery method. Do not assume SMS:
# some types (e.g. Firebase SMS) can only be completed in official mobile apps.
_LOGIN_CODE_HINTS = {
    "SentCodeTypeApp": (
        "Telegram 应用内",
        "请在 B 账号已登录的 Telegram 客户端查看官方“Telegram”服务消息；不是手机短信。",
    ),
    "SentCodeTypeSms": ("手机短信", "查看 B 账号手机号的短信收件箱。"),
    "SentCodeTypeSmsWord": ("短信单词验证码", "查看 B 账号手机短信。"),
    "SentCodeTypeSmsPhrase": ("短信短语验证码", "查看 B 账号手机短信。"),
    "SentCodeTypeCall": ("语音电话", "留意 B 账号绑定号码的来电。"),
    "SentCodeTypeMissedCall": ("未接来电", "请查看 B 账号绑定号码的未接来电。"),
    "SentCodeTypeFlashCall": ("闪电来电", "请查看 B 账号绑定号码的来电。"),
    "SentCodeTypeEmailCode": ("登录邮箱", "请检查 B 账号已设置的 Telegram 登录邮箱（含垃圾邮件）。"),
    "SentCodeTypeSetUpEmailRequired": (
        "需先设置登录邮箱",
        "Telegram 要求设置或验证登录邮箱；本预览版不支持此设置流程，请先用官方客户端完成。",
    ),
    "SentCodeTypeFragmentSms": (
        "Fragment",
        "Telegram 选择了 Fragment 验证方式；需使用自己的 Fragment 账号获取验证码。",
    ),
    "SentCodeTypeFirebaseSms": (
        "官方手机 App 专用短信验证",
        "该类型需要官方手机 App 的 Firebase 认证，Telethon 无法接收此类短信；请先用官方手机 App 登录。",
    ),
    "SentCodePaymentRequired": (
        "需要官方客户端处理的付费验证",
        "该验证方式需在官方 Telegram 客户端完成。",
    ),
}


def describe_login_code_delivery(sent_code: object) -> tuple[str, str]:
    """Return safe user-facing delivery info without logging phone/code/hash."""
    type_name = type(getattr(sent_code, "type", sent_code)).__name__
    return _LOGIN_CODE_HINTS.get(
        type_name, ("未知投递方式", f"Telegram 返回 {type_name}；请先检查 B 账号其他已登录客户端。")
    )


class AccountMonitorRuntime:
    """One service per secondary account; no Tk work on the asyncio thread."""

    def __init__(
        self, account: Account, registry: AccountRegistry, *,
        ask: Callable[[str, bool], Awaitable[str]],
        on_log: Callable[[str, str], None],
        on_saved: Callable[[str, str], None],
        on_progress: Callable[[str, str, int, int, str], None],
        login_method: str = "code",
        on_qr: Callable[[str, Any], None] | None = None,
    ) -> None:
        self.account = account
        self.registry = registry
        self.ask = ask
        self.on_log = on_log
        self.on_saved = on_saved
        self.on_progress = on_progress
        self.login_method = login_method
        self.on_qr = on_qr
        self.stop_event = threading.Event()
        self.status = "未启动"
        self.active_count = 0
        self.peer_by_target: dict[str, int] = {}
        self._next_at: dict[str, float] = {}
        self._batch_used: dict[str, int] = {}
        self._slice_cursor = 0
        self._blocked: set[str] = set()
        self.client = None
        self._run_task: asyncio.Task | None = None
        self._download_task: asyncio.Task | None = None
        self._interrupt_requested = False
        self._run_finishing = False

    def log(self, text: str) -> None:
        self.on_log(self.account.id, text)

    def stop(self, *, cancel: bool = False) -> None:
        """Stop claiming work; optionally cancel transfers rather than draining."""
        self.stop_event.set()
        task = self._run_task
        if task is not None:
            # Never cancel the cross-thread Future used to join this runtime:
            # it reports cancelled immediately, before asyncio finishes cleanup.
            task.get_loop().call_soon_threadsafe(self._interrupt_for_stop, cancel)

    def _interrupt_for_stop(self, cancel: bool) -> None:
        if self._interrupt_requested:
            return
        download = self._download_task
        if download is None:
            if self._run_finishing:
                return             # Leave the in-flight disconnect alone.
            target = self._run_task  # Interrupt login or an outstanding prompt.
        elif cancel:
            target = download
        else:
            return                  # Drain and renew any started files.
        if target is not None and not target.done():
            self._interrupt_requested = True
            target.cancel()

    def _channel_resolved(self, target: str, title: str, peer_id: int) -> None:
        self.peer_by_target[target] = peer_id

    def _channels(self) -> list[dict]:
        return load_channels(self.registry.monitor_path(self.account.id))

    async def _scan_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                channels = self._channels()
                now = time.time()
                item = next_due_channel(
                    [x for x in channels if x["target"] not in self._blocked],
                    self._next_at, now, self._slice_cursor,
                )
                if item is None:
                    await asyncio.sleep(0.7)
                    continue
                self._slice_cursor += 1
                target = item["target"]
                quota = int(item.get("batch_size", 100))
                remaining = max(1, quota - self._batch_used.get(target, 0))
                try:
                    result = await scan_monitor_slice(
                        self.client,
                        target=target,
                        types=set(x.strip() for x in item["types"].split(",") if x.strip()),
                        account_id=self.account.id,
                        monitor_direction=item.get("direction", "newest"),
                        slice_size=20, match_limit=min(20, remaining),
                        stop_event=self.stop_event,
                        on_channel_resolved=lambda title, peer_id: self._channel_resolved(
                            target, title, peer_id
                        ),
                    )
                except asyncio.CancelledError:
                    raise
                except UnsupportedSharedMonitorPeer as exc:
                    self.peer_by_target.pop(target, None)
                    self._blocked.add(target)
                    self.log(f"[监控已暂停] {item.get('name') or '该目标'}：{exc}")
                    continue
                except Exception as exc:
                    self.log(f"[扫描异常] {target}: {exc}; 60秒后重试")
                    self._next_at[target] = time.time() + 60
                    await asyncio.sleep(0)
                    continue
                self._batch_used[target] = self._batch_used.get(target, 0) + result.matched
                at_limit = self._batch_used[target] >= quota
                if not result.more_pending or at_limit:
                    self.log(
                        f"[扫描] {item.get('name') or target} 完成: "
                        f"检查{result.scanned_messages}条, 匹配{result.matched}, "
                        f"新增{result.queued}, 跳过{result.skipped}"
                    )
                    self._batch_used[target] = 0
                    self._next_at[target] = time.time() + int(item["minutes"]) * 60
                else:
                    self._next_at[target] = time.time()
                # Ensure the scan loop yields before requesting more messages.
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log(f"[扫描调度异常] {type(exc).__name__}: {exc}")
                await asyncio.sleep(5)

    async def _transfer(
        self, history: DownloadHistory, item: dict, job: PendingMedia,
        settings: Any, limiter: SharedSpeedLimiter, retries: int, chunks: int,
    ) -> None:
        peer, mid = job.peer_id, job.message_id
        success = False
        cooldown = 0.0
        target = item["target"]  # resolve using THIS account, not the scanner's alias
        try:
            entity = await resolve_channel(self.client, target)
            require_shared_monitor_peer(entity)
            if history_peer_id(entity) != peer:
                # A username/invite may have been rebound since the scan.
                if self.peer_by_target.get(target) == peer:
                    self.peer_by_target.pop(target, None)
                    self._next_at[target] = 0
                self.log("[下载跳过] 监控目标已指向其他频道，重新扫描后再领取")
                return
            message = await self.client.get_messages(entity, ids=mid)
            if message is None or getattr(message, "date", None) is None:
                cooldown = 3600
                self.log(f"[下载跳过] {target} 消息 {mid} 在本账号不可读取")
                return
            kind = classify_message(message)
            enabled_kinds = {value.strip() for value in item.get("types", "photo,video").split(",")
                             if value.strip()}
            if kind is None or kind not in enabled_kinds:
                cooldown = 3600
                self.log(f"[下载跳过] {target} 消息 {mid} 不符合本账号监控类型")
                return
            folder = peer_download_folder(
                settings.download_dir, getattr(entity, "title", None) or job.title, entity,
                account_id=self.account.id,
            ) / ("photos" if kind == "photo" else "videos")
            stats = DownloadStats()
            flow = MonitorFlow(
                pause=MonitorPause(), limiter=limiter, chunks=lambda: chunks
            )
            path = await _download_with_retry(
                self.client, message, folder,
                job.base or f"{message.date.strftime('%Y%m%d')}_{mid}",
                stats, skip_existing=True, retries=retries,
                chunk_concurrency=chunks, history=history, peer_id=peer,
                on_saved=lambda detail: self.on_saved(
                    self.account.id, f"[监控下载] [{job.title}] {detail}"
                ),
                temp_download_dir=settings.temp_download_dir,
                on_progress=lambda key, label, done, total, state: self.on_progress(
                    f"{self.account.id}:{key}", f"[{account_identity.label(self.account.id)}] {label}",
                    done, total, state
                ),
                monitor_flow=flow,
            )
            if path is not None or history.contains(peer, mid):
                history.attribute_saved(peer, mid, self.account.id)
                success = True
            elif stats.failed:
                cooldown = 60
                self.log(f"[下载失败] {target} 消息 {mid}；稍后重试")
            else:
                cooldown = 300
        except asyncio.CancelledError:
            raise
        except UnsupportedSharedMonitorPeer as exc:
            self.peer_by_target.pop(target, None)
            self._blocked.add(target)
            self.log(f"[监控已暂停] {item.get('name') or '该目标'}：{exc}")
        except FloodWaitError as exc:
            cooldown = max(60, int(exc.seconds))
            self.log(f"[账号限流] {target} 消息 {mid}；等待{cooldown}秒")
        except Exception as exc:
            cooldown = 60
            if isinstance(exc, ChannelAccessError) or type(exc).__name__ in (
                "ChannelPrivateError", "ChatAdminRequiredError",
                "ChannelInvalidError", "UserBannedInChannelError",
            ):
                # Never allow an inaccessible account to consume this channel's work.
                self._blocked.add(target)
                self.log(f"[频道无权限] {target} 已暂停本账号的下载/扫描: {exc}")
            else:
                self.log(f"[下载异常] {target} 消息 {mid}: {exc}")
        finally:
            if not success:
                history.release_claim(self.account.id, peer, mid, cooldown=cooldown)

    async def _download_loop(self, settings: Any) -> None:
        history = DownloadHistory(self.registry.root / "tgdl_download_history.sqlite3")
        active: dict[tuple[int, int], tuple[asyncio.Task, str, float]] = {}
        limiter = SharedSpeedLimiter(settings.media_speed_kib * 1024)
        workers, retries, chunks = (
            settings.media_concurrency, settings.media_retries, settings.chunk_concurrency
        )
        last_refresh = 0.0
        last_renew = 0.0
        occupancy: dict[str, float] = {}
        try:
            while not self.stop_event.is_set() or active:
                now = time.monotonic()
                for key, (task, target, began) in list(active.items()):
                    if not task.done():
                        continue
                    del active[key]
                    occupancy[target] = occupancy.get(target, 0) + max(0, now - began)
                    try:
                        await task
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.log(f"[下载协程异常] {type(exc).__name__}: {exc}")
                self.active_count = len(active)
                if now - last_renew >= 30:
                    for peer, mid in active:
                        history.renew_claim(self.account.id, peer, mid)
                    last_renew = now
                if now - last_refresh >= 5:
                    env = read_env_values(self.registry.env_path(self.account.id))
                    try:
                        workers = min(8, max(1, int(env["media_concurrency"])))
                        retries = min(5, max(0, int(env["media_retries"])))
                        chunks = min(8, max(1, int(env["chunk_concurrency"])))
                        limiter.bytes_per_second = (
                            min(1048576, max(0, int(env["media_speed_kib"]))) * 1024
                        )
                    except (ValueError, TypeError):
                        self.log("[下载设置] 无效的并发/限速参数；继续使用原值")
                    last_refresh = now
                if not self.stop_event.is_set() and len(active) < workers:
                    try:
                        channels = [
                            item for item in self._channels()
                            if item.get("enabled", True)
                            and item["target"] in self.peer_by_target
                            and item["target"] not in self._blocked
                        ]
                        candidates = []
                        for item in channels:
                            peer = self.peer_by_target[item["target"]]
                            for job in history.pending_for_peer(
                                peer, account_id=self.account.id, limit=6,
                                kinds={value.strip() for value in
                                       item.get("types", "photo,video").split(",") if value.strip()},
                            ):
                                if (job.peer_id, job.message_id) not in active:
                                    candidates.append((item, job))
                                    break
                        counts: dict[str, int] = {}
                        elapsed = dict(occupancy)
                        for _key, (_task, target, began) in active.items():
                            counts[target] = counts.get(target, 0) + 1
                            elapsed[target] = elapsed.get(target, 0) + now - began
                        selected = choose_monitor_download(candidates, counts, elapsed)
                        if selected is not None:
                            item, job = selected
                            if history.try_claim(
                                self.account.id, job.peer_id, job.message_id
                            ):
                                key = (job.peer_id, job.message_id)
                                task = asyncio.create_task(
                                    self._transfer(history, item, job, settings,
                                                   limiter, retries, chunks)
                                )
                                active[key] = (task, item["target"], time.monotonic())
                                continue
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.log(f"[下载调度异常] {type(exc).__name__}: {exc}")
                await asyncio.sleep(0.25 if active else 0.8)
        finally:
            # On cancellation or scheduler error, wait for each downloaded
            # file to remove temporary data before releasing its claim.
            for task, _target, _began in active.values():
                if not task.done():
                    task.cancel()
            if active:
                await asyncio.gather(
                    *(task for task, _, _ in active.values()), return_exceptions=True
                )
            try:
                for peer, mid in active:
                    history.release_claim(self.account.id, peer, mid)
            finally:
                self.active_count = 0
                history.close()

    async def run(self) -> None:
        """Login, run one independent scan loop and download loop; stop gracefully."""
        self.status = "正在登录"
        self._run_task = asyncio.current_task()
        token = current_account_id.set(self.account.id)
        scan = None
        error_status = None
        try:
            if self.stop_event.is_set():
                return
            settings = load_settings(
                env_path=self.registry.env_path(self.account.id),
                session_path=self.registry.session_path(self.account.id),
            )
            configured_phone = validate_login_phone(settings.phone) if settings.phone else None
            self.client = build_client(settings)
            if self.login_method == "qr":
                if self.on_qr is None:
                    raise RuntimeError("没有二维码显示窗口")
                self.status = "等待 Telegram 扫码授权"
                self.log("[扫码登录] 请在 B 账号 Telegram App 的 设置 → 设备 → 连接桌面设备 中扫码")
                await login_with_qr(
                    self.client,
                    show_qr=self.on_qr,
                    ask_password=lambda: self.ask("Telegram 两步验证密码", True),
                    should_stop=self.stop_event.is_set,
                )
            else:
                # Telethon.start() requests a login code internally, but hides
                # the server-selected channel. Observe the same request.
                original_send_code = self.client.send_code_request
                delivery = {"label": "请查看 B 账号 Telegram App、短信或邮箱"}

                async def observed_send_code(*args, **kwargs):
                    sent = await original_send_code(*args, **kwargs)
                    label, guidance = describe_login_code_delivery(sent)
                    delivery["label"] = label
                    self.status = f"等待验证码（{label}）"
                    self.log(f"[登录验证码] Telegram 响应：{label}。{guidance}")
                    return sent

                self.client.send_code_request = observed_send_code
                try:
                    async def ask_phone() -> str:
                        return validate_login_phone(await self.ask("手机号（带国际区号）", False))

                    await self.client.start(
                        phone=configured_phone or ask_phone,
                        code_callback=lambda: self.ask(
                            f"Telegram 验证码（{delivery['label']}）", False
                        ),
                        password=lambda: self.ask("Telegram 两步验证密码", True),
                    )
                finally:
                    self.client.send_code_request = original_send_code
            me = await self.client.get_me()
            if me is None:
                raise RuntimeError("Telegram login failed")
            if configured_phone and getattr(me, "phone", None):
                if configured_phone != validate_login_phone(me.phone):
                    raise RuntimeError(
                        "此账号 Session 已登录其他号码；请检查 B 账号是否误用了别的 Session 文件"
                    )
            self.status = f"已连接：{account_identity.update(self.account.id, me)}"
            self.log(f"[账号连接] {self.status}")
            scan = asyncio.create_task(self._scan_loop())
            self._download_task = asyncio.create_task(self._download_loop(settings))
            while not self.stop_event.is_set():
                for task in (scan, self._download_task):
                    if task.done():
                        await task  # surface a fatal worker failure
                        self.stop_event.set()
                        break
                await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            # If run itself is externally cancelled, interrupt transfers too.
            if self._download_task is not None:
                self._interrupt_for_stop(cancel=True)
        except Exception as exc:
            error_status = f"异常：{type(exc).__name__}"
            self.log(f"[账号运行异常] {type(exc).__name__}: {exc}")
        finally:
            self._run_finishing = True
            self.stop_event.set()
            # While draining, UI operations must no longer treat this client
            # as available. Explicit cancel can still interrupt the drain.
            self.status = error_status or "正在停止"
            try:
                if scan is not None:
                    if not scan.done():
                        scan.cancel()
                    await asyncio.gather(scan, return_exceptions=True)
                if self._download_task is not None:
                    await asyncio.gather(self._download_task, return_exceptions=True)
                if self.client is not None:
                    await self.client.disconnect()
                    self.log("[账号停止] 已断开连接")
            finally:
                self.client = None
                self._download_task = None
                self._run_task = None
                self.status = error_status or "已停止"
                current_account_id.reset(token)
