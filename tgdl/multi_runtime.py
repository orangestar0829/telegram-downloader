"""Independent secondary-account monitoring and downloading on one asyncio loop.

The original account keeps its legacy UI/worker. Each added account owns
its Telethon session, proxy, limiter, scanner and file slots, but all accounts
coordinate pending files through DownloadHistory's SQLite claims.
"""
from __future__ import annotations

import asyncio
import stat
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from telethon.errors import AuthKeyDuplicatedError, FloodPremiumWaitError, FloodWaitError, UnauthorizedError

from tgdl.accounts import Account, AccountRegistry
from tgdl.account_context import current_account_id
from tgdl.account_identity import account_identity
from tgdl.client import build_client, validate_login_phone
from tgdl.config import load_settings, parse_monitor_auto_throttle, read_env_values
from tgdl.downloader import (
    DownloadLeaseLost, DownloadStats, _download_with_retry, classify_message, resolve_channel,
    history_peer_id, peer_download_folder, require_shared_monitor_peer, scan_monitor_slice,
    UnsupportedSharedMonitorPeer,
)
from tgdl.errors import ChannelAccessError
from tgdl.history import DownloadHistory, PendingMedia, UnreadableRetry
from tgdl.monitor import MonitorUnreadableSummary, choose_monitor_download, load_channels, next_due_channel
from tgdl.monitor_flow import MonitorFlow, MonitorPause, SharedSpeedLimiter
from tgdl.realtime_burst import RealtimeBurst, realtime_candidates
from tgdl.slots import SlotCoordinator
from tgdl.realtime_runtime import RealtimeDispatch
from tgdl.qr_auth import login_with_qr



# The Telegram server chooses the code delivery method. Do not assume SMS:
# some types (e.g. Firebase SMS) can only be completed in official mobile apps.
_LOGIN_CODE_HINTS = {
    "SentCodeTypeApp": (
        "Telegram 应用内",
        "请在 该账号已登录的 Telegram 客户端查看官方“Telegram”服务消息；不是手机短信。",
    ),
    "SentCodeTypeSms": ("手机短信", "查看 该账号的手机号的短信收件箱。"),
    "SentCodeTypeSmsWord": ("短信单词验证码", "查看 该账号的手机短信。"),
    "SentCodeTypeSmsPhrase": ("短信短语验证码", "查看 该账号的手机短信。"),
    "SentCodeTypeCall": ("语音电话", "留意 该账号绑定号码的来电。"),
    "SentCodeTypeMissedCall": ("未接来电", "请查看 该账号绑定号码的未接来电。"),
    "SentCodeTypeFlashCall": ("闪电来电", "请查看 该账号绑定号码的来电。"),
    "SentCodeTypeEmailCode": ("登录邮箱", "请检查 该账号已设置的 Telegram 登录邮箱（含垃圾邮件）。"),
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
        type_name, ("未知投递方式", f"Telegram 返回 {type_name}；请先检查 该账号其他已登录客户端。")
    )


class _SavedSessionUnavailable(Exception):
    """Expected, noninteractive resume failure without mutating authorization."""


class AccountMonitorRuntime:
    """One service per secondary account; no Tk work on the asyncio thread."""

    def __init__(
        self, account: Account, registry: AccountRegistry, *,
        ask: Callable[[str, bool], Awaitable[str]],
        on_log: Callable[[str, str], None],
        on_saved: Callable[[str, str], None],
        on_progress: Callable[[str, str, int, int, str], None],
        on_progress_state: Callable[[int, int, str], None] | None = None,
        login_method: str = "code",
        on_qr: Callable[[str, Any], None] | None = None,
        authenticate: Callable[[Any, str | None], Awaitable[Any]] | None = None,
        on_authenticated: Callable[[], None] | None = None,
        restore_session_only: bool = False,
        slots: SlotCoordinator | None = None,
    ) -> None:
        self._slots = slots if slots is not None else SlotCoordinator()
        self._realtime_ready = RealtimeDispatch()
        self.account = account
        self.registry = registry
        self.ask = ask
        self.on_log = on_log
        self.on_saved = on_saved
        self.on_progress = on_progress
        self.on_progress_state = on_progress_state
        self.login_method = login_method
        self.on_qr = on_qr
        self.authenticate = authenticate
        self.on_authenticated = on_authenticated
        self.restore_session_only = restore_session_only
        self.stop_event = threading.Event()
        self._monitor_controls = threading.RLock()
        self._monitor_scan_stop = threading.Event()
        self._monitor_scan_stop.set()
        self._configured_paths: tuple[Path, Path | None] | None = None
        self._cancel_transfers_requested = threading.Event()
        self.status = "未启动"
        self.active_count = 0
        self.running_count = 0
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

    @property
    def monitoring(self) -> bool:
        return not (self._monitor_scan_stop.is_set() or self.stop_event.is_set())

    @property
    def monitoring_draining(self) -> bool:
        return not self.monitoring and (
            self.active_count > 0 or self._cancel_transfers_requested.is_set()
        )

    def start_monitoring(self) -> bool:
        """Begin scanning and claiming, without reconnecting the account."""
        with self._monitor_controls:
            if self.stop_event.is_set() or self.monitoring or self.monitoring_draining:
                return False
            self._next_at.clear()
            self._batch_used.clear()
            self._blocked.clear()
            self.peer_by_target.clear()
            # A scan still unwinding from an earlier Stop keeps its set event.
            self._monitor_scan_stop = threading.Event()
            return True

    def stop_monitoring(self) -> None:
        """Stop claiming new work and let active files finish."""
        with self._monitor_controls:
            self._monitor_scan_stop.set()

    def cancel_transfers(self) -> None:
        """Pause the monitor and interrupt incomplete files, without logout."""
        with self._monitor_controls:
            self._monitor_scan_stop.set()
            self._cancel_transfers_requested.set()

    def set_download_paths(self, directory: Path, staging: Path | None) -> None:
        """Apply storage settings only to this account's future files."""
        self._configured_paths = directory, staging

    def stop(self, *, cancel: bool = False) -> None:
        """Stop the account itself (application shutdown), not just monitoring."""
        self.stop_event.set()
        self.stop_monitoring()
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

    def _channel_resolved(
        self, target: str, title: str, peer_id: int, scan_stop: threading.Event,
    ) -> None:
        with self._monitor_controls:
            if not scan_stop.is_set():
                self.peer_by_target[target] = peer_id

    def _channels(self) -> list[dict]:
        return load_channels(self.registry.monitor_path(self.account.id))

    async def _scan_loop(self) -> None:
        while not self.stop_event.is_set():
            scan_stop = self._monitor_scan_stop
            if scan_stop.is_set():
                await asyncio.sleep(0.25)
                continue
            try:
                channels = self._channels()
                now = time.time()
                with self._monitor_controls:
                    if scan_stop.is_set():
                        continue
                    item = next_due_channel(
                        [x for x in channels if x["target"] not in self._blocked],
                        self._next_at, now, self._slice_cursor,
                    )
                    if item is not None:
                        self._slice_cursor += 1
                        target = item["target"]
                        quota = int(item.get("batch_size", 100))
                        remaining = max(1, quota - self._batch_used.get(target, 0))
                if item is None:
                    await asyncio.sleep(0.7)
                    continue
                try:
                    result = await scan_monitor_slice(
                        self.client,
                        target=target,
                        types=set(x.strip() for x in item["types"].split(",") if x.strip()),
                        account_id=self.account.id,
                        monitor_direction=item.get("direction", "newest"),
                        slice_size=20, match_limit=min(20, remaining),
                        stop_event=scan_stop,
                        on_channel_resolved=lambda title, peer_id: self._channel_resolved(
                            target, title, peer_id, scan_stop
                        ),
                    )
                except asyncio.CancelledError:
                    raise
                except UnsupportedSharedMonitorPeer as exc:
                    with self._monitor_controls:
                        if scan_stop.is_set():
                            continue
                        self.peer_by_target.pop(target, None)
                        self._blocked.add(target)
                    self.log(f"[监控已暂停] {item.get('name') or '该目标'}：{exc}")
                    continue
                except Exception as exc:
                    with self._monitor_controls:
                        if scan_stop.is_set():
                            continue
                        self._next_at[target] = time.time() + 60
                    self.log(f"[扫描异常] {target}: {exc}; 60秒后重试")
                    await asyncio.sleep(0)
                    continue
                with self._monitor_controls:
                    if scan_stop.is_set():
                        continue
                    self._batch_used[target] = self._batch_used.get(target, 0) + result.matched
                    round_done = not result.more_pending or self._batch_used[target] >= quota
                    if round_done:
                        self._batch_used[target] = 0
                        self._next_at[target] = time.time() + int(item["minutes"]) * 60
                    else:
                        self._next_at[target] = time.time()
                if round_done:
                    self.log(
                        f"[扫描] {item.get('name') or target} 完成: "
                        f"检查{result.scanned_messages}条, 匹配{result.matched}, "
                        f"新增{result.queued}, 跳过{result.skipped}"
                    )
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
        claim_token: str, *, on_claim_committed: Callable[[], None] | None = None,
        on_unreadable: Callable[[PendingMedia, UnreadableRetry], None] | None = None,
        pause: MonitorPause | None = None,
    ) -> None:
        peer, mid = job.peer_id, job.message_id
        cooldown = 0
        unreadable_reply = False
        target = item["target"]  # resolve using THIS account, not the scanner's alias
        realtime = item.get("mode") == "realtime"
        label = "实时下载" if realtime else "监控下载"
        try:
            # Do not import invites when real-time mode needs to reopen a peer.
            if realtime:
                from tgdl.realtime import resolve_realtime_peer
                entity = await resolve_realtime_peer(
                    self.client, target, expected_peer_id=peer,
                )
            else:
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
            if getattr(message, "date", None) is None:
                unreadable_reply = True
                return
            history.record_readable_message(self.account.id, peer, mid, token=claim_token)
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
                pause=pause if pause is not None else MonitorPause(),
                limiter=limiter, chunks=lambda: chunks,
            )
            path = await _download_with_retry(
                self.client, message, folder,
                job.base or f"{message.date.strftime('%Y%m%d')}_{mid}",
                stats, skip_existing=True, retries=retries,
                chunk_concurrency=chunks, history=history, peer_id=peer,
                account_id=self.account.id, claim_token=claim_token,
                on_claim_committed=on_claim_committed,
                on_saved=lambda detail: self.on_saved(
                    self.account.id, f"[{label}] [{job.title}] {detail}"
                ),
                temp_download_dir=settings.temp_download_dir,
                on_progress=lambda key, label, done, total, state: self.on_progress(
                    f"{self.account.id}:{key}", f"[{account_identity.label(self.account.id)}] {label}",
                    done, total, state
                ),
                monitor_flow=flow,
            )
            if stats.failed:
                cooldown = 60
                self.log(f"[下载失败] {target} 消息 {mid}；稍后重试")
            elif path is None and not history.contains(peer, mid):
                cooldown = 300
        except asyncio.CancelledError:
            raise
        except DownloadLeaseLost:
            self.log(f"[下载跳过] 下载租约已失效，放弃保存 {job.title}/{mid}")
        except UnsupportedSharedMonitorPeer as exc:
            if realtime:
                cooldown = 300
                self.log(f"[实时下载] {target} 频道暂不可用，稍后重试: {exc}")
            else:
                self.peer_by_target.pop(target, None)
                self._blocked.add(target)
                self.log(f"[监控已暂停] {item.get('name') or '该目标'}：{exc}")
        except (FloodWaitError, FloodPremiumWaitError) as exc:
            cooldown = max(60, int(exc.seconds))
            self.log(f"[账号限流] {target} 消息 {mid}；等待{cooldown}秒")
        except Exception as exc:
            cooldown = 60
            if isinstance(exc, ChannelAccessError) or type(exc).__name__ in (
                "ChannelPrivateError", "ChatAdminRequiredError",
                "ChannelInvalidError", "UserBannedInChannelError",
            ):
                # An inaccessible account cannot consume this work. Real-time
                # subscriptions may regain permission without restarting;
                # their read-only resolver rechecks on each new attempt.
                if realtime:
                    cooldown = 300
                    self.log(f"[实时下载] {target} 频道暂不可用，稍后重试: {exc}")
                else:
                    self._blocked.add(target)
                    self.log(f"[频道无权限] {target} 已暂停本账号的下载/扫描: {exc}")
            else:
                self.log(f"[下载异常] {target} 消息 {mid}: {exc}")
        finally:
            if unreadable_reply:
                retry = history.finish_unreadable_claim(self.account.id, peer, mid, token=claim_token)
                if retry is not None and on_unreadable is not None:
                    on_unreadable(job, retry)
            else:
                history.release_claim(
                    self.account.id, peer, mid, token=claim_token, cooldown=cooldown
                )

    async def _download_loop(self, settings: Any) -> None:
        from tgdl.slots import SlotCoordinator
        slots = getattr(self, "_slots", None) or SlotCoordinator()
        history = DownloadHistory(self.registry.root / "tgdl_download_history.sqlite3")
        active: dict[tuple[int, int], tuple[asyncio.Task, str, float, str]] = {}
        active_slots = {}
        active_jobs = {}
        pauses: dict[tuple[int, int], MonitorPause] = {}
        paused_since: dict[tuple[int, int], float] = {}
        staged_jobs = set()
        realtime_jobs: set[tuple[int, int]] = set()
        burst = RealtimeBurst()
        retiring: set[tuple[int, int]] = set()
        committed: set[str] = set()
        limiter = governor = None
        governor_generation = None
        self._monitor_throttle = None
        manual_kib = settings.media_speed_kib
        auto_throttle = getattr(settings, "monitor_auto_throttle", False)
        workers, retries, chunks = (
            settings.media_concurrency, settings.media_retries, settings.chunk_concurrency
        )
        last_refresh = 0.0
        last_renew = 0.0
        occupancy: dict[str, float] = {}
        unreadable_log = MonitorUnreadableSummary()
        account_id = self.account.id

        def progress_state(key, status):
            if self.on_progress_state is not None:
                self.on_progress_state(*key, status)

        def resume(key):
            task, target, _began, token = active[key]
            if key not in active_slots:
                slot = slots.acquire(account_id, workers, file_key=key, scheduled_monitor=True)
                if slot is None:
                    return False
                active_slots[key] = slot
                active[key] = task, target, time.monotonic(), token
                paused_since.pop(key, None)
                self.running_count = len(active_slots)
                progress_state(key, "恢复中")
            burst.ordinary_started(key, time.monotonic())
            pauses[key].resume()
            return True

        def launch(item, job, generation, *, realtime=False, borrow=False):
            storage = self._configured_paths
            transfer_settings = settings if storage is None else replace(
                settings, download_dir=storage[0], temp_download_dir=storage[1],
            )
            key = (job.peer_id, job.message_id)
            slot = slots.acquire(
                account_id, workers, job.peer_id if realtime else None,
                file_key=key, scheduled_monitor=not realtime, borrow=borrow,
                realtime_item=item if realtime else None,
            )
            if slot is None:
                return False
            token = None
            try:
                token = history.try_claim(account_id, *key)
                with self._monitor_controls:
                    if (token is None or not self.monitoring
                            or self._monitor_scan_stop is not generation):
                        return False
                    pause = MonitorPause()
                    task = asyncio.create_task(self._transfer(
                        history, item, job, transfer_settings, limiter, retries, chunks, token,
                        on_claim_committed=lambda: committed.add(token),
                        on_unreadable=unreadable_log.add, pause=pause,
                    ))
                    active[key] = (task, item["target"], time.monotonic(), token)
                    active_slots[key] = slot
                    active_jobs[key] = job
                    pauses[key] = pause
                    if transfer_settings.temp_download_dir is not None:
                        staged_jobs.add(key)
                    if realtime:
                        realtime_jobs.add(key)
                        if borrow:
                            burst.borrowed.add(key)
                    else:
                        burst.ordinary_started(key, time.monotonic())
                    self.active_count = len(active)
                    self.running_count = len(active_slots)
                    return True
            finally:
                if key not in active:
                    slots.release(slot)
                    if token is not None:
                        history.release_claim(account_id, *key, token=token)

        try:
            while not self.stop_event.is_set() or active:
                now = time.monotonic()
                for line in unreadable_log.drain(now=now):
                    self.log(line)
                if self._cancel_transfers_requested.is_set():
                    for key, (task, _target, _began, token) in active.items():
                        if key not in retiring and token not in committed and not task.done():
                            retiring.add(key)
                            task.cancel()
                    self._cancel_transfers_requested.clear()
                for key, (task, target, began, token) in list(active.items()):
                    if not task.done():
                        continue
                    del active[key]
                    slots.release(active_slots.pop(key, None))
                    retiring.discard(key)
                    committed.discard(token)
                    burst.finished(key)
                    active_jobs.pop(key)
                    pauses.pop(key)
                    staged_jobs.discard(key)
                    if key in realtime_jobs:
                        realtime_jobs.discard(key)
                    elif key not in paused_since:
                        occupancy[target] = occupancy.get(target, 0) + max(0, now - began)
                    paused_since.pop(key, None)
                    progress_state(key, "结束")
                    try:
                        await task
                    except asyncio.CancelledError:
                        history.release_claim(self.account.id, *key, token=token)
                    except Exception as exc:
                        self.log(f"[下载协程异常] {type(exc).__name__}: {exc}")
                self.active_count = len(active)
                self.running_count = len(active_slots)
                generation = self._monitor_scan_stop
                if not active:
                    # Unlike the primary worker, this loop outlives monitoring.
                    # Preserve the old cap while jobs drain, but reset it before
                    # claiming for a newly started generation, even if Stop
                    # and Start both happened during one scheduler sleep.
                    if generation is not governor_generation or generation.is_set():
                        limiter = governor = self._monitor_throttle = None
                        governor_generation = None
                    if generation.is_set() or self.stop_event.is_set():
                        await asyncio.sleep(0.25)
                        continue
                if now - last_renew >= 30:
                    for key, (task, _target, _began, token) in active.items():
                        if key in retiring or token in committed:
                            continue
                        if not history.renew_claim(self.account.id, *key, token=token):
                            retiring.add(key)
                            task.cancel()
                            self.log(f"[下载跳过] 下载租约已失效，停止任务 {key}")
                    last_renew = now
                if governor is None or now - last_refresh >= 5:
                    # Re-read on every fresh monitoring run, even if settings
                    # were saved while idle and no live governor received them.
                    env = read_env_values(self.registry.env_path(self.account.id))
                    try:
                        workers = min(8, max(1, int(env["media_concurrency"])))
                        retries = min(5, max(0, int(env["media_retries"])))
                        chunks = min(8, max(1, int(env["chunk_concurrency"])))
                        manual_kib = min(1048576, max(0, int(env["media_speed_kib"])))
                        auto_throttle = parse_monitor_auto_throttle(env.get("monitor_auto_throttle"))
                    except (ValueError, TypeError):
                        self.log("[下载设置] 无效的并发/限速参数；继续使用原值")
                    if governor is None:
                        limiter = SharedSpeedLimiter()
                        governor = limiter.bind_auto_throttle(log=self.log)
                        governor_generation = generation
                        self._monitor_throttle = governor
                    governor.configure(manual_kib=manual_kib, enabled=auto_throttle)
                    last_refresh = now
                try:
                    monitoring = self.monitoring
                    channels = self._channels() if monitoring else ()
                    by_target = {item["target"]: item for item in channels}
                    reserved = slots.reserved_peers()
                    ordinary_capacity = slots.ordinary_capacity(account_id, workers)
                    # A yielded file still owns its lease and bytes. Retire it
                    # only when an edit made an ordinary resume impossible.
                    for key in burst.preempted.copy():
                        if key in retiring or not pauses[key].safely_suspended:
                            continue
                        permit = active_slots.pop(key, None)
                        if permit is not None:
                            slots.release(permit)
                            _task, target, began, _token = active[key]
                            occupancy[target] = occupancy.get(target, 0) + max(0, now - began)
                            paused_since[key] = now
                            progress_state(key, "已暂停")
                            self.log(f"[调度] {target}：暂停定时下载，让出名额给实时任务")
                        task, target, _began, token = active[key]
                        item = by_target.get(target)
                        if (not ordinary_capacity or key[0] in reserved
                                or (monitoring and (item is None or item.get("mode") == "realtime"))):
                            retiring.add(key)
                            task.cancel()
                            self.log(f"[调度] {target} 的暂停任务已返回队列｜普通下载名额或监控目标已变更")
                    self.running_count = len(active_slots)
                    if not monitoring:
                        for key in burst.preempted.copy():
                            if key not in retiring:
                                resume(key)
                        await asyncio.sleep(0.25)
                        continue

                    ready = getattr(self, "_realtime_ready", ())
                    if any(launch(item, job, generation, realtime=True)
                           for item, job in realtime_candidates(
                               channels, ready, slots, history, account_id, workers, active,
                           )):
                        continue
                    overflow = list(realtime_candidates(
                        channels, ready, slots, history, account_id, workers, active, borrow=True,
                    )) if ordinary_capacity else []
                    if not overflow or burst.ordinary_turn:
                        burst.withdraw(pauses, active_slots, retiring)
                    if not overflow and not burst.borrowed:
                        burst.no_ordinary_demand()

                    def launch_overflow():
                        return any(launch(item, job, generation, realtime=True, borrow=True)
                                   for item, job in overflow)

                    if overflow and not burst.ordinary_turn:
                        if launch_overflow():
                            continue
                        if not slots.available(account_id, workers):
                            victims = sorted(
                                (key for key in active_slots if key not in realtime_jobs
                                 and key not in retiring and key not in staged_jobs
                                 and active[key][3] not in committed),
                                key=lambda key: (
                                    int(by_target.get(active[key][1], {}).get("priority", 0)),
                                    active[key][2],
                                ),
                            )
                            burst.request(pauses, victims, now)
                    elif slots.available(account_id, workers):
                        ordinary_candidates = []
                        for item in channels:
                            target = item["target"]
                            peer = self.peer_by_target.get(target)
                            if item.get("mode") == "realtime":
                                continue
                            suspended = [
                                active_jobs[key] for key in paused_since
                                if key not in retiring and active[key][1] == target
                            ]
                            if suspended:
                                ordinary_candidates.extend((item, job) for job in suspended)
                                continue
                            if (not peer or not item.get("enabled", True) or peer in reserved
                                    or target in self._blocked):
                                continue
                            kinds = {value.strip() for value in
                                     item.get("types", "photo,video").split(",") if value.strip()}
                            for job in history.pending_for_peer(
                                peer, account_id=account_id, limit=6, kinds=kinds,
                            ):
                                key = (job.peer_id, job.message_id)
                                if key not in active and slots.available(
                                    account_id, workers, file_key=key,
                                ):
                                    ordinary_candidates.append((item, job))
                                    break
                        counts: dict[str, int] = {}
                        elapsed = dict(occupancy)
                        for key in active_slots:
                            if key in realtime_jobs:
                                continue
                            _task, target, began, _token = active[key]
                            counts[target] = counts.get(target, 0) + 1
                            elapsed[target] = elapsed.get(target, 0) + now - began
                        selected = choose_monitor_download(ordinary_candidates, counts, elapsed)
                        if selected is not None:
                            item, job = selected
                            # At the same scheduled priority an interrupted
                            # file gets its permit back before a new file.
                            same_rank_suspended = [
                                (candidate_job.peer_id, candidate_job.message_id)
                                for candidate_item, candidate_job in ordinary_candidates
                                if int(candidate_item.get("priority", 0)) == int(item.get("priority", 0))
                                and (candidate_job.peer_id, candidate_job.message_id) in paused_since
                            ]
                            if same_rank_suspended:
                                key = min(same_rank_suspended, key=paused_since.__getitem__)
                                if resume(key):
                                    self.log(f"[调度] {active[key][1]}：恢复定时下载")
                                    continue
                            elif launch(item, job, generation):
                                continue
                        elif overflow:
                            # No eligible ordinary job needs the return turn.
                            burst.no_ordinary_demand()
                            if launch_overflow():
                                continue
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.log(f"[下载调度异常] {type(exc).__name__}: {exc}")
                live = getattr(self, "_realtime_ready", None)
                if isinstance(live, RealtimeDispatch) and live.service is not None:
                    await live.wait(0.1 if burst.preempted else 0.25 if active else 1)
                else:
                    await asyncio.sleep(0.1 if burst.preempted else 0.25 if active else 0.8)
        finally:
            # On cancellation or scheduler error, wait for each downloaded
            # file to remove temporary data before releasing its claim.
            for key, (task, _target, _began, token) in active.items():
                if key not in retiring and token not in committed and not task.done():
                    task.cancel()
            if active:
                await asyncio.gather(
                    *(task for task, _, _, _ in active.values()), return_exceptions=True
                )
            for line in unreadable_log.drain(force=True):
                self.log(line)
            for slot in active_slots.values():
                slots.release(slot)
            for key in active:
                progress_state(key, "结束")
            try:
                for key, (_task, _target, _began, token) in active.items():
                    history.release_claim(self.account.id, *key, token=token)
            finally:
                self.active_count = self.running_count = 0
                if self._monitor_throttle is governor:
                    self._monitor_throttle = None
                history.close()

    async def _restore_saved_session(self, settings):
        configured_phone = validate_login_phone(settings.phone) if settings.phone else None
        self.client = build_client(settings)
        # A previously abandoned login can leave a SQLite file without an
        # authorization key. Never initiate authentication for that file.
        if not self.client.session.auth_key:
            raise _SavedSessionUnavailable("登录已失效")
        try:
            if not self.client.is_connected():
                await self.client.connect()
            me = await self.client.get_me()
        except (UnauthorizedError, AuthKeyDuplicatedError):
            raise _SavedSessionUnavailable("登录已失效") from None
        if me is None:
            raise _SavedSessionUnavailable("登录已失效")
        if configured_phone and getattr(me, "phone", None):
            if configured_phone != validate_login_phone(me.phone):
                raise _SavedSessionUnavailable("会话与账号不匹配")
        return me

    async def run(self) -> None:
        """Login, run one independent scan loop and download loop; stop gracefully."""
        self.status = "正在恢复登录" if self.restore_session_only else "正在登录"
        self._run_task = asyncio.current_task()
        token = current_account_id.set(self.account.id)
        scan = watch = None
        error_status = None
        connected = False
        try:
            if self.stop_event.is_set():
                return
            session_path = self.registry.session_path(self.account.id)
            if self.restore_session_only:
                session_file = session_path.with_suffix(".session")
                try:
                    session_stat = session_file.stat()
                except (FileNotFoundError, NotADirectoryError):
                    raise _SavedSessionUnavailable("未登录") from None
                if not stat.S_ISREG(session_stat.st_mode):
                    raise _SavedSessionUnavailable("未登录")
                if session_stat.st_size == 0:
                    raise _SavedSessionUnavailable("登录已失效")
            settings = load_settings(
                env_path=self.registry.env_path(self.account.id), session_path=session_path,
            )
            if self.restore_session_only:
                me = await self._restore_saved_session(settings)
            elif self.authenticate is not None:
                self.client = build_client(settings)
                me = await self.authenticate(self.client, settings.phone)
            else:
                # No live dialog: preserve the noninteractive/legacy callbacks.
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
            connected = True
            self.log(f"[账号连接] {self.status}")
            if self.on_authenticated is not None:
                self.on_authenticated()
            from tgdl.realtime_runtime import supervise
            watch = asyncio.create_task(supervise(
                account_id=self.account.id, slots=self._slots, ready=self._realtime_ready,
                get_client=lambda: self.client, active=lambda: self.monitoring,
                # A restart replaces the stop event: old operations retain their generation.
                generation=lambda: self._monitor_scan_stop, finished=self.stop_event.is_set,
                path=self.registry.root / "tgdl_download_history.sqlite3", log=self.log,
            ))
            scan = asyncio.create_task(self._scan_loop())
            self._download_task = asyncio.create_task(self._download_loop(settings))
            while not self.stop_event.is_set():
                for task in (scan, self._download_task, watch):
                    if task.done():
                        await task  # surface a fatal worker failure
                        self.stop_event.set()
                        break
                await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            # If run itself is externally cancelled, interrupt transfers too.
            if self._download_task is not None:
                self._interrupt_for_stop(cancel=True)
        except _SavedSessionUnavailable as exc:
            error_status = str(exc)
            self.log(f"[登录恢复] {error_status}")
        except Exception as exc:
            if self.restore_session_only and not connected:
                # Network/proxy and config failures are not evidence that the
                # persisted login expired. Don't expose transport credentials.
                error_status = "恢复登录失败"
                self.log(f"[登录恢复] 无法连接或读取设置：{type(exc).__name__}")
            else:
                error_status = f"异常：{type(exc).__name__}"
                self.log(f"[账号运行异常] {type(exc).__name__}: {exc}")
        finally:
            self._run_finishing = True
            self.stop_event.set()
            # While draining, UI operations must no longer treat this client
            # as available. Explicit cancel can still interrupt the drain.
            self.status = error_status or "正在停止"
            try:
                if watch is not None:
                    watch.cancel()
                    await asyncio.gather(watch, return_exceptions=True)
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
