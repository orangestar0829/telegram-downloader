"""Runtime-only receive governor for one account's monitored downloads."""

from __future__ import annotations

import math
import time
from collections import deque
from collections.abc import Callable
from typing import Protocol

MIN_KIB = 1024
MAX_KIB = 1048576
PAIR_SECONDS = 5 * 60
HEALTHY_SECONDS = 10 * 60
INCIDENT_GUARD_SECONDS = 2
SPEED_WINDOW_SECONDS = 60
# Slow/stalled RPCs do not prove healthy work for their entire wait. The
# receive limiter separately reports the short, explicit pacing intervals.
MAX_HEALTHY_RPC_SECONDS = 10
MIN_SAMPLE_BYTES = 4 * 1024 * 1024
MIN_SAMPLE_SECONDS = 3
MIN_SAMPLE_CHUNKS = 4
UNKNOWN_UNLIMITED_PROBE_KIB = 2048


class _Limiter(Protocol):
    bytes_per_second: float


class _Intervals:
    """A bounded union, receiving intervals in nondecreasing end order."""

    def __init__(self, *, merge_gap: float = 0) -> None:
        self._parts: deque[tuple[float, float]] = deque()
        self._sealed_end = -math.inf
        self._merge_gap = merge_gap
        self.seconds = 0.0

    def add(self, start: float, end: float) -> None:
        start = max(start, self._sealed_end)
        if end <= start:
            return
        while self._parts and start <= self._parts[-1][1] + self._merge_gap:
            left, right = self._parts.pop()
            self.seconds -= right - left
            start = min(left, start)
            end = max(right, end)
        self._parts.append((start, end))
        self.seconds += end - start

    def prune(self, before: float, *, keep_elapsed: bool = False) -> None:
        while self._parts and (self._parts[0][1] <= before or len(self._parts) > 2048):
            start, end = self._parts.popleft()
            if keep_elapsed:
                # Future late completions cannot count these seconds twice.
                self._sealed_end = end
            else:
                self.seconds -= end - start

    def seconds_since(self, timestamp: float) -> float:
        return sum(end - max(start, timestamp) for start, end in self._parts if end > timestamp)


class _RecentSpeed:
    """Bytes divided by active elapsed time across all of the account's lanes."""

    def __init__(self) -> None:
        # Whole-second completion buckets bound memory even at very high bitrates.
        self._buckets: deque[tuple[int, int, int]] = deque()
        # Include brief inter-block pauses/dispatch overhead, not queue idle time.
        self._active = _Intervals(merge_gap=1)

    def observe(self, start: float, end: float, amount: int) -> None:
        second = math.floor(end)
        count = 1
        if self._buckets and self._buckets[-1][0] == second:
            _, old_amount, old_count = self._buckets.pop()
            amount += old_amount
            count += old_count
        self._buckets.append((second, amount, count))
        self._active.add(start, end)
        self._prune(end)

    def _prune(self, now: float) -> int:
        cutoff = math.ceil(now - SPEED_WINDOW_SECONDS)
        while self._buckets and self._buckets[0][0] < cutoff:
            self._buckets.popleft()
        self._active.prune(cutoff)
        return cutoff

    def estimate(self, now: float) -> int | None:
        cutoff = self._prune(now)
        amounts = sum(amount for _, amount, _ in self._buckets)
        count = sum(number for _, _, number in self._buckets)
        elapsed = self._active.seconds_since(cutoff)
        if amounts < MIN_SAMPLE_BYTES or elapsed < MIN_SAMPLE_SECONDS or count < MIN_SAMPLE_CHUNKS:
            return None
        return min(MAX_KIB, max(1, int(amounts / elapsed / 1024)))


def _rate_label(value: int) -> str:
    return f"{value} KiB/s" if value else "不限速"


class MonitorAutoThrottle:
    """Monitor middleware only: never changes RPCs, concurrency, or saved settings.

    Observations and configuration run on the account's Telegram event loop.
    Callers capture `generation` when sending RPCs/entering a limiter wait, and
    deliver results only to that same generation. This prevents delayed results
    from a previous enabled run from affecting a newly enabled governor.
    """

    def __init__(
        self,
        limiter: _Limiter,
        *,
        clock: Callable[[], float] = time.monotonic,
        log: Callable[[str], None] = lambda _line: None,
    ) -> None:
        self._limiter = limiter
        self._clock = clock
        self._log = log
        self._enabled = False
        self._generation = 0
        self._enabled_since = -math.inf
        self._manual_kib = 0
        self._auto_kib: int | None = None
        self._probe_kib: int | None = None
        self._first_wait: float | None = None
        self._incident_end = -math.inf
        self._floor_logged_at = -math.inf
        self._have_success = False
        self._health_since = -math.inf
        self._health = _Intervals()
        self._sample = _RecentSpeed()

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def effective_kib(self) -> int:
        if self._auto_kib is None:
            return self._manual_kib
        return min(self._manual_kib, self._auto_kib) if self._manual_kib else self._auto_kib

    def _sync(self) -> None:
        self._limiter.bytes_per_second = self.effective_kib * 1024

    def _clear_health(self) -> None:
        self._have_success = False
        self._health_since = self._clock()
        self._health = _Intervals()

    def _transition(self, reason: str, old: int) -> None:
        self._sync()
        self._log(f"[监控自动限速] {reason}｜{_rate_label(old)} → {_rate_label(self.effective_kib)}")

    def configure(self, manual_kib: int, enabled: bool) -> None:
        previous = self.effective_kib
        self._manual_kib = min(MAX_KIB, max(0, int(manual_kib)))
        if bool(enabled) != self._enabled:
            self._enabled = bool(enabled)
            self._generation += 1
            self._enabled_since = self._clock()
            self._auto_kib = self._probe_kib = None
            self._first_wait = None
            self._incident_end = self._floor_logged_at = -math.inf
            self._sample = _RecentSpeed()
            self._clear_health()
            if self._enabled:
                self._transition("已启用", previous)
            else:
                self._transition("已关闭，恢复手动设置", previous)
            return
        if self._enabled and previous != self.effective_kib:
            self._clear_health()
            self._transition("手动上限已调整", previous)
        else:
            self._sync()

    def on_wait(self, sent_at: float, seconds: int, kind: str = "FLOOD_WAIT") -> None:
        now = self._clock()
        if not self._enabled or not self._enabled_since <= sent_at <= now:
            return
        if kind not in ("FLOOD_WAIT", "FLOOD_PREMIUM_WAIT"):
            return

        # Any fresh raw flood (even a duplicate from the same incident) breaks
        # the healthy-run evidence. Do not modify or sleep for the RPC.
        self._clear_health()
        independent = sent_at > self._incident_end
        self._incident_end = max(self._incident_end, now + max(0, seconds) + INCIDENT_GUARD_SECONDS)

        old = self.effective_kib
        if 0 < old <= MIN_KIB and now - self._floor_logged_at >= PAIR_SECONDS:
            self._floor_logged_at = now
            self._log(
                f"[监控自动限速] 收到 {kind}（{seconds}s），已到自动下限"
                f"或手动上限更低，保持 {_rate_label(old)}，继续遵守服务端要求"
            )

        if not independent:
            return
        if self._first_wait is None or now - self._first_wait > PAIR_SECONDS:
            self._first_wait = now
            return
        # Two separate observed incidents per reduction; don't reuse the
        # second half of one pair to ratchet down on the very next incident.
        self._first_wait = None
        if 0 < old <= MIN_KIB:
            return

        reason = f"5 分钟内第 2 次独立限流（{kind} {seconds}s）"
        if old:
            self._auto_kib = max(MIN_KIB, old * 3 // 4)
            if self._probe_kib is None:
                self._probe_kib = old
        else:
            measured = self._sample.estimate(now)
            if measured is None:
                reason += "，近期有效测速样本不足，使用保守限速"
                self._auto_kib = MIN_KIB
                self._probe_kib = UNKNOWN_UNLIMITED_PROBE_KIB
            else:
                reason += f"，近期监控实测速率 {measured} KiB/s"
                self._auto_kib = max(MIN_KIB, measured * 3 // 4)
                self._probe_kib = (measured if measured > self._auto_kib
                                   else UNKNOWN_UNLIMITED_PROBE_KIB)
        self._transition(reason, old)

    def on_success(self, sent_at: float, amount: int) -> None:
        now = self._clock()
        if (not self._enabled or sent_at < self._enabled_since or sent_at <= self._incident_end
                or not 0 < now - sent_at <= MAX_HEALTHY_RPC_SECONDS or amount <= 0):
            return
        if self.effective_kib == 0:
            self._sample.observe(sent_at, now, amount)
        self._have_success = True
        self._advance_health(sent_at, now)

    def on_paced(self, started_at: float, ended_at: float) -> None:
        """Count a brief receive-limiter sleep for already received bytes only."""
        if (not self._enabled or not self._have_success or started_at < self._enabled_since
                or started_at <= self._incident_end or not 0 < ended_at - started_at <= 1):
            return
        self._advance_health(started_at, ended_at)

    def _advance_health(self, start: float, end: float) -> None:
        if self._auto_kib is None:
            return
        self._health.add(max(start, self._health_since), end)
        self._health.prune(end - MAX_HEALTHY_RPC_SECONDS, keep_elapsed=True)
        if self._health.seconds + 1e-7 < HEALTHY_SECONDS:
            return
        previous = self.effective_kib
        self._clear_health()
        candidate = self._auto_kib * 5 // 4
        if self._manual_kib:
            self._auto_kib = candidate if candidate < self._manual_kib else None
        else:
            assert self._probe_kib is not None
            self._auto_kib = min(candidate, self._probe_kib) if self._auto_kib < self._probe_kib else None
        if self._auto_kib is None:
            self._probe_kib = None
        if previous != self.effective_kib:
            self._transition("已累计 10 分钟健康下载，逐步恢复限速", previous)
        else:
            self._sync()
