from dataclasses import dataclass

from tgdl.auto_throttle import MonitorAutoThrottle


@dataclass
class Limiter:
    bytes_per_second: float = 0


class Driver:
    def __init__(self, manual=2048, *, enabled=True):
        self.now = 0.0
        self.limiter = Limiter()
        self.lines = []
        self.policy = MonitorAutoThrottle(self.limiter, clock=lambda: self.now, log=self.lines.append)
        self.policy.configure(manual, enabled)

    @property
    def effective(self):
        assert self.limiter.bytes_per_second == self.policy.effective_kib * 1024
        return self.policy.effective_kib

    def wait(self, *, after=20, seconds=3, sent=None, kind="FLOOD_WAIT"):
        self.now += after
        self.policy.on_wait(self.now - 0.5 if sent is None else sent, seconds, kind)

    def pair(self):
        self.wait()
        self.wait(kind="FLOOD_PREMIUM_WAIT")

    def bytes(self, elapsed, amount=512 * 1024):
        sent = self.now
        self.now += elapsed
        self.policy.on_success(sent, amount)

    def work(self, elapsed, *, lane_seconds=1):
        # Model successful, contiguous transfer. Let the account finish all
        # lingering server cooldown / burst guard before sending fresh RPCs.
        self.now += 10
        for _ in range(elapsed // lane_seconds):
            self.bytes(lane_seconds)
        if elapsed % lane_seconds:
            self.bytes(elapsed % lane_seconds)


def test_default_off_and_generation_changes_only_on_enable_or_disable():
    d = Driver(2048, enabled=False)
    d.pair()
    assert d.effective == 2048 and not d.lines
    before = d.policy.generation
    d.policy.configure(2048, True)
    enabled = d.policy.generation
    assert enabled != before
    d.policy.configure(4096, True)
    assert d.policy.generation == enabled
    d.pair()
    assert d.effective == 3072
    d.policy.configure(4096, False)
    assert d.effective == 4096
    assert d.policy.generation != enabled
    assert "恢复手动设置" in d.lines[-1]
    d.policy.configure(4096, True)
    d.wait(sent=20)  # reply to a request sent during an older enabled run
    d.wait()
    assert d.effective == 4096
    d.wait()
    assert d.effective == 3072


def test_two_independent_reports_in_rolling_window_with_nonoverlapping_pairs():
    d = Driver(4096)
    d.wait(after=16, seconds=7)  # t=16 service wait through t=23
    d.wait(after=3, seconds=5)  # wait overlaps; one episode
    d.wait(after=14, sent=15, seconds=1)  # late reply from the same request batch
    assert d.effective == 4096
    d.wait(after=5, seconds=3, kind="FLOOD_PREMIUM_WAIT")  # fresh send before t=38
    assert d.effective == 3072
    assert "第 2 次独立限流" in d.lines[-1]
    d.wait()
    assert d.effective == 3072
    d.wait()
    assert d.effective == 2304


def test_burst_guard_merges_delayed_concurrent_requests():
    d = Driver()
    d.wait(seconds=1)
    d.wait(after=2.8, seconds=1)  # sent within 2s of the wait ending
    assert d.effective == 2048
    d.wait()
    assert d.effective == 1536


def test_window_boundary_and_unrelated_errors_do_not_count():
    d = Driver(4096)
    d.wait(kind="TimedOutError")
    d.wait()
    d.wait(after=301)
    assert d.effective == 4096
    d.wait(after=300)  # inclusive at exactly five minutes
    assert d.effective == 3072


def test_floor_and_user_setting_below_floor_are_preserved_and_logged_sparingly():
    d = Driver(1200)
    d.pair()
    assert d.effective == 1024
    d.pair()
    d.pair()
    assert d.effective == 1024
    assert sum("自动下限" in line for line in d.lines) == 1
    d.wait(after=301)
    assert sum("自动下限" in line for line in d.lines) == 2
    d.policy.configure(500, True)
    assert d.effective == 500
    d.pair()
    assert d.effective == 500
    d.policy.configure(1200, True)
    assert d.effective == 1024
    d.policy.configure(500, False)
    assert d.effective == 500


def test_unlimited_initial_rate_is_recent_aggregate_not_single_lane_peak():
    d = Driver(0)
    for _ in range(8):
        # Two overlapping 512 KiB responses during 0.5 seconds (2048 KiB/s).
        started = d.now
        d.now += 0.5
        d.policy.on_success(started, 512 * 1024)
        d.policy.on_success(started, 512 * 1024)
    d.wait(after=2)
    d.wait(after=10)
    assert d.effective == 1536
    assert "实测速率 2048 KiB/s" in d.lines[-1]
    d.work(600)
    assert d.effective == 1920
    d.work(600)
    assert d.effective == 2048
    d.work(600)
    assert d.effective == 0


def test_speed_sample_includes_short_interblock_overhead_not_just_rtt():
    d = Driver(0)
    for _ in range(8):
        d.bytes(0.25, 1024 * 1024)
        d.now += 0.25
    # Confirmed 8192 KiB over 3.75s including 7 dispatch gaps of 0.25s.
    d.wait(after=3)
    d.wait(after=10)
    assert d.effective == (int(8192 / 3.75) * 3 // 4)
    assert "实测速率 2184 KiB/s" in d.lines[-1]


def test_unlimited_old_or_insufficient_samples_use_guarded_fallback():
    d = Driver(0)
    for _ in range(8):
        d.bytes(1, 1024 * 1024)
    d.wait(after=70)
    d.wait()
    assert d.effective == 1024
    assert "有效测速样本不足" in d.lines[-1]
    for expected in (1280, 1600, 2000, 2048, 0):
        d.work(600)
        assert d.effective == expected


def test_recovery_excludes_idle_stalled_and_overlapping_time():
    d = Driver()
    d.pair()
    assert d.effective == 1536
    d.work(300)
    d.now += 4800
    d.bytes(77)  # unhealthily slow; successful bytes don't validate a long stall
    assert d.effective == 1536
    for _ in range(299):
        sent = d.now
        d.now += 1
        d.policy.on_success(sent, 512 * 1024)
        d.policy.on_success(sent, 512 * 1024)
    assert d.effective == 1536  # 599 actual seconds, not 898 concurrent lane-seconds
    d.bytes(1)
    assert d.effective == 1920
    d.work(600)
    assert d.effective == 2048


def test_limiter_pacing_counts_once_and_never_without_recent_success():
    d = Driver()
    d.pair()
    d.now += 60
    for _ in range(600):
        start = d.now
        d.now += 1
        d.policy.on_paced(start, d.now)
    assert d.effective == 1536
    d.work(596)
    for _ in range(2):
        start = d.now
        d.now += 1
        d.policy.on_success(start, 512 * 1024)
        d.policy.on_paced(start, d.now)
    assert d.effective == 1536  # overlapping paced/RPC intervals only counted once
    d.policy.on_paced(d.now - 2, d.now)  # too long for a real limiter sleep
    assert d.effective == 1536
    for _ in range(2):
        start = d.now
        d.now += 1
        d.policy.on_paced(start, d.now)
    assert d.effective == 1920


def test_any_raw_flood_resets_health_and_no_cooldown_seconds_count():
    d = Driver()
    d.pair()
    d.work(590)
    d.wait(after=0.1, seconds=10)
    d.wait(after=1, seconds=12)  # same incident still resets/extends health gate
    d.bytes(8)
    assert d.effective == 1536
    d.work(599)
    assert d.effective == 1536
    d.bytes(1)
    assert d.effective == 1920


def test_recovery_does_not_double_count_late_intervals_across_steps():
    d = Driver()
    d.pair()
    d.work(600)
    assert d.effective == 1920
    d.now += 1
    d.policy.on_success(d.now - 10, 512 * 1024)  # starts before prior recovery
    for _ in range(598):
        d.bytes(1)
    assert d.effective == 1920
    d.bytes(1)
    assert d.effective == 2048


def test_account_instances_have_independent_observations_and_limiters():
    a = Driver(2048)
    b = Driver(4096)
    a.wait()
    b.wait()
    assert (a.effective, b.effective) == (2048, 4096)
    a.wait()
    assert (a.effective, b.effective) == (1536, 4096)
    b.wait()
    a.work(600)
    assert (a.effective, b.effective) == (1920, 3072)


def test_persistent_same_episode_at_floor_can_be_reported_without_ratchet():
    d = Driver(1024)
    d.wait(seconds=360)
    assert sum("自动下限" in line for line in d.lines) == 1
    d.wait(after=4)
    assert sum("自动下限" in line for line in d.lines) == 1
    d.wait(after=300)  # fresh raw response but still within the server wait
    assert sum("自动下限" in line for line in d.lines) == 2
    assert d.effective == 1024
