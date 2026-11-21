"""Compact dispatch logging groups events without misreporting pause requests."""

from tgdl.monitor import MonitorDispatchSummary


def test_pause_and_resume_are_grouped_by_channel():
    summary = MonitorDispatchSummary(quiet_seconds=0.9, max_seconds=2.5)
    summary.add("paused", "stripchat", now=10.0)
    summary.add("paused", "stripchat", now=10.3)
    assert summary.drain(now=10.5) == []
    assert summary.drain(now=11.3) == [
        "[调度] 高优先级任务优先｜已暂停 stripchat 的2个下载任务"
    ]
    summary.add("resumed", "stripchat", now=12.0)
    summary.add("resumed", "stripchat", now=12.4)
    assert summary.drain(now=13.4) == [
        "[调度] stripchat：恢复2个下载任务｜从原进度继续"
    ]


def test_grouped_events_preserve_type_and_original_order():
    summary = MonitorDispatchSummary()
    summary.add("paused", "A", now=0)
    summary.add("paused", "B", now=0.2)
    summary.add("resumed", "A", now=0.5)
    assert summary.drain(now=1.0) == []
    assert summary.drain(force=True) == [
        "[调度] 高优先级任务优先｜已暂停 A 的1个下载任务",
        "[调度] 高优先级任务优先｜已暂停 B 的1个下载任务",
        "[调度] A：恢复1个下载任务｜从原进度继续",
    ]
    assert summary.drain(force=True) == []


def test_long_series_eventually_flushes_and_rejects_unknown_state():
    summary = MonitorDispatchSummary(quiet_seconds=5, max_seconds=2)
    summary.add("paused", "C", now=0)
    summary.add("paused", "C", now=1)
    assert summary.drain(now=1.5) == []
    assert len(summary.drain(now=2.1)) == 1
    try:
        summary.add("requested", "C")
    except ValueError:
        pass
    else:
        raise AssertionError("Pause requests must never be logged as completed pauses")
