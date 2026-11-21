"""Scan log reports discovery, not completed media downloads."""

from types import SimpleNamespace

from tgdl import gui


def make_app():
    app = object.__new__(gui._App)
    app._monitored = [{
        "target": "@photos", "name": "图片频道",
        "minutes": 40, "batch_size": 100, "enabled": True,
    }]
    app._monitor_active = True
    app._monitor_scan_inflight = True
    app._monitor_totals = {}
    app._monitor_batch_used = {}
    app._monitor_next = {}
    messages = []
    app._log = messages.append
    return app, messages


def result(*, matched, queued, skipped=0, scanned=20, more_pending=False):
    return SimpleNamespace(
        matched=matched, queued=queued, skipped=skipped,
        scanned_messages=scanned, more_pending=more_pending,
    )


def test_scan_finished_log_does_not_mean_download_finished():
    app, messages = make_app()
    app._monitor_slice_done("@photos", result(matched=1, queued=1, scanned=3))
    assert len(messages) == 1
    text = messages[0]
    assert "[扫描]" in text
    assert "完成" in text
    assert "检查3条" in text
    assert "匹配1" in text
    assert "新增1" in text
    assert "[下载完成]" not in text
    assert "@photos" not in app._monitor_totals
    assert app._monitor_next["@photos"] > 0


def test_quota_log_says_next_scheduled_scan_not_immediate_next_batch():
    app, messages = make_app()
    for _ in range(4):
        app._monitor_slice_done(
            "@photos", result(matched=20, queued=20, more_pending=True)
        )
    assert messages == [], "Intermediate 20-message scan slices should be silent"
    app._monitor_slice_done(
        "@photos", result(matched=20, queued=4, skipped=16, more_pending=True)
    )
    text = messages[-1]
    assert len(messages) == 1
    assert "达本轮上限100，下轮续扫" in text
    assert "检查100条" in text
    assert "匹配100" in text
    assert "新增84" in text
    assert "跳过16" in text
    assert "还有下一批" not in text
    assert app._monitor_batch_used["@photos"] == 0
    assert "@photos" not in app._monitor_totals


def test_next_scan_round_starts_with_fresh_totals():
    app, messages = make_app()
    app._monitor_slice_done("@photos", result(matched=1, queued=1, scanned=2))
    app._monitor_slice_done("@photos", result(matched=1, queued=1, scanned=2))
    assert len(messages) == 2
    assert messages[-1].count("检查2条") == 1
    assert "匹配1" in messages[-1]


def test_scan_failure_count_preserved_in_compact_summary():
    app, messages = make_app()
    partial = result(matched=1, queued=1, more_pending=True)
    partial.failed = 1
    app._monitor_slice_done("@photos", partial)
    assert messages == []
    completed = result(matched=1, queued=1, scanned=3)
    app._monitor_slice_done("@photos", completed)
    assert len(messages) == 1
    assert "失败1" in messages[0]
    assert "检查23条" in messages[0]
