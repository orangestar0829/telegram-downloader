"""Paused jobs remain visible, but don't consume monitor download slots."""

import queue

from tgdl import gui


class Tree:
    def __init__(self):
        self.rows = {}

    def exists(self, key):
        return key in self.rows

    def item(self, key, *, values):
        self.rows[key] = values

    def insert(self, parent, where, *, iid, values):
        self.rows[iid] = values

    def delete(self, key):
        del self.rows[key]


class Variable:
    def __init__(self):
        self.value = None

    def set(self, value):
        self.value = value


def test_paused_job_does_not_count_against_active_concurrency(monkeypatch):
    app = object.__new__(gui._App)
    app.progress_queue = queue.Queue()
    app._progress_entries = {}
    app._monitor_progress_states = {}
    app.progress_tree = Tree()
    app.progress_summary = Variable()
    now = [100.0]
    monkeypatch.setattr(gui.time, "monotonic", lambda: now[0])
    for peer_id, mid in ((456, 4069), (456, 4068), (789, 558334)):
        app._update_download_progress(
            f"{peer_id}:{mid}:20260816_{mid}", f"group / {mid}",
            6 * 1048576, 176 * 1048576, "下载中",
        )
    app._drain_progress()
    app._monitor_progress_state(456, 4068, "已暂停")
    now[0] = 101.0
    app._drain_progress()
    paused_row = app.progress_tree.rows["456:4068:20260816_4068"]
    assert paused_row[-1] == "已暂停"
    assert paused_row[-2] == "—"
    assert "正在下载 2 个文件，已暂停 1 个" in app.progress_summary.value

    app._monitor_progress_state(456, 4068, "恢复中")
    now[0] = 102.0
    app._drain_progress()
    assert app.progress_tree.rows["456:4068:20260816_4068"][-1] == "恢复中"
    app._update_download_progress(
        "456:4068:20260816_4068", "group / 4068",
        10 * 1048576, 176 * 1048576, "下载中",
    )
    now[0] = 103.0
    app._drain_progress()
    assert app.progress_tree.rows["456:4068:20260816_4068"][-1] == "下载中"
    assert "已暂停" not in app.progress_summary.value


