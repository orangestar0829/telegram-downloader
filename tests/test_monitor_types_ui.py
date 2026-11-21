"""Monitor media types remain independent of the bulk-download page."""

from types import SimpleNamespace

from tgdl import gui, monitor


class Var:
    def __init__(self, value):
        self.value = value
    def set(self, value):
        self.value = value
    def get(self):
        return self.value


class Tree:
    def __init__(self):
        self.selected = ("0",)
    def selection(self):
        return self.selected


def app_with_channels(channels):
    app = object.__new__(gui._App)
    app._monitored = channels
    app._monitor_next = {}
    app._monitor_batch_used = {}
    app._monitor_totals = {}
    app.monitor_tree = Tree()
    app.monitor_minutes_var = Var("5")
    app.monitor_priority_var = Var("0")
    app.monitor_batch_var = Var("100")
    app.monitor_direction_var = Var("从新到旧")
    app.monitor_type_photo = Var(True)
    app.monitor_type_video = Var(True)
    app.monitor_type_anim = Var(False)
    app.type_photo = Var(False)
    app.type_video = Var(True)
    app.type_anim = Var(False)
    app.messagebox = SimpleNamespace(showwarning=lambda *args: None)
    app._monitor_refresh = lambda: None
    return app


def test_monitor_types_selection_and_saved_update_do_not_affect_batch(tmp_path, monkeypatch):
    channels = [
        {"target": "a", "minutes": 5, "priority": 99, "batch_size": 500,
         "direction": "oldest", "types": "photo", "enabled": True},
        {"target": "b", "minutes": 60, "priority": 0, "batch_size": 100,
         "direction": "newest", "types": "video,animation", "enabled": True},
    ]
    app = app_with_channels(channels)
    path = tmp_path / "channels.json"
    monkeypatch.setattr(gui, "save_channels", lambda items: monitor.save_channels(items, path))

    app._monitor_selection_changed()
    assert app._monitor_types_string() == "photo"
    assert app._types_string() == "video"
    app.monitor_tree.selected = ("1",)
    app._monitor_selection_changed()
    assert app._monitor_types_string() == "video,animation"
    app.monitor_tree.selected = ("0",)
    app._monitor_selection_changed()
    app.monitor_type_photo.set(False)
    app.monitor_type_video.set(True)
    app._monitor_update()
    assert [item["types"] for item in monitor.load_channels(path)] == [
        "video", "video,animation"
    ]
    assert app._types_string() == "video"


def test_monitor_types_label_supports_legacy_values():
    assert gui._App._monitor_types_label("photo,video") == "图片、视频"
    assert gui._App._monitor_types_label("all") == "图片、视频、GIF"
    assert gui._App._monitor_types_label("gif") == "GIF"
