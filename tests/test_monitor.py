from tgdl.monitor import load_channels, save_channels


def test_channel_intervals_persist_independently(tmp_path):
    path = tmp_path / "data" / "monitored_channels.json"
    items = [
        {"target": "-100123", "minutes": 5, "types": "video", "enabled": True},
        {"target": "-100456", "minutes": 60, "types": "photo,video", "enabled": False},
    ]
    save_channels(items, path)
    assert load_channels(path) == items
    assert path.parent.name == "data"


def test_invalid_interval_ignored(tmp_path):
    path = tmp_path / "monitored_channels.json"
    save_channels([
        {"target": "a", "minutes": 0, "enabled": True},
        {"target": "b", "minutes": 30, "enabled": True},
    ], path)
    assert [item["target"] for item in load_channels(path)] == ["b"]
