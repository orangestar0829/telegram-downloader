from tgdl.monitor import load_channels, save_channels


def test_channel_intervals_persist_independently(tmp_path):
    path = tmp_path / "data" / "monitored_channels.json"
    items = [
        {"target": "-100123", "minutes": 5, "types": "video", "enabled": True},
        {"target": "-100456", "minutes": 60, "types": "photo,video", "enabled": False},
    ]
    save_channels(items, path)
    assert load_channels(path) == [dict(item, direction="newest", name="") for item in items]
    assert path.parent.name == "data"


def test_invalid_interval_ignored(tmp_path):
    path = tmp_path / "monitored_channels.json"
    save_channels([
        {"target": "a", "minutes": 0, "enabled": True},
        {"target": "b", "minutes": 30, "enabled": True},
    ], path)
    assert [item["target"] for item in load_channels(path)] == ["b"]


def test_existing_config_defaults_to_newest_direction(tmp_path):
    path = tmp_path / "channels.json"
    save_channels([
        {"target": "@older", "minutes": 15, "types": "video", "enabled": True}
    ], path)
    assert load_channels(path)[0]["direction"] == "newest"
    assert load_channels(path)[0]["name"] == ""


def test_channel_name_is_preserved_across_restart(tmp_path):
    path = tmp_path / "monitored_channels.json"
    items = [
        {"target": "-100777", "minutes": 15, "types": "video", "enabled": True,
         "direction": "oldest", "name": "测试频道"}
    ]
    save_channels(items, path)
    assert load_channels(path) == items
