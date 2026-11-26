from tgdl.monitor import load_channels, next_due_channel, save_channels


def test_channel_intervals_persist_independently(tmp_path):
    path = tmp_path / "data" / "accounts" / "default" / "monitored_channels.json"
    items = [
        {"target": "-100123", "minutes": 5, "types": "video", "enabled": True},
        {"target": "-100456", "minutes": 60, "types": "photo,video", "enabled": False},
    ]
    save_channels(items, path)
    assert load_channels(path) == [
        dict(item, direction="newest", name="", priority=0, batch_size=100, mode="scheduled")
        for item in items
    ]
    assert path.parent.name == "default"


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
    assert load_channels(path) == [dict(item, priority=0, batch_size=100, mode="scheduled") for item in items]


def test_channel_priority_and_batch_persist(tmp_path):
    path = tmp_path / "monitored_channels.json"
    save_channels([
        {"target": "@photos", "minutes": 5, "priority": 12, "batch_size": 500},
        {"target": "@videos", "minutes": 5, "priority": -3, "batch_size": 20},
    ], path)
    channels = load_channels(path)
    assert [(ch["priority"], ch["batch_size"]) for ch in channels] == [(12, 500), (-3, 20)]


def test_invalid_optional_channel_fields_default_safely(tmp_path):
    path = tmp_path / "monitored_channels.json"
    save_channels([{
        "target": "@legacy", "minutes": 5, "priority": "invalid",
        "batch_size": 0,
    }], path)
    loaded = load_channels(path)[0]
    assert loaded["priority"] == 0
    assert loaded["batch_size"] == 100


def test_priority_order_uses_round_robin_slices():
    channels = [
        {"target": "low", "priority": -1, "enabled": True},
        {"target": "high", "priority": 20, "enabled": True},
        {"target": "default", "priority": 0, "enabled": True},
    ]
    next_at = {item["target"]: 0 for item in channels}
    selected = [
        next_due_channel(channels, next_at, now=100, turn=turn)["target"]
        for turn in range(6)
    ]
    assert selected == ["high", "default", "low", "high", "default", "low"]
    next_at["high"] = 200
    assert next_due_channel(channels, next_at, now=100, turn=0)["target"] == "default"
    assert next_due_channel(channels, {"high": 200, "low": 200, "default": 200}, 100, 0) is None
