"""Window geometry survives restarts but never restores off-screen coordinates."""

from tgdl import window_state


def test_save_and_restore_normal_window(tmp_path, monkeypatch):
    target = tmp_path / "window_state.json"
    monkeypatch.setattr(window_state, "window_state_path", lambda: target)
    assert window_state.load_window_geometry(1920, 1080) == "1080x820"
    assert window_state.save_window_geometry("1180x760+220+110", 1920, 1080)
    assert window_state.load_window_geometry(1920, 1080) == "1180x760+220+110"
    assert target.exists()


def test_restore_clamps_and_rejects_unavailable_screen_placement(tmp_path, monkeypatch):
    target = tmp_path / "window_state.json"
    monkeypatch.setattr(window_state, "window_state_path", lambda: target)
    assert window_state.save_window_geometry("1000x700+800+200", 1920, 1080)
    assert window_state.load_window_geometry(1280, 800) == "1000x700+280+100"
    assert window_state.load_window_geometry(800, 600) == "1080x820"
    target.write_text('{"geometry":"9999x9999+5000+5000"}', encoding="utf-8")
    assert window_state.load_window_geometry(1920, 1080) == "1080x820"


def test_rejects_invalid_or_corrupt_state(tmp_path, monkeypatch):
    target = tmp_path / "window_state.json"
    monkeypatch.setattr(window_state, "window_state_path", lambda: target)
    for value in ("100x100+20+20", "1080x820-300-100", "zoomed", "foo"):
        assert not window_state.save_window_geometry(value, 1920, 1080)
    target.write_text("not-json", encoding="utf-8")
    assert window_state.load_window_geometry(1920, 1080) == "1080x820"
