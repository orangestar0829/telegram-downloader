from tgdl.gui import _App


class FakeLog:
    def __init__(self, bottom):
        self.bottom = bottom
        self.inserted = []
        self.scrolled = False

    def yview(self):
        return (0.5, self.bottom)

    def configure(self, **kwargs):
        pass

    def insert(self, where, content):
        self.inserted.append(content)

    def see(self, where):
        self.scrolled = True


def test_log_does_not_jump_while_reading_history():
    widget = FakeLog(0.5)
    _App._append_to_log(widget, "new line\n")
    assert widget.inserted == ["new line\n"]
    assert not widget.scrolled


def test_log_follows_when_already_at_bottom():
    widget = FakeLog(1.0)
    _App._append_to_log(widget, "new line\n")
    assert widget.scrolled
