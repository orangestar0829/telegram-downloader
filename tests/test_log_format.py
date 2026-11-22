from datetime import datetime
from queue import Queue

from tgdl.gui import _QueueWriter
from tgdl.log_format import TimestampedLines


def test_timestamp_each_line_even_when_print_splits_writes():
    formatter = TimestampedLines(lambda: datetime(2026, 11, 19, 15, 4, 9))
    assert formatter.format("Download") == "[2026-11-19 15:04:09] Download"
    assert formatter.format(" started\nNext\n") == " started\n[2026-11-19 15:04:09] Next\n"
    assert formatter.format("Done\n") == "[2026-11-19 15:04:09] Done\n"


def test_stdout_and_stderr_share_line_state():
    sink = Queue()
    formatter = TimestampedLines(lambda: datetime(2026, 11, 19, 15, 4, 9))
    stdout = _QueueWriter(sink, formatter)
    stderr = _QueueWriter(sink, formatter)
    stdout.write("First")
    stderr.write(" line\n")
    stdout.write("Second\n")
    assert "".join(sink.get_nowait() for _ in range(3)) == (
        "[2026-11-19 15:04:09] [账号:原账号（未登录）] First line\n"
        "[2026-11-19 15:04:09] [账号:原账号（未登录）] Second\n"
    )
