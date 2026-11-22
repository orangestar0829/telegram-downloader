"""A crash-safe per-user-session single-instance guard for the desktop GUI."""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from pathlib import Path


class AlreadyRunning(RuntimeError):
    pass


@contextmanager
def gui_instance_guard(data_directory: Path):
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create = kernel32.CreateMutexW
        create.argtypes = [ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR]
        create.restype = wintypes.HANDLE
        handle = create(None, False, "Local\\TelegramDownloaderDesktopGUI")
        if not handle:
            raise OSError(ctypes.get_last_error(), "CreateMutexW failed")
        try:
            if ctypes.get_last_error() == 183:  # ERROR_ALREADY_EXISTS
                raise AlreadyRunning("Telegram 下载器已经在运行，请切换到已打开的窗口。")
            yield
        finally:
            kernel32.CloseHandle(handle)
    else:
        import fcntl
        directory = Path(data_directory)
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / ".gui-instance.lock").open("a+b") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise AlreadyRunning("Telegram 下载器已经在运行。") from exc
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
