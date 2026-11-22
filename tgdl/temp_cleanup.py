"""Conservative temporary media cleanup with cross-process file ownership locks."""
from __future__ import annotations

import json
import os
import shutil
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

MARKER = ".tgdl-owner.lock"
SIGNATURE = "tgdl-owned-v1"
CANDIDATE_PREFIXES = ("tgdl-", "tgdl-publish-")


def _lock(file, *, nonblocking=True):
    file.seek(0)
    if os.name == "nt":
        import msvcrt
        try:
            msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK if nonblocking else msvcrt.LK_LOCK, 1)
        except OSError:
            return False
    else:
        import fcntl
        try:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0))
        except BlockingIOError:
            return False
    return True


def _unlock(file):
    file.seek(0)
    if os.name == "nt":
        import msvcrt
        msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)


@contextmanager
def own_temporary(directory: Path):
    """Hold an OS lock for the entire lifetime of a temporary directory."""
    directory = Path(directory)
    marker = directory / MARKER
    with marker.open("x+b") as handle:
        handle.write(bytes([0]))
        handle.flush()
        if not _lock(handle):
            raise RuntimeError("Unable to lock new download temporary directory")
        handle.seek(1)
        handle.write(json.dumps({
            "signature": SIGNATURE,
            "pid": os.getpid(),
            "created": time.time(),
            "nonce": uuid.uuid4().hex,
        }).encode("utf-8"))
        handle.flush()
        try:
            yield
        finally:
            _unlock(handle)


def _valid_marker(handle):
    try:
        handle.seek(1)
        entry = json.loads(handle.read().decode("utf-8"))
        return entry.get("signature") == SIGNATURE and bool(entry.get("nonce"))
    except (OSError, UnicodeError, ValueError, AttributeError):
        return False


def _size(directory: Path):
    total = 0
    for root, dirs, files in os.walk(directory, followlinks=False):
        dirs[:] = [d for d in dirs if not (Path(root) / d).is_symlink()]
        for name in files:
            try:
                file = Path(root) / name
                if not file.is_symlink():
                    total += file.stat().st_size
            except OSError:
                continue
    return total


@dataclass
class CleanupResult:
    deleted: int = 0
    bytes_freed: int = 0
    active: int = 0
    legacy: list[Path] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"[缓存清理] 清理 {self.deleted} 项，释放 {self.bytes_freed / 1024**3:.2f} GiB；"
            f"正在使用 {self.active} 项，未识别旧临时文件 {len(self.legacy)} 项，"
            f"失败 {len(self.failed)} 项"
        )


def _candidates(ssd_dir: Path | None, download_dir: Path | None):
    parents = []
    if ssd_dir is not None and Path(ssd_dir).is_dir():
        parents.append(Path(ssd_dir))
    if download_dir is not None and Path(download_dir).is_dir():
        base = Path(download_dir)
        parents.append(base)
        # Media files live only under chat/photos or chat/videos. Do not scan
        # arbitrary nested user folders or recurse through symlinks.
        for channel in base.iterdir():
            if channel.is_dir() and not channel.is_symlink():
                for media in ("photos", "videos"):
                    folder = channel / media
                    if folder.is_dir() and not folder.is_symlink():
                        parents.append(folder)
    seen = set()
    for parent in parents:
        for child in parent.iterdir():
            if child in seen or child.is_symlink():
                continue
            seen.add(child)
            if child.is_dir() and child.name.startswith("tgdl-"):
                yield child
            elif child.is_file() and child.name.startswith(".tgdl-publishing-"):
                yield child


def inspect_cleanup(ssd_dir: Path | None, download_dir: Path | None,
                    *, include_legacy: bool = False) -> CleanupResult:
    """Delete only certified dead owned folders; legacy needs user consent.

    This function is safe to call on startup or manually. Never remove a
    recognized live folder. Unknown or old files only become eligible when
    caller explicitly confirms the risk.
    """
    result = CleanupResult()
    try:
        candidates = list(_candidates(ssd_dir, download_dir))
    except OSError as exc:
        result.failed.append(f"扫描目录失败: {exc}")
        return result
    for candidate in candidates:
        marker = candidate / MARKER if candidate.is_dir() else None
        if marker is None or not marker.is_file() or marker.is_symlink():
            if not include_legacy:
                result.legacy.append(candidate)
                continue
            # Manual legacy deletion only. No markers means no reliable way
            # to verify a concurrent old-version download; user must confirm.
            try:
                length = _size(candidate) if candidate.is_dir() else candidate.stat().st_size
                if candidate.is_dir():
                    shutil.rmtree(candidate)
                else:
                    candidate.unlink()
                result.deleted += 1
                result.bytes_freed += length
            except OSError as exc:
                result.failed.append(f"{candidate}: {exc}")
            continue
        try:
            with marker.open("r+b") as lock_handle:
                if not _valid_marker(lock_handle):
                    result.legacy.append(candidate)
                    continue
                if not _lock(lock_handle):
                    result.active += 1
                    continue
                try:
                    length = _size(candidate)
                    # Keep the locked marker open while removing the files.
                    # Windows refuses removal of opened files; explicitly
                    # delete other contents, then release marker before rmdir.
                    for child in candidate.iterdir():
                        if child == marker:
                            continue
                        if child.is_symlink() or child.is_file():
                            child.unlink()
                        elif child.is_dir():
                            shutil.rmtree(child)
                    result.bytes_freed += length
                    result.deleted += 1
                finally:
                    _unlock(lock_handle)
            marker.unlink()
            candidate.rmdir()
        except OSError as exc:
            result.failed.append(f"{candidate}: {exc}")
    return result
