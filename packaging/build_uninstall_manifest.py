"""Generate matching NSIS install/uninstall commands for a fresh build."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import stat


_UNSAFE = re.compile(r'[<>:"/\\|?*$\x00-\x1f]')
_USER_DATA = {"data", "accounts", "logs", "downloads", "portable.flag",
              "accounts.json", "monitored_channels.json", ".env"}
_STATE_FILE = re.compile(r"\.(?:session|sqlite3)(?:-(?:wal|shm|journal)|\.migrating)?$", re.I)


def _is_runtime_data(relative: Path) -> bool:
    parts = tuple(part.casefold() for part in relative.parts)
    root = parts[1:] if parts[0] == "_internal" else parts
    return (bool(root) and (root[0] in _USER_DATA or root[0].startswith(".env."))) or bool(
        _STATE_FILE.search(parts[-1])
    )


def build_manifest(dist_dir: Path) -> str:
    if not (dist_dir / "TelegramDownloader.exe").is_file():
        raise ValueError("Generate the manifest from a fresh PyInstaller output directory")

    install = ['!macro TGDL_InstallFiles']
    delete = ['!macro TGDL_RemoveFiles']
    current_parent = None
    parents: set[Path] = set()
    names: set[str] = set()
    for entry in sorted(dist_dir.rglob("*")):
        relative = entry.relative_to(dist_dir)
        if _is_runtime_data(relative):
            raise ValueError(f"Build directory contains user data: {relative}")
        if any(_UNSAFE.search(part) or part.endswith((" ", ".")) for part in relative.parts):
            raise ValueError(f"Unsupported Windows installer file name: {relative}")
        if relative.as_posix().casefold() in names:
            raise ValueError(f"Case-insensitive installer path collision: {relative}")
        names.add(relative.as_posix().casefold())
        attrs = getattr(entry.lstat(), "st_file_attributes", 0)
        if entry.is_symlink() or attrs & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise ValueError(f"Build directory must not contain links: {relative}")
        if entry.is_dir():
            continue
        if not entry.is_file():
            raise ValueError(f"Not a regular installer file: {relative}")
        if relative.parent != current_parent:
            suffix = "\\" + str(relative.parent).replace("/", "\\") if relative.parent.parts else ""
            install.append(f'  SetOutPath "$INSTDIR{suffix}"')
            current_parent = relative.parent
        path = str(relative).replace("/", "\\")
        install.append(f'  File "${{DISTDIR}}\\{path}"')
        delete.append(f'  Delete "$INSTDIR\\{path}"')
        parents.update(parent for parent in relative.parents if parent.parts)

    # Never recurse: directories containing files the installer did not own
    # (including extra files in _internal) remain intact.
    for directory in sorted(parents, key=lambda path: (-len(path.parts), path)):
        path = str(directory).replace("/", "\\")
        delete.append(f'  RMDir "$INSTDIR\\{path}"')

    return "\n".join(install + ['!macroend', ''] + delete + ['!macroend', ''])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dist_dir", type=Path)
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    args.manifest.write_text(build_manifest(args.dist_dir), encoding="utf-8-sig")


if __name__ == "__main__":
    main()
