"""Installer manifest tests use plain files and never run an uninstaller."""

from pathlib import Path
import re
from runpy import run_path

import pytest


ROOT = Path(__file__).resolve().parents[1]
build_manifest = run_path(str(ROOT / "packaging" / "build_uninstall_manifest.py"))["build_manifest"]


def test_uninstall_removes_exactly_packaged_files_and_only_empty_directories(tmp_path):
    entries = ["TelegramDownloader.exe", "_internal/third_party/mo.dll",
               "_internal/third_party/含空格 file.txt", "_internal/one.dll"]
    for name in entries:
        entry = tmp_path / name
        entry.parent.mkdir(parents=True, exist_ok=True)
        entry.touch()
    manifest = build_manifest(tmp_path)
    added = re.findall(r'^  File "\$\{DISTDIR\}\\([^"]+)"$', manifest, re.M)
    removed = re.findall(r'^  Delete "\$INSTDIR\\([^"]+)"$', manifest, re.M)
    assert added == removed
    assert set(removed) == {item.replace("/", "\\") for item in entries}
    assert 'RMDir "$INSTDIR\\_internal\\third_party"' in manifest
    assert "RMDir /r" not in manifest
    installer = (ROOT / "packaging" / "installer.nsi").read_text()
    assert "RMDir /r" not in installer
    assert "!insertmacro TGDL_RemoveFiles" in installer


@pytest.mark.parametrize("entry", ["data/notes.txt", "_internal/tgdl_session.session", "portable.flag",
    "_internal/.env", "_internal/monitored_channels.json", "_internal/accounts.json",
    "_internal/.env.migrating", "_internal/tgdl_download_history.sqlite3-wal",
    "_internal/other/tgdl_session.session-journal",])
def test_installer_manifest_rejects_runtime_data(tmp_path, entry):
    (tmp_path / "TelegramDownloader.exe").touch()
    item = tmp_path / entry
    item.parent.mkdir(parents=True, exist_ok=True)
    item.touch()
    with pytest.raises(ValueError, match="contains user data"):
        build_manifest(tmp_path)


def test_installer_manifest_rejects_nsis_interpolation(tmp_path):
    (tmp_path / "TelegramDownloader.exe").touch()
    (tmp_path / "$INSTDIR.dll").touch()
    with pytest.raises(ValueError, match="Unsupported Windows installer"):
        build_manifest(tmp_path)
