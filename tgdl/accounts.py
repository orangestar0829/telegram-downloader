"""Per-account configuration locations with backwards-compatible migration."""
from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

from tgdl.paths import prepare_data_dir

DEFAULT_ACCOUNT = "default"
_SAFE_ID = re.compile(r"^[a-z0-9_-]{1,48}$")


@dataclass(frozen=True)
class Account:
    id: str
    name: str


class AccountRegistry:
    """All account config/session/monitor files live in accounts/<id>/.

    Root-level legacy files remain only as downgrade backups. The shared
    download-history SQLite database stays at the data root.
    """

    def __init__(self, root: Path | None = None) -> None:
        self.root = Path(root) if root is not None else prepare_data_dir()
        self.registry_file = self.root / "accounts.json"
        # Keep the default's optional remark alongside its other account data.
        # Older builds expect accounts.json to contain secondary accounts only.
        self._default_remark_file = self.root / "accounts" / DEFAULT_ACCOUNT / "remark.json"

    @staticmethod
    def validate(account_id: str) -> str:
        if not _SAFE_ID.fullmatch(account_id):
            raise ValueError("Invalid account id")
        return account_id

    def accounts(self) -> list[Account]:
        try:
            raw_remark = json.loads(self._default_remark_file.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # An optional display setting must not block access to any account.
            raw_remark = ""
        remark = raw_remark.strip() if isinstance(raw_remark, str) else ""
        result = [Account(DEFAULT_ACCOUNT, remark)]
        if not self.registry_file.exists():
            return result
        rows = json.loads(self.registry_file.read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise ValueError("Invalid account registry")
        seen = {DEFAULT_ACCOUNT}
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("Invalid account registry entry")
            account_id = self.validate(str(row.get("id", "")))
            raw_name = row.get("remark", row.get("name", ""))
            name = raw_name.strip() if isinstance(raw_name, str) else ""
            if account_id in seen:
                raise ValueError("Duplicate account id")
            seen.add(account_id)
            result.append(Account(account_id, name))
        return result

    @staticmethod
    def _write_json(path: Path, data: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        try:
            temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    def _write(self, rows: list[Account]) -> None:
        # Older builds require a nonempty name when reading accounts.json.
        # Carry an explicit empty remark without making downgrade lose accounts.
        self._write_json(self.registry_file, [
            {"id": a.id, "name": a.name} if a.name else
            {"id": a.id, "name": f"账号 {a.id[:8]}", "remark": ""}
            for a in rows
        ])

    def add(self, name: str = "") -> Account:
        name = name.strip()
        existing = self.accounts()
        account = Account(uuid.uuid4().hex[:16], name)
        while account.id in {a.id for a in existing}:
            account = Account(uuid.uuid4().hex[:16], name)
        from tgdl.config import read_env_values, update_env

        original = read_env_values(self.env_path(DEFAULT_ACCOUNT))
        values = {
            "api_id": original["api_id"], "api_hash": original["api_hash"],
            "phone": "", "download_dir": original["download_dir"],
            "temp_download_dir": original["temp_download_dir"],
            "proxy_host": "", "proxy_port": "", "proxy_username": "",
            "proxy_password": "", "media_concurrency": "1",
            "media_retries": original["media_retries"],
            "chunk_concurrency": original["chunk_concurrency"],
            "media_speed_kib": original["media_speed_kib"],
            "monitor_auto_throttle": "0",  # Never inherit another account's opt-in.
        }
        update_env(values, self.env_path(account.id))
        self._write(existing[1:] + [account])
        return account

    def rename(self, account_id: str, name: str) -> None:
        name = name.strip()
        if account_id == DEFAULT_ACCOUNT:
            self._write_json(self._default_remark_file, name)
            return
        rows = self.accounts()[1:]
        if not any(a.id == account_id for a in rows):
            raise ValueError("Unknown account")
        self._write([Account(a.id, name if a.id == account_id else a.name) for a in rows])

    def account_dir(self, account_id: str) -> Path:
        self.validate(account_id)
        if account_id == DEFAULT_ACCOUNT:
            return self.root
        return self.root / "accounts" / account_id

    def env_path(self, account_id: str) -> Path:
        self.validate(account_id)
        return self.root / "accounts" / account_id / ".env"

    def session_path(self, account_id: str) -> Path:
        self.validate(account_id)
        return self.root / "accounts" / account_id / "tgdl_session"

    def monitor_path(self, account_id: str) -> Path:
        self.validate(account_id)
        return self.root / "accounts" / account_id / "monitored_channels.json"
