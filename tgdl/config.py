from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from tgdl.paths import prepare_data_dir

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = prepare_data_dir()
SESSION_PATH = DATA_DIR / "tgdl_session"
ENV_PATH = DATA_DIR / ".env"

_ENV_KEYS = {
    "TELEGRAM_API_ID": "api_id",
    "TELEGRAM_API_HASH": "api_hash",
    "TELEGRAM_PHONE": "phone",
    "DOWNLOAD_DIR": "download_dir",
    "TEMP_DOWNLOAD_DIR": "temp_download_dir",
    "SOCKS5_HOST": "proxy_host",
    "SOCKS5_PORT": "proxy_port",
    "SOCKS5_USERNAME": "proxy_username",
    "SOCKS5_PASSWORD": "proxy_password",
    "MEDIA_CONCURRENCY": "media_concurrency",
    "MEDIA_RETRIES": "media_retries",
    "CHUNK_CONCURRENCY": "chunk_concurrency",
}


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    phone: str | None
    download_dir: Path
    proxy_host: str | None = None
    proxy_port: int | None = None
    proxy_username: str | None = None
    proxy_password: str | None = None
    media_concurrency: int = 3
    media_retries: int = 2
    chunk_concurrency: int = 1
    temp_download_dir: Path | None = None


def default_download_dir() -> Path:
    """Sensible default so users never have to pick a folder."""
    return Path.home() / "Downloads" / "TelegramDownloader"


def _bundled_defaults() -> tuple[str, str] | None:
    """Optional api_id/api_hash baked into the build (tgdl/_defaults.py).

    This lets a distributed build ship its own credentials so end users only
    log in with their phone — exactly like the official client. The file is
    generated at build time from repo secrets and is never committed.
    """
    try:
        from tgdl import _defaults  # type: ignore[attr-defined]

        api_id = str(getattr(_defaults, "API_ID", "")).strip()
        api_hash = str(getattr(_defaults, "API_HASH", "")).strip()
        if api_id and api_hash:
            return api_id, api_hash
    except Exception:
        pass
    return None


def resolve_api_credentials(env: dict[str, str] | None = None) -> tuple[str, str] | None:
    """Return (api_id, api_hash) from env/.env, else bundled defaults, else None."""
    if env is None:
        load_dotenv(ENV_PATH, override=True)
        source = os.environ
    else:
        source = env
    api_id = (source.get("TELEGRAM_API_ID", "") or "").strip()
    api_hash = (source.get("TELEGRAM_API_HASH", "") or "").strip()
    if api_id and api_hash:
        return api_id, api_hash
    return _bundled_defaults()


def credentials_available() -> bool:
    """True when the app already has api_id/api_hash (saved or bundled)."""
    return resolve_api_credentials() is not None


def read_env_values() -> dict[str, str]:
    """Read raw .env values for pre-filling the GUI (no validation)."""
    values = {
        "api_id": "", "api_hash": "", "phone": "",
        "download_dir": str(default_download_dir()),
        "temp_download_dir": "",
        "proxy_host": "", "proxy_port": "",
        "proxy_username": "", "proxy_password": "",
        "media_concurrency": "3", "media_retries": "2",
        "chunk_concurrency": "1",
    }
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in line:
                continue
            key, _, raw = line.partition("=")
            field = _ENV_KEYS.get(key.strip())
            value = raw.strip()
            if field and value:
                values[field] = value
    return values


def update_env(values: dict[str, str]) -> Path:
    """Create/update .env with the given values, preserving other lines."""
    desired = {
        "TELEGRAM_API_ID": values.get("api_id", ""),
        "TELEGRAM_API_HASH": values.get("api_hash", ""),
        "TELEGRAM_PHONE": values.get("phone", ""),
        "DOWNLOAD_DIR": values.get("download_dir", "") or "./downloads",
    }
    # Preserve existing optional settings when a caller updates only credentials or directory.
    for key, name in (
        ("SOCKS5_HOST", "proxy_host"),
        ("SOCKS5_PORT", "proxy_port"),
        ("SOCKS5_USERNAME", "proxy_username"),
        ("SOCKS5_PASSWORD", "proxy_password"),
        ("MEDIA_CONCURRENCY", "media_concurrency"),
        ("MEDIA_RETRIES", "media_retries"),
        ("CHUNK_CONCURRENCY", "chunk_concurrency"),
        ("TEMP_DOWNLOAD_DIR", "temp_download_dir"),
    ):
        if name in values:
            desired[key] = values[name]
    existing_lines = ENV_PATH.read_text(encoding="utf-8").splitlines() if ENV_PATH.exists() else []
    seen: set[str] = set()
    out: list[str] = []
    for line in existing_lines:
        key = line.partition("=")[0].strip()
        if key in desired:
            out.append(f"{key}={desired[key]}")
            seen.add(key)
        else:
            out.append(line)
    for key, value in desired.items():
        if key not in seen:
            out.append(f"{key}={value}")
    ENV_PATH.write_text("\n".join(out) + "\n", encoding="utf-8")
    return ENV_PATH


def load_settings() -> Settings:
    creds = resolve_api_credentials()  # loads .env and checks bundled defaults
    phone = os.getenv("TELEGRAM_PHONE", "").strip() or None
    dd_raw = os.getenv("DOWNLOAD_DIR", "").strip()
    download_dir = Path(dd_raw).expanduser() if dd_raw else default_download_dir()
    if not download_dir.is_absolute():
        download_dir = DATA_DIR / download_dir
    staging_raw = os.getenv("TEMP_DOWNLOAD_DIR", "").strip()
    staging_dir = Path(staging_raw).expanduser() if staging_raw else None
    if staging_dir is not None and not staging_dir.is_absolute():
        staging_dir = DATA_DIR / staging_dir

    if creds is None:
        raise SystemExit(
            "Missing TELEGRAM_API_ID / TELEGRAM_API_HASH.\n"
            "1. Open https://my.telegram.org -> API development tools\n"
            "2. Enter api_id and api_hash once in the app (they are remembered)."
        )
    api_id_raw, api_hash = creds
    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise SystemExit("TELEGRAM_API_ID must be a number.") from exc

    host = os.getenv("SOCKS5_HOST", "").strip() or None
    raw_port = os.getenv("SOCKS5_PORT", "").strip()
    if raw_port and host is None:
        raise SystemExit("SOCKS5_PORT requires SOCKS5_HOST.")
    proxy_port = _bounded_int(raw_port or "1080", "SOCKS5_PORT", 1, 65535) if host else None

    return Settings(
        api_id=api_id,
        api_hash=api_hash,
        phone=phone,
        download_dir=download_dir,
        proxy_host=host,
        proxy_port=proxy_port,
        proxy_username=os.getenv("SOCKS5_USERNAME", "").strip() or None,
        proxy_password=os.getenv("SOCKS5_PASSWORD", "") or None,
        media_concurrency=_bounded_int(
            os.getenv("MEDIA_CONCURRENCY", "3"), "MEDIA_CONCURRENCY", 1, 8
        ),
        media_retries=_bounded_int(
            os.getenv("MEDIA_RETRIES", "2"), "MEDIA_RETRIES", 0, 5
        ),
        chunk_concurrency=_bounded_int(
            os.getenv("CHUNK_CONCURRENCY", "1"), "CHUNK_CONCURRENCY", 1, 8
        ),
        temp_download_dir=staging_dir,
    )


def _bounded_int(raw: str, name: str, minimum: int, maximum: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"{name} must be a number.") from exc
    if not minimum <= value <= maximum:
        raise SystemExit(f"{name} must be between {minimum} and {maximum}.")
    return value
