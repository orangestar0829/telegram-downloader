from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from dotenv import dotenv_values
from dotenv.parser import parse_stream
from tgdl.errors import TGDLInputError
from tgdl.paths import prepare_data_dir

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = prepare_data_dir()
SESSION_PATH = DATA_DIR / "accounts" / "default" / "tgdl_session"
ENV_PATH = DATA_DIR / "accounts" / "default" / ".env"

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
    "MEDIA_SPEED_KIB": "media_speed_kib",
    "MONITOR_AUTO_THROTTLE": "monitor_auto_throttle",
}


_LITERAL_ENV_VALUE = re.compile(r"[a-zA-Z0-9_.:/+@,-]+")
_ENV_BINDING = re.compile(
    r"(?P<key>\s*(?:export[^\S\r\n]+)?(?:'[^']+'|[^=\s#]+))"
    r"(?P<equals>[^\S\r\n]*=[^\S\r\n]*)?"
)
_ENV_QUOTED_VALUE = re.compile(r"""(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')""", re.DOTALL)
_ENV_EOL = re.compile(r"\r\n|\r|\n")
_ENV_INLINE_COMMENT = re.compile(r"[^\S\r\n]+#")
_ENV_ESCAPES = str.maketrans({"\\": r"\\", '"': r'\"', "\n": r"\n", "\r": r"\r", "\t": r"\t"})


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
    media_speed_kib: int = 0
    temp_download_dir: Path | None = None
    session_path: Path | None = None
    monitor_auto_throttle: bool = False


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


def _file_env(path: Path) -> dict[str, str]:
    """Read a private account .env without altering global process state."""
    if not path.exists():
        return {}
    with path.open(encoding="utf-8-sig", newline="") as stream:
        parsed = dotenv_values(stream=stream, interpolate=False)
    return {key: value or "" for key, value in parsed.items()}


def resolve_api_credentials(env: dict[str, str] | None = None) -> tuple[str, str] | None:
    """Use this account file for credentials, otherwise packaged defaults."""
    if env is None:
        # Each account is isolated: never inject .env into global os.environ.
        # The default account's authoritative config is accounts/default/.env.
        source = _file_env(ENV_PATH)
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


def read_env_values(path: Path | None = None) -> dict[str, str]:
    """Read the same literal .env values as the runtime, with GUI defaults."""
    values = {
        "api_id": "", "api_hash": "", "phone": "",
        "download_dir": str(default_download_dir()),
        "temp_download_dir": "",
        "proxy_host": "", "proxy_port": "",
        "proxy_username": "", "proxy_password": "",
        "media_concurrency": "3", "media_retries": "2",
        "chunk_concurrency": "1", "media_speed_kib": "0",
        "monitor_auto_throttle": "0",
    }
    source_path = Path(path) if path is not None else ENV_PATH
    for key, value in _file_env(source_path).items():
        field = _ENV_KEYS.get(key)
        if field and value:
            values[field] = value
    return values


def _encode_env_value(key: str, value: str) -> str:
    if "\x00" in value:
        raise TGDLInputError(f"{key} cannot contain a NUL character.")
    if _LITERAL_ENV_VALUE.fullmatch(value):
        return value
    return '"' + value.translate(_ENV_ESCAPES) + '"'


def _replace_env_binding(original: str, old_value: str | None, encoded: str) -> str:
    """Replace only the value, retaining indentation, export, comments and EOL."""
    head = _ENV_BINDING.match(original)
    if head is None:  # The dotenv parser only gets here for a recognized key.
        raise TGDLInputError("Cannot safely update an existing .env setting.")
    rest = original[head.end():]
    equals = head["equals"]
    if equals is None:
        suffix = rest
    elif rest.startswith(('"', "'")):
        quoted = _ENV_QUOTED_VALUE.match(rest)
        if quoted is None:
            raise TGDLInputError("Cannot safely update an existing quoted .env setting.")
        suffix = rest[quoted.end():]
    elif old_value == "" and rest.startswith("#"):
        suffix = rest
    else:
        comment = _ENV_INLINE_COMMENT.search(rest)
        suffix = rest[comment.start():] if comment else rest[len(rest.rstrip()):]
    if suffix.startswith("#"):
        suffix = " " + suffix
    return head["key"] + (equals or "=") + encoded + suffix


def update_env(values: dict[str, str], path: Path | None = None) -> Path:
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
        ("MEDIA_SPEED_KIB", "media_speed_kib"),
        ("MONITOR_AUTO_THROTTLE", "monitor_auto_throttle"),
        ("TEMP_DOWNLOAD_DIR", "temp_download_dir"),
    ):
        if name in values:
            desired[key] = values[name]
    encoded = {key: _encode_env_value(key, value) for key, value in desired.items()}
    destination = Path(path) if path is not None else ENV_PATH
    if destination.exists():
        with destination.open(encoding="utf-8", newline="") as stream:
            existing = stream.read()
    else:
        existing = ""
    bom = "\ufeff" if existing.startswith("\ufeff") else ""
    bindings = list(parse_stream(StringIO(existing.removeprefix("\ufeff"))))
    seen: set[str] = set()
    output: list[str] = [bom]
    for binding in bindings:
        key, value, original, _error = binding
        if key in encoded:
            seen.add(key)
            if value != desired[key]:
                output.append(_replace_env_binding(original.string, value, encoded[key]))
                continue
        output.append(original.string)
    updated = "".join(output)
    if missing := [key for key in desired if key not in seen]:
        newline = _ENV_EOL.search(existing)
        eol = newline[0] if newline else "\n"
        if updated != bom and not updated.endswith(("\r", "\n")):
            updated += eol
        updated += "".join(f"{key}={encoded[key]}{eol}" for key in missing)

    if updated == existing:
        return destination

    # A parse-back check also protects installations whose older dotenv parser
    # misinterprets a quoted value ending in a literal backslash.
    expected = {binding.key: binding.value for binding in bindings if binding.key is not None}
    expected.update(desired)
    reparsed = {
        binding.key: binding.value
        for binding in parse_stream(StringIO(updated.removeprefix("\ufeff")))
        if binding.key is not None
    }
    if expected != reparsed:
        raise TGDLInputError("Cannot save .env without changing its values. Upgrade python-dotenv and check the file.")

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=destination.parent,
            prefix=destination.name + ".", suffix=".tmp", delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(updated)
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination


def load_settings(*, env_path: Path | None = None, session_path: Path | None = None) -> Settings:
    settings_path = Path(env_path) if env_path is not None else ENV_PATH
    file_values = _file_env(settings_path)
    # Missing account credentials may use the optional bundled API keys, but
    # never those from another account or a stale legacy root .env.
    creds = resolve_api_credentials(file_values)
    raw = file_values.get
    phone = raw("TELEGRAM_PHONE", "").strip() or None
    dd_raw = raw("DOWNLOAD_DIR", "")
    download_dir = Path(dd_raw).expanduser() if dd_raw else default_download_dir()
    if not download_dir.is_absolute():
        download_dir = DATA_DIR / download_dir
    staging_raw = raw("TEMP_DOWNLOAD_DIR", "")
    staging_dir = Path(staging_raw).expanduser() if staging_raw else None
    if staging_dir is not None and not staging_dir.is_absolute():
        staging_dir = DATA_DIR / staging_dir

    if creds is None:
        raise TGDLInputError(
            "Missing TELEGRAM_API_ID / TELEGRAM_API_HASH.\n"
            "1. Open https://my.telegram.org -> API development tools\n"
            "2. Enter api_id and api_hash once in the app (they are remembered)."
        )
    api_id_raw, api_hash = creds
    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise TGDLInputError("TELEGRAM_API_ID must be a number.") from exc

    host = raw("SOCKS5_HOST", "").strip() or None
    raw_port = raw("SOCKS5_PORT", "").strip()
    if raw_port and host is None:
        raise TGDLInputError("SOCKS5_PORT requires SOCKS5_HOST.")
    proxy_port = _bounded_int(raw_port or "1080", "SOCKS5_PORT", 1, 65535) if host else None

    return Settings(
        api_id=api_id,
        api_hash=api_hash,
        phone=phone,
        download_dir=download_dir,
        proxy_host=host,
        proxy_port=proxy_port,
        proxy_username=raw("SOCKS5_USERNAME", "") or None,
        proxy_password=raw("SOCKS5_PASSWORD", "") or None,
        media_concurrency=_bounded_int(
            raw("MEDIA_CONCURRENCY", "3"), "MEDIA_CONCURRENCY", 1, 8
        ),
        media_retries=_bounded_int(
            raw("MEDIA_RETRIES", "2"), "MEDIA_RETRIES", 0, 5
        ),
        chunk_concurrency=_bounded_int(
            raw("CHUNK_CONCURRENCY", "1"), "CHUNK_CONCURRENCY", 1, 8
        ),
        media_speed_kib=_bounded_int(
            raw("MEDIA_SPEED_KIB", "0"), "MEDIA_SPEED_KIB", 0, 1048576
        ),
        temp_download_dir=staging_dir,
        session_path=session_path if session_path is not None else SESSION_PATH,
        monitor_auto_throttle=parse_monitor_auto_throttle(raw("MONITOR_AUTO_THROTTLE")),
    )


def parse_monitor_auto_throttle(raw: str | None) -> bool:
    """An explicit, recognized per-account value is required to opt in."""
    return (raw or "").strip().casefold() in {"1", "true", "yes", "on"}


def _bounded_int(raw: str, name: str, minimum: int, maximum: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise TGDLInputError(f"{name} must be a number.") from exc
    if not minimum <= value <= maximum:
        raise TGDLInputError(f"{name} must be between {minimum} and {maximum}.")
    return value
