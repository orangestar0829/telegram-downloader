"""Context carried by asyncio tasks to identify the account behind print/log output."""
from contextvars import ContextVar

current_account_id: ContextVar[str] = ContextVar("tgdl_current_account_id", default="default")
