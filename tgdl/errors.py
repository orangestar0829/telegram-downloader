"""Expected user-input failures, safe to handle in GUI workers."""


class TGDLInputError(ValueError):
    """Invalid or unusable input; only the CLI may turn it into a process exit."""


class ChannelAccessError(TGDLInputError):
    """An account cannot resolve the requested Telegram channel or group."""
