"""Login-code diagnostics must not send extra codes or expose secrets."""
import asyncio
from types import SimpleNamespace

from tgdl.accounts import Account, AccountRegistry
from tgdl import multi_runtime


def delivery(kind: str):
    return SimpleNamespace(type=type(kind, (), {})(), phone_code_hash="SECRET_HASH")


def test_telegram_delivery_types_are_actionable_and_private():
    for kind, expected in {
        "SentCodeTypeApp": "Telegram 应用内",
        "SentCodeTypeSms": "手机短信",
        "SentCodeTypeEmailCode": "登录邮箱",
        "SentCodeTypeFirebaseSms": "官方手机 App 专用短信验证",
        "SentCodeTypeFragmentSms": "Fragment",
        "SentCodeTypeSetUpEmailRequired": "需先设置登录邮箱",
    }.items():
        label, hint = multi_runtime.describe_login_code_delivery(delivery(kind))
        assert label == expected
        assert hint
        assert "SECRET_HASH" not in hint
    label, hint = multi_runtime.describe_login_code_delivery(delivery("NovelCodeType"))
    assert label == "未知投递方式"
    assert "NovelCodeType" in hint


def test_login_reports_actual_channel_and_does_not_request_again(monkeypatch, tmp_path):
    registry = AccountRegistry(tmp_path)
    logs, prompts = [], []

    class FakeClient:
        def __init__(self):
            self.requests = 0
            self.sent = self._send

        async def _send(self, phone):
            self.requests += 1
            assert phone == "+12223334444"
            return delivery("SentCodeTypeApp")

        async def start(self, *, phone, code_callback, password):
            self.phone = phone()
            await self.send_code_request(self.phone)
            # The GUI will see the actual channel before asking for the code.
            assert code_callback() == "12345"

        async def get_me(self):
            return SimpleNamespace(id=12345, first_name="B", phone="12223334444")

        async def disconnect(self):
            pass

        send_code_request = property(lambda self: self.sent, lambda self, fn: setattr(self, "sent", fn))

    fake_client = FakeClient()
    monkeypatch.setattr(multi_runtime, "load_settings",
                        lambda **_: SimpleNamespace(phone="+12223334444"))
    monkeypatch.setattr(multi_runtime, "build_client", lambda settings: fake_client)
    account = Account("acc-b", "B")
    runtime = multi_runtime.AccountMonitorRuntime(
        account, registry,
        ask=lambda prompt, secret: prompts.append((prompt, secret)) or "12345",
        on_log=lambda _id, line: logs.append(line),
        on_saved=lambda *_: None,
        on_progress=lambda *_: None,
    )

    async def done_worker(*args, **kwargs):
        runtime.stop()

    monkeypatch.setattr(runtime, "_scan_loop", done_worker)
    monkeypatch.setattr(runtime, "_download_loop", done_worker)
    asyncio.run(runtime.run())
    assert fake_client.requests == 1
    assert any("Telegram 应用内" in message for message in logs)
    assert any("Telegram 应用内" in prompt for prompt, _ in prompts)
    assert not any("SECRET_HASH" in message for message in logs)
    assert not any("12345" in message for message in logs)
