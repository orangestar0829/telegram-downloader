"""A single account-pinned Telegram login session with a Tk modal front end."""
from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from tgdl.client import validate_login_phone
from tgdl.errors import TGDLInputError
from tgdl.qr_auth import login_with_qr, qr_image


def masked_phone(phone: str | None) -> str:
    if not phone:
        return ""
    digits = phone.lstrip("+")
    return f"+•••• {digits[-4:]}" if len(digits) > 4 else "+••••"


@dataclass(frozen=True)
class LoginUpdate:
    kind: str
    message: str = ""
    prompt: int | None = None
    saved_phone: str = ""
    qr_url: str = field(default="", repr=False)


class TelegramLoginFlow:
    """Bridge one asynchronous authorization with one modal.

    Tk posts method changes or answers from its thread. A changed method first
    joins the obsolete authorization task, then starts the new task using the
    same account client. Generation and prompt IDs fence late UI callbacks.
    """

    def __init__(
        self, post: Callable[[int, LoginUpdate], None], *,
        remember_phone: Callable[[str], None] | None = None,
    ) -> None:
        self._post = post
        self._remember_phone = remember_phone
        self._lock = threading.Lock()
        self._mode: str | None = "qr"
        self._completed = False
        self._generation = 0
        self._serial = 0
        self._answer: tuple[int, int, concurrent.futures.Future] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._changes: asyncio.Queue[tuple[int, str | None]] = asyncio.Queue()

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    def choose(self, mode: str | None) -> None:
        if mode not in ("qr", "phone", None):
            raise ValueError("Unknown Telegram login method")
        with self._lock:
            if self._mode is None or self._completed:
                return
            self._mode = mode
            self._generation += 1
            choice = (self._generation, mode)
            if self._loop is not None:
                self._loop.call_soon_threadsafe(self._changes.put_nowait, choice)

    def cancel(self) -> None:
        self.choose(None)

    def answer(self, prompt: int, value: str | None, remember: bool = False) -> bool:
        with self._lock:
            pending = self._answer
            if pending is None or pending[:2] != (self._generation, prompt) or self._mode is None:
                return False
            try:
                pending[2].set_result((value, remember))
            except concurrent.futures.InvalidStateError:
                return False
            return True

    async def _ask(
        self, generation: int, kind: str, message: str, *, saved_phone: str = "",
    ) -> tuple[str | None, bool]:
        future: concurrent.futures.Future = concurrent.futures.Future()
        with self._lock:
            if self._generation != generation:
                raise asyncio.CancelledError()
            self._serial += 1
            prompt = self._serial
            self._answer = (generation, prompt, future)
        try:
            self._post(generation, LoginUpdate(kind, message, prompt, saved_phone))
            value = await asyncio.wrap_future(future)
            with self._lock:
                if self._generation != generation:
                    raise asyncio.CancelledError()
            return value
        finally:
            with self._lock:
                if self._answer is not None and self._answer[2] is future:
                    self._answer = None

    async def _password(self, generation: int) -> str:
        value, _remember = await self._ask(generation, "password", "请输入 Telegram 两步验证密码。")
        return value or ""

    @staticmethod
    def _check_phone(user, phone: str | None) -> None:
        if phone and getattr(user, "phone", None) and validate_login_phone(user.phone) != phone:
            raise TGDLInputError("这个会话已登录另一个 Telegram 号码。请确认所选账号后再试。")

    async def _phone_login(self, client, generation: int, saved_phone: str | None):
        from tgdl.multi_runtime import describe_login_code_delivery

        message = "确认已保存的手机号，然后发送验证码。" if saved_phone else "请输入带国际区号的手机号。"
        while True:
            raw, remember = await self._ask(generation, "phone", message, saved_phone=masked_phone(saved_phone))
            try:
                phone = saved_phone or validate_login_phone(raw or "")
            except TGDLInputError as exc:
                message = str(exc)
                continue
            break

        self._post(generation, LoginUpdate("waiting", "正在向 Telegram 请求验证码…"))
        delivery = ("Telegram", "请检查该账号已登录的 Telegram App、短信或邮箱。")
        send_code = client.send_code_request

        async def observe_delivery(*args, **kwargs):
            nonlocal delivery
            sent = await send_code(*args, **kwargs)
            delivery = describe_login_code_delivery(sent)
            return sent

        async def ask_code() -> str:
            label, guidance = delivery
            value, _remember = await self._ask(generation, "code", f"输入验证码（{label}）。{guidance}")
            self._post(generation, LoginUpdate("waiting", "正在验证验证码…"))
            return (value or "").strip()

        client.send_code_request = observe_delivery
        try:
            await client.start(phone=phone, code_callback=ask_code,
                               password=lambda: self._password(generation))
        finally:
            client.send_code_request = send_code
        return phone, remember and saved_phone is None

    async def _attempt(self, client, generation: int, mode: str, saved_phone: str | None):
        if not client.is_connected():
            await client.connect()
        user = await client.get_me()
        if user is not None:
            self._check_phone(user, saved_phone)
            return user, None

        remember = False
        phone = saved_phone
        if mode == "qr":
            self._post(generation, LoginUpdate("qr", "正在生成二维码…"))
            await login_with_qr(
                client,
                show_qr=lambda url, _: self._post(generation, LoginUpdate(
                    "qr", "请用手机 Telegram 扫码。二维码过期后会自动刷新。", qr_url=url,
                )),
                ask_password=lambda: self._password(generation),
            )
        else:
            phone, remember = await self._phone_login(client, generation, saved_phone)

        user = await client.get_me()
        if user is None:
            raise TGDLInputError("Telegram 没有确认登录，请重试。")
        self._check_phone(user, phone)
        return user, phone if remember else None

    async def authenticate(self, client, configured_phone: str | None):
        """Wait for a choice to succeed or be cancelled, returning authorized user."""
        saved_phone = None
        if configured_phone:
            try:
                saved_phone = validate_login_phone(configured_phone)
            except TGDLInputError:
                pass  # Live phone step allows correction of invalid old settings.
        with self._lock:
            self._loop = asyncio.get_running_loop()
            generation, mode = self._generation, self._mode

        attempt = change = stopping_attempt = None
        try:
            if mode is None:
                raise asyncio.CancelledError()
            while mode is not None:
                attempt = asyncio.create_task(self._attempt(client, generation, mode, saved_phone))
                change = asyncio.create_task(self._changes.get())
                done, _ = await asyncio.wait((attempt, change), return_when=asyncio.FIRST_COMPLETED)
                if attempt in done and not attempt.cancelled():
                    try:
                        user, remembered = attempt.result()
                    except Exception as exc:
                        # Never render an arbitrary exception message: it might
                        # contain a credential, a login URL, or a phone number.
                        detail = str(exc) if isinstance(exc, TGDLInputError) else type(exc).__name__
                        self._post(generation, LoginUpdate("error", f"未能登录：{detail}。"))
                    else:
                        with self._lock:
                            current = self._generation == generation
                            if current:
                                self._completed = True
                        if current:
                            if remembered and self._remember_phone is not None:
                                try:
                                    self._remember_phone(remembered)
                                except OSError:
                                    pass  # An optional local write cannot undo authorization.
                            return user
                        # A switch/cancel won the race. Never publish an old
                        # attempt's result or remember its phone here.
                generation, mode = await change
                if not attempt.done():
                    attempt.cancel()
                    stopping_attempt = attempt
                # App shutdown may cancel the owning coroutine as a switch
                # is already joining the old QR/phone request. Do not issue
                # a second cancel into the old attempt's cleanup.
                await asyncio.shield(asyncio.gather(attempt, return_exceptions=True))
                stopping_attempt = None
                # Coalesce rapid changes without briefly launching intermediate
                # QR attempts or sending requests for obsolete phone forms.
                while not self._changes.empty():
                    generation, mode = self._changes.get_nowait()
            raise asyncio.CancelledError()
        finally:
            for task in (attempt, change):
                if task is not None and task is not stopping_attempt and not task.done():
                    task.cancel()
            await asyncio.gather(*(task for task in (attempt, change) if task is not None),
                                 return_exceptions=True)
            with self._lock:
                self._loop = None


class TelegramLoginDialog:
    def __init__(self, app, account_name: str, *, on_close, remember_phone=None) -> None:
        tk, ttk = app.tk, app.ttk
        self.app = app
        self._closed = False
        self._on_close = on_close
        self._image = None
        self._submit = None
        self._prompt: int | None = None
        self.flow = TelegramLoginFlow(
            lambda generation, update: app.root.after(0, lambda: self._update(generation, update)),
            remember_phone=remember_phone,
        )
        self.win = tk.Toplevel(app.root)
        self.win.title("登录 Telegram")
        self.win.transient(app.root)
        self.win.resizable(False, False)
        self.win.protocol("WM_DELETE_WINDOW", self.cancel)
        self.win.bind("<Escape>", lambda _: self.cancel())
        self.win.bind("<Return>", lambda _: self._submit() if self._submit else None)
        body = ttk.Frame(self.win, padding=16, width=460)
        body.pack(fill="both", expand=True)
        ttk.Label(body, text=f"登录账号：{account_name}", font=("Arial", 12, "bold")).pack(anchor="w")
        tabs = ttk.Frame(body)
        tabs.pack(anchor="w", pady=(10, 6))
        self._method = tk.StringVar(master=self.win, value="qr")
        for key, title in (("qr", "扫码登录"), ("phone", "手机号登录")):
            ttk.Radiobutton(tabs, text=title, value=key, variable=self._method,
                            command=self._switch_method).pack(side="left", padx=(0, 12))
        self._stage = ttk.Frame(body)
        self._stage.pack(fill="both", expand=True, pady=(4, 8))
        ttk.Separator(body).pack(fill="x", pady=(0, 8))
        ttk.Button(body, text="取消", command=self.cancel).pack(anchor="e")
        self._render(LoginUpdate("qr", "正在准备扫码登录…"))
        self.win.bind("<Map>", self._on_map)

    def _on_map(self, event) -> None:
        if self._closed or event.widget is not self.win:
            return
        self.win.grab_set()
        self.win.focus_set()

    def _switch_method(self) -> None:
        mode = self._method.get()
        self.flow.choose(mode)
        self._render(LoginUpdate("qr" if mode == "qr" else "waiting", "正在准备登录…"))

    def _update(self, generation: int, update: LoginUpdate) -> None:
        if not self._closed and generation == self.flow.generation:
            self._render(update)

    def _render(self, update: LoginUpdate) -> None:
        tk, ttk = self.app.tk, self.app.ttk
        self._submit = None
        self._prompt = update.prompt
        for child in self._stage.winfo_children():
            child.destroy()
        self._image = None
        if update.kind == "qr":
            ttk.Label(self._stage, text="手机 Telegram → 设置 → 设备 → 连接桌面设备",
                      wraplength=420).pack(anchor="w")
            if update.qr_url:
                from PIL import ImageTk

                self._image = ImageTk.PhotoImage(qr_image(update.qr_url, pixels=280), master=self.win)
                ttk.Label(self._stage, image=self._image).pack(pady=8)
        ttk.Label(self._stage, text=update.message, wraplength=420).pack(anchor="w", pady=(4, 8))
        if update.kind == "error":
            ttk.Button(self._stage, text="重试", command=self._switch_method).pack(anchor="e")
            return
        if update.prompt is None:
            return

        entry = None
        remember = None
        button_text = "发送验证码" if update.kind == "phone" else "继续"
        if update.saved_phone:
            ttk.Label(self._stage, text=update.saved_phone).pack(anchor="w")
        else:
            entry = ttk.Entry(self._stage, width=39, show="*" if update.kind == "password" else "")
            entry.pack(fill="x")
            entry.focus_set()
            if update.kind == "phone":
                remember = tk.BooleanVar(master=self.win, value=False)
                ttk.Checkbutton(self._stage, text="登录成功后记住此手机号", variable=remember).pack(
                    anchor="w", pady=(5, 0),
                )

        def submit():
            if self._prompt != update.prompt:
                return
            value = entry.get() if entry is not None else None
            if self.flow.answer(update.prompt, value, bool(remember and remember.get())):
                if entry is not None:
                    entry.delete(0, "end")
                    entry.configure(state="disabled")
                button.configure(state="disabled")
                self._submit = None

        button = ttk.Button(self._stage, text=button_text, command=submit)
        button.pack(anchor="e", pady=(8, 0))
        self._submit = submit

    def cancel(self) -> None:
        self.flow.cancel()
        self.done()

    def done(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._image = None
        self._submit = None
        self.win.destroy()
        self._on_close(self)
