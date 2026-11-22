"""Tkinter GUI for tgdl — a login-first, Telegram-client-like front end.

Flow mirrors the official client: log in with your phone (code / 2FA in a
dialog), then your groups/channels are listed automatically; pick one and
download. api_id/api_hash are only needed once (or baked into the build), and
the download folder defaults automatically, so there is nothing else to fill.

The asyncio event loop runs in a background thread; the UI submits coroutines
to it and receives log output through a thread-safe queue.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
import concurrent.futures
import os
import queue
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path

from telethon.errors import AuthKeyDuplicatedError, UnauthorizedError

from tgdl.accounts import AccountRegistry, DEFAULT_ACCOUNT
from tgdl.account_context import current_account_id
from tgdl.account_identity import account_identity
from tgdl.account_view import progress_owner
from tgdl.log_store import DailyLogStore

from tgdl.browser import list_media, save_items
from tgdl.channels import list_media_dialogs
from tgdl.dialog_filter import filter_dialogs
from tgdl.client import build_client, validate_login_phone
from tgdl.config import (
    Settings,
    credentials_available,
    default_download_dir,
    load_settings,
    read_env_values,
    resolve_api_credentials,
    update_env,
)
from tgdl.downloader import download_channel, resolve_channel, scan_monitor_slice
from tgdl.errors import TGDLInputError
from tgdl.history import DownloadHistory
from tgdl.login_dialog import TelegramLoginDialog
from tgdl.log_format import LogChunk, TimestampedLines
from tgdl.monitor import (
    MonitorDispatchSummary, choose_monitor_download, load_channels, next_due_channel,
    save_channels,
)

from tgdl.paths import data_dir, monitor_migration_error
from tgdl.single_instance import AlreadyRunning, gui_instance_guard
from tgdl.temp_cleanup import inspect_cleanup
from tgdl.telethon_diagnostics import install_telethon_diagnostics
from tgdl.window_state import load_window_geometry, save_window_geometry

from tgdl import downloader as _downloader
from tgdl import http_fetch as _http_fetch
from tgdl import parser as _parser
from tgdl import torrent_fetch as _torrent_fetch
from tgdl.cli import parse_day, parse_types
from tgdl.parser import parse_channel


_ADD_ACCOUNT_CHOICE = "＋ 添加账号…"

class _SavedSessionMissing(TGDLInputError):
    """The stored session file no longer exists."""

class _SavedSessionExpired(TGDLInputError):
    """Telegram did not authorize the saved session."""

class _SavedSessionMismatch(TGDLInputError):
    """The authenticated profile differs from the configured account."""


class _SilentBar:
    """Minimal no-op stand-in for tqdm used inside the GUI."""

    def __init__(self, *args, **kwargs) -> None:
        self.n = 0
        self.total = kwargs.get("total")

    def __enter__(self) -> "_SilentBar":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def update(self, _n: int = 1) -> None:
        pass

    def refresh(self) -> None:
        pass

    def set_postfix(self, *args, **kwargs) -> None:
        pass

    def close(self) -> None:
        pass


def _silence_progress_bars() -> None:
    for module in (_downloader, _http_fetch, _torrent_fetch, _parser):
        if hasattr(module, "tqdm"):
            module.tqdm = _SilentBar  # type: ignore[attr-defined]


def _icon_path() -> Path | None:
    candidates = []
    if getattr(sys, "frozen", False):
        base = Path(getattr(sys, "_MEIPASS", ""))
        candidates.append(base / "tgdl" / "assets" / "app.png")
    candidates.append(Path(__file__).resolve().parent / "assets" / "app.png")
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


class AsyncLoop:
    """Runs an asyncio event loop in a dedicated background thread."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def submit(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)


class _QueueWriter:
    def __init__(self, sink: "queue.Queue[LogChunk]", formatter: TimestampedLines | None = None,
                 persist: Callable[[str], None] | None = None) -> None:
        self._sink = sink
        self._formatter = formatter or TimestampedLines()
        self._persist = persist

    def write(self, text: str) -> int:
        if text:
            owner = current_account_id.get()
            formatted = self._formatter.format(
                text, prefix=f"[账号:{account_identity.label(owner)}] "
            )
            self._sink.put(LogChunk(owner, formatted))
            if self._persist is not None:
                self._persist(formatted)
        return len(text)

    def flush(self) -> None:
        pass


def run_gui() -> None:
    try:
        import tkinter as tk  # noqa: F401
        from tkinter import ttk  # noqa: F401
    except Exception as exc:  # pragma: no cover - platform dependent
        print(
            "Tkinter is not available. On Debian/Ubuntu install it with:\n"
            "  sudo apt-get install -y python3-tk\n"
            f"({exc})"
        )
        raise SystemExit(1)

    _silence_progress_bars()
    try:
        with gui_instance_guard(data_dir()):
            _App().run()
    except AlreadyRunning as exc:
        from tkinter import messagebox
        messagebox.showinfo("已在运行", str(exc))


class _App:
    def __init__(self) -> None:
        import tkinter as tk
        from tkinter import messagebox, scrolledtext, simpledialog, ttk

        self.tk = tk
        self.ttk = ttk
        self.messagebox = messagebox
        self.simpledialog = simpledialog
        self.scrolledtext = scrolledtext

        self.loop = AsyncLoop()
        self.client = None
        self._client_lock = asyncio.Lock()
        self._client_starting_task = None
        self._client_starting_cleanup = False
        self._prompt_lock = asyncio.Lock()
        self.settings: Settings | None = None
        self._account_registry = AccountRegistry(data_dir())
        for registered in self._account_registry.accounts():
            account_identity.register(registered.id, registered.name)
        self._extra_runtimes = {}
        self._extra_futures = {}
        self._login_dialog = None
        self._default_login_future = None
        self._default_login_task = None
        self._view_account_id = "default"
        self._view_monitored = []
        self._default_dialog_rows = []
        self._default_status_text = "未登录"
        self._account_dialog_cache = {}
        self._account_dialog_fetches = set()
        self._log_store = DailyLogStore(data_dir() / "logs")
        self._run_log_records = deque(maxlen=5000)
        self._download_log_records = deque(maxlen=5000)
        self.log_queue: "queue.Queue[LogChunk]" = queue.Queue()
        self.download_queue: "queue.Queue[LogChunk]" = queue.Queue()
        self.progress_queue: "queue.Queue[tuple[str, str, int, int, str]]" = queue.Queue()
        self._progress_entries: dict[str, dict] = {}
        self._monitor_progress_states: dict[str, str] = {}
        self._log_popup = None
        self._log_popup_text = None
        self._download_popup = None
        self._download_popup_text = None

        self.root = tk.Tk()
        self.log_account_filter = tk.StringVar(value="全部账号")
        self._log_filter_options: dict[str, str | None] = {"全部账号": None}
        self.root.title("Telegram 下载器")
        self.root.geometry(load_window_geometry(
            self.root.winfo_screenwidth(), self.root.winfo_screenheight()
        ))
        self.root.minsize(790, 620)
        self._app_icon = None
        try:
            icon = _icon_path()
            if icon is not None:
                self._app_icon = tk.PhotoImage(file=str(icon))
                self.root.iconphoto(True, self._app_icon)
        except Exception:
            pass

        env = read_env_values()
        self.creds_available = credentials_available()
        self.api_id_var = tk.StringVar(value=env["api_id"])
        self.api_hash_var = tk.StringVar(value=env["api_hash"])
        self.download_dir_var = tk.StringVar(value=env["download_dir"] or str(default_download_dir()))
        self.temp_download_dir_var = tk.StringVar(value=env["temp_download_dir"])
        # The GUI lock is acquired before initialization; no other GUI
        # process can race our startup orphan cleanup.
        startup_cache = inspect_cleanup(
            self._cleanup_path(env["temp_download_dir"]),
            self._cleanup_path(env["download_dir"]),
        )
        self._log_startup(startup_cache.summary() + "\n")
        for error in startup_cache.failed:
            self._log_startup(f"[缓存清理] {error}\n")
        self.proxy_host_var = tk.StringVar(value=env["proxy_host"])
        self.proxy_port_var = tk.StringVar(value=env["proxy_port"])
        self.proxy_username_var = tk.StringVar(value=env["proxy_username"])
        self.proxy_password_var = tk.StringVar(value=env["proxy_password"])
        self.media_concurrency_var = tk.StringVar(value=env["media_concurrency"])
        self.media_retries_var = tk.StringVar(value=env["media_retries"])
        self.chunk_concurrency_var = tk.StringVar(value=env["chunk_concurrency"])
        self.media_speed_kib_var = tk.StringVar(value=env.get("media_speed_kib", "0"))
        # Editable form values are drafts. Only the Apply button publishes a
        # validated snapshot to the running monitor download worker.
        self._applied_media_settings = (
            int(env["media_concurrency"]),
            int(env["media_retries"]),
            int(env["chunk_concurrency"]),
            int(env.get("media_speed_kib", "0")),
        )
        self.status_var = tk.StringVar(value="未登录")
        self.manual_var = tk.StringVar()

        self.type_photo = tk.BooleanVar(value=True)
        self.type_video = tk.BooleanVar(value=True)
        self.type_anim = tk.BooleanVar(value=False)
        self.dl_limit = tk.StringVar()
        self._download_future = None
        self._download_task = None
        self._stop_download = threading.Event()
        self._closing = False
        self._monitor_cursor = 0
        try:
            self._monitored = load_channels()
            if migration_error := monitor_migration_error(data_dir()):
                self._log_startup(
                    f"[监控] 旧监控配置未迁移，原文件未删除，可修复后重启：{migration_error}\n"
                )
        except (ValueError, OSError) as exc:
            self._monitored = []
            self._log_startup(f"[监控] 配置读取失败：{exc}\n")
        self._monitor_next: dict[str, float] = {}
        self._monitor_totals: dict[str, dict[str, int]] = {}
        self._monitor_scan_future = None
        self._monitor_download_future = None
        self._monitor_worker_task = None
        self._monitor_scan_stop = threading.Event()
        self._monitor_download_stop = threading.Event()
        self._monitor_slice_cursor = 0
        self._monitor_batch_used: dict[str, int] = {}
        self._monitor_scan_inflight = False
        self._monitor_active_transfers = 0
        self._monitor_total_transfers = 0
        self._monitor_live_concurrency = self._applied_media_settings[0]
        self._monitor_live_retries = self._applied_media_settings[1]
        self._monitor_live_chunks = self._applied_media_settings[2]
        self._monitor_live_speed_kib = self._applied_media_settings[3]
        self._monitor_worker_retry_at = 0.0
        self._monitor_job = None
        self._monitor_active = False

        # gallery state
        self.g_limit = tk.StringVar(value="60")
        self.gallery_cells: list = []
        self._thumb_refs: list = []
        self._gallery_title = ""
        self._gallery_entity = None

        self.p_no_previews = tk.BooleanVar(value=False)
        self.p_catalog_only = tk.BooleanVar(value=False)
        self.p_no_bt = tk.BooleanVar(value=False)
        self.p_dry = tk.BooleanVar(value=False)
        self.p_limit = tk.StringVar()

        self._build_ui()
        self._set_main_log_account_filter("default")
        # No trace_add on numeric inputs: intermediate text (e.g. 3 -> 30 ->
        # 3096) must never cause a live bandwidth change.

        self._stdout, self._stderr = sys.stdout, sys.stderr
        self._timestamp_formatter = TimestampedLines()
        self._download_timestamp_formatter = TimestampedLines()
        writer = _QueueWriter(self.log_queue, self._timestamp_formatter,
                              persist=self._log_store.write)
        sys.stdout = writer
        sys.stderr = writer
        self._telethon_diag_uninstall = install_telethon_diagnostics(self._log)
        self._log("[网络诊断] 已启用 Telethon FloodWait、连接恢复和分块停顿诊断\n")

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(100, self._drain_log)
        self.root.after(1000, self._refresh_main_account_status)
        self.root.after_idle(self._check_saved_sessions_at_startup)

    # ---------- UI ----------
    def _build_ui(self) -> None:
        tk, ttk = self.tk, self.ttk
        pad = {"padx": 6, "pady": 4}

        # First-run credentials (only when the app has none yet).
        if not self.creds_available:
            setup = ttk.LabelFrame(self.root, text="首次设置(仅一次):Telegram API")
            setup.pack(fill="x", **pad)
            msg = (
                "打开 https://my.telegram.org → API development tools,创建应用,"
                "把 api_id 和 api_hash 填到这里(只需一次,之后会记住)。"
            )
            ttk.Label(setup, text=msg, wraplength=760, foreground="#555").grid(
                row=0, column=0, columnspan=4, sticky="w", **pad
            )
            ttk.Label(setup, text="api_id").grid(row=1, column=0, sticky="w", **pad)
            ttk.Entry(setup, textvariable=self.api_id_var, width=18).grid(row=1, column=1, sticky="w", **pad)
            ttk.Label(setup, text="api_hash").grid(row=1, column=2, sticky="w", **pad)
            ttk.Entry(setup, textvariable=self.api_hash_var, width=40).grid(row=1, column=3, sticky="w", **pad)
            ttk.Button(setup, text="保存", command=self.on_save_creds).grid(row=1, column=4, **pad)
            self._setup_frame = setup

        # Shared account header: selection and status on the left, actions on the right.
        login = ttk.Frame(self.root)
        self._login_frame = login
        login.pack(fill="x", **pad)
        login.columnconfigure(3, weight=1)
        ttk.Label(login, text="当前账号").grid(row=0, column=0, sticky="w", **pad)
        self.main_account_choice = tk.StringVar()
        self._main_account_choices = {}
        self._main_account_selector = ttk.Combobox(
            login, textvariable=self.main_account_choice, state="readonly", width=27
        )
        self._main_account_selector.grid(row=0, column=1, sticky="w", **pad)
        self._main_account_selector.bind(
            "<<ComboboxSelected>>", lambda _event: self._switch_main_account()
        )
        self._refresh_main_account_choices()
        self.login_btn = ttk.Button(login, text="Telegram 登录", command=self.on_login)
        self.login_btn.grid(row=0, column=5, **pad)
        ttk.Button(login, text="账号设置", command=self._open_settings).grid(row=0, column=4, **pad)
        self._account_status_label = ttk.Label(login, textvariable=self.status_var, foreground="#546174")
        self._account_status_label.grid(row=0, column=2, sticky="w", **pad)

        # Let the user give the log panel more room without enlarging
        # the entire window. Pack the outer login row above the splitter.
        self.main_split = ttk.Panedwindow(self.root, orient="vertical")
        self.main_split.pack(fill="both", expand=True, padx=6, pady=(2, 6))
        main_panel = ttk.Frame(self.main_split)
        log_panel = ttk.Frame(self.main_split)
        self.main_split.add(main_panel, weight=1)
        self.main_split.add(log_panel, weight=1)

        # Reserve the folder row before the flexible panes use the available height.
        folder = ttk.Frame(main_panel)
        folder.pack(side="bottom", fill="x", **pad)
        ttk.Label(folder, text="下载到:").pack(side="left")
        ttk.Label(folder, textvariable=self.download_dir_var, foreground="#333").pack(side="left", padx=6)
        ttk.Button(folder, text="更改", command=self._change_dir).pack(side="left")
        ttk.Button(folder, text="打开下载文件夹", command=self._open_folder).pack(side="left", padx=6)

        # Your chats.
        chats = ttk.LabelFrame(main_panel, text="我的群组 / 频道(登录后自动加载)")
        chats.pack(fill="both", expand=True, **pad)
        self._dialog_rows: list[tuple[str, str, str, str]] = []
        self.dialog_search_var = tk.StringVar()
        search_row = ttk.Frame(chats)
        search_row.pack(side="top", fill="x", padx=6, pady=(4, 0))
        ttk.Label(search_row, text="筛选群组/频道").pack(side="left", padx=(0, 6))
        ttk.Entry(search_row, textvariable=self.dialog_search_var, width=38).pack(
            side="left", fill="x", expand=True
        )
        ttk.Button(search_row, text="清空", command=lambda: self.dialog_search_var.set("")).pack(
            side="left", padx=6
        )
        self.dialog_search_count = tk.StringVar(value="")
        ttk.Label(search_row, textvariable=self.dialog_search_count).pack(side="left")
        self.dialog_search_var.trace_add("write", lambda *_: self._filter_dialog_tree())
        cols = ("id", "kind", "name")
        self.tree = ttk.Treeview(chats, columns=cols, show="headings", height=5, selectmode="browse")
        self.tree.heading("id", text="ID")
        self.tree.heading("kind", text="类型")
        self.tree.heading("name", text="名称")
        self.tree.column("id", width=140, anchor="w")
        self.tree.column("kind", width=100, anchor="w")
        self.tree.column("name", width=460, anchor="w")
        self.tree.pack(side="left", fill="both", expand=True, padx=(6, 0), pady=6)
        sb = ttk.Scrollbar(chats, orient="vertical", command=self.tree.yview)
        sb.pack(side="left", fill="y", pady=6)
        self.tree.configure(yscrollcommand=sb.set)

        side = ttk.Frame(chats)
        side.pack(side="left", fill="y", padx=6, pady=6)
        self.refresh_btn = ttk.Button(side, text="刷新列表", command=self.on_list)
        self.refresh_btn.pack(fill="x", pady=2)
        ttk.Label(side, text="或手动输入\nID/@用户名/邀请链接", foreground="#555").pack(anchor="w", pady=(8, 0))
        ttk.Entry(side, textvariable=self.manual_var, width=22).pack(fill="x", pady=2)

        # Download / advanced tabs.
        tabs_row = ttk.Frame(main_panel)
        tabs_row.pack(fill="x", **pad)
        nb = ttk.Notebook(tabs_row)
        nb.pack(side="left", fill="x", expand=True)
        nb.add(self._build_gallery_tab(), text="浏览媒体(选图/视频)")
        nb.add(self._build_download_tab(), text="批量下载")
        monitor_tab = self._build_monitor_tab()
        nb.add(monitor_tab, text="多频道监控")
        nb.add(self._build_download_settings_tab(), text="下载设置")
        nb.add(self._build_parse_tab(), text="解析链接(高级)")
        nb.select(monitor_tab)
        def resize_tab(_event=None):
            self.root.after_idle(self._resize_selected_feature_tab, nb)

        nb.bind("<<NotebookTabChanged>>", resize_tab, add="+")
        resize_tab()

        logframe = ttk.LabelFrame(log_panel, text="日志和实时进度（拖动上方分隔线调整高度）")
        logframe.pack(fill="both", expand=True, **pad)
        progress_frame = ttk.LabelFrame(logframe, text="当前下载进度（实时）")
        progress_frame.pack(fill="x", padx=4, pady=(4, 2))
        self.progress_summary = tk.StringVar(value="当前没有正在下载的文件")
        ttk.Label(progress_frame, textvariable=self.progress_summary).pack(anchor="w", padx=5, pady=(2, 0))
        progress_cols = ("file", "progress", "speed", "status")
        self.progress_tree = ttk.Treeview(
            progress_frame, columns=progress_cols, show="headings", height=3,
            selectmode="none",
        )
        for column, title, width in (
            ("file", "群组 / 文件", 270),
            ("progress", "下载进度", 240),
            ("speed", "速度", 100),
            ("status", "状态", 100),
        ):
            self.progress_tree.heading(column, text=title)
            self.progress_tree.column(column, width=width, stretch=(column == "file"))
        self.progress_tree.pack(fill="x", padx=5, pady=(1, 4))
        toolbar = ttk.Frame(logframe)
        toolbar.pack(fill="x", padx=4, pady=(4, 0))
        self.log_tabs = ttk.Notebook(logframe)
        self.log_tabs.pack(fill="both", expand=True, padx=4, pady=(2, 4))
        runtime_tab = ttk.Frame(self.log_tabs)
        downloads_tab = ttk.Frame(self.log_tabs)
        self.log_tabs.add(runtime_tab, text="运行日志")
        self.log_tabs.add(downloads_tab, text="下载记录")
        self.log = self.scrolledtext.ScrolledText(runtime_tab, height=15, wrap="word", state="disabled", font=("Consolas", 10))
        self.log.pack(fill="both", expand=True)
        self.download_log = self.scrolledtext.ScrolledText(downloads_tab, height=15, wrap="word", state="disabled", font=("Consolas", 10))
        self.download_log.pack(fill="both", expand=True)
        ttk.Button(toolbar, text="跳转最新", command=self._jump_to_latest_log).pack(side="right", padx=3)
        ttk.Button(toolbar, text="放大查看", command=self._open_active_log_window).pack(side="right", padx=3)
        self._log_filter_selector = ttk.Combobox(
            toolbar, textvariable=self.log_account_filter, state="readonly", width=23
        )
        self._log_filter_selector.pack(side="left", padx=3)
        self._log_filter_selector.bind(
            "<<ComboboxSelected>>", lambda _event: self._refresh_log_views()
        )
        self._refresh_log_accounts()
        self.root.after_idle(self._set_initial_log_split, main_panel, log_panel)

    @staticmethod
    def _resize_selected_feature_tab(notebook) -> None:
        """Do not let a tall gallery create empty space in compact pages."""
        if notebook.winfo_exists() and (selected := notebook.select()):
            page = notebook.nametowidget(selected)
            notebook.configure(height=max(1, page.winfo_reqheight()))

    @staticmethod
    def _initial_split_position(
        height: int, main_height: int | None = None, log_height: int | None = None,
    ) -> int:
        """Aim for the requested log height without crowding out the main panel."""
        last = max(180, height - 220)
        if main_height is None or log_height is None:
            return max(180, min(int(height * 0.52), last))
        return max(180, min(last, max(main_height, height - log_height - 5)))

    def _set_initial_log_split(self, main_panel, log_panel) -> None:
        """Set the sash once; later user adjustments remain untouched."""
        if not self.main_split.winfo_exists():
            return
        height = self.main_split.winfo_height()
        if height <= 1:
            self.root.after(100, self._set_initial_log_split, main_panel, log_panel)
            return
        self.main_split.sashpos(0, self._initial_split_position(
            height, main_panel.winfo_reqheight(), log_panel.winfo_reqheight(),
        ))

    def _active_log_widget(self):
        return self.download_log if self.log_tabs.index("current") == 1 else self.log

    def _jump_to_latest_log(self):
        self._active_log_widget().see("end")

    def _open_active_log_window(self):
        if self.log_tabs.index("current") == 1:
            self._open_download_window()
        else:
            self._open_log_window()

    def _build_monitor_tab(self):
        tk, ttk = self.tk, self.ttk
        frame = ttk.Frame(self.root)
        controls = ttk.Frame(frame)
        controls.pack(fill="x", padx=6, pady=4)
        ttk.Label(controls, text="扫描间隔(分钟)").pack(side="left")
        self.monitor_minutes_var = tk.StringVar(value="15")
        ttk.Entry(controls, textvariable=self.monitor_minutes_var, width=6).pack(side="left", padx=5)
        ttk.Label(controls, text="优先级").pack(side="left", padx=4)
        self.monitor_priority_var = tk.StringVar(value="0")
        ttk.Entry(controls, textvariable=self.monitor_priority_var, width=5).pack(side="left")
        ttk.Label(controls, text="每轮数量").pack(side="left", padx=4)
        self.monitor_batch_var = tk.StringVar(value="100")
        ttk.Entry(controls, textvariable=self.monitor_batch_var, width=7).pack(side="left")
        ttk.Label(controls, text="方向").pack(side="left", padx=4)
        self.monitor_direction_var = tk.StringVar(value="从新到旧")
        ttk.Combobox(
            controls, textvariable=self.monitor_direction_var, state="readonly", width=10,
            values=("从新到旧", "从旧到新"),
        ).pack(side="left", padx=4)
        monitor_types = ttk.Frame(frame)
        monitor_types.pack(fill="x", padx=6, pady=(0, 3))
        ttk.Label(monitor_types, text="监控下载类型（每个频道独立）:").pack(side="left")
        self.monitor_type_photo = tk.BooleanVar(value=True)
        self.monitor_type_video = tk.BooleanVar(value=True)
        self.monitor_type_anim = tk.BooleanVar(value=False)
        ttk.Checkbutton(monitor_types, text="图片", variable=self.monitor_type_photo).pack(side="left", padx=4)
        ttk.Checkbutton(monitor_types, text="视频", variable=self.monitor_type_video).pack(side="left", padx=4)
        ttk.Checkbutton(monitor_types, text="GIF", variable=self.monitor_type_anim).pack(side="left", padx=4)
        edit_actions = ttk.Frame(frame)
        edit_actions.pack(fill="x", padx=6, pady=2)
        ttk.Button(edit_actions, text="添加当前频道", command=self._monitor_add).pack(side="left")
        ttk.Button(edit_actions, text="更新所选", command=self._monitor_update).pack(side="left", padx=4)
        ttk.Button(edit_actions, text="删除所选", command=self._monitor_remove).pack(side="left", padx=5)
        ttk.Button(edit_actions, text="启用/暂停所选", command=self._monitor_toggle).pack(side="left")
        columns = ("name", "target", "types", "minutes", "priority", "batch_size", "direction", "enabled", "next")
        table = ttk.Frame(frame)
        table.pack(fill="x", padx=6)
        self.monitor_tree = ttk.Treeview(table, columns=columns, show="headings", height=5)
        monitor_scroll = ttk.Scrollbar(table, orient="vertical", command=self.monitor_tree.yview)
        monitor_scroll.pack(side="right", fill="y")
        self.monitor_tree.configure(yscrollcommand=monitor_scroll.set)
        for column, label, width in (
            ("name", "群组 / 频道名称", 180),
            ("target", "频道 ID / 用户名", 155),
            ("types", "下载类型", 125),
            ("minutes", "间隔(分钟)", 85),
            ("priority", "优先级", 70),
            ("batch_size", "每轮数量", 80),
            ("direction", "扫描方向", 85),
            ("enabled", "状态", 65),
            ("next", "下次扫描", 135),
        ):
            self.monitor_tree.heading(column, text=label)
            self.monitor_tree.column(column, width=width)
        self.monitor_tree.pack(side="left", fill="x", expand=True)
        self.monitor_tree.bind("<<TreeviewSelect>>", self._monitor_selection_changed)
        actions = ttk.Frame(frame)
        actions.pack(fill="x", padx=6, pady=4)
        self.monitor_button = ttk.Button(actions, text="启动当前账号监控", command=self._monitor_start_stop)
        self.monitor_button.pack(side="left")
        ttk.Button(actions, text="取消当前账号下载", command=self._cancel_current_download).pack(side="left", padx=6)
        ttk.Label(actions, text="停止会等文件下完；取消会中断下载并暂停监控", foreground="#555").pack(
            side="left", padx=10
        )
        self._monitor_refresh()
        return frame

    def _monitor_refresh(self):
        self._refresh_monitor_button()
        monitors = self._visible_monitors()
        due_map = self._visible_monitor_next()
        is_running = self._visible_monitor_running()
        expected = {str(i) for i in range(len(monitors))}
        for iid in self.monitor_tree.get_children():
            if iid not in expected:
                self.monitor_tree.delete(iid)
        for index, item in enumerate(monitors):
            due = due_map.get(item["target"])
            next_time = time.strftime("%H:%M:%S", time.localtime(due)) if due and item["enabled"] and is_running else "-"
            values = (
                item.get("name") or "（待获取）", item["target"],
                self._monitor_types_label(item.get("types", "photo,video")), item["minutes"],
                item.get("priority", 0), item.get("batch_size", 100),
                "旧→新" if item.get("direction") == "oldest" else "新→旧",
                "启用" if item["enabled"] else "暂停", next_time,
            )
            iid = str(index)
            if self.monitor_tree.exists(iid):
                if tuple(map(str, values)) != tuple(self.monitor_tree.item(iid, "values")):
                    self.monitor_tree.item(iid, values=values)
            else:
                self.monitor_tree.insert("", "end", iid=iid, values=values)
        for rank, index in enumerate(sorted(
            range(len(monitors)),
            key=lambda i: -int(monitors[i].get("priority", 0)),
        )):
            self.monitor_tree.move(str(index), "", rank)

    def _monitor_set_name(self, target: str, title: str, peer_id: int | None = None) -> None:
        """Persist canonical channel peer ID for cross-account task matching."""
        for item in self._monitored:
            if item["target"] != target:
                continue
            changed = False
            if title and title != item.get("name"):
                item["name"] = title
                changed = True
            if peer_id is not None and int(peer_id) != item.get("peer_id"):
                item["peer_id"] = int(peer_id)
                changed = True
            if changed:
                save_channels(self._monitored)
                self._monitor_refresh()
            break

    def _monitor_add(self):
        target = self._selected_target()
        if not target:
            self.messagebox.showwarning("监控", "请先选择或输入频道")
            return
        try:
            minutes, priority, batch_size = self._monitor_form_values()
        except ValueError as exc:
            self.messagebox.showwarning("监控", str(exc))
            return
        types = self._monitor_types_string()
        if not types:
            self.messagebox.showwarning("监控", "至少选择一种下载类型")
            return
        monitors = self._visible_monitors()
        existing_name = next(
            (item.get("name", "") for item in monitors if item["target"] == target),
            "",
        )
        if not existing_name:
            selected = self.tree.selection()
            if selected:
                values = self.tree.item(selected[0], "values")
                if values and str(values[0]) == target and len(values) > 2:
                    existing_name = str(values[2])
        self.monitor_tree.selection_remove(self.monitor_tree.selection())
        monitors[:] = [item for item in monitors if item["target"] != target]
        monitors.append({
            "target": target, "minutes": minutes, "types": types, "enabled": True,
            "direction": "oldest" if self.monitor_direction_var.get() == "从旧到新" else "newest",
            "name": existing_name, "priority": priority, "batch_size": batch_size,
        })
        self._visible_monitor_next()[target] = time.time()
        self._save_visible_monitors()
        self._monitor_refresh()

    def _monitor_selected_index(self):
        selected = self.monitor_tree.selection()
        return int(selected[0]) if selected else None

    def _monitor_selection_changed(self, _event=None):
        index = self._monitor_selected_index()
        monitors = self._visible_monitors()
        if index is not None and index < len(monitors):
            item = monitors[index]
            self.monitor_minutes_var.set(str(item["minutes"]))
            self.monitor_priority_var.set(str(item.get("priority", 0)))
            self.monitor_batch_var.set(str(item.get("batch_size", 100)))
            self.monitor_direction_var.set(
                "从旧到新" if item.get("direction") == "oldest" else "从新到旧"
            )
            self._monitor_set_types(item.get("types", "photo,video"))

    def _monitor_update(self):
        index = self._monitor_selected_index()
        if index is None:
            self.messagebox.showwarning("监控", "请先在监控列表中选一个频道")
            return
        try:
            minutes, priority, batch_size = self._monitor_form_values()
        except ValueError as exc:
            self.messagebox.showwarning("监控", str(exc))
            return
        types = self._monitor_types_string()
        if not types:
            self.messagebox.showwarning("监控", "至少选择一种监控下载类型")
            return
        item = self._visible_monitors()[index]
        item["minutes"] = minutes
        item["priority"] = priority
        item["batch_size"] = batch_size
        item["direction"] = "oldest" if self.monitor_direction_var.get() == "从旧到新" else "newest"
        item["types"] = types
        if self._selected_view_account() == "default":
            self._monitor_batch_used.pop(item["target"], None)
            self._monitor_totals.pop(item["target"], None)
        self._visible_monitor_next()[item["target"]] = time.time()
        self._save_visible_monitors()
        self._monitor_refresh()

    @staticmethod
    def _monitor_types_label(raw: str) -> str:
        try:
            selected = parse_types(str(raw))
        except ValueError:
            return "未知类型"
        return "、".join(
            label for kind, label in (
                ("photo", "图片"), ("video", "视频"), ("animation", "GIF")
            ) if kind in selected
        )

    def _monitor_set_types(self, raw: str) -> None:
        try:
            selected = parse_types(str(raw))
        except ValueError:
            selected = set()
        self.monitor_type_photo.set("photo" in selected)
        self.monitor_type_video.set("video" in selected)
        self.monitor_type_anim.set("animation" in selected)

    def _monitor_types_string(self) -> str:
        return ",".join(
            name for name, variable in (
                ("photo", self.monitor_type_photo),
                ("video", self.monitor_type_video),
                ("animation", self.monitor_type_anim),
            ) if variable.get()
        )

    def _monitor_form_values(self) -> tuple[int, int, int]:
        try:
            minutes = int(self.monitor_minutes_var.get().strip())
            priority = int(self.monitor_priority_var.get().strip())
            batch_size = int(self.monitor_batch_var.get().strip())
        except ValueError as exc:
            raise ValueError("间隔、优先级及每轮数量必须填写整数") from exc
        if not 1 <= minutes <= 1440:
            raise ValueError("扫描间隔应为 1～1440 分钟")
        if not 1 <= batch_size <= 100_000:
            raise ValueError("每轮数量应为 1～100000")
        return minutes, priority, batch_size

    def _monitor_remove(self):
        index = self._monitor_selected_index()
        if index is not None:
            self.monitor_tree.selection_remove(self.monitor_tree.selection())
            item = self._visible_monitors().pop(index)
            self._visible_monitor_next().pop(item["target"], None)
            if self._selected_view_account() == "default":
                self._monitor_batch_used.pop(item["target"], None)
                self._monitor_totals.pop(item["target"], None)
            self._save_visible_monitors()
            self._monitor_refresh()

    def _monitor_toggle(self):
        index = self._monitor_selected_index()
        if index is not None:
            item = self._visible_monitors()[index]
            item["enabled"] = not item["enabled"]
            if self._selected_view_account() == "default":
                self._monitor_batch_used.pop(item["target"], None)
                self._monitor_totals.pop(item["target"], None)
            self._visible_monitor_next()[item["target"]] = time.time()
            self._save_visible_monitors()
            self._monitor_refresh()

    def _primary_monitor_draining(self) -> bool:
        return not getattr(self, "_monitor_active", False) and any(
            worker is not None and not worker.done()
            for worker in (
                getattr(self, "_monitor_download_future", None),
                getattr(self, "_monitor_worker_task", None),
            )
        )

    def _refresh_monitor_button(self) -> None:
        button = getattr(self, "monitor_button", None)
        if button is None:
            return
        account_id = self._selected_view_account()
        runtime = self._extra_runtimes.get(account_id) if account_id != DEFAULT_ACCOUNT else None
        draining = (
            self._primary_monitor_draining() if account_id == DEFAULT_ACCOUNT
            else bool(getattr(runtime, "monitoring_draining", False))
        )
        if draining:
            button.configure(text="正在停止监控…", state="disabled")
        else:
            text = "停止当前账号监控" if self._visible_monitor_running() else "启动当前账号监控"
            button.configure(text=text, state="normal")

    def _stop_primary_monitor(self) -> None:
        self._monitor_active = False
        self._monitor_scan_stop.set()
        self._monitor_download_stop.set()
        if self._monitor_job is not None:
            self.root.after_cancel(self._monitor_job)
            self._monitor_job = None
        self._log("[监控] 已停止调度，正在下载的文件会继续完成\n")
        self._monitor_refresh()

    def _monitor_start_stop(self):
        if getattr(self, "_closing", False):
            return
        account_id = self._selected_view_account()
        if account_id != DEFAULT_ACCOUNT:
            runtime = self._extra_runtimes.get(account_id)
            if runtime is None or runtime.client is None or not runtime.status.startswith("已连接"):
                self.messagebox.showinfo("当前账号未登录", "请先使用主窗口的「Telegram 登录」。")
                return
            if runtime.monitoring:
                runtime.stop_monitoring()
                self._log_account(account_id, "[监控] 已停止调度，正在下载的文件会继续完成")
            elif runtime.start_monitoring():
                self._log_account(account_id, "[监控] 已启动当前账号监控")
            else:
                return
            self._monitor_refresh()
            return
        if self._monitor_active:
            self._stop_primary_monitor()
            return
        if self._primary_monitor_draining():
            return
        if self.client is None:
            self.messagebox.showinfo("当前账号未登录", "请先使用主窗口的「Telegram 登录」。")
            return
        self._monitor_active = True
        # Existing scans get the old (possibly set) event and cannot resume
        # unobserved when a new monitor session starts.
        self._monitor_scan_stop = threading.Event()
        self._monitor_download_stop.clear()
        self._monitor_batch_used.clear()
        self._monitor_totals.clear()
        self._monitor_next = {item["target"]: time.time() for item in self._monitored}
        self._log("[监控] 已启动当前账号监控\n")
        self._monitor_tick()

    def _apply_media_settings(self) -> None:
        """Atomically validate, save, and apply all numeric download settings."""
        try:
            raw = (
                self.media_concurrency_var.get(),
                self.media_retries_var.get(),
                self.chunk_concurrency_var.get(),
                self.media_speed_kib_var.get(),
            )
            settings = tuple(int(value.strip()) for value in raw)
        except (ValueError, TypeError, AttributeError):
            self.messagebox.showwarning("下载设置", "请填写完整的整数参数，未应用任何更改")
            return
        workers, retries, chunks, speed = settings
        if not (1 <= workers <= 8 and 0 <= retries <= 5
                and 1 <= chunks <= 8 and 0 <= speed <= 1048576):
            self.messagebox.showwarning(
                "下载设置", "并发/分块范围 1-8，失败重试范围 0-5，总限速范围 0-1048576 KiB/s"
            )
            return
        account_id = self._selected_view_account()
        previous = getattr(self, "_applied_media_settings", None)
        try:
            registry = getattr(self, "_account_registry", None)
            path = registry.env_path(account_id) if registry is not None else None
            values = read_env_values(path) if path is not None else read_env_values()
            values.update(
                media_concurrency=str(workers), media_retries=str(retries),
                chunk_concurrency=str(chunks), media_speed_kib=str(speed),
            )
            if path is None:
                update_env(values)
            else:
                update_env(values, path)
        except OSError as exc:
            self.messagebox.showwarning("下载设置", f"保存失败，设置未生效：{exc}")
            return
        if account_id != "default":
            self._log_account(account_id, (
                f"[下载设置] 已保存｜文件并发{workers}｜重试{retries}｜"
                f"分块并发{chunks}｜限速{speed} KiB/s；监控运行中自动读取\n"
            ))
            return
        self._applied_media_settings = settings
        self._monitor_live_concurrency = workers
        self._monitor_live_retries = retries
        self._monitor_live_chunks = chunks
        self._monitor_live_speed_kib = speed
        if previous is None or previous != settings:
            self._log(
                f"[下载设置] 已应用｜文件并发{workers}｜重试{retries}｜"
                f"分块并发{chunks}｜监控总限速{speed} KiB/s"
                "（0=不限速；运行中的监控下载逐步生效）\n"
            )

    def _monitor_tick(self):
        self._monitor_job = None
        if not self._monitor_active:
            return
        now = time.time()
        scan_busy = self._monitor_scan_inflight
        if not scan_busy:
            # Rotate a 20-message slice per channel, not a complete 500-media
            # batch. Sort by numeric priority, with cyclic fairness among
            # channels eligible in this tick.
            item = next_due_channel(
                self._monitored, self._monitor_next, now, self._monitor_slice_cursor
            )
            if item is not None:
                self._monitor_slice_cursor += 1
                target = item["target"]
                quota = int(item.get("batch_size", 100))
                used = self._monitor_batch_used.get(target, 0)
                remaining = max(1, quota - used)
                if target not in self._monitor_totals:
                    display = item.get("name") or target
                    self._log(
                        f"[扫描] {display}：开始｜间隔{item['minutes']}分钟｜"
                        f"本轮上限{quota}\n"
                    )
                scan_stop = self._monitor_scan_stop
                def resolved(title, peer_id):
                    self.root.after(0, lambda: None if scan_stop.is_set() else
                                    self._monitor_set_name(target, title, peer_id))
                async def scan():
                    await self._ensure_client()
                    return await scan_monitor_slice(
                        self.client, target=target, types=parse_types(item["types"]),
                        monitor_direction=item.get("direction", "newest"),
                        slice_size=20, match_limit=min(20, remaining),
                        stop_event=scan_stop,
                        on_channel_resolved=resolved,
                    )
                # Reserve a slice for this channel without blocking Tk or
                # waiting for the media file downloads to finish.
                future = self.loop.submit(scan())
                self._monitor_scan_future = future
                self._monitor_scan_inflight = True
                def finish_scan(result=None, error=None):
                    self._monitor_scan_inflight = False
                    if scan_stop.is_set():
                        return
                    if error is not None:
                        self._monitor_scan_error(target, error)
                    elif result is not None:
                        self._monitor_slice_done(target, result)

                def scanned(done):
                    try:
                        result = done.result()
                    except concurrent.futures.CancelledError:
                        self.root.after(0, finish_scan)
                    except Exception as exc:
                        self.root.after(0, lambda exc=exc: finish_scan(error=exc))
                    else:
                        self.root.after(0, lambda: finish_scan(result=result))
                future.add_done_callback(scanned)
        if self._monitor_download_future is not None and self._monitor_download_future.done():
            try:
                self._monitor_download_future.result()
            except concurrent.futures.CancelledError:
                pass
            except Exception as exc:
                self._monitor_worker_retry_at = now + 30
                self._log(f"[监控下载错误] {exc}；30 秒后尝试恢复\n")
            self._monitor_download_future = None
        worker = self._monitor_worker_task
        download_busy = (
            self._monitor_download_future is not None
            and not self._monitor_download_future.done()
        ) or (worker is not None and not worker.done())
        if (not download_busy and not self._monitor_download_stop.is_set()
                and now >= self._monitor_worker_retry_at):
            try:
                concurrency, retries, chunks = self._media_options()
            except ValueError as exc:
                self._log(f"[监控] 下载设置错误：{exc}\n")
            else:
                self._monitor_download_future = self.loop.submit(
                    self._monitor_download_worker(concurrency, retries, chunks)
                )
        self._monitor_refresh()
        self._monitor_job = self.root.after(1000, self._monitor_tick)

    def _monitor_scan_error(self, target: str, exc: Exception) -> None:
        self._monitor_scan_inflight = False
        if not self._monitor_active:
            return
        self._monitor_totals.pop(target, None)
        self._monitor_next[target] = time.time() + 60
        item = next((x for x in self._monitored if x["target"] == target), None)
        name = (item.get("name") or target) if item else target
        self._log(f"[扫描异常] {name}：失败｜60秒后重试｜{exc}\n")

    def _monitor_slice_done(self, target: str, result) -> None:
        self._monitor_scan_inflight = False
        if not self._monitor_active:
            return
        item = next((x for x in self._monitored if x["target"] == target), None)
        if item is None:
            return
        self._monitor_batch_used[target] = self._monitor_batch_used.get(target, 0) + result.matched
        quota_reached = self._monitor_batch_used[target] >= int(item.get("batch_size", 100))
        self._monitor_log_progress(target, result, quota_reached=quota_reached)
        if not result.more_pending or quota_reached:
            self._monitor_batch_used[target] = 0
            self._monitor_next[target] = time.time() + item["minutes"] * 60
        else:
            self._monitor_next[target] = time.time()

    async def _monitor_download_worker(self, concurrency: int, retries: int, chunks: int):
        """Consume durable media jobs independently from the channel scanner."""
        from tgdl.downloader import (
            DownloadLeaseLost, DownloadStats, _download_with_retry, peer_download_folder,
        )
        from tgdl.monitor_flow import MonitorFlow, MonitorPause, SharedSpeedLimiter
        self._monitor_worker_task = asyncio.current_task()
        history = DownloadHistory()
        active = {}
        active_jobs = {}
        claims: dict[tuple[int, int], str] = {}
        retiring: set[tuple[int, int]] = set()
        committed: set[str] = set()
        staged_jobs = set()
        pauses = {}
        active_target: dict[tuple[int, int], str] = {}
        active_started: dict[tuple[int, int], float] = {}
        occupied_seconds: dict[str, float] = {}
        suspended_accounted: set[tuple[int, int]] = set()
        dispatch_log = MonitorDispatchSummary()
        priority_logged = False
        pause_requested_at: dict[tuple[int, int], float] = {}
        pause_delay_reported: set[tuple[int, int]] = set()
        paused_since: dict[tuple[int, int], float] = {}
        limiter = SharedSpeedLimiter(getattr(self, "_monitor_live_speed_kib", 0) * 1024)
        try:
            await self._ensure_client()
            last_lease_refresh = time.monotonic()
            while not self._monitor_download_stop.is_set() or active:
                for line in dispatch_log.drain():
                    print(line)
                limiter.bytes_per_second = getattr(self, "_monitor_live_speed_kib", 0) * 1024
                now_lease = time.monotonic()
                finished = [key for key, task in active.items() if task.done()]
                for key in finished:
                    task = active.pop(key)
                    token = claims.pop(key)
                    retiring.discard(key)
                    committed.discard(token)
                    active_jobs.pop(key)
                    staged_jobs.discard(key)
                    pauses.pop(key)
                    self._monitor_progress_state(key[0], key[1], "结束")
                    pause_requested_at.pop(key, None)
                    pause_delay_reported.discard(key)
                    paused_since.pop(key, None)
                    target = active_target.pop(key)
                    started_at = active_started.pop(key)
                    if key not in suspended_accounted:
                        occupied_seconds[target] = (
                            occupied_seconds.get(target, 0.0)
                            + max(0.0, time.monotonic() - started_at)
                        )
                    suspended_accounted.discard(key)
                    try:
                        await task
                    except asyncio.CancelledError:
                        # Independent transfer cancellation must not stop the
                        # scheduler. A coroutine cancelled before its first
                        # step won't have executed its own finally clause.
                        history.release_claim("default", *key, token=token)
                    except Exception as exc:
                        print(f"[监控] 下载任务 {key} 异常：{exc}")
                if now_lease - last_lease_refresh >= 30:
                    for key, task in active.items():
                        token = claims[key]
                        # A completed SQLite commit consumes its token before
                        # the coroutine finishes releasing any SSD reservation.
                        if key in retiring or token in committed:
                            continue
                        if not history.renew_claim("default", *key, token=token):
                            retiring.add(key)
                            task.cancel()
                            print(f"[监控] 下载租约已失效，停止任务 {key}")
                    last_lease_refresh = now_lease
                if self._monitor_download_stop.is_set():
                    # Drain already-started files (including paused ones)
                    # while continuing their lease heartbeat. Do not claim
                    # anything new, even if a transfer frees a slot.
                    for key, ctl in pauses.items():
                        if key not in retiring and ctl.requested:
                            ctl.resume()
                            self._monitor_progress_state(key[0], key[1], "恢复中")
                    self._monitor_active_transfers = len(active)
                    self._monitor_total_transfers = len(active)
                    if active:
                        await asyncio.sleep(0.25)
                    continue
                # Paused wall time must not count as occupied transfer time.
                for key, ctl in pauses.items():
                    if key not in retiring and ctl.suspended and key not in suspended_accounted:
                        paused_since[key] = time.monotonic()
                        pause_requested_at.pop(key, None)
                        pause_delay_reported.discard(key)
                        self._monitor_progress_state(key[0], key[1], "已暂停")
                        target = active_target[key]
                        occupied_seconds[target] = (
                            occupied_seconds.get(target, 0.0)
                            + max(0.0, time.monotonic() - active_started[key])
                        )
                        suspended_accounted.add(key)
                        channel = next(
                            (item for item in self._monitored if item["target"] == target),
                            None,
                        )
                        dispatch_log.add(
                            "paused", (channel.get("name") or target) if channel else target
                        )
                # A server-side GetFileRequest may still be waiting on Telegram
                # when a pause is requested. Explain a delayed handoff once,
                # instead of silently appearing to ignore priority.
                for key, requested_at in list(pause_requested_at.items()):
                    ctl = pauses.get(key)
                    if ctl is None or ctl.suspended or key in retiring:
                        pause_requested_at.pop(key, None)
                        continue
                    if (time.monotonic() - requested_at >= 5.0
                            and key not in pause_delay_reported):
                        pause_delay_reported.add(key)
                        print(
                            f"[调度] 等待暂停 {active_target[key]} 的下载任务｜"
                            "当前网络请求尚未到安全暂停点，优先任务暂时等待"
                        )
                running_keys = [
                    key for key, ctl in pauses.items() if key in retiring or not ctl.suspended
                ]
                if not any(ctl.suspended or ctl.requested for ctl in pauses.values()):
                    priority_logged = False
                self._monitor_active_transfers = len(running_keys)
                self._monitor_total_transfers = len(active)
                # Preserve the existing manual download behaviour: don't
                # launch new monitor transfers while a manual job is running.
                manual_busy = self._download_future is not None and not self._download_future.done()
                # Only the live monitor limit changes. In-flight files are
                # never preempted when the user reduces the limit.
                effective_concurrency = getattr(self, "_monitor_live_concurrency", None) or concurrency
                if not manual_busy:
                    candidates = sorted(
                        (
                            item for item in self._monitored
                            if item["enabled"] or any(
                                key not in retiring and ctl.suspended
                                and active_target[key] == item["target"]
                                for key, ctl in pauses.items()
                            )
                        ),
                        key=lambda item: -int(item.get("priority", 0)),
                    )
                    pending = []
                    for item in candidates:
                        suspended_jobs = [
                            active_jobs[key] for key, ctl in pauses.items()
                            if key not in retiring and ctl.suspended
                            and active_target[key] == item["target"]
                        ]
                        pending.extend((item, suspended) for suspended in suspended_jobs)
                        if suspended_jobs:
                            continue  # Resume all paused files before new ones at this priority.
                        if not item["enabled"]:
                            continue  # Only finish an already-started disabled channel.
                        enabled_kinds = {
                            value.strip() for value in item.get("types", "photo,video").split(",")
                            if value.strip()
                        }
                        if item.get("peer_id"):
                            eligible_jobs = history.pending_for_peer(
                                item["peer_id"], account_id="default", limit=16,
                                kinds=enabled_kinds,
                            )
                        else:
                            eligible_jobs = history.pending_for_target(
                                item["target"], limit=16, account_id="default",
                                kinds=enabled_kinds,
                            )
                        for job in eligible_jobs:
                            if (job.peer_id, job.message_id) not in active:
                                pending.append((item, job))
                                break
                    # Staged transfers may hold the SSD reservation for the
                    # entire file. Pausing one while starting another could
                    # make the new transfer wait for that paused reservation.
                    # With staging, switch priorities only at the next free slot.
                    if pending and not staged_jobs and len(running_keys) >= effective_concurrency:
                        high_priority = max(int(item.get("priority", 0)) for item, _ in pending)
                        priority_by_target = {
                            item["target"]: int(item.get("priority", 0))
                            for item in candidates
                        }
                        transitioning = any(
                            ctl.requested and not ctl.suspended
                            for ctl in pauses.values()
                        )
                        victims = [
                            key for key in running_keys
                            if key not in retiring and not pauses[key].requested
                            and priority_by_target.get(active_target[key], 0) < high_priority
                        ]
                        if victims and not transitioning:
                            victim = min(
                                victims,
                                key=lambda key: (
                                    priority_by_target.get(active_target[key], 0),
                                    active_started[key],
                                ),
                            )
                            pauses[victim].pause()
                            pause_requested_at[victim] = time.monotonic()
                            await asyncio.sleep(0.1)
                            continue
                    if len(running_keys) >= effective_concurrency:
                        await asyncio.sleep(0.25)
                        continue
                    active_counts: dict[str, int] = {}
                    current_occupied = dict(occupied_seconds)
                    now = time.monotonic()
                    for key, target in active_target.items():
                        if pauses[key].suspended:
                            continue
                        active_counts[target] = active_counts.get(target, 0) + 1
                        current_occupied[target] = (
                            current_occupied.get(target, 0.0)
                            + max(0.0, now - active_started[key])
                        )
                    selected = None
                    if pending:
                        # Strict priority: never resume or start a lower-rank
                        # job while any higher-rank file is still running or
                        # queued. At the winning rank, paused jobs get the
                        # first slot, before new media from the same rank.
                        highest = max(int(item.get("priority", 0)) for item, _ in pending)
                        highest_running = max(
                            (
                                int(next(
                                    (item.get("priority", 0) for item in candidates
                                     if item["target"] == active_target[key]), 0
                                ))
                                for key in running_keys
                            ),
                            default=None,
                        )
                        if highest_running is not None and highest_running > highest:
                            await asyncio.sleep(0.25)
                            continue  # Higher-priority transfer has not finished yet.
                        matching = [
                            pair for pair in pending
                            if int(pair[0].get("priority", 0)) == highest
                        ]
                        suspended_first = [
                            pair for pair in matching
                            if (pair[1].peer_id, pair[1].message_id) in paused_since
                        ]
                        if suspended_first:
                            selected = min(
                                suspended_first,
                                key=lambda pair: paused_since[
                                    (pair[1].peer_id, pair[1].message_id)
                                ],
                            )
                        else:
                            selected = choose_monitor_download(matching, active_counts, current_occupied)
                    if selected is not None:
                        item, job = selected
                        key = (job.peer_id, job.message_id)
                        if key in active:
                            paused_since.pop(key, None)
                            suspended_accounted.discard(key)
                            active_started[key] = time.monotonic()
                            pauses[key].resume()
                            self._monitor_progress_state(key[0], key[1], "恢复中")
                            dispatch_log.add("resumed", item.get("name") or item["target"])
                            continue
                        token = history.try_claim("default", job.peer_id, job.message_id)
                        if token is None:
                            await asyncio.sleep(0.03)
                            continue
                        if self._monitor_download_stop.is_set():
                            history.release_claim("default", job.peer_id, job.message_id, token=token)
                            continue
                        pause = MonitorPause()
                        preempted = any(ctl.suspended for ctl in pauses.values())
                        flow = MonitorFlow(
                            pause=pause,
                            limiter=limiter,
                            chunks=lambda: getattr(self, "_monitor_live_chunks", None) or chunks,
                        )
                        settings = self.settings
                        if settings.temp_download_dir is not None:
                            staged_jobs.add(key)

                        async def transfer(job=job, flow=flow, target=item["target"],
                                           media_types=item.get("types", "photo,video"),
                                           transfer_settings=settings, claim_token=token):
                            cooldown = 0
                            try:
                                # Always resolve with the account's own channel target.
                                entity = await resolve_channel(self.client, target)
                                resolved_peer = _downloader.history_peer_id(entity)
                                if resolved_peer != job.peer_id:
                                    cooldown = 3600
                                    title = getattr(entity, "title", None) or job.title
                                    self.root.after(0, lambda: self._monitor_set_name(
                                        target, title, resolved_peer
                                    ))
                                    print("[监控] 监控目标与队列记录不符，跳过旧任务并重新识别频道")
                                    return
                                message = await self.client.get_messages(entity, ids=job.message_id)
                                if message is None or message.date is None:
                                    cooldown = 3600
                                    print(f"[监控] {target}/{job.message_id} 在本账号不可读取")
                                    return
                                kind = job.kind or _downloader.classify_message(message)
                                if kind is None or kind not in {
                                    value.strip() for value in media_types.split(",") if value.strip()
                                }:
                                    cooldown = 3600
                                    return
                                folder = peer_download_folder(
                                    transfer_settings.download_dir,
                                    getattr(entity, "title", None) or job.title, entity,
                                ) / ("photos" if kind == "photo" else "videos")
                                stats = DownloadStats()
                                path = await _download_with_retry(
                                    self.client, message, folder,
                                    job.base or f"{message.date.strftime('%Y%m%d')}_{message.id}",
                                    stats, skip_existing=True,
                                    retries=getattr(self, "_monitor_live_retries", retries),
                                    chunk_concurrency=chunks, history=history,
                                    peer_id=job.peer_id, account_id="default", claim_token=claim_token,
                                    on_claim_committed=lambda: committed.add(claim_token),
                                    on_saved=lambda detail: self._log_download(f"[监控下载] [{job.title}] {detail}"),
                                    temp_download_dir=transfer_settings.temp_download_dir,
                                    on_progress=self._update_download_progress,
                                    monitor_flow=flow,
                                )
                                if stats.failed:
                                    cooldown = 60
                                elif path is None and not history.contains(job.peer_id, job.message_id):
                                    cooldown = 300
                            except asyncio.CancelledError:
                                raise
                            except DownloadLeaseLost:
                                print(f"[监控] 下载租约已失效，放弃保存 {job.title}/{job.message_id}")
                            except Exception as exc:
                                cooldown = 60
                                print(f"[监控] 下载 {job.title}/{job.message_id} 失败：{exc}")
                            finally:
                                # Keep the global pending row retryable by another
                                # eligible account instead of deleting it on failure.
                                history.release_claim(
                                    "default", job.peer_id, job.message_id,
                                    token=claim_token, cooldown=cooldown,
                                )
                        active[key] = asyncio.create_task(transfer())
                        active_jobs[key] = job
                        claims[key] = token
                        pauses[key] = pause
                        active_target[key] = item["target"]
                        active_started[key] = time.monotonic()
                        self._monitor_active_transfers = len(running_keys) + 1
                        if preempted and not priority_logged:
                            high_name = item.get("name") or item["target"]
                            print(f"[调度] {high_name}：已插队开始下载｜低优先级任务稍后恢复")
                            priority_logged = True
                        continue
                await asyncio.sleep(0.25 if active else 1)
        finally:
            for line in dispatch_log.drain(force=True):
                print(line)
            # Graceful shutdown drained this set above. If the worker itself
            # was cancelled or failed, cancel any transfer it can no longer renew.
            for key, task in active.items():
                if key not in retiring and claims[key] not in committed and not task.done():
                    task.cancel()
            if active:
                await asyncio.gather(*active.values(), return_exceptions=True)
            for key in active:
                self._monitor_progress_state(key[0], key[1], "结束")
            self._monitor_active_transfers = 0
            self._monitor_total_transfers = 0
            try:
                for key, token in claims.items():
                    history.release_claim("default", *key, token=token)
            finally:
                history.close()
                self._monitor_worker_task = None

    def _build_gallery_tab(self):
        tk, ttk = self.tk, self.ttk
        pad = {"padx": 4, "pady": 4}
        frame = ttk.Frame(self.root)

        ctrl = ttk.Frame(frame)
        ctrl.pack(fill="x", **pad)
        ttk.Label(ctrl, text="加载数量").pack(side="left")
        ttk.Entry(ctrl, textvariable=self.g_limit, width=6).pack(side="left", padx=4)
        self.load_gallery_btn = ttk.Button(ctrl, text="加载媒体", command=self.on_load_gallery)
        self.load_gallery_btn.pack(side="left", padx=4)
        ttk.Button(ctrl, text="全选", command=lambda: self._gallery_set_all(True)).pack(side="left")
        ttk.Button(ctrl, text="全不选", command=lambda: self._gallery_set_all(False)).pack(side="left", padx=4)
        self.save_sel_btn = ttk.Button(ctrl, text="保存所选", command=self.on_save_selected)
        self.save_sel_btn.pack(side="right")
        ttk.Label(ctrl, text="(用上面『下载类型』筛选图片/视频)", foreground="#777").pack(side="right", padx=8)

        wrap = ttk.Frame(frame)
        wrap.pack(fill="both", expand=True, **pad)
        self.gallery_canvas = tk.Canvas(wrap, height=300, background="#f5f5f5", highlightthickness=0)
        vbar = ttk.Scrollbar(wrap, orient="vertical", command=self.gallery_canvas.yview)
        self.gallery_canvas.configure(yscrollcommand=vbar.set)
        self.gallery_canvas.pack(side="left", fill="both", expand=True)
        vbar.pack(side="left", fill="y")
        self.gallery_inner = ttk.Frame(self.gallery_canvas)
        self.gallery_canvas.create_window((0, 0), window=self.gallery_inner, anchor="nw")
        self.gallery_inner.bind(
            "<Configure>",
            lambda e: self.gallery_canvas.configure(scrollregion=self.gallery_canvas.bbox("all")),
        )
        return frame

    def _make_thumb_image(self, thumb: bytes | None):
        if not thumb:
            return None
        try:
            import io

            from PIL import Image, ImageTk

            image = Image.open(io.BytesIO(thumb))
            image.thumbnail((130, 130))
            return ImageTk.PhotoImage(image)
        except Exception:
            return None

    def _gallery_set_all(self, value: bool) -> None:
        for var, _item in self.gallery_cells:
            var.set(value)

    def _populate_gallery(self, items: list) -> None:
        for child in self.gallery_inner.winfo_children():
            child.destroy()
        self.gallery_cells = []
        self._thumb_refs = []
        tk, ttk = self.tk, self.ttk
        columns = 4
        for index, item in enumerate(items):
            row, col = divmod(index, columns)
            cell = ttk.Frame(self.gallery_inner, relief="groove", borderwidth=1)
            cell.grid(row=row, column=col, padx=4, pady=4, sticky="n")
            image = self._make_thumb_image(item.thumb)
            if image is not None:
                self._thumb_refs.append(image)
                thumb_label = ttk.Label(cell, image=image)
            else:
                thumb_label = ttk.Label(cell, text=f"[{item.kind}]", width=16, anchor="center")
            thumb_label.pack(padx=2, pady=2)
            var = tk.BooleanVar(value=False)
            caption = f"{item.kind}  #{item.id}"
            ttk.Checkbutton(cell, text=caption, variable=var).pack()
            self.gallery_cells.append((var, item))
        if not items:
            ttk.Label(self.gallery_inner, text="没有找到媒体。", foreground="#777").grid(row=0, column=0, padx=8, pady=8)

    def on_load_gallery(self) -> None:
        if self._selected_view_account() != "default":
            self.messagebox.showinfo("媒体浏览", "此入口尚未适配多账号下载，请先切回原账号。")
            return
        target = self._selected_target()
        if not target:
            self.messagebox.showwarning("未选择", "请先在上面列表里选一个群组/频道,或手动输入。")
            return
        limit_raw = self.g_limit.get().strip()
        types_raw = self._types_string()

        async def coro():
            await self._ensure_client()
            entity = await resolve_channel(self.client, target)
            gallery_title = (
                getattr(entity, "title", None) or getattr(entity, "username", None) or str(target)
            )
            print(f"正在加载『{gallery_title}』的媒体缩略图…")
            items = await list_media(
                self.client,
                entity,
                limit=int(limit_raw) if limit_raw else 60,
                types=set(types_raw.split(",")) if types_raw else None,
            )
            def apply():
                if self._selected_view_account() == "default":
                    self._gallery_entity = entity
                    self._gallery_title = gallery_title
                    self._populate_gallery(items)
            self.root.after(0, apply)
            print(f"已加载 {len(items)} 个媒体。勾选要保存的,点『保存所选』。")

        self._run_task(coro(), self.load_gallery_btn)

    def on_save_selected(self) -> None:
        if self._selected_view_account() != "default":
            self.messagebox.showinfo("保存媒体", "请先切回原账号；不会使用 A 账号替 B 下载。")
            return
        if self._download_future is not None and not self._download_future.done():
            self._log("[下载] 已有下载任务在执行，请先完成或取消当前任务。\n")
            return
        selected = [item for var, item in self.gallery_cells if var.get()]
        if not selected:
            self.messagebox.showwarning("未选择", "请先勾选要保存的图片/视频。")
            return
        try:
            concurrency, retries, chunk_concurrency = self._media_options()
        except ValueError as exc:
            self.messagebox.showwarning("设置错误", str(exc))
            return

        self._stop_download.clear()
        entity = self._gallery_entity
        title = self._gallery_title

        async def coro():
            self._download_task = asyncio.current_task()
            try:
                await self._ensure_client()
                await save_items(
                    self.client,
                    selected,
                    download_dir=self.settings.download_dir,
                    title=title or "gallery",
                    entity=entity,
                    concurrency=concurrency,
                    retries=retries,
                    chunk_concurrency=chunk_concurrency,
                    temp_download_dir=self.settings.temp_download_dir,
                    on_progress=self._update_download_progress,
                )
                print("保存完成。点『打开下载文件夹』查看。\n")
            finally:
                self._download_task = None

        self.save_sel_btn.config(state="disabled")
        future = self.loop.submit(coro())
        self._download_future = future

        def finished(fut: concurrent.futures.Future) -> None:
            try:
                fut.result()
            except concurrent.futures.CancelledError:
                self._log("[下载] 已取消浏览媒体中选定的下载任务\n")
            except Exception as exc:
                self._log(f"[下载错误] 保存所选失败：{exc}\n")
            finally:
                def on_ui() -> None:
                    self.save_sel_btn.config(
                        state="normal" if self._selected_view_account() == "default" else "disabled"
                    )
                    if self._closing:
                        self._finish_close()
                self.root.after(0, on_ui)

        future.add_done_callback(finished)

    def _build_download_tab(self):
        ttk = self.ttk
        pad = {"padx": 6, "pady": 4}
        frame = ttk.Frame(self.root)
        types = ttk.Frame(frame)
        types.grid(row=0, column=0, columnspan=4, sticky="w", **pad)
        ttk.Label(types, text="下载类型:").pack(side="left")
        ttk.Checkbutton(types, text="图片", variable=self.type_photo).pack(side="left")
        ttk.Checkbutton(types, text="视频", variable=self.type_video).pack(side="left")
        ttk.Checkbutton(types, text="GIF", variable=self.type_anim).pack(side="left")
        ttk.Label(frame, text="数量上限(可空)").grid(row=1, column=0, sticky="w", **pad)
        ttk.Entry(frame, textvariable=self.dl_limit, width=10).grid(row=1, column=1, sticky="w", **pad)
        self.download_btn = ttk.Button(frame, text="下载所选", command=self.on_download)
        self.download_btn.grid(row=1, column=3, sticky="e", **pad)
        ttk.Label(
            frame, text="手动下载一次。自动任务请使用「多频道监控」。", foreground="#555",
        ).grid(row=2, column=0, columnspan=4, sticky="w", **pad)
        frame.columnconfigure(2, weight=1)
        return frame

    def _build_download_settings_tab(self):
        ttk = self.ttk
        pad = {"padx": 6, "pady": 4}
        frame = ttk.Frame(self.root)
        ttk.Label(frame, text="这些设置只应用于当前账号。", foreground="#555").grid(
            row=0, column=0, columnspan=4, sticky="w", **pad
        )
        for row, col, title, var in (
            (1, 0, "文件并发数 (1–8)", self.media_concurrency_var),
            (1, 2, "失败重试 (0–5)", self.media_retries_var),
            (2, 0, "单文件分块并发 (1–8)", self.chunk_concurrency_var),
            (2, 2, "监控总限速 (KiB/s，0=不限)", self.media_speed_kib_var),
        ):
            ttk.Label(frame, text=title).grid(row=row, column=col, sticky="w", **pad)
            ttk.Entry(frame, textvariable=var, width=10).grid(
                row=row, column=col + 1, sticky="w", **pad
            )
        ttk.Button(
            frame, text="应用并保存", command=self._apply_media_settings,
        ).grid(row=3, column=3, sticky="e", **pad)
        ttk.Label(
            frame, text="保存后监控下载逐步使用新设置；已开始的手动任务不受影响。",
            foreground="#555",
        ).grid(row=3, column=0, columnspan=3, sticky="w", **pad)
        frame.columnconfigure(0, weight=1, uniform="settings_labels")
        frame.columnconfigure(2, weight=1, uniform="settings_labels")
        return frame

    def _build_parse_tab(self):
        ttk = self.ttk
        pad = {"padx": 6, "pady": 4}
        frame = ttk.Frame(self.root)
        ttk.Label(frame, text="扫描消息里的链接:保留链接、下预览、直链、磁力/种子", foreground="#555").grid(
            row=0, column=0, columnspan=4, sticky="w", **pad
        )
        ttk.Label(frame, text="数量上限(可空)").grid(row=1, column=0, sticky="w", **pad)
        ttk.Entry(frame, textvariable=self.p_limit, width=10).grid(row=1, column=1, sticky="w", **pad)
        ttk.Checkbutton(frame, text="不下预览", variable=self.p_no_previews).grid(row=2, column=0, sticky="w", **pad)
        ttk.Checkbutton(frame, text="只存清单", variable=self.p_catalog_only).grid(row=2, column=1, sticky="w", **pad)
        ttk.Checkbutton(frame, text="不下磁力/种子", variable=self.p_no_bt).grid(row=2, column=2, sticky="w", **pad)
        ttk.Checkbutton(frame, text="试运行", variable=self.p_dry).grid(row=2, column=3, sticky="w", **pad)
        self.parse_btn = ttk.Button(frame, text="解析所选", command=self.on_parse)
        self.parse_btn.grid(row=3, column=3, sticky="e", **pad)
        frame.columnconfigure(2, weight=1)
        return frame

    # ---------- helpers ----------
    def _log_startup(self, text: str) -> None:
        owner = "default"
        self.log_queue.put(LogChunk(owner, f"[账号:{account_identity.label(owner)}] {text}"))

    def _log(self, text: str, *, owner_id: str | None = None) -> None:
        owner = current_account_id.get() if owner_id is None else owner_id
        line = self._timestamp_formatter.format(text, prefix=f"[账号:{account_identity.label(owner)}] ")
        self.log_queue.put(LogChunk(owner, line))
        store = getattr(self, "_log_store", None)
        if store is not None:
            store.write(line)

    def _log_download(self, text: str, *, owner_id: str | None = None) -> None:
        owner = current_account_id.get() if owner_id is None else owner_id
        line = self._download_timestamp_formatter.format(
            text + "\n", prefix=f"[账号:{account_identity.label(owner)}] "
        )
        self.download_queue.put(LogChunk(owner, line))
        store = getattr(self, "_log_store", None)
        if store is not None:
            store.write("[下载记录] " + line)

    def _log_account(self, account_id: str, text: str) -> None:
        self._log(text.rstrip() + "\n", owner_id=account_id)

    def _log_download_account(self, account_id: str, text: str) -> None:
        self._log_download(text, owner_id=account_id)

    def _add_main_account(self) -> None:
        if self._closing:
            return
        name = self.simpledialog.askstring(
            "添加 Telegram 账号", "输入用于区分此账号的备注", parent=self.root
        )
        if not name or not name.strip():
            self._refresh_main_account_choices()
            return
        try:
            account = self._account_registry.add(name)
            account_identity.register(account.id, account.name)
        except (ValueError, OSError) as exc:
            self.messagebox.showerror("添加失败", str(exc), parent=self.root)
            self._refresh_main_account_choices()
            return
        self._switch_main_account(account.id)
        self._log_account(account.id, "[账号] 已创建；使用「Telegram 登录」连接此账号")

    def _save_account_remark(self, account_id: str, remark: str) -> None:
        self._account_registry.rename(account_id, remark)
        if account_id != DEFAULT_ACCOUNT:
            account_identity.rename_fallback(account_id, remark)
        self._refresh_main_account_choices()
        self._refresh_log_accounts()

    def _selected_view_account(self) -> str:
        return getattr(self, "_view_account_id", "default")

    def _visible_monitors(self) -> list[dict]:
        return (self._monitored if self._selected_view_account() == "default"
                else getattr(self, "_view_monitored", []))

    def _visible_monitor_next(self):
        if self._selected_view_account() == "default":
            return self._monitor_next
        runtime = self._extra_runtimes.get(self._selected_view_account())
        return runtime._next_at if runtime is not None else {}

    def _visible_monitor_running(self) -> bool:
        if self._selected_view_account() == "default":
            return self._monitor_active
        runtime = self._extra_runtimes.get(self._selected_view_account())
        return bool(getattr(runtime, "monitoring", False))

    def _save_visible_monitors(self) -> None:
        account_id = self._selected_view_account()
        if account_id == "default":
            save_channels(self._monitored)
        else:
            path = self._account_registry.monitor_path(account_id)
            save_channels(self._view_monitored, path)
            self._view_monitor_stamp = path.stat().st_mtime_ns

    def _refresh_main_account_choices(self) -> None:
        from tgdl.account_view import account_choices
        choices = account_choices(self._account_registry.accounts())
        selected = self._selected_view_account()
        changed = choices != getattr(self, "_main_account_choices", {})
        self._main_account_choices = choices
        selector = getattr(self, "_main_account_selector", None)
        if selector is not None and changed:
            selector.configure(values=(*choices, _ADD_ACCOUNT_CHOICE))
        choice = getattr(self, "main_account_choice", None)
        if choice is not None:
            current = next((label for label, account_id in choices.items()
                            if account_id == selected), None)
            if current is not None and choice.get() != current:
                choice.set(current)

    def _switch_main_account(self, account_id: str | None = None) -> None:
        """Switch the visible account without changing any running client's identity."""
        if account_id is None:
            selection = self.main_account_choice.get()
            if selection == _ADD_ACCOUNT_CHOICE:
                self._add_main_account()
                return
            account_id = self._main_account_choices.get(selection)
        if account_id is None or account_id == self._selected_view_account():
            return
        if account_id not in {a.id for a in self._account_registry.accounts()}:
            return
        try:
            env = read_env_values(self._account_registry.env_path(account_id))
            if account_id != DEFAULT_ACCOUNT:
                monitor_path = self._account_registry.monitor_path(account_id)
                monitors = load_channels(monitor_path)
                stamp = monitor_path.stat().st_mtime_ns if monitor_path.exists() else None
        except (ValueError, OSError) as exc:
            self._refresh_main_account_choices()
            self.messagebox.showwarning("无法切换账号", str(exc))
            return
        self._view_account_id = account_id
        self._refresh_main_account_choices()
        setup = getattr(self, "_setup_frame", None)
        if setup is not None:
            if account_id == DEFAULT_ACCOUNT:
                setup.pack(before=self._login_frame, fill="x", padx=6, pady=4)
            else:
                setup.pack_forget()
        for name, var in (
            ("api_id", self.api_id_var), ("api_hash", self.api_hash_var),
            ("download_dir", self.download_dir_var),
            ("temp_download_dir", self.temp_download_dir_var),
            ("proxy_host", self.proxy_host_var), ("proxy_port", self.proxy_port_var),
            ("proxy_username", self.proxy_username_var),
            ("proxy_password", self.proxy_password_var),
            ("media_concurrency", self.media_concurrency_var),
            ("media_retries", self.media_retries_var),
            ("chunk_concurrency", self.chunk_concurrency_var),
            ("media_speed_kib", self.media_speed_kib_var),
        ):
            var.set(env.get(name, ""))
        self.manual_var.set("")
        self.dialog_search_var.set("")
        if account_id == "default":
            self._dialog_rows = getattr(self, "_default_dialog_rows", [])
        else:
            from tgdl.account_view import dialog_rows
            self._view_monitored = monitors
            self._view_monitor_stamp = stamp
            self._dialog_rows = dialog_rows(
                getattr(self, "_account_dialog_cache", {}).get(account_id, [])
            )
            if not self._dialog_rows:
                self._load_secondary_dialogs(account_id)
        self.tree.selection_remove(self.tree.selection())
        self._filter_dialog_tree()
        self.monitor_tree.selection_remove(self.monitor_tree.selection())
        self.monitor_tree.delete(*self.monitor_tree.get_children())
        self.monitor_minutes_var.set("15")
        self.monitor_priority_var.set("0")
        self.monitor_batch_var.set("100")
        self.monitor_direction_var.set("从新到旧")
        self._monitor_set_types("photo,video")
        self._monitor_refresh()
        # Only A's manual download/parse/gallery tasks have an independent
        # cancellation and lifecycle in the legacy main window. Never direct
        # those actions to account A while B is selected.
        safe_default = account_id == "default"
        for widget in (
            self.download_btn, self.load_gallery_btn, self.save_sel_btn,
            self.parse_btn,
        ):
            widget.configure(state="normal" if safe_default else "disabled")
        self._populate_gallery([])
        self._gallery_entity = None
        self._gallery_title = ""
        self._set_main_log_account_filter(account_id)
        self._refresh_log_views()
        self._paint_account_status(
            getattr(self, "_default_status_text", "未登录") if account_id == "default"
            else (self._extra_runtimes[account_id].status
                  if account_id in self._extra_runtimes else "未登录")
        )
        self._restore_saved_session(account_id)

    def _set_main_log_account_filter(self, account_id: str) -> None:
        self._refresh_log_accounts()
        key = next((label for label, owner in self._log_filter_options.items()
                    if owner == account_id), None)
        if key is not None:
            self.log_account_filter.set(key)

    def _load_secondary_dialogs(self, account_id: str, *, force: bool = False) -> None:
        """Fetch only for the account's actual client; reuse cached dialogs."""
        from tgdl.account_view import dialog_rows
        cache = getattr(self, "_account_dialog_cache", None)
        if cache is None:
            cache = {}
            self._account_dialog_cache = cache
        if not force and account_id in cache:
            if account_id == self._selected_view_account():
                self._dialog_rows = dialog_rows(cache[account_id])
                self._filter_dialog_tree()
            return
        pending = getattr(self, "_account_dialog_fetches", set())
        self._account_dialog_fetches = pending
        if account_id in pending:
            return
        runtime = self._extra_runtimes.get(account_id)
        if runtime is None or runtime.client is None or not runtime.status.startswith("已连接"):
            return
        pending.add(account_id)

        async def fetch():
            context_token = current_account_id.set(account_id)
            try:
                return await list_media_dialogs(runtime.client)
            finally:
                current_account_id.reset(context_token)

        future = self.loop.submit(fetch())
        def finished(done):
            try:
                items, error = done.result(), None
            except Exception as exc:
                items, error = None, exc
            def apply():
                pending.discard(account_id)
                if items is not None:
                    cache[account_id] = items
                    if self._selected_view_account() == account_id:
                        self._dialog_rows = dialog_rows(items)
                        self._filter_dialog_tree()
                elif error is not None:
                    self._log_account(account_id, f"[频道列表] 加载失败：{error}\n")
            self.root.after(0, apply)
        future.add_done_callback(finished)

    def _sync_secondary_monitor_settings(self, account_id: str) -> None:
        """Observe edits to this account's on-disk monitors without Telegram polls."""
        if account_id == "default":
            return
        path = self._account_registry.monitor_path(account_id)
        stamp = path.stat().st_mtime_ns if path.exists() else None
        if stamp == getattr(self, "_view_monitor_stamp", None):
            return
        self._view_monitored = load_channels(path)
        self._view_monitor_stamp = stamp
        self.monitor_tree.selection_remove(self.monitor_tree.selection())
        self._monitor_refresh()

    def _refresh_main_account_status(self) -> None:
        if getattr(self, "_closing", False):
            return
        self._refresh_main_account_choices()
        account_id = self._selected_view_account()
        from tgdl.account_view import account_choices
        expected_logs = {"全部账号": None}
        expected_logs.update(account_choices(self._account_registry.accounts()))
        if expected_logs != getattr(self, "_log_filter_options", {}):
            self._refresh_log_accounts()
        if account_id == DEFAULT_ACCOUNT:
            self._paint_account_status(getattr(self, "_default_status_text", "未登录"))
        else:
            runtime = self._extra_runtimes.get(account_id)
            status = runtime.status if runtime is not None else "未登录"
            count = runtime.active_count if runtime is not None else 0
            monitor = ("监控运行中" if getattr(runtime, "monitoring", False) else
                       "监控停止中" if getattr(runtime, "monitoring_draining", False) else "监控未启动")
            if status.startswith("已连接"):
                status = f"{status}｜{monitor}｜下载中 {count} 个"
            self._paint_account_status(status)
            cache = getattr(self, "_account_dialog_cache", {})
            if (runtime is not None and runtime.status.startswith("已连接")
                    and account_id not in cache):
                # Retry with backoff if the network fails; do not poll Telegram.
                if not hasattr(self, "_account_dialog_retry_at"):
                    self._account_dialog_retry_at = {}
                if time.monotonic() >= self._account_dialog_retry_at.get(account_id, 0):
                    self._account_dialog_retry_at[account_id] = time.monotonic() + 60
                    self._load_secondary_dialogs(account_id)
        if account_id != "default":
            from tgdl.account_view import dialog_rows
            if not self._dialog_rows and account_id in getattr(self, "_account_dialog_cache", {}):
                self._dialog_rows = dialog_rows(self._account_dialog_cache[account_id])
                self._filter_dialog_tree()
            self._sync_secondary_monitor_settings(account_id)
            self._monitor_refresh()
        else:
            self._refresh_monitor_button()
        self.root.after(1000, self._refresh_main_account_status)

    def _refresh_log_accounts(self) -> None:
        from tgdl.account_view import account_choices
        registry = getattr(self, "_account_registry", None)
        choices = {"全部账号": None}
        if registry is not None:
            choices.update(account_choices(registry.accounts()))
        current = getattr(self, "log_account_filter", None)
        selected_id = (
            self._log_filter_options.get(current.get())
            if current is not None else None
        )
        self._log_filter_options = choices
        if current is not None and selected_id is not None:
            new_value = next((name for name, account in choices.items()
                              if account == selected_id), "全部账号")
            current.set(new_value)
        selector = getattr(self, "_log_filter_selector", None)
        if selector is not None:
            selector.configure(values=list(choices))
        var = getattr(self, "log_account_filter", None)
        if var is not None and var.get() not in choices:
            var.set("全部账号")

    def _log_visible(self, chunk: LogChunk) -> bool:
        var = getattr(self, "log_account_filter", None)
        selected = self._log_filter_options.get(var.get()) if var is not None else None
        return selected is None or selected == chunk.owner_id

    def _refresh_log_views(self) -> None:
        for widget, records, popup in (
            (self.log, getattr(self, "_run_log_records", ()), self._log_popup_text),
            (self.download_log, getattr(self, "_download_log_records", ()), self._download_popup_text),
        ):
            visible = "".join(chunk.text for chunk in records if self._log_visible(chunk))
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.insert("end", visible)
            widget.configure(state="disabled")
            if popup is not None:
                popup.configure(state="normal")
                popup.delete("1.0", "end")
                popup.insert("end", visible)
                popup.configure(state="disabled")

    def _update_download_progress(self, key: str, label: str, done: int, total: int, status: str) -> None:
        """Called from background downloads; only enqueue, never touch Tk."""
        self.progress_queue.put((key, label, done, total, status))

    def _monitor_progress_state(self, peer_id: int, message_id: int, status: str) -> None:
        """Publish a worker pause/resume state to Tk without resetting bytes."""
        self._update_download_progress(
            f"__monitor_state__:{peer_id}:{message_id}", "", 0, 0, status
        )

    def _drain_progress(self) -> None:
        now = time.monotonic()
        latest = {}
        states = getattr(self, "_monitor_progress_states", None)
        if states is None:
            states = {}
            self._monitor_progress_states = states
        try:
            while True:
                event = self.progress_queue.get_nowait()
                if event[0].startswith("__monitor_state__:"):
                    prefix = event[0][len("__monitor_state__:"):] + ":"
                    if event[4] == "结束":
                        states.pop(prefix, None)
                    else:
                        states[prefix] = event[4]
                    continue
                if event[4] == "下载中":
                    prefix = ":".join(event[0].split(":", 2)[:2]) + ":"
                    if states.get(prefix) == "恢复中":
                        states.pop(prefix, None)
                latest[event[0]] = event
        except queue.Empty:
            pass
        for key, label, done, total, status in latest.values():
            old = self._progress_entries.get(key)
            if old is None:
                old = {"last_bytes": 0, "last_time": now, "speed": 0.0}
                self._progress_entries[key] = old
            interval = max(now - old["last_time"], 0.001)
            if status == "下载中" and done >= old["last_bytes"]:
                measured = (done - old["last_bytes"]) / interval
                old["speed"] = measured if old["speed"] == 0 else old["speed"] * 0.5 + measured * 0.5
            elif status in ("普通下载回退", "重试等待", "等待缓存", "准备文件"):
                old["speed"] = 0.0
            old.update(label=label, done=done, total=total, status=status,
                       last_bytes=done, last_time=now)
            if status in ("完成", "失败", "限流等待", "已取消"):
                old["expires"] = now + 5
            else:
                old.pop("expires", None)
        running, paused = [], []
        for key, entry in list(self._progress_entries.items()):
            if entry.get("expires", float("inf")) <= now:
                self._progress_entries.pop(key)
                if self.progress_tree.exists(key):
                    self.progress_tree.delete(key)
                continue
            if progress_owner(key, getattr(self, "_extra_runtimes", {})) != self._selected_view_account():
                if self.progress_tree.exists(key):
                    self.progress_tree.delete(key)
                continue
            display_status = next(
                (status for prefix, status in states.items() if key.startswith(prefix)),
                entry["status"],
            )
            total = entry["total"]
            done = entry["done"]
            ratio = min(1.0, done / total) if total else 0
            cells = int(ratio * 10)
            bar = "━" * cells + "─" * (10 - cells)
            portion = f"{done / 1048576:.1f}/{total / 1048576:.1f} MiB  {ratio:.0%}" if total else f"{done / 1048576:.1f} MiB"
            speed = (
                f"{entry['speed'] / 1048576:.2f} MiB/s"
                if display_status == "下载中" and now - entry["last_time"] < 3
                else "—"
            )
            values = (entry["label"], f"{bar} {portion}", speed, display_status)
            if self.progress_tree.exists(key):
                self.progress_tree.item(key, values=values)
            else:
                self.progress_tree.insert("", "end", iid=key, values=values)
            if display_status == "已暂停":
                paused.append(entry)
            elif display_status not in ("完成", "失败", "限流等待", "已取消"):
                running.append(entry)
        self.progress_summary.set(
            f"正在下载 {len(running)} 个文件"
            + (f"，已暂停 {len(paused)} 个" if paused else "")
            + f"，合计速度 {sum(v['speed'] for v in running if v['status'] == '下载中' and now - v['last_time'] < 3) / 1048576:.2f} MiB/s"
            if running or paused else "当前没有正在下载的文件"
        )

    def _drain_log(self) -> None:
        self._drain_progress()
        store = getattr(self, "_log_store", None)
        if (store is not None and store.failed.is_set()
                and not getattr(self, "_log_write_warning", False)):
            self._log_write_warning = True
            self._log("[日志] 无法继续保存运行日志，请检查日志目录权限或磁盘空间。\n")
        try:
            while True:
                chunk = self.log_queue.get_nowait()
                records = getattr(self, "_run_log_records", None)
                if records is not None:
                    records.append(chunk)
                if self._log_visible(chunk):
                    self._append_to_log(self.log, chunk.text)
                    if self._log_popup_text is not None:
                        self._append_to_log(self._log_popup_text, chunk.text)
        except queue.Empty:
            pass
        try:
            while True:
                chunk = self.download_queue.get_nowait()
                records = getattr(self, "_download_log_records", None)
                if records is not None:
                    records.append(chunk)
                if self._log_visible(chunk):
                    self._append_to_log(self.download_log, chunk.text)
                    if self._download_popup_text is not None:
                        self._append_to_log(self._download_popup_text, chunk.text)
        except queue.Empty:
            pass
        self.root.after(100, self._drain_log)

    @staticmethod
    def _append_to_log(widget, chunk: str) -> None:
        follow = widget.yview()[1] >= 0.999
        widget.configure(state="normal")
        widget.insert("end", chunk)
        # Tk text widgets otherwise retain unlimited lines during long downloads.
        if hasattr(widget, "index") and hasattr(widget, "delete"):
            try:
                lines = int(widget.index("end-1c").split(".", 1)[0])
                if lines > 6000:
                    widget.delete("1.0", f"{lines - 5000}.0")
            except (ValueError, TypeError):
                pass
        if follow:
            widget.see("end")
        widget.configure(state="disabled")

    def _open_log_window(self) -> None:
        """Show complete live log even if the main window is short."""
        if self._log_popup is not None and self._log_popup.winfo_exists():
            self._log_popup.lift()
            return
        win = self.tk.Toplevel(self.root)
        win.title("Telegram 下载器 - 实时日志")
        win.geometry("850x480")
        view = self.scrolledtext.ScrolledText(win, wrap="word", state="normal")
        view.pack(fill="both", expand=True, padx=8, pady=8)
        self.ttk.Button(win, text="跳转最新", command=lambda: view.see("end")).pack(anchor="e", padx=8, pady=4)
        view.insert("1.0", self.log.get("1.0", "end-1c"))
        view.see("end")
        view.configure(state="disabled")
        self._log_popup = win
        self._log_popup_text = view

        def on_close() -> None:
            self._log_popup_text = None
            self._log_popup = None
            win.destroy()

        win.protocol("WM_DELETE_WINDOW", on_close)

    def _open_download_window(self) -> None:
        if self._download_popup is not None and self._download_popup.winfo_exists():
            self._download_popup.lift()
            return
        win = self.tk.Toplevel(self.root)
        win.title("Telegram 下载器 - 下载记录")
        win.geometry("850x480")
        view = self.scrolledtext.ScrolledText(win, wrap="word", state="normal")
        view.pack(fill="both", expand=True, padx=8, pady=8)
        self.ttk.Button(win, text="跳转最新", command=lambda: view.see("end")).pack(anchor="e", padx=8, pady=4)
        view.insert("1.0", self.download_log.get("1.0", "end-1c"))
        view.see("end")
        view.configure(state="disabled")
        self._download_popup = win
        self._download_popup_text = view

        def on_close() -> None:
            self._download_popup_text = None
            self._download_popup = None
            win.destroy()

        win.protocol("WM_DELETE_WINDOW", on_close)

    def _paint_account_status(self, status: str) -> None:
        color = "#546174"
        if status.startswith(("已登录", "已连接")):
            color = "#13794d"
        elif status in ("登录已失效", "会话与账号不匹配"):
            color = "#b62d32"
            status += "｜请重新登录"
        elif status == "恢复登录失败":
            color = "#a05a14"
            status += "｜请检查此账号的网络、代理或设置"
        self.status_var.set(status)
        if (label := getattr(self, "_account_status_label", None)) is not None:
            label.configure(foreground=color)

    def _set_status(self, text: str) -> None:
        def apply():
            self._default_status_text = text
            if self._selected_view_account() == "default":
                self._paint_account_status(text)
        self.root.after(0, apply)

    def _current_download_dir(self) -> Path:
        raw = self.download_dir_var.get()
        if not raw:
            return default_download_dir()
        path = Path(raw).expanduser()
        return path if path.is_absolute() else data_dir() / path

    def _media_options(self) -> tuple[int, int, int]:
        # Pending edits must not affect a scheduled/manual download until
        # the user explicitly commits all fields with the Apply button.
        workers, retries, chunks, _rate = self._applied_media_settings
        return workers, retries, chunks

    def _selected_target(self) -> str | None:
        sel = self.tree.selection()
        if sel:
            values = self.tree.item(sel[0], "values")
            if values:
                return str(values[0])
        manual = self.manual_var.get().strip()
        return manual or None

    def _show_login_prompt(self, prompt: str, secret: bool, submit):
        # Do not use simpledialog.askstring: its nested Tk event loop is
        # difficult to dismiss safely when another account or shutdown cancels.
        win = self.tk.Toplevel(self.root)
        win.title("Telegram")
        win.transient(self.root)
        win.resizable(False, False)
        body = self.ttk.Frame(win, padding=14)
        body.pack(fill="both", expand=True)
        self.ttk.Label(body, text=prompt, wraplength=420).pack(anchor="w", pady=(0, 8))
        entry = self.ttk.Entry(body, show="*" if secret else "", width=36)
        entry.pack(fill="x", pady=(0, 8))
        buttons = self.ttk.Frame(body)
        buttons.pack(anchor="e")

        def accept(_event=None):
            value = entry.get()
            entry.delete(0, "end")
            submit(value)

        def cancel(_event=None):
            entry.delete(0, "end")
            submit(None)

        self.ttk.Button(buttons, text="取消", command=cancel).pack(side="left", padx=4)
        self.ttk.Button(buttons, text="确定", command=accept).pack(side="left")
        win.bind("<Return>", accept)
        win.bind("<Escape>", cancel)
        win.protocol("WM_DELETE_WINDOW", cancel)

        def activate():
            if win.winfo_exists():
                win.grab_set()
                entry.focus_set()

        win.after_idle(activate)
        return win

    async def _ask(self, prompt: str, secret: bool = False) -> str:
        # The dialog lives on Tk's thread. Only the account waiting for input
        # suspends, and cancellation dismisses the dialog before the next opens.
        async with self._prompt_lock:
            if self._closing:
                raise asyncio.CancelledError()
            answer: concurrent.futures.Future[str | None] = concurrent.futures.Future()
            window = None

            def dismiss() -> None:
                if window is not None and window.winfo_exists():
                    window.destroy()

            def finish(value: str | None) -> None:
                dismiss()
                try:
                    answer.set_result(value)
                except concurrent.futures.InvalidStateError:
                    pass  # cancelled on the other thread

            def ask_on_ui() -> None:
                nonlocal window
                if answer.done() or self._closing:
                    finish(None)
                    return
                try:
                    window = self._show_login_prompt(prompt, secret, finish)
                except Exception as exc:
                    try:
                        answer.set_exception(exc)
                    except concurrent.futures.InvalidStateError:
                        pass

            self.root.after(0, ask_on_ui)
            try:
                value = await asyncio.wrap_future(answer)
            except asyncio.CancelledError:
                answer.cancel()
                self.root.after(0, dismiss)
                raise
            if value is None:
                raise RuntimeError("已取消")
            return value if secret else value.strip()

    def _run_task(self, coro, button) -> None:
        if button is not None:
            button.config(state="disabled")
        future = self.loop.submit(coro)

        def finished(fut) -> None:
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001
                self._log(f"\n[错误] {exc}\n")
            finally:
                if button is not None:
                    def restore():
                        legacy_only = button in (
                            self.load_gallery_btn, self.parse_btn, self.download_btn,
                            self.save_sel_btn,
                        )
                        state = ("disabled" if legacy_only and
                                 self._selected_view_account() != "default" else "normal")
                        button.config(state=state)
                    self.root.after(0, restore)

        future.add_done_callback(finished)

    def _save_dir_only(self) -> None:
        account_id = self._selected_view_account()
        path = self._account_registry.env_path(account_id)
        values = read_env_values(path)
        values["download_dir"] = self.download_dir_var.get() or str(default_download_dir())
        update_env(values, path)
        self._apply_storage_paths(account_id, values)

    def _apply_storage_paths(self, account_id: str, values: dict[str, str]) -> None:
        directory = self._cleanup_path(values["download_dir"]) or default_download_dir()
        staging = self._cleanup_path(values["temp_download_dir"])
        if account_id == DEFAULT_ACCOUNT:
            if self.settings is not None:
                from dataclasses import replace

                self.settings = replace(
                    self.settings, download_dir=directory, temp_download_dir=staging,
                )
        else:
            runtime = self._extra_runtimes.get(account_id)
            if set_paths := getattr(runtime, "set_download_paths", None):
                set_paths(directory, staging)

    # ---------- client ----------
    async def _ensure_client(self, *, authenticate=None):
        if self.client is not None:
            return self.client
        async with self._client_lock:
            if self.client is not None:
                return self.client
            if self._closing:
                raise asyncio.CancelledError()

            # Initialization can yield to other workers while the user is
            # entering a code. Only publish a fully authorized client.
            self._client_starting_task = asyncio.current_task()
            try:
                if authenticate is None:
                    try:
                        saved = self._saved_session_file(DEFAULT_ACCOUNT).stat()
                    except (FileNotFoundError, NotADirectoryError):
                        raise _SavedSessionMissing("没有旧会话，请点击主窗口的「Telegram 登录」。") from None
                    if not stat.S_ISREG(saved.st_mode):
                        raise _SavedSessionMissing("没有旧会话，请点击主窗口的「Telegram 登录」。")
                    if not saved.st_size:
                        raise _SavedSessionExpired("旧会话没有授权")
                    settings = load_settings(
                        env_path=self._account_registry.env_path(DEFAULT_ACCOUNT),
                        session_path=self._account_registry.session_path(DEFAULT_ACCOUNT),
                    )
                else:
                    settings = load_settings()
                client = build_client(settings)
                try:
                    if authenticate is None and not client.session.auth_key:
                        raise _SavedSessionExpired("旧会话没有授权")
                    if authenticate is not None:
                        me = await authenticate(client, settings.phone)
                    else:
                        # Normal handlers may reuse stored authorization, but
                        # only the explicit Login action opens an auth dialog.
                        if not client.is_connected():
                            await client.connect()
                        me = await client.get_me()
                        if me is None:
                            raise _SavedSessionExpired("登录已失效，请点击主窗口的「Telegram 登录」。")
                        if settings.phone and getattr(me, "phone", None):
                            if validate_login_phone(settings.phone) != validate_login_phone(me.phone):
                                raise _SavedSessionMismatch("当前会话的 Telegram 号码与设置不符。")
                except (asyncio.CancelledError, Exception):
                    self._client_starting_cleanup = True
                    await client.disconnect()
                    raise
                self.settings = settings
                self.client = client
            finally:
                self._client_starting_task = None
                self._client_starting_cleanup = False

            name = " ".join(p for p in (me.first_name, me.last_name) if p)
            handle = f"@{me.username}" if me.username else ""
            account_identity.update("default", me)
            self.root.after(0, self._refresh_log_accounts)
            print(f"已登录:{name} {handle}".strip())
            self._set_status(f"已登录:{name} {handle}".strip())
            return client

    async def client_start(self, client, settings):
        async def ask_phone() -> str:
            return validate_login_phone(await self._ask("手机号(带国际区号,如 +8613800138000):"))

        await client.start(
            phone=validate_login_phone(settings.phone) if settings.phone else ask_phone,
            code_callback=lambda: self._ask("Telegram 发来的验证码:"),
            password=lambda: self._ask("两步验证密码:", secret=True),
        )

    # ---------- handlers ----------
    def on_save_creds(self) -> None:
        if self._selected_view_account() != "default":
            self.messagebox.showinfo("首次设置", "请切回原账号，或在当前账号的设置中修改 API 凭据。")
            return
        api_id = self.api_id_var.get().strip()
        api_hash = self.api_hash_var.get().strip()
        if not api_id or not api_hash:
            self.messagebox.showwarning("缺少信息", "请先填写 api_id 和 api_hash。")
            return
        path = self._account_registry.env_path("default")
        values = read_env_values(path)
        values.update(api_id=api_id, api_hash=api_hash)
        update_env(values, path)
        self.creds_available = True
        if getattr(self, "_setup_frame", None) is not None:
            self._setup_frame.destroy()
            self._setup_frame = None
        self._log("已保存凭据。现在点「Telegram 登录」。\n")

    def _saved_session_file(self, account_id: str) -> Path:
        # AccountRegistry provides a stem; Telethon's SQLite backend appends .session.
        return self._account_registry.session_path(account_id).with_suffix(".session")

    def _check_saved_sessions_at_startup(self) -> None:
        if self._closing:
            return
        for account in self._account_registry.accounts():
            self._restore_saved_session(account.id)

    def _restore_saved_session(self, account_id: str) -> bool:
        """Reuse only the selected account's existing authorization; never show an auth dialog."""
        if getattr(self, "_closing", False):
            return False
        if account_id == DEFAULT_ACCOUNT:
            if getattr(self, "client", None) is not None:
                return False
            pending = getattr(self, "_default_login_future", None)
        else:
            runtime = self._extra_runtimes.get(account_id)
            if (runtime is not None and runtime.client is not None
                    and runtime.status.startswith("已连接")):
                return False
            pending = getattr(self, "_extra_futures", {}).get(account_id)
        if pending is not None and not pending.done():
            return True
        try:
            if not self._saved_session_file(account_id).is_file():
                return False
        except OSError as exc:
            self._log_account(account_id, f"[会话恢复] 无法读取会话文件：{type(exc).__name__}")
            return False

        if account_id == DEFAULT_ACCOUNT:
            async def restore_default():
                if self._closing:
                    return
                self._default_login_task = asyncio.current_task()
                try:
                    self._set_status("正在恢复登录")
                    await self._ensure_client()
                    try:
                        await self._load_dialogs()
                    except Exception as exc:
                        self._log_account(account_id, f"[频道列表] 加载失败：{type(exc).__name__}")
                except _SavedSessionMissing:
                    self._set_status("未登录")
                except (_SavedSessionExpired, UnauthorizedError, AuthKeyDuplicatedError):
                    self._set_status("登录已失效")
                except _SavedSessionMismatch:
                    self._set_status("会话与账号不匹配")
                except Exception as exc:
                    self._set_status("恢复登录失败")
                    self._log_account(account_id, f"[会话恢复] 连接失败：{type(exc).__name__}")
                finally:
                    self._default_login_task = None

            self._default_login_future = self.loop.submit(restore_default())
            return True

        from tgdl.multi_runtime import AccountMonitorRuntime

        account = next((item for item in self._account_registry.accounts()
                        if item.id == account_id), None)
        if account is None:
            return False

        def connected():
            if (not self._closing and self._extra_runtimes.get(account_id) is runtime
                    and self._selected_view_account() == account_id):
                self._load_secondary_dialogs(account_id)

        runtime = AccountMonitorRuntime(
            account, self._account_registry, ask=self._ask,
            on_log=self._log_account, on_saved=self._log_download_account,
            on_progress=self._update_download_progress, restore_session_only=True,
            on_authenticated=lambda: self.root.after(0, connected),
        )
        self._extra_runtimes[account_id] = runtime
        self._extra_futures[account_id] = self.loop.submit(runtime.run())
        return True

    def on_login(self) -> None:
        self._open_account_login(self._selected_view_account())

    def _open_account_login(self, account_id: str) -> None:
        """Open the same pinned login dialog for a selected account."""
        from tgdl.multi_runtime import AccountMonitorRuntime

        if self._closing:
            return
        existing = self._login_dialog
        if existing is not None:
            existing.win.lift()
            return

        account = next((item for item in self._account_registry.accounts() if item.id == account_id), None)
        if account is None:
            return
        if account_id == "default" and self.client is not None:
            self._run_task(self._load_dialogs(), self.login_btn)
            return
        if account_id != "default":
            runtime = self._extra_runtimes.get(account_id)
            if (runtime is not None and runtime.client is not None
                    and runtime.status.startswith("已连接")):
                self._load_secondary_dialogs(account_id, force=True)
                return
            pending = self._extra_futures.get(account_id)
        else:
            pending = self._default_login_future
        if pending is not None and not pending.done():
            self.messagebox.showinfo("登录 Telegram", "该账号正在连接或清理上一次连接，请稍候。")
            return

        path = self._account_registry.env_path(account_id)
        values = read_env_values(path)
        if resolve_api_credentials({
            "TELEGRAM_API_ID": values["api_id"], "TELEGRAM_API_HASH": values["api_hash"],
        }) is None:
            if account_id == "default" and self.api_id_var.get().strip() and self.api_hash_var.get().strip():
                self.on_save_creds()
            else:
                self.messagebox.showwarning("缺少信息", "请先在当前账号的设置中填写 API ID 和 API Hash。")
                return

        def remember(phone: str) -> None:
            try:
                current = read_env_values(path)
                current["phone"] = phone
                update_env(current, path)
            except (OSError, ValueError):
                self._log_account(account_id, "[登录] 登录成功，但未能保存手机号。")

        def closed(dialog) -> None:
            if self._login_dialog is dialog:
                self._login_dialog = None

        dialog = TelegramLoginDialog(
            self, account_identity.label(account_id),
            on_close=closed, remember_phone=remember,
        )
        self._login_dialog = dialog

        def connected() -> None:
            if self._login_dialog is dialog:
                dialog.done()
            if not self._closing and account_id != "default":
                self._load_secondary_dialogs(account_id)

        if account_id == "default":
            async def start_default():
                if self._closing:
                    return
                self._default_login_task = asyncio.current_task()
                try:
                    await self._ensure_client(authenticate=dialog.flow.authenticate)
                    self.root.after(0, connected)
                    try:
                        await self._load_dialogs()
                    except Exception as exc:
                        self._log_account(account_id, f"[频道列表] 加载失败：{type(exc).__name__}")
                finally:
                    self._default_login_task = None

            future = self.loop.submit(start_default())
            self._default_login_future = future
        else:
            runtime = AccountMonitorRuntime(
                account, self._account_registry, ask=self._ask,
                on_log=self._log_account, on_saved=self._log_download_account,
                on_progress=self._update_download_progress,
                authenticate=dialog.flow.authenticate,
                on_authenticated=lambda: self.root.after(0, connected),
            )
            self._extra_runtimes[account_id] = runtime
            future = self.loop.submit(runtime.run())
            self._extra_futures[account_id] = future

        def finished(done) -> None:
            try:
                done.result()
            except concurrent.futures.CancelledError:
                pass
            except Exception as exc:
                self._log_account(account_id, f"[登录] 启动失败：{type(exc).__name__}")

            def apply() -> None:
                if self._login_dialog is dialog:
                    dialog.done()
                    if not self._closing:
                        self.messagebox.showwarning(
                            "登录未完成", "请检查当前账号设置或日志，然后重试登录。",
                        )
            if not self._closing:
                self.root.after(0, apply)

        future.add_done_callback(finished)

    def on_list(self) -> None:
        account_id = self._selected_view_account()
        if account_id != "default":
            runtime = self._extra_runtimes.get(account_id)
            if runtime is None or runtime.client is None or not runtime.status.startswith("已连接"):
                if self._restore_saved_session(account_id):
                    return
                self.messagebox.showinfo("当前账号未连接", "请先使用主窗口的「Telegram 登录」。")
                return
            self._load_secondary_dialogs(account_id, force=True)
            return
        async def coro():
            await self._ensure_client()
            await self._load_dialogs()

        self._run_task(coro(), self.refresh_btn)

    async def _load_dialogs(self) -> None:
        items = await list_media_dialogs(self.client)
        rows = [
            (str(it.entity_id), it.kind, it.title, it.username or "")
            for it in items
        ]

        def apply():
            self._default_dialog_rows = rows
            if self._selected_view_account() == "default":
                self._dialog_rows = rows
                self._filter_dialog_tree()
            names = {str(row[0]): str(row[2]) for row in rows if row[2]}
            changed = False
            for item in self._monitored:
                title = names.get(str(item["target"]))
                if title and title != item.get("name"):
                    item["name"] = title
                    changed = True
            if changed:
                save_channels(self._monitored)
                self._monitor_refresh()

        self.root.after(0, apply)
        print(f"已加载 {len(rows)} 个群组/频道。点选一个,然后『下载所选』。")

    def _filter_dialog_tree(self) -> None:
        rows = getattr(self, "_dialog_rows", [])
        keyword = self.dialog_search_var.get()
        selected = self.tree.selection()
        chosen = str(self.tree.item(selected[0], "values")[0]) if selected else None
        visible = filter_dialogs(rows, keyword)
        self.tree.delete(*self.tree.get_children())
        for row in visible:
            iid = str(row[0])
            self.tree.insert("", "end", iid=iid, values=row[:3])
            if iid == chosen:
                self.tree.selection_set(iid)
        self.dialog_search_count.set(f"{len(visible)} / {len(rows)}")

    def _types_string(self) -> str:
        return ",".join(
            name
            for name, var in (
                ("photo", self.type_photo),
                ("video", self.type_video),
                ("animation", self.type_anim),
            )
            if var.get()
        )

    def on_download(self) -> None:
        if self._selected_view_account() != "default":
            self.messagebox.showinfo("批量下载", "此入口尚未接入 B 账号，请使用 B 的频道监控。")
            return
        if self._download_future is not None and not self._download_future.done():
            self._log("[下载] 已有批量任务在执行，本次跳过。\n")
            return
        target = self._selected_target()
        if not target:
            self.messagebox.showwarning("未选择", "请先在列表里选一个群组/频道,或手动输入。")
            return
        types_raw = self._types_string()
        if not types_raw:
            self.messagebox.showwarning("未选择", "至少选择一种下载类型。")
            return
        limit = self.dl_limit.get().strip()
        try:
            concurrency, retries, chunk_concurrency = self._media_options()
        except ValueError as exc:
            self.messagebox.showwarning("设置错误", str(exc))
            return

        self._start_channel_download(
            target, types_raw, limit, concurrency, retries, chunk_concurrency
        )

    def _start_channel_download(
        self, target, types_raw, limit, concurrency, retries, chunk_concurrency,
        monitor_item=None,
    ) -> None:
        if self._closing:
            return
        if self._download_future is not None and not self._download_future.done():
            self._log("[定时] 上一次下载尚未结束，跳过本轮。\n")
            return

        self._stop_download.clear()

        def channel_resolved(title: str, peer_id: int) -> None:
            if monitor_item is not None:
                self.root.after(0, lambda: self._monitor_set_name(target, title, peer_id))

        async def coro():
            self._download_task = asyncio.current_task()
            try:
                await self._ensure_client()
                result = await download_channel(
                    self.client,
                    target=target,
                    download_dir=self.settings.download_dir,
                    temp_download_dir=self.settings.temp_download_dir,
                    types=parse_types(types_raw),
                    limit=int(limit) if limit else None,
                    since=None,
                    until=None,
                    skip_existing=True,
                    dry_run=False,
                    concurrency=concurrency,
                    retries=retries,
                    chunk_concurrency=chunk_concurrency,
                    on_saved=self._log_download,
                    on_progress=self._update_download_progress,
                    monitor_direction=monitor_item.get("direction", "newest") if monitor_item else None,
                    monitor_batch_size=100,
                    stop_event=self._stop_download,
                    on_channel_resolved=channel_resolved,
                )
                print("下载完成。点『打开下载文件夹』查看。\n")
                return result
            finally:
                self._download_task = None

        self.download_btn.config(state="disabled")
        future = self.loop.submit(coro())
        self._download_future = future

        def finished(fut: concurrent.futures.Future) -> None:
            try:
                result = fut.result()
                if monitor_item is not None and result.more_pending:
                    # Run the next bounded batch soon, after giving other channels a turn.
                    self._monitor_next[target] = time.time()
                if monitor_item is not None:
                    self.root.after(0, lambda: self._monitor_log_progress(target, result))
            except concurrent.futures.CancelledError:
                self._log("[下载] 已取消当前任务\n")
            except Exception as exc:
                self._log(f"[下载错误] {exc}\n")
            finally:
                def on_ui():
                    self.download_btn.config(
                        state="normal" if self._selected_view_account() == "default" else "disabled"
                    )
                    if self._closing:
                        self._finish_close()
                self.root.after(0, on_ui)

        future.add_done_callback(finished)

    def _monitor_log_progress(self, target: str, result, *, quota_reached: bool = False) -> None:
        """Accumulate slices silently; report only a completed scan round."""
        total = self._monitor_totals.setdefault(
            target, {"matched": 0, "queued": 0, "skipped": 0, "failed": 0,
                     "scanned_messages": 0, "batches": 0},
        )
        total["batches"] += 1
        for key in ("matched", "queued", "skipped", "failed", "scanned_messages"):
            total[key] += getattr(result, key, 0)
        if result.more_pending and not quota_reached:
            return
        channel = next((item for item in self._monitored if item["target"] == target), None)
        name = (channel.get("name") or target) if channel else target
        suffix = ""
        if quota_reached:
            limit = channel.get("batch_size", 100) if channel else 100
            suffix += f"｜达本轮上限{limit}，下轮续扫"
        if total["failed"]:
            suffix += f"｜失败{total['failed']}"
        self._log(
            f"[扫描] {name}：完成｜检查{total['scanned_messages']}条｜"
            f"匹配{total['matched']}｜新增{total['queued']}｜"
            f"跳过{total['skipped']}{suffix}\n"
        )
        self._monitor_totals.pop(target, None)

    def on_parse(self) -> None:
        if self._selected_view_account() != "default":
            self.messagebox.showinfo("解析链接", "此入口尚未接入 B 账号，请先切回原账号。")
            return
        target = self._selected_target()
        if not target:
            self.messagebox.showwarning("未选择", "请先在列表里选一个群组/频道,或手动输入。")
            return
        limit = self.p_limit.get().strip()
        opts = {
            "no_previews": self.p_no_previews.get(),
            "catalog_only": self.p_catalog_only.get(),
            "no_bt": self.p_no_bt.get(),
            "dry": self.p_dry.get(),
        }

        async def coro():
            await self._ensure_client()
            kwargs = dict(
                target=target,
                download_dir=self.settings.download_dir,
                limit=int(limit) if limit else None,
                since=None,
                until=None,
                keep_previews=not opts["no_previews"],
                catalog_only=opts["catalog_only"],
                download_torrents=not opts["no_bt"],
                show_sensitive_details=False,
                dry_run=opts["dry"],
            )
            await parse_channel(self.client, **kwargs)
            print("解析完成。\n")

        self._run_task(coro(), self.parse_btn)

    def _change_dir(self) -> None:
        from tkinter import filedialog

        account_id = self._selected_view_account()
        chosen = filedialog.askdirectory()
        if chosen and self._selected_view_account() == account_id:
            previous = self.download_dir_var.get()
            self.download_dir_var.set(chosen)
            try:
                self._save_dir_only()
            except (ValueError, OSError) as exc:
                self.download_dir_var.set(previous)
                self.messagebox.showwarning("无法保存下载目录", str(exc))

    def _open_folder(self) -> None:
        folder = self._current_download_dir()
        folder.mkdir(parents=True, exist_ok=True)
        try:
            if sys.platform.startswith("win"):
                os.startfile(str(folder))  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(folder)])
            else:
                subprocess.Popen(["xdg-open", str(folder)])
        except Exception as exc:  # noqa: BLE001
            self._log_account(self._selected_view_account(), f"[错误] 打不开文件夹:{exc}\n")

    def _show_centered_settings(self, window) -> None:
        """Position the completed form relative to the running app, not the desktop."""
        window.update_idletasks()
        parent = self.root
        left = parent.winfo_rootx() + (parent.winfo_width() - window.winfo_reqwidth()) // 2
        top = parent.winfo_rooty() + (parent.winfo_height() - window.winfo_reqheight()) // 2
        window.geometry(f"+{left}+{top}")
        window.deiconify()

    def _open_settings(self) -> None:
        settings_account_id = self._selected_view_account()
        account = next((row for row in self._account_registry.accounts()
                        if row.id == settings_account_id), None)
        if account is None:
            return
        tk, ttk = self.tk, self.ttk
        win = tk.Toplevel(self.root)
        win.withdraw()
        win.title(f"账号设置：{account_identity.label(settings_account_id)}")
        win.transient(self.root)
        pad = {"padx": 8, "pady": 6}
        path = self._account_registry.env_path(settings_account_id)
        original = read_env_values(path)
        fields = (
            ("api_id", "api_id", self.api_id_var, False),
            ("api_hash", "api_hash", self.api_hash_var, True),
            ("proxy_host", "SOCKS5 主机（留空则不用代理）", self.proxy_host_var, False),
            ("proxy_port", "SOCKS5 端口（例如 1080）", self.proxy_port_var, False),
            ("proxy_username", "代理用户名（可空）", self.proxy_username_var, False),
            ("proxy_password", "代理密码（可空）", self.proxy_password_var, True),
            ("download_dir", "下载目录", self.download_dir_var, False),
            ("temp_download_dir", "分块临时目录（建议 SSD，留空=下载目录）", self.temp_download_dir_var, False),
            ("phone", "手机号（选填，带国际区号）", None, False),
        )
        draft = {key: tk.StringVar(master=win, value=original[key])
                 for key, _label, _main_var, _secret in fields}
        remark_label = "本地备注（可留空）" if settings_account_id == DEFAULT_ACCOUNT else "本地备注"
        remark = tk.StringVar(master=win, value=account.name)
        ttk.Label(win, text=remark_label).grid(row=0, column=0, sticky="w", **pad)
        ttk.Entry(win, textvariable=remark, width=43).grid(row=0, column=1, sticky="we", **pad)
        for row, (key, label, _main_var, secret) in enumerate(fields, start=1):
            ttk.Label(win, text=label).grid(row=row, column=0, sticky="w", **pad)
            ttk.Entry(win, textvariable=draft[key], width=43, show="*" if secret else "").grid(
                row=row, column=1, sticky="we", **pad
            )
        ttk.Label(
            win,
            text="SSD 缓存：最大 20 GiB，至少保留 10 GiB 可用空间；满额排队等待，不回退机械盘",
            foreground="#555",
        ).grid(row=len(fields) + 1, column=0, columnspan=2, sticky="w", **pad)
        ttk.Button(win, text="检查并清理残留缓存", command=lambda: self._manual_cache_cleanup(
            draft["temp_download_dir"].get(), draft["download_dir"].get(),
            owner_id=settings_account_id,
        )).grid(
            row=len(fields) + 2, column=0, sticky="w", **pad
        )

        def save_and_close():
            if self._closing:
                return
            if self._selected_view_account() != settings_account_id:
                self.messagebox.showwarning(
                    "账号已切换", "设置窗口对应的账号已切换，请关闭窗口后重新打开。"
                )
                return
            try:
                new_remark = remark.get().strip()
                if settings_account_id != DEFAULT_ACCOUNT and not new_remark:
                    raise ValueError("附加账号的本地备注不能为空。")
                values = read_env_values(path)
                edits = {}
                for key, var in draft.items():
                    value = var.get()
                    if value != original[key]:
                        edits[key] = value if key in (
                            "proxy_username", "proxy_password", "download_dir", "temp_download_dir",
                        ) else value.strip()
                values.update(edits)
                if edits:
                    host, port = values["proxy_host"], values["proxy_port"]
                    if port and not host:
                        raise ValueError("填写端口时必须填写 SOCKS5 主机。")
                    if host and not 1 <= int(port or "1080") <= 65535:
                        raise ValueError("SOCKS5 端口必须在 1-65535 之间。")
                    update_env(values, path)
                if new_remark != account.name:
                    self._save_account_remark(settings_account_id, new_remark)
            except (ValueError, OSError) as exc:
                self.messagebox.showwarning("账号设置无效", str(exc), parent=win)
                return
            for key, _label, main_var, _secret in fields:
                if main_var is not None:
                    main_var.set(values[key])
            self._apply_storage_paths(settings_account_id, values)
            win.destroy()
            runtime = self._extra_runtimes.get(settings_account_id)
            connected = (self.client is not None if settings_account_id == DEFAULT_ACCOUNT
                         else runtime is not None and runtime.client is not None)
            network_fields = {"api_id", "api_hash", "phone", "proxy_host", "proxy_port",
                              "proxy_username", "proxy_password"}
            if connected and network_fields.intersection(edits):
                self.messagebox.showinfo("设置已保存", "此账号的登录 / SOCKS5 更改在重启下载器后生效。")
            self._log_account(settings_account_id, "设置已保存。\n")

        ttk.Button(win, text="保存", command=save_and_close).grid(
            row=len(fields) + 2, column=1, sticky="e", **pad
        )
        self._show_centered_settings(win)

    # ---------- lifecycle ----------
    @staticmethod
    def _cleanup_path(raw: str) -> Path | None:
        if not raw:
            return None
        path = Path(raw).expanduser()
        return path if path.is_absolute() else data_dir() / path

    def _manual_cache_cleanup(self, ssd_raw=None, hdd_raw=None, *, owner_id=None):
        owner = self._selected_view_account() if owner_id is None else owner_id
        if ssd_raw is None:
            ssd_raw = self.temp_download_dir_var.get()
        if hdd_raw is None:
            hdd_raw = self.download_dir_var.get()
        ssd = self._cleanup_path(ssd_raw)
        hdd = self._cleanup_path(hdd_raw)
        # Work outside Tk's event loop: scanning a large HDD folder can block.
        def worker():
            report = inspect_cleanup(ssd, hdd)
            def finish():
                self._log_account(owner, report.summary() + "\n")
                if report.legacy:
                    total = len(report.legacy)
                    for path in report.legacy:
                        self._log_account(owner, f"[缓存清理] 待确认：{path}\n")
                    confirmed = self.messagebox.askyesno(
                        "旧版临时文件",
                        f"发现 {total} 项没有所有权标记的旧临时文件。"
                        "\n请先确认旧版本已关闭，并检查运行日志中列出的路径。"
                        "\n\n仍要清理这些文件吗？",
                        detail="\n".join(str(path) for path in report.legacy[:8]),
                    )
                    if confirmed:
                        threading.Thread(
                            target=legacy_worker, args=(set(report.legacy),), daemon=True,
                        ).start()
                for error in report.failed:
                    self._log_account(owner, f"[缓存清理] {error}\n")
            self.root.after(0, finish)

        def legacy_worker(approved):
            result = inspect_cleanup(ssd, hdd, approved_legacy=approved)
            self.root.after(0, lambda: self._log_account(owner, result.summary() + "\n"))
        threading.Thread(target=worker, daemon=True).start()

    def _interrupt_primary_monitor(self) -> None:
        worker = getattr(self, "_monitor_worker_task", None)
        if worker is None or worker is getattr(self, "_interrupted_monitor_worker", None):
            return
        self._interrupted_monitor_worker = worker
        # Keep the cross-thread Future joinable until child cleanup finishes.
        worker.get_loop().call_soon_threadsafe(lambda: None if worker.done() else worker.cancel())

    def _cancel_current_download(self) -> None:
        if getattr(self, "_closing", False):
            return
        account_id = self._selected_view_account()
        if account_id != DEFAULT_ACCOUNT:
            runtime = self._extra_runtimes.get(account_id)
            if runtime is not None and runtime.active_count:
                runtime.cancel_transfers()
                self._log_account(account_id, "[监控] 正在取消当前下载并暂停监控；重新启动后继续未完成项目")
                self._monitor_refresh()
            else:
                self._log_account(account_id, "[下载] 当前没有可取消的下载任务")
            return
        self._stop_download.set()
        cancelled = False
        if self._download_future is not None and not self._download_future.done():
            self._download_future.cancel()
            self._log("[下载] 正在取消当前扫描及未完成文件；已成功的文件不会删除\n")
            cancelled = True
        future = getattr(self, "_monitor_download_future", None)
        worker_running = future is not None and not future.done()
        if worker_running and getattr(self, "_monitor_total_transfers", 0) > 0:
            self._stop_primary_monitor()
            self._interrupt_primary_monitor()
            self._log("[监控] 正在取消当前下载并暂停监控；重新启动后继续未完成项目\n")
            cancelled = True
        if not cancelled:
            self._log("[下载] 当前没有可取消的下载任务\n")

    def _finish_close(self) -> None:
        if self._download_future is not None and not self._download_future.done():
            return
        if getattr(self, "_shutdown_started", False):
            return
        self._shutdown_started = True

        async def disconnect():
            starting = getattr(self, "_client_starting_task", None)
            if starting is not None and not starting.done():
                # Task.cancelling() was added in Python 3.11. Also track
                # our own disconnect on older versions, to avoid a second cancel.
                if (not getattr(self, "_client_starting_cleanup", False)
                        and not getattr(starting, "cancelling", lambda: 0)()):
                    starting.cancel()
                await asyncio.gather(starting, return_exceptions=True)
            # The authentication lock is released before Telegram's channel
            # list loads; join the still-running explicit login handler too.
            login = getattr(self, "_default_login_task", None)
            if login is not None and not login.done():
                login.cancel()
                await asyncio.gather(login, return_exceptions=True)
            # Cancellation can settle the thread-safe Future before the real
            # asyncio download task has removed its temporary chunk files.
            active = self._download_task
            if active is not None and not active.done():
                try:
                    await asyncio.wait_for(asyncio.shield(active), timeout=30)
                except (asyncio.CancelledError, Exception):
                    pass
            # A cancelled concurrent.futures.Future can report done() while
            # its underlying asyncio task still cleans up temporary chunks.
            monitor_worker = getattr(self, "_monitor_worker_task", None)
            if monitor_worker is not None and not monitor_worker.done():
                try:
                    await asyncio.shield(monitor_worker)
                except (asyncio.CancelledError, Exception):
                    pass
            for future in (self._monitor_scan_future, self._monitor_download_future):
                if future is not None and not future.done():
                    try:
                        await asyncio.wrap_future(future)
                    except (asyncio.CancelledError, Exception):
                        pass
            # Secondary runtimes are interrupted through runtime.stop().
            # These Futures are never cancelled from Tk, so completion means
            # the async tasks have cleaned up and their clients disconnected.
            for extra in getattr(self, "_extra_futures", {}).values():
                if not extra.done():
                    try:
                        await asyncio.wrap_future(extra)
                    except (asyncio.CancelledError, Exception):
                        pass
            if self.client is not None:
                await self.client.disconnect()

        try:
            future = self.loop.submit(disconnect())
            future.add_done_callback(lambda _f: self.root.after(0, self._destroy_after_disconnect))
        except Exception:
            self._destroy_after_disconnect()

    def _destroy_after_disconnect(self) -> None:
        self._telethon_diag_uninstall()
        sys.stdout, sys.stderr = self._stdout, self._stderr
        store = getattr(self, "_log_store", None)
        if store is not None:
            store.close()
        self.loop.stop()
        self.root.destroy()

    def _on_close(self) -> None:
        if self._closing:
            return
        manual_busy = self._download_future is not None and not self._download_future.done()
        monitor_busy = getattr(self, "_monitor_total_transfers", 0) > 0
        extra_busy = any(
            runtime.active_count > 0 for runtime in getattr(self, "_extra_runtimes", {}).values()
        )
        busy = manual_busy or monitor_busy or extra_busy
        decision = True
        if busy:
            decision = self.messagebox.askyesnocancel(
                "安全退出",
                "当前有文件正在下载。\n\n"
                "是：等待已开始的文件完成后退出（不再启动新文件）\n"
                "否：取消正在下载的文件并退出\n"
                "取消：继续下载，不退出",
            )
            if decision is None:
                return
        # Keep a normal window's most recent dimensions and placement. Never
        # replace saved geometry with a minimized/maximized or invalid rect.
        if getattr(self, "root", None) is not None and self.root.state() == "normal":
            save_window_geometry(
                self.root.geometry(),
                self.root.winfo_screenwidth(),
                self.root.winfo_screenheight(),
            )
        self._closing = True
        if dialog := getattr(self, "_login_dialog", None):
            dialog.cancel()
        if self._monitor_active:
            self._stop_primary_monitor()
        self._stop_download.set()
        for runtime in getattr(self, "_extra_runtimes", {}).values():
            runtime.stop(cancel=not decision)
        if hasattr(self, "_monitor_scan_stop"):
            self._monitor_scan_stop.set()
        if hasattr(self, "_monitor_download_stop"):
            self._monitor_download_stop.set()
        if busy:
            if not decision:
                if manual_busy:
                    self._download_future.cancel()
                if monitor_busy:
                    self._interrupt_primary_monitor()
            self._log(
                "[退出] 等待当前已开始的文件完成后关闭\n"
                if decision else "[退出] 正在取消下载\n"
            )
            if manual_busy:
                return
        self._finish_close()

    def run(self) -> None:
        self.root.mainloop()


if __name__ == "__main__":
    run_gui()
