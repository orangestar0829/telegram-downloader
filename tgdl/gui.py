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
import concurrent.futures
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

from tgdl.browser import list_media, save_items
from tgdl.channels import list_media_dialogs
from tgdl.client import build_client
from tgdl.config import (
    Settings,
    credentials_available,
    default_download_dir,
    load_settings,
    read_env_values,
    update_env,
)
from tgdl.downloader import download_channel, resolve_channel
from tgdl.log_format import TimestampedLines
from tgdl.monitor import load_channels, save_channels

from tgdl import downloader as _downloader
from tgdl import http_fetch as _http_fetch
from tgdl import parser as _parser
from tgdl import torrent_fetch as _torrent_fetch
from tgdl.cli import parse_day, parse_types
from tgdl.parser import parse_channel


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
    def __init__(self, sink: "queue.Queue[str]", formatter: TimestampedLines | None = None) -> None:
        self._sink = sink
        self._formatter = formatter or TimestampedLines()

    def write(self, text: str) -> int:
        if text:
            self._sink.put(self._formatter.format(text))
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
    _App().run()


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
        self.settings: Settings | None = None
        self.log_queue: "queue.Queue[str]" = queue.Queue()
        self.download_queue: "queue.Queue[str]" = queue.Queue()
        self._log_popup = None
        self._log_popup_text = None
        self._download_popup = None
        self._download_popup_text = None

        self.root = tk.Tk()
        self.root.title("Telegram 下载器")
        self.root.geometry("820x680")
        self.root.minsize(720, 560)
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
        self.phone_var = tk.StringVar(value=env["phone"])
        self.download_dir_var = tk.StringVar(value=env["download_dir"] or str(default_download_dir()))
        self.proxy_host_var = tk.StringVar(value=env["proxy_host"])
        self.proxy_port_var = tk.StringVar(value=env["proxy_port"])
        self.proxy_username_var = tk.StringVar(value=env["proxy_username"])
        self.proxy_password_var = tk.StringVar(value=env["proxy_password"])
        self.media_concurrency_var = tk.StringVar(value=env["media_concurrency"])
        self.media_retries_var = tk.StringVar(value=env["media_retries"])
        self.chunk_concurrency_var = tk.StringVar(value=env["chunk_concurrency"])
        self.status_var = tk.StringVar(value="未登录")
        self.manual_var = tk.StringVar()

        self.type_photo = tk.BooleanVar(value=True)
        self.type_video = tk.BooleanVar(value=True)
        self.type_anim = tk.BooleanVar(value=False)
        self.dl_limit = tk.StringVar()
        self.auto_minutes_var = tk.StringVar(value="30")
        self.auto_status_var = tk.StringVar(value="定时下载：未启动")
        self._auto_job_id = None
        self._auto_config = None
        self._download_future = None
        self._download_task = None
        self._stop_download = threading.Event()
        self._closing = False
        self._monitor_cursor = 0
        try:
            self._monitored = load_channels()
        except (ValueError, OSError) as exc:
            self._monitored = []
            self.log_queue.put(f"[监控] 配置读取失败：{exc}\n")
        self._monitor_next: dict[str, float] = {}
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

        self._stdout, self._stderr = sys.stdout, sys.stderr
        self._timestamp_formatter = TimestampedLines()
        self._download_timestamp_formatter = TimestampedLines()
        writer = _QueueWriter(self.log_queue, self._timestamp_formatter)
        sys.stdout = writer
        sys.stderr = writer

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(100, self._drain_log)

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

        # Login row (phone → code, like the official client).
        login = ttk.LabelFrame(self.root, text="登录")
        login.pack(fill="x", **pad)
        ttk.Label(login, text="手机号(带国际区号,可留空登录时再输)").grid(
            row=0, column=0, sticky="w", **pad
        )
        ttk.Entry(login, textvariable=self.phone_var, width=22).grid(row=0, column=1, sticky="w", **pad)
        self.login_btn = ttk.Button(login, text="登录 Telegram", command=self.on_login)
        self.login_btn.grid(row=0, column=2, **pad)
        ttk.Button(login, text="设置", command=self._open_settings).grid(row=0, column=3, **pad)
        ttk.Label(login, text="状态:").grid(row=0, column=4, sticky="e", **pad)
        ttk.Label(login, textvariable=self.status_var, foreground="#0a6").grid(row=0, column=5, sticky="w", **pad)

        # Your chats.
        chats = ttk.LabelFrame(self.root, text="我的群组 / 频道(登录后自动加载)")
        chats.pack(fill="both", expand=True, **pad)
        cols = ("id", "kind", "name")
        self.tree = ttk.Treeview(chats, columns=cols, show="headings", height=9, selectmode="browse")
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
        tabs_row = ttk.Frame(self.root)
        tabs_row.pack(fill="x", **pad)
        nb = ttk.Notebook(tabs_row)
        nb.pack(side="left", fill="x", expand=True)
        ttk.Button(tabs_row, text="运行日志", command=self._open_log_window).pack(
            side="right", padx=(8, 0)
        )
        ttk.Button(tabs_row, text="下载记录", command=self._open_download_window).pack(
            side="right", padx=(8, 0)
        )
        nb.add(self._build_gallery_tab(), text="浏览媒体(选图/视频)")
        nb.add(self._build_download_tab(), text="批量下载")
        nb.add(self._build_monitor_tab(), text="多频道监控")
        nb.add(self._build_parse_tab(), text="解析链接(高级)")

        # Download folder row.
        folder = ttk.Frame(self.root)
        folder.pack(fill="x", **pad)
        ttk.Label(folder, text="下载到:").pack(side="left")
        ttk.Label(folder, textvariable=self.download_dir_var, foreground="#333").pack(side="left", padx=6)
        ttk.Button(folder, text="更改", command=self._change_dir).pack(side="left")
        ttk.Button(folder, text="打开下载文件夹", command=self._open_folder).pack(side="left", padx=6)

        logframe = ttk.LabelFrame(self.root, text="运行日志")
        logframe.pack(fill="both", expand=True, **pad)
        self.log = self.scrolledtext.ScrolledText(logframe, height=8, wrap="word", state="disabled")
        self.log.pack(fill="both", expand=True, padx=4, pady=4)
        ttk.Button(logframe, text="跳转最新", command=lambda: self.log.see("end")).pack(anchor="e", padx=4, pady=2)
        # Download completions have their own buffer and window; stdout/stderr
        # remain in the runtime log above.
        self.download_log = self.scrolledtext.ScrolledText(logframe, height=1, wrap="word", state="disabled")
        self.download_log.pack_forget()

    def _build_monitor_tab(self):
        tk, ttk = self.tk, self.ttk
        frame = ttk.Frame(self.root)
        controls = ttk.Frame(frame)
        controls.pack(fill="x", padx=6, pady=4)
        ttk.Label(controls, text="扫描间隔(分钟)").pack(side="left")
        self.monitor_minutes_var = tk.StringVar(value="15")
        ttk.Entry(controls, textvariable=self.monitor_minutes_var, width=6).pack(side="left", padx=5)
        ttk.Label(controls, text="方向").pack(side="left", padx=4)
        self.monitor_direction_var = tk.StringVar(value="从新到旧")
        ttk.Combobox(
            controls, textvariable=self.monitor_direction_var, state="readonly", width=10,
            values=("从新到旧", "从旧到新"),
        ).pack(side="left", padx=4)
        ttk.Button(controls, text="添加当前频道", command=self._monitor_add).pack(side="left")
        ttk.Button(controls, text="更新所选", command=self._monitor_update).pack(side="left", padx=4)
        ttk.Button(controls, text="删除所选", command=self._monitor_remove).pack(side="left", padx=5)
        ttk.Button(controls, text="启用/暂停所选", command=self._monitor_toggle).pack(side="left")
        columns = ("target", "minutes", "direction", "enabled", "next")
        self.monitor_tree = ttk.Treeview(frame, columns=columns, show="headings", height=7)
        for column, label, width in (
            ("target", "频道 ID / 用户名", 220),
            ("minutes", "间隔(分钟)", 95),
            ("direction", "扫描方向", 105),
            ("enabled", "状态", 85),
            ("next", "下次扫描", 145),
        ):
            self.monitor_tree.heading(column, text=label)
            self.monitor_tree.column(column, width=width)
        self.monitor_tree.pack(fill="both", expand=True, padx=6)
        self.monitor_tree.bind("<<TreeviewSelect>>", self._monitor_selection_changed)
        actions = ttk.Frame(frame)
        actions.pack(fill="x", padx=6, pady=4)
        self.monitor_button = ttk.Button(actions, text="启动全部监控", command=self._monitor_start_stop)
        self.monitor_button.pack(side="left")
        ttk.Button(actions, text="取消当前下载", command=self._cancel_current_download).pack(side="left", padx=6)
        ttk.Label(actions, text="重启后保留频道配置；需手动启动监控", foreground="#555").pack(side="left", padx=10)
        self._monitor_refresh()
        return frame

    def _monitor_refresh(self):
        expected = {str(i) for i in range(len(self._monitored))}
        for iid in self.monitor_tree.get_children():
            if iid not in expected:
                self.monitor_tree.delete(iid)
        for index, item in enumerate(self._monitored):
            due = self._monitor_next.get(item["target"])
            next_time = time.strftime("%H:%M:%S", time.localtime(due)) if due and item["enabled"] and self._monitor_active else "-"
            values = (
                item["target"], item["minutes"],
                "旧→新" if item.get("direction") == "oldest" else "新→旧",
                "启用" if item["enabled"] else "暂停", next_time,
            )
            iid = str(index)
            if self.monitor_tree.exists(iid):
                if tuple(map(str, values)) != tuple(self.monitor_tree.item(iid, "values")):
                    self.monitor_tree.item(iid, values=values)
            else:
                self.monitor_tree.insert("", "end", iid=iid, values=values)

    def _monitor_add(self):
        target = self._selected_target()
        if not target:
            self.messagebox.showwarning("监控", "请先选择或输入频道")
            return
        try:
            minutes = int(self.monitor_minutes_var.get())
            if not 1 <= minutes <= 1440:
                raise ValueError
        except ValueError:
            self.messagebox.showwarning("监控", "间隔应为 1～1440 分钟")
            return
        types = self._types_string()
        if not types:
            self.messagebox.showwarning("监控", "至少选择一种下载类型")
            return
        self.monitor_tree.selection_remove(self.monitor_tree.selection())
        self._monitored = [item for item in self._monitored if item["target"] != target]
        self._monitored.append({
            "target": target, "minutes": minutes, "types": types, "enabled": True,
            "direction": "oldest" if self.monitor_direction_var.get() == "从旧到新" else "newest",
        })
        self._monitor_next[target] = time.time()
        save_channels(self._monitored)
        self._monitor_refresh()

    def _monitor_selected_index(self):
        selected = self.monitor_tree.selection()
        return int(selected[0]) if selected else None

    def _monitor_selection_changed(self, _event=None):
        index = self._monitor_selected_index()
        if index is not None and index < len(self._monitored):
            item = self._monitored[index]
            self.monitor_minutes_var.set(str(item["minutes"]))
            self.monitor_direction_var.set(
                "从旧到新" if item.get("direction") == "oldest" else "从新到旧"
            )

    def _monitor_update(self):
        index = self._monitor_selected_index()
        if index is None:
            self.messagebox.showwarning("监控", "请先在监控列表中选一个频道")
            return
        try:
            minutes = int(self.monitor_minutes_var.get())
            if not 1 <= minutes <= 1440:
                raise ValueError
        except ValueError:
            self.messagebox.showwarning("监控", "间隔应为 1～1440 分钟")
            return
        item = self._monitored[index]
        item["minutes"] = minutes
        item["direction"] = "oldest" if self.monitor_direction_var.get() == "从旧到新" else "newest"
        self._monitor_next[item["target"]] = time.time()
        save_channels(self._monitored)
        self._monitor_refresh()

    def _monitor_remove(self):
        index = self._monitor_selected_index()
        if index is not None:
            self.monitor_tree.selection_remove(self.monitor_tree.selection())
            item = self._monitored.pop(index)
            self._monitor_next.pop(item["target"], None)
            save_channels(self._monitored)
            self._monitor_refresh()

    def _monitor_toggle(self):
        index = self._monitor_selected_index()
        if index is not None:
            item = self._monitored[index]
            item["enabled"] = not item["enabled"]
            self._monitor_next[item["target"]] = time.time()
            save_channels(self._monitored)
            self._monitor_refresh()

    def _monitor_start_stop(self):
        self._monitor_active = not self._monitor_active
        if self._monitor_active:
            self._stop_auto() if self._auto_config is not None else None
            self._monitor_next = {item["target"]: time.time() for item in self._monitored}
            self.monitor_button.config(text="停止全部监控")
            self._log("[监控] 多频道监控已启动\n")
            self._monitor_tick()
        else:
            if self._monitor_job is not None:
                self.root.after_cancel(self._monitor_job)
                self._monitor_job = None
            self.monitor_button.config(text="启动全部监控")
            self._log("[监控] 已停止调度，正在下载的任务不受影响\n")
            self._monitor_refresh()

    def _monitor_tick(self):
        self._monitor_job = None
        if not self._monitor_active:
            return
        now = time.time()
        busy = self._download_future is not None and not self._download_future.done()
        if not busy:
            total = len(self._monitored)
            for offset in range(total):
                index = (self._monitor_cursor + offset) % total
                item = self._monitored[index]
                if not item["enabled"] or self._monitor_next.get(item["target"], 0) > now:
                    continue
                try:
                    concurrency, retries, chunks = self._media_options()
                except ValueError as exc:
                    self._log(f"[监控] 设置错误：{exc}\n")
                    break
                self._monitor_cursor = (index + 1) % total
                self._monitor_next[item["target"]] = now + item["minutes"] * 60
                self._log(f"[监控] 开始扫描 {item['target']}，间隔 {item['minutes']} 分钟\n")
                self._start_channel_download(
                    item["target"], item["types"], "", concurrency, retries, chunks,
                    monitor_item=item,
                )
                break
        self._monitor_refresh()
        self._monitor_job = self.root.after(1000, self._monitor_tick)

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
        target = self._selected_target()
        if not target:
            self.messagebox.showwarning("未选择", "请先在上面列表里选一个群组/频道,或手动输入。")
            return
        limit_raw = self.g_limit.get().strip()
        types_raw = self._types_string()

        async def coro():
            await self._ensure_client()
            entity = await resolve_channel(self.client, target)
            self._gallery_entity = entity
            self._gallery_title = (
                getattr(entity, "title", None) or getattr(entity, "username", None) or str(target)
            )
            print(f"正在加载『{self._gallery_title}』的媒体缩略图…")
            items = await list_media(
                self.client,
                entity,
                limit=int(limit_raw) if limit_raw else 60,
                types=set(types_raw.split(",")) if types_raw else None,
            )
            self.root.after(0, lambda: self._populate_gallery(items))
            print(f"已加载 {len(items)} 个媒体。勾选要保存的,点『保存所选』。")

        self._run_task(coro(), self.load_gallery_btn)

    def on_save_selected(self) -> None:
        selected = [item for var, item in self.gallery_cells if var.get()]
        if not selected:
            self.messagebox.showwarning("未选择", "请先勾选要保存的图片/视频。")
            return
        try:
            concurrency, retries, chunk_concurrency = self._media_options()
        except ValueError as exc:
            self.messagebox.showwarning("设置错误", str(exc))
            return

        async def coro():
            await self._ensure_client()
            await save_items(
                self.client,
                selected,
                download_dir=self.settings.download_dir,
                title=self._gallery_title or "gallery",
                concurrency=concurrency,
                retries=retries,
                chunk_concurrency=chunk_concurrency,
            )
            print("保存完成。点『打开下载文件夹』查看。\n")

        self._run_task(coro(), self.save_sel_btn)

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
        ttk.Label(frame, text="自动下载间隔(分钟)").grid(row=4, column=0, sticky="w", **pad)
        ttk.Entry(frame, textvariable=self.auto_minutes_var, width=8).grid(
            row=4, column=1, sticky="w", **pad
        )
        self.auto_btn = ttk.Button(frame, text="启动定时", command=self.on_toggle_auto)
        self.auto_btn.grid(row=4, column=2, sticky="w", **pad)
        ttk.Label(frame, textvariable=self.auto_status_var).grid(
            row=5, column=0, columnspan=4, sticky="w", **pad
        )
        ttk.Label(frame, text="并发数 (1-8)").grid(row=2, column=0, sticky="w", **pad)
        ttk.Entry(frame, textvariable=self.media_concurrency_var, width=8).grid(
            row=2, column=1, sticky="w", **pad
        )
        ttk.Label(frame, text="失败重试 (0-5)").grid(row=2, column=2, sticky="w", **pad)
        ttk.Entry(frame, textvariable=self.media_retries_var, width=8).grid(
            row=2, column=3, sticky="w", **pad
        )
        ttk.Label(frame, text="单文件分块并发 (1-8)").grid(row=3, column=0, sticky="w", **pad)
        ttk.Entry(frame, textvariable=self.chunk_concurrency_var, width=8).grid(
            row=3, column=1, sticky="w", **pad
        )
        frame.columnconfigure(2, weight=1)
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
    def _log(self, text: str) -> None:
        self.log_queue.put(self._timestamp_formatter.format(text))

    def _log_download(self, text: str) -> None:
        self.download_queue.put(self._download_timestamp_formatter.format(text + "\n"))

    def _drain_log(self) -> None:
        try:
            while True:
                chunk = self.log_queue.get_nowait()
                self._append_to_log(self.log, chunk)
                if self._log_popup_text is not None:
                    self._append_to_log(self._log_popup_text, chunk)
        except queue.Empty:
            pass
        try:
            while True:
                chunk = self.download_queue.get_nowait()
                self._append_to_log(self.download_log, chunk)
                if self._download_popup_text is not None:
                    self._append_to_log(self._download_popup_text, chunk)
        except queue.Empty:
            pass
        self.root.after(100, self._drain_log)

    @staticmethod
    def _append_to_log(widget, chunk: str) -> None:
        follow = widget.yview()[1] >= 0.999
        widget.configure(state="normal")
        widget.insert("end", chunk)
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

    def _set_status(self, text: str) -> None:
        self.root.after(0, lambda: self.status_var.set(text))

    def _current_download_dir(self) -> Path:
        raw = self.download_dir_var.get().strip()
        return Path(raw).expanduser() if raw else default_download_dir()

    def _media_options(self) -> tuple[int, int, int]:
        try:
            workers = int(self.media_concurrency_var.get().strip())
            retries = int(self.media_retries_var.get().strip())
            chunks = int(self.chunk_concurrency_var.get().strip())
            if not 1 <= workers <= 8 or not 0 <= retries <= 5 or not 1 <= chunks <= 8:
                raise ValueError
        except ValueError as exc:
            raise ValueError("文件/分块并发数必须为 1-8，失败重试必须为 0-5") from exc
        values = read_env_values()
        values.update(media_concurrency=str(workers), media_retries=str(retries),
                      chunk_concurrency=str(chunks))
        update_env(values)
        return workers, retries, chunks

    def _selected_target(self) -> str | None:
        sel = self.tree.selection()
        if sel:
            values = self.tree.item(sel[0], "values")
            if values:
                return str(values[0])
        manual = self.manual_var.get().strip()
        return manual or None

    def _ask(self, prompt: str, secret: bool = False) -> str:
        answer: dict[str, str | None] = {}
        done = threading.Event()

        def ask_on_ui() -> None:
            show = "*" if secret else ""
            answer["value"] = self.simpledialog.askstring("Telegram", prompt, show=show, parent=self.root)
            done.set()

        self.root.after(0, ask_on_ui)
        done.wait()
        value = answer.get("value")
        if value is None:
            raise RuntimeError("已取消")
        return value.strip()

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
                    self.root.after(0, lambda: button.config(state="normal"))

        future.add_done_callback(finished)

    def _save_dir_only(self) -> None:
        values = read_env_values()
        values["download_dir"] = self.download_dir_var.get().strip() or str(default_download_dir())
        update_env(values)

    # ---------- client ----------
    async def _ensure_client(self):
        if self.client is not None:
            return self.client
        settings = load_settings()
        self.settings = settings
        await self.client_start(settings)
        me = await self.client.get_me()
        if me is None:
            raise RuntimeError("登录失败")
        name = " ".join(p for p in (me.first_name, me.last_name) if p)
        handle = f"@{me.username}" if me.username else ""
        print(f"已登录:{name} {handle} (id={me.id})".strip())
        self._set_status(f"已登录:{name} {handle}".strip())
        return self.client

    async def client_start(self, settings) -> None:
        self.client = build_client(settings)
        await self.client.start(
            phone=lambda: settings.phone or self._ask("手机号(带国际区号,如 +8613800138000):"),
            code_callback=lambda: self._ask("Telegram 发来的验证码:"),
            password=lambda: self._ask("两步验证密码:", secret=True),
        )

    # ---------- handlers ----------
    def on_save_creds(self) -> None:
        if not self.api_id_var.get().strip() or not self.api_hash_var.get().strip():
            self.messagebox.showwarning("缺少信息", "请先填写 api_id 和 api_hash。")
            return
        update_env(
            {
                "api_id": self.api_id_var.get().strip(),
                "api_hash": self.api_hash_var.get().strip(),
                "phone": self.phone_var.get().strip(),
                "download_dir": self.download_dir_var.get().strip() or str(default_download_dir()),
            }
        )
        self.creds_available = True
        if getattr(self, "_setup_frame", None) is not None:
            self._setup_frame.destroy()
            self._setup_frame = None
        self._log("已保存凭据。现在点『登录 Telegram』。\n")

    def on_login(self) -> None:
        if not credentials_available() and not (
            self.api_id_var.get().strip() and self.api_hash_var.get().strip()
        ):
            self.messagebox.showwarning("缺少信息", "请先在『首次设置』里填写 api_id / api_hash 并保存。")
            return
        # persist phone/dir so they are remembered
        update_env(
            {
                "api_id": self.api_id_var.get().strip(),
                "api_hash": self.api_hash_var.get().strip(),
                "phone": self.phone_var.get().strip(),
                "download_dir": self.download_dir_var.get().strip() or str(default_download_dir()),
            }
        )
        self._log("正在登录…(首次会向你的 Telegram 发送验证码)\n")

        async def coro():
            await self._ensure_client()
            await self._load_dialogs()

        self._run_task(coro(), self.login_btn)

    def on_list(self) -> None:
        async def coro():
            await self._ensure_client()
            await self._load_dialogs()

        self._run_task(coro(), self.refresh_btn)

    async def _load_dialogs(self) -> None:
        items = await list_media_dialogs(self.client)
        rows = [(str(it.entity_id), it.kind, it.title) for it in items]

        def apply():
            self.tree.delete(*self.tree.get_children())
            for row in rows:
                self.tree.insert("", "end", values=row)

        self.root.after(0, apply)
        print(f"已加载 {len(rows)} 个群组/频道。点选一个,然后『下载所选』。")

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
        if self._download_future is not None and not self._download_future.done():
            self._log("[定时] 上一次下载尚未结束，跳过本轮。\n")
            return

        self._stop_download.clear()

        async def coro():
            self._download_task = asyncio.current_task()
            try:
                await self._ensure_client()
                result = await download_channel(
                    self.client,
                    target=target,
                    download_dir=self.settings.download_dir,
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
                    monitor_direction=monitor_item.get("direction", "newest") if monitor_item else None,
                    monitor_batch_size=100,
                    stop_event=self._stop_download,
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
            except concurrent.futures.CancelledError:
                self._log("[下载] 已取消当前任务\n")
            except Exception as exc:
                self._log(f"[下载错误] {exc}\n")
            finally:
                def on_ui():
                    self.download_btn.config(state="normal")
                    if self._closing:
                        self._finish_close()
                self.root.after(0, on_ui)

        future.add_done_callback(finished)

    def on_toggle_auto(self) -> None:
        if self._auto_config is not None:
            self._stop_auto()
            return
        if self._monitor_active:
            self._monitor_start_stop()
        target = self._selected_target()
        types_raw = self._types_string()
        if not target or not types_raw:
            self.messagebox.showwarning("定时下载", "请先选择频道和下载类型。")
            return
        try:
            minutes = int(self.auto_minutes_var.get().strip())
            if not 1 <= minutes <= 1440:
                raise ValueError("间隔必须在 1-1440 分钟之间")
            limit = self.dl_limit.get().strip()
            if limit and int(limit) <= 0:
                raise ValueError("数量上限必须为正整数")
            concurrency, retries, chunks = self._media_options()
        except ValueError as exc:
            self.messagebox.showwarning("定时下载", f"参数错误：{exc}")
            return
        self._auto_config = (target, types_raw, limit, concurrency, retries, chunks)
        self._auto_interval_ms = minutes * 60 * 1000
        self.auto_btn.config(text="停止定时")
        self._log(f"[定时] 已启动：频道 {target}，每 {minutes} 分钟检查一次，立即执行首轮。\n")
        self._auto_tick()

    def _auto_tick(self) -> None:
        if self._auto_config is None:
            return
        self._auto_job_id = None
        self._start_channel_download(*self._auto_config)
        self.auto_status_var.set(
            f"定时下载：运行中（{self._auto_config[0]}，每 {self._auto_interval_ms // 60000} 分钟）"
        )
        self._auto_job_id = self.root.after(self._auto_interval_ms, self._auto_tick)

    def _stop_auto(self) -> None:
        if self._auto_job_id is not None:
            self.root.after_cancel(self._auto_job_id)
            self._auto_job_id = None
        self._auto_config = None
        self.auto_btn.config(text="启动定时")
        self.auto_status_var.set("定时下载：未启动")
        self._log("[定时] 已停止后续检查；当前下载不会中断。\n")

    def on_parse(self) -> None:
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
                download_direct=not opts["catalog_only"],
                download_torrents=not opts["catalog_only"] and not opts["no_bt"],
                dry_run=opts["dry"],
            )
            await parse_channel(self.client, **kwargs)
            print("解析完成。\n")

        self._run_task(coro(), self.parse_btn)

    def _change_dir(self) -> None:
        from tkinter import filedialog

        chosen = filedialog.askdirectory()
        if chosen:
            self.download_dir_var.set(chosen)
            self._save_dir_only()
            if self.settings is not None:
                from dataclasses import replace

                self.settings = replace(self.settings, download_dir=Path(chosen))

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
            self._log(f"[错误] 打不开文件夹:{exc}\n")

    def _open_settings(self) -> None:
        tk, ttk = self.tk, self.ttk
        win = tk.Toplevel(self.root)
        win.title("设置：Telegram API / SOCKS5 代理")
        win.transient(self.root)
        pad = {"padx": 8, "pady": 6}
        fields = (
            ("api_id", self.api_id_var, False),
            ("api_hash", self.api_hash_var, False),
            ("SOCKS5 主机（留空则不用代理）", self.proxy_host_var, False),
            ("SOCKS5 端口（例如 1080）", self.proxy_port_var, False),
            ("代理用户名（可空）", self.proxy_username_var, False),
            ("代理密码（可空）", self.proxy_password_var, True),
            ("下载目录", self.download_dir_var, False),
        )
        for row, (name, var, secret) in enumerate(fields):
            ttk.Label(win, text=name).grid(row=row, column=0, sticky="w", **pad)
            ttk.Entry(win, textvariable=var, width=43, show="*" if secret else "").grid(
                row=row, column=1, sticky="we", **pad
            )

        def save_and_close():
            host = self.proxy_host_var.get().strip()
            port = self.proxy_port_var.get().strip()
            if port and not host:
                self.messagebox.showwarning("代理设置", "填写端口时必须填写 SOCKS5 主机。")
                return
            if host:
                try:
                    port_number = int(port or "1080")
                    if not 1 <= port_number <= 65535:
                        raise ValueError
                except ValueError:
                    self.messagebox.showwarning("代理设置", "SOCKS5 端口必须在 1-65535 之间。")
                    return
                port = str(port_number)
            else:
                port = ""
            values = read_env_values()
            values.update({
                "api_id": self.api_id_var.get().strip(),
                "api_hash": self.api_hash_var.get().strip(),
                "phone": self.phone_var.get().strip(),
                "download_dir": self.download_dir_var.get().strip() or str(default_download_dir()),
                "proxy_host": host,
                "proxy_port": port,
                "proxy_username": self.proxy_username_var.get().strip(),
                "proxy_password": self.proxy_password_var.get(),
            })
            update_env(values)
            win.destroy()
            if self.client is not None:
                self.messagebox.showinfo("设置已保存", "代理设置在重启下载器后生效。")
            else:
                self._log("设置已保存。\n")

        ttk.Button(win, text="保存", command=save_and_close).grid(
            row=len(fields), column=1, sticky="e", **pad
        )

    # ---------- lifecycle ----------
    def _cancel_current_download(self) -> None:
        self._stop_download.set()
        if self._download_future is not None and not self._download_future.done():
            self._download_future.cancel()
            self._log("[下载] 正在取消当前扫描及未完成文件；已成功的文件不会删除\n")

    def _finish_close(self) -> None:
        if self._download_future is not None and not self._download_future.done():
            return
        sys.stdout, sys.stderr = self._stdout, self._stderr

        async def disconnect():
            # Cancellation can settle the thread-safe Future before the real
            # asyncio download task has removed its temporary chunk files.
            active = self._download_task
            if active is not None and not active.done():
                try:
                    await asyncio.wait_for(asyncio.shield(active), timeout=30)
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
        self.loop.stop()
        self.root.destroy()

    def _on_close(self) -> None:
        if self._closing:
            return
        busy = self._download_future is not None and not self._download_future.done()
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
        if self._monitor_active:
            self._monitor_start_stop()
        if self._auto_config is not None:
            self._stop_auto()
        self._closing = True
        self._stop_download.set()
        if busy:
            if not decision:
                self._download_future.cancel()
            self._log(
                "[退出] 等待当前已开始的文件完成后关闭\n"
                if decision else "[退出] 正在取消下载\n"
            )
            return
        self._finish_close()

    def run(self) -> None:
        self.root.mainloop()


if __name__ == "__main__":
    run_gui()
