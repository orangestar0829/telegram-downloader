"""Tk account-management window: independent account settings and channel lists."""
from __future__ import annotations

from tgdl.accounts import DEFAULT_ACCOUNT
from tgdl.config import read_env_values, update_env
from tgdl.monitor import load_channels, save_channels

_ACCOUNT_FIELDS = (
    ("phone", "手机号（国际区号）"),
    ("proxy_host", "SOCKS5 主机"),
    ("proxy_port", "SOCKS5 端口"),
    ("proxy_username", "代理用户名"),
    ("proxy_password", "代理密码"),
    ("download_dir", "下载目录"),
    ("temp_download_dir", "临时缓存目录"),
    ("media_concurrency", "同时下载文件数（1～8）"),
    ("media_retries", "文件重试次数（0～5）"),
    ("chunk_concurrency", "单文件分块数（1～8）"),
    ("media_speed_kib", "下载限速 KiB/s（0=不限）"),
)


class AccountWindow:
    def __init__(self, app) -> None:
        self.app = app
        self.registry = app._account_registry
        tk, ttk = app.tk, app.ttk
        self.tk, self.ttk = tk, ttk
        self.win = tk.Toplevel(app.root)
        self.win.title("多账号管理 · 各账号独立扫描和代理")
        self.win.geometry("1050x760")
        self.win.minsize(900, 660)
        self.choice = tk.StringVar()
        self._ids: dict[str, str] = {}
        self.status = tk.StringVar()
        self._channel_rows: list[dict] = []
        self._fields = {key: tk.StringVar() for key, _label in _ACCOUNT_FIELDS}

        toolbar = ttk.Frame(self.win)
        toolbar.pack(fill="x", padx=10, pady=8)
        ttk.Label(toolbar, text="当前管理账号").pack(side="left")
        self.selector = ttk.Combobox(toolbar, textvariable=self.choice, state="readonly", width=30)
        self.selector.pack(side="left", padx=7)
        self.selector.bind("<<ComboboxSelected>>", lambda _e: self._load_selected())
        ttk.Button(toolbar, text="添加账号", command=self._add_account).pack(side="left", padx=3)
        ttk.Button(toolbar, text="重命名", command=self._rename_account).pack(side="left", padx=3)
        ttk.Button(toolbar, text="登录并启动监控", command=self._start_account).pack(side="left", padx=3)
        ttk.Button(toolbar, text="扫码登录", command=lambda: self._start_account(qr=True)).pack(side="left", padx=3)
        ttk.Button(toolbar, text="停止该账号", command=self._stop_account).pack(side="left", padx=3)

        ttk.Label(self.win, textvariable=self.status).pack(anchor="w", padx=12, pady=(0, 5))

        settings_frame = ttk.LabelFrame(self.win, text="账号级别设置（修改代理/登录信息后需停止并重新启动账号）")
        settings_frame.pack(fill="x", padx=10, pady=4)
        for index, (key, label) in enumerate(_ACCOUNT_FIELDS):
            column = (index // 6) * 2
            row = index % 6
            ttk.Label(settings_frame, text=label).grid(row=row, column=column, sticky="w", padx=5, pady=4)
            var = self._fields[key]
            ttk.Entry(settings_frame, textvariable=var, width=30,
                      show="*" if key == "proxy_password" else "").grid(
                          row=row, column=column + 1, sticky="we", padx=5, pady=4
                      )
        self.save_settings_btn = ttk.Button(
            settings_frame, text="保存该账号设置", command=self._save_settings
        )
        self.save_settings_btn.grid(row=6, column=3, sticky="e", padx=5, pady=6)
        for column in (1, 3):
            settings_frame.columnconfigure(column, weight=1)

        monitors = ttk.LabelFrame(
            self.win, text="该账号独立监控的频道（未开启的频道绝不参与下载）"
        )
        monitors.pack(fill="both", expand=True, padx=10, pady=5)
        cols = ("target", "name", "minutes", "priority", "batch_size", "types", "direction", "enabled")
        self.tree = ttk.Treeview(monitors, columns=cols, show="headings", height=6)
        for col, label, width in (
            ("target", "频道 ID/@用户名", 170),
            ("name", "名称", 130),
            ("minutes", "扫描间隔", 80),
            ("priority", "优先级", 70),
            ("batch_size", "每轮数量", 80),
            ("types", "媒体类型", 115),
            ("direction", "方向", 80),
            ("enabled", "状态", 65),
        ):
            self.tree.heading(col, text=label)
            self.tree.column(col, width=width, stretch=col in ("target", "name"))
        self.tree.pack(fill="both", expand=True, padx=5, pady=4)
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._channel_selected())
        editor = ttk.Frame(monitors)
        editor.pack(fill="x", padx=5, pady=2)
        self.target = tk.StringVar()
        self.channel_name = tk.StringVar()
        self.minutes = tk.StringVar(value="5")
        self.priority = tk.StringVar(value="0")
        self.batch_size = tk.StringVar(value="100")
        self.types = tk.StringVar(value="photo,video")
        self.direction = tk.StringVar(value="newest")
        self.enabled = tk.BooleanVar(value=True)
        fields = (
            ("频道", self.target, 23), ("名称", self.channel_name, 14),
            ("分钟", self.minutes, 5), ("优先级", self.priority, 5),
            ("每轮上限", self.batch_size, 7), ("类型", self.types, 17),
        )
        for label, var, width in fields:
            ttk.Label(editor, text=label).pack(side="left", padx=(3, 0))
            ttk.Entry(editor, textvariable=var, width=width).pack(side="left", padx=3)
        ttk.Combobox(editor, textvariable=self.direction, values=("newest", "oldest"),
                     width=7, state="readonly").pack(side="left", padx=3)
        ttk.Checkbutton(editor, text="监控开启", variable=self.enabled).pack(side="left", padx=3)

        actions = ttk.Frame(monitors)
        actions.pack(fill="x", padx=6, pady=4)
        self.add_btn = ttk.Button(actions, text="新增/更新频道", command=self._save_channel)
        self.add_btn.pack(side="left", padx=3)
        self.remove_btn = ttk.Button(actions, text="删除所选", command=self._remove_channel)
        self.remove_btn.pack(side="left", padx=3)
        ttk.Button(actions, text="查看已加入的频道", command=self._list_dialogs).pack(side="left", padx=3)
        ttk.Label(
            actions, text="两个账号都监控同一频道时，将自动领取不同文件；扫描游标互不影响。"
        ).pack(side="left", padx=15)

        self._refresh_choices(DEFAULT_ACCOUNT)
        self._refresh_status()

    def _selected_id(self) -> str:
        return self._ids.get(self.choice.get(), DEFAULT_ACCOUNT)

    def _refresh_choices(self, selected: str) -> None:
        accounts = self.registry.accounts()
        self._ids = {f"{a.name}（{a.id}）": a.id for a in accounts}
        self.selector.configure(values=list(self._ids))
        for label, account_id in self._ids.items():
            if account_id == selected:
                self.choice.set(label)
                break
        self._load_selected()
        self.app._refresh_log_accounts()

    def _load_selected(self) -> None:
        account_id = self._selected_id()
        env = read_env_values(self.registry.env_path(account_id))
        for key in self._fields:
            self._fields[key].set(env.get(key, ""))
        self._channel_rows = load_channels(self.registry.monitor_path(account_id))
        self._refresh_channel_tree()
        # The original account is controlled by the main window to avoid
        # stale Tk drafts overwriting live legacy settings.
        state = "disabled" if account_id == DEFAULT_ACCOUNT else "normal"
        self.save_settings_btn.configure(state=state)
        self.add_btn.configure(state=state)
        self.remove_btn.configure(state=state)

    def _add_account(self) -> None:
        name = self.app.simpledialog.askstring(
            "添加 Telegram 账号", "账号备注名称", parent=self.win
        )
        if not name:
            return
        try:
            account = self.registry.add(name)
            self._refresh_choices(account.id)
            self.app._log_account(account.id, "[账号管理] 已创建账号（尚未登录）\n")
        except (ValueError, OSError) as exc:
            self.app.messagebox.showerror("添加失败", str(exc), parent=self.win)

    def _rename_account(self) -> None:
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            self.app.messagebox.showinfo("账号管理", "原有账号名称保持不变", parent=self.win)
            return
        name = self.app.simpledialog.askstring(
            "重命名", "新名称", parent=self.win
        )
        if name:
            self.registry.rename(account_id, name)
            self._refresh_choices(account_id)

    def _save_settings(self) -> bool:
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            self.app.messagebox.showinfo(
                "账号管理", "请使用主窗口的设置按钮修改原有账号", parent=self.win
            )
            return False
        values = read_env_values(self.registry.env_path(account_id))
        values.update({key: var.get().strip() for key, var in self._fields.items()})
        try:
            host = values["proxy_host"]
            port = values["proxy_port"]
            if port and not host:
                raise ValueError("设置端口时需要指定 SOCKS5 主机")
            if host:
                if not 1 <= int(port or "1080") <= 65535:
                    raise ValueError("SOCKS5 端口超出范围")
            for name, low, high in (
                ("media_concurrency", 1, 8), ("media_retries", 0, 5),
                ("chunk_concurrency", 1, 8), ("media_speed_kib", 0, 1048576),
            ):
                value = int(values[name])
                if not low <= value <= high:
                    raise ValueError(f"{name} 必须在 {low}～{high} 范围内")
            update_env(values, self.registry.env_path(account_id))
        except (ValueError, OSError) as exc:
            self.app.messagebox.showwarning("账号设置无效", str(exc), parent=self.win)
            return False
        self.app._log_account(account_id, "[账号设置] 已保存；代理变更重启账号生效\n")
        return True

    def _start_account(self, qr: bool = False) -> None:
        from tgdl.multi_runtime import AccountMonitorRuntime
        from tgdl.qr_gui import QrLoginWindow
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            self.app.messagebox.showinfo(
                "原有账号", "请在主窗口登录并启动多频道监控", parent=self.win
            )
            return
        existing = self.app._extra_futures.get(account_id)
        if existing is not None and not existing.done():
            self.app.messagebox.showinfo("账号运行中", "该账号已启动", parent=self.win)
            return
        if not self._save_settings():
            return
        account = next(a for a in self.registry.accounts() if a.id == account_id)
        qr_window = None
        runtime = AccountMonitorRuntime(
            account, self.registry,
            ask=lambda prompt, secret: self.app._ask(
                f"[{account.name}] {prompt}", secret
            ),
            on_log=self.app._log_account,
            on_saved=self.app._log_download_account,
            on_progress=self.app._update_download_progress,
            login_method="qr" if qr else "code",
            on_qr=lambda url, expires: qr_window.on_qr(url, expires) if qr_window else None,
        )
        if qr:
            qr_window = QrLoginWindow(self.app, account.name, runtime.stop)
        self.app._extra_runtimes[account_id] = runtime
        future = self.app.loop.submit(runtime.run())
        self.app._extra_futures[account_id] = future
        if qr_window is not None:
            def finish(_done):
                self.app.root.after(0, qr_window.done)
            future.add_done_callback(finish)
            self._poll_qr_login(qr_window, runtime)

    def _poll_qr_login(self, window, runtime) -> None:
        if window._closed or not window.win.winfo_exists():
            return
        if runtime.status.startswith("已连接"):
            window.done()
            return
        if runtime.stop_event.is_set():
            window.done()
            return
        self.win.after(500, lambda: self._poll_qr_login(window, runtime))

    def _stop_account(self) -> None:
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            self.app.messagebox.showinfo(
                "原有账号", "请在主窗口停止多频道监控", parent=self.win
            )
            return
        runtime = self.app._extra_runtimes.get(account_id)
        if runtime is not None:
            runtime.stop()

    def _refresh_channel_tree(self) -> None:
        self.tree.delete(*self.tree.get_children())
        for index, item in enumerate(self._channel_rows):
            self.tree.insert("", "end", iid=str(index), values=(
                item["target"], item.get("name", ""), item["minutes"],
                item.get("priority", 0), item.get("batch_size", 100),
                item.get("types", "photo,video"), item.get("direction", "newest"),
                "开启" if item.get("enabled", True) else "暂停",
            ))

    def _channel_selected(self) -> None:
        selected = self.tree.selection()
        if not selected:
            return
        row = self._channel_rows[int(selected[0])]
        for var, key in (
            (self.target, "target"), (self.channel_name, "name"),
            (self.minutes, "minutes"), (self.priority, "priority"),
            (self.batch_size, "batch_size"), (self.types, "types"),
            (self.direction, "direction"),
        ):
            var.set(str(row.get(key, "")))
        self.enabled.set(row.get("enabled", True))

    def _save_channel(self) -> None:
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            return
        try:
            target = self.target.get().strip()
            minutes = int(self.minutes.get())
            priority = int(self.priority.get())
            batch_size = int(self.batch_size.get())
            types = ",".join(t.strip() for t in self.types.get().split(",") if t.strip())
            if not target or not 1 <= minutes <= 1440 or not 1 <= batch_size <= 100000:
                raise ValueError("频道不可为空，间隔为 1～1440 分钟，上限为 1～100000")
            if any(t not in ("photo", "video", "animation") for t in types.split(",")):
                raise ValueError("类型只能为 photo、video、animation")
            direction = self.direction.get()
            if direction not in ("newest", "oldest"):
                raise ValueError("扫描方向不正确")
            rows = [r for r in self._channel_rows if r["target"] != target]
            rows.append({
                "target": target, "name": self.channel_name.get().strip(),
                "minutes": minutes, "priority": priority,
                "batch_size": batch_size, "types": types,
                "enabled": self.enabled.get(), "direction": direction,
            })
            save_channels(rows, self.registry.monitor_path(account_id))
            self._channel_rows = load_channels(self.registry.monitor_path(account_id))
            self._refresh_channel_tree()
            self.app._log_account(account_id, f"[频道配置] 已更新 {target}\n")
        except (ValueError, OSError) as exc:
            self.app.messagebox.showwarning("频道配置无效", str(exc), parent=self.win)

    def _remove_channel(self) -> None:
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            return
        chosen = self.tree.selection()
        if not chosen:
            return
        row = self._channel_rows[int(chosen[0])]
        self._channel_rows = [r for r in self._channel_rows if r["target"] != row["target"]]
        save_channels(self._channel_rows, self.registry.monitor_path(account_id))
        self._refresh_channel_tree()

    def _list_dialogs(self) -> None:
        from tgdl.channels import list_media_dialogs
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            self.app.messagebox.showinfo(
                "频道列表", "原有账号的频道列表在主窗口查看", parent=self.win
            )
            return
        runtime = self.app._extra_runtimes.get(account_id)
        if runtime is None or runtime.client is None:
            self.app.messagebox.showinfo(
                "频道列表", "请先登录并启动该账号", parent=self.win
            )
            return

        async def fetch():
            return await list_media_dialogs(runtime.client)

        future = self.app.loop.submit(fetch())

        def finished(done):
            try:
                channels = done.result()
            except Exception as exc:
                self.app.root.after(
                    0, lambda exc=exc: self.app.messagebox.showwarning(
                        "频道列表", str(exc), parent=self.win
                    )
                )
                return
            self.app.root.after(0, lambda: self._show_dialogs(channels))

        future.add_done_callback(finished)

    def _show_dialogs(self, channels) -> None:
        win = self.tk.Toplevel(self.win)
        win.title("该账号已加入的 Telegram 频道 / 群组")
        win.geometry("620x460")
        tree = self.ttk.Treeview(win, columns=("id", "name"), show="headings")
        tree.heading("id", text="ID")
        tree.heading("name", text="名称")
        tree.column("id", width=180)
        tree.column("name", width=400)
        tree.pack(fill="both", expand=True)
        for item in channels:
            tree.insert("", "end", values=(str(item.entity_id), item.title))

        def select():
            chosen = tree.selection()
            if chosen:
                target, name = tree.item(chosen[0], "values")
                self.target.set(str(target))
                self.channel_name.set(str(name))
                win.destroy()
        self.ttk.Button(win, text="填入频道配置", command=select).pack(pady=6)

    def _refresh_status(self) -> None:
        if not self.win.winfo_exists():
            return
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            selected_state = "主窗口管理（" + ("监控运行中" if self.app._monitor_active else "监控未启动") + "）"
        else:
            runtime = self.app._extra_runtimes.get(account_id)
            selected_state = (
                f"{runtime.status}，下载中 {runtime.active_count} 个"
                if runtime is not None else "未登录"
            )
        all_state = []
        for account in self.registry.accounts()[1:]:
            runtime = self.app._extra_runtimes.get(account.id)
            if runtime is not None:
                all_state.append(f"{account.name}: {runtime.active_count} 文件")
        self.status.set(
            f"当前：{selected_state}   |   其他账号："
            + ("；".join(all_state) if all_state else "无运行账号")
        )
        self.win.after(1000, self._refresh_status)
