"""Tk account-management window: independent account settings and channel lists."""
from __future__ import annotations

from tgdl.account_context import current_account_id
from tgdl.accounts import DEFAULT_ACCOUNT
from tgdl.account_identity import account_identity
from tgdl.dialog_filter import filter_dialogs
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
        self.win.geometry("1140x900")
        self.win.minsize(930, 750)
        self.choice = tk.StringVar()
        self._ids: dict[str, str] = {}
        self.status = tk.StringVar()
        self._channel_rows: list[dict] = []
        self._fields = {key: tk.StringVar() for key, _label in _ACCOUNT_FIELDS}
        self._loaded_fields: dict[str, str] = {}

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

        # Embedded joined-channel selector; per-account cache avoids reloading on every switch.
        self._dialog_cache = getattr(app, "_account_dialog_cache", {})
        app._account_dialog_cache = self._dialog_cache
        # Share in-flight request tracking with the main account selector to
        # avoid fetching the same Telegram dialogs in both windows.
        self._dialog_loading = getattr(app, "_account_dialog_fetches", set())
        app._account_dialog_fetches = self._dialog_loading
        self._dialog_retry_at: dict[str, float] = {}
        self.dialog_keyword = tk.StringVar()
        self.dialog_counter = tk.StringVar(value="未加载")
        joined = ttk.LabelFrame(self.win, text="我的群组 / 频道（当前 TG 账号）")
        joined.pack(fill="both", expand=False, padx=10, pady=(5, 2))
        search = ttk.Frame(joined)
        search.pack(fill="x", padx=6, pady=3)
        ttk.Label(search, text="筛选群组/频道").pack(side="left")
        ttk.Entry(search, textvariable=self.dialog_keyword).pack(
            side="left", fill="x", expand=True, padx=7
        )
        ttk.Button(search, text="清空", command=lambda: self.dialog_keyword.set("")).pack(side="left")
        ttk.Button(search, text="刷新列表", command=lambda: self._ensure_dialogs(force=True)).pack(
            side="left", padx=6
        )
        ttk.Label(search, textvariable=self.dialog_counter).pack(side="left")
        self.dialog_keyword.trace_add("write", lambda *_: self._refresh_dialog_tree())
        self.dialog_tree = ttk.Treeview(
            joined, columns=("id", "kind", "name"), show="headings", height=5,
            selectmode="browse",
        )
        for column, label, width in (
            ("id", "ID", 175), ("kind", "类型", 105), ("name", "名称", 560),
        ):
            self.dialog_tree.heading(column, text=label)
            self.dialog_tree.column(column, width=width, stretch=(column == "name"))
        self.dialog_tree.pack(fill="both", expand=True, padx=6, pady=(0, 5))
        self.dialog_tree.bind("<<TreeviewSelect>>", self._dialog_selected)

        monitors = ttk.LabelFrame(
            self.win, text="该账号独立监控的频道（未开启的频道绝不参与下载）"
        )
        monitors.pack(fill="both", expand=True, padx=10, pady=5)
        cols = ("name", "target", "types", "minutes", "priority", "batch_size", "direction", "enabled", "next")
        self.tree = ttk.Treeview(monitors, columns=cols, show="headings", height=5)
        for col, label, width in (
            ("name", "群组 / 频道名称", 180),
            ("target", "频道 ID / 用户名", 155),
            ("types", "下载类型", 125),
            ("minutes", "间隔(分钟)", 85),
            ("priority", "优先级", 70),
            ("batch_size", "每轮数量", 80),
            ("direction", "扫描方向", 85),
            ("enabled", "状态", 65),
            ("next", "下次扫描", 115),
        ):
            self.tree.heading(col, text=label)
            self.tree.column(col, width=width, stretch=(col in ("name", "target")))
        self.tree.pack(fill="both", expand=True, padx=5, pady=4)
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._channel_selected())

        self.target = tk.StringVar()
        self.channel_name = tk.StringVar()
        self.minutes = tk.StringVar(value="5")
        self.priority = tk.StringVar(value="0")
        self.batch_size = tk.StringVar(value="100")
        self.direction = tk.StringVar(value="从新到旧")
        self.enabled = tk.BooleanVar(value=True)
        self.type_photo = tk.BooleanVar(value=True)
        self.type_video = tk.BooleanVar(value=True)
        self.type_anim = tk.BooleanVar(value=False)

        editor = ttk.Frame(monitors)
        editor.pack(fill="x", padx=5, pady=2)
        for label, var, width in (
            ("扫描间隔(分钟)", self.minutes, 6),
            ("优先级", self.priority, 5),
            ("每轮数量", self.batch_size, 7),
        ):
            ttk.Label(editor, text=label).pack(side="left", padx=(5, 0))
            ttk.Entry(editor, textvariable=var, width=width).pack(side="left", padx=3)
        ttk.Label(editor, text="方向").pack(side="left", padx=4)
        ttk.Combobox(
            editor, textvariable=self.direction, state="readonly", width=10,
            values=("从新到旧", "从旧到新"),
        ).pack(side="left", padx=4)

        kinds = ttk.Frame(monitors)
        kinds.pack(fill="x", padx=5, pady=2)
        ttk.Label(kinds, text="监控下载类型（每个频道独立）:").pack(side="left")
        for label, var in (
            ("图片", self.type_photo), ("视频", self.type_video), ("GIF", self.type_anim)
        ):
            ttk.Checkbutton(kinds, text=label, variable=var).pack(side="left", padx=4)
        ttk.Checkbutton(kinds, text="监控开启", variable=self.enabled).pack(side="left", padx=12)
        manual = ttk.Frame(monitors)
        manual.pack(fill="x", padx=5, pady=(2, 0))
        ttk.Label(manual, text="当前频道 ID/@用户名").pack(side="left")
        ttk.Entry(manual, textvariable=self.target, width=25).pack(side="left", padx=4)
        ttk.Label(manual, text="名称").pack(side="left", padx=4)
        ttk.Entry(manual, textvariable=self.channel_name, width=30).pack(side="left", padx=4)

        actions = ttk.Frame(monitors)
        actions.pack(fill="x", padx=6, pady=4)
        self.add_btn = ttk.Button(actions, text="添加当前频道", command=self._add_current_channel)
        self.add_btn.pack(side="left", padx=3)
        self.update_btn = ttk.Button(actions, text="更新所选", command=self._update_selected)
        self.update_btn.pack(side="left", padx=3)
        self.remove_btn = ttk.Button(actions, text="删除所选", command=self._remove_channel)
        self.remove_btn.pack(side="left", padx=3)
        self.toggle_btn = ttk.Button(actions, text="启用/暂停所选", command=self._toggle_selected)
        self.toggle_btn.pack(side="left", padx=3)
        ttk.Label(
            actions, text="两个账号监控同一频道时自动领取不同文件，扫描游标独立。"
        ).pack(side="left", padx=10)


        self._refresh_choices(DEFAULT_ACCOUNT)
        self._refresh_status()

    def _selected_id(self) -> str:
        return self._ids.get(self.choice.get(), DEFAULT_ACCOUNT)

    def _refresh_choices(self, selected: str) -> None:
        accounts = self.registry.accounts()
        self._ids = {f"{account_identity.label(a.id)}（{a.name}）": a.id for a in accounts}
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
        self._loaded_fields = {key: env.get(key, "") for key in self._fields}
        for key, value in self._loaded_fields.items():
            self._fields[key].set(value)
        self._channel_rows = load_channels(self.registry.monitor_path(account_id))
        self.tree.selection_remove(self.tree.selection())
        self._refresh_channel_tree()
        # The original account is controlled by the main window to avoid
        # stale Tk drafts overwriting live legacy settings.
        state = "disabled" if account_id == DEFAULT_ACCOUNT else "normal"
        self.save_settings_btn.configure(state=state)
        self.add_btn.configure(state=state)
        self.remove_btn.configure(state=state)
        self.update_btn.configure(state=state)
        self.toggle_btn.configure(state=state)
        self.dialog_keyword.set("")
        self._refresh_dialog_tree()
        self._ensure_dialogs()

    def _add_account(self) -> None:
        name = self.app.simpledialog.askstring(
            "添加 Telegram 账号", "账号备注名称", parent=self.win
        )
        if not name:
            return
        try:
            account = self.registry.add(name)
            account_identity.register(account.id, account.name)
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
        path = self.registry.env_path(account_id)
        try:
            # The main settings dialog can save this account while this window
            # stays open. Apply only fields actually edited in this form.
            values = read_env_values(path)
            edits = {}
            for key, var in self._fields.items():
                value = var.get()
                if value != self._loaded_fields.get(key, ""):
                    edits[key] = value if key == "proxy_password" else value.strip()
            values.update(edits)
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
            if edits:
                update_env(values, path)
        except (ValueError, OSError) as exc:
            self.app.messagebox.showwarning("账号设置无效", str(exc), parent=self.win)
            return False
        self._loaded_fields = {key: values[key] for key in self._fields}
        for key, value in self._loaded_fields.items():
            self._fields[key].set(value)
        if edits:
            self.app._log_account(account_id, "[账号设置] 已保存；代理变更重启账号生效\n")
        return True

    def _start_account(self, qr: bool = False) -> None:
        if self.app._closing:
            return
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
        window.win.after(500, lambda: self._poll_qr_login(window, runtime))

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
        # Update in place; clearing Treeview every second would reset unsaved
        # editor values via <<TreeviewSelect>>.
        expected = {str(i) for i in range(len(self._channel_rows))}
        for iid in self.tree.get_children():
            if iid not in expected:
                self.tree.delete(iid)
        account_id = self._selected_id()
        runtime = self.app._extra_runtimes.get(account_id)
        order = sorted(
            range(len(self._channel_rows)),
            key=lambda i: -int(self._channel_rows[i].get("priority", 0)),
        )
        for position, index in enumerate(order):
            item = self._channel_rows[index]
            next_scan = "-"
            if runtime is not None and runtime.client is not None and item.get("enabled", True):
                due = runtime._next_at.get(item["target"])
                if due:
                    import time
                    next_scan = time.strftime("%H:%M:%S", time.localtime(due))
            values = (
                item.get("name") or "（待获取）",
                item["target"], self.app._monitor_types_label(item.get("types", "photo,video")),
                item["minutes"], item.get("priority", 0), item.get("batch_size", 100),
                "旧→新" if item.get("direction") == "oldest" else "新→旧",
                "启用" if item.get("enabled", True) else "暂停", next_scan,
            )
            iid = str(index)
            if self.tree.exists(iid):
                if tuple(map(str, values)) != tuple(self.tree.item(iid, "values")):
                    self.tree.item(iid, values=values)
                self.tree.move(iid, "", position)
            else:
                self.tree.insert("", position, iid=iid, values=values)

    def _channel_selected(self) -> None:
        selected = self.tree.selection()
        if not selected:
            return
        row = self._channel_rows[int(selected[0])]
        self.target.set(str(row["target"]))
        self.channel_name.set(row.get("name", ""))
        self.minutes.set(str(row.get("minutes", 5)))
        self.priority.set(str(row.get("priority", 0)))
        self.batch_size.set(str(row.get("batch_size", 100)))
        self.direction.set("从旧到新" if row.get("direction") == "oldest" else "从新到旧")
        kinds = {item.strip() for item in row.get("types", "photo,video").split(",")}
        self.type_photo.set("photo" in kinds)
        self.type_video.set("video" in kinds)
        self.type_anim.set("animation" in kinds)
        self.enabled.set(row.get("enabled", True))

    def _monitor_form(self) -> tuple[int, int, int, str]:
        try:
            minutes = int(self.minutes.get().strip())
            priority = int(self.priority.get().strip())
            batch_size = int(self.batch_size.get().strip())
        except ValueError as exc:
            raise ValueError("扫描间隔、优先级、每轮数量必须填写整数") from exc
        if not 1 <= minutes <= 1440 or not 1 <= batch_size <= 100000:
            raise ValueError("扫描间隔应为1～1440分钟，每轮数量应为1～100000")
        types = ",".join(kind for kind, var in (
            ("photo", self.type_photo), ("video", self.type_video),
            ("animation", self.type_anim),
        ) if var.get())
        if not types:
            raise ValueError("至少选择一种监控下载类型")
        return minutes, priority, batch_size, types

    def _add_current_channel(self) -> None:
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            self.app.messagebox.showinfo(
                "原有账号", "请在主窗口『多频道监控』管理原账号", parent=self.win
            )
            return
        # Clicking the joined-channel list fills the same fields as manual
        # input; never override a subsequently edited ID with stale selection.
        target = self.target.get().strip()
        name = self.channel_name.get().strip()
        if not target:
            self.app.messagebox.showwarning("监控", "请先在上方列表选择频道，或手动输入频道ID", parent=self.win)
            return
        if any(row["target"] == target for row in self._channel_rows):
            self.app.messagebox.showinfo("监控", "此频道已在监控列表中，请选择后点击『更新所选』", parent=self.win)
            return
        self._upsert_channel(target, name)

    def _upsert_channel(self, target: str, name: str, *, index: int | None = None) -> None:
        try:
            minutes, priority, batch_size, types = self._monitor_form()
            account_id = self._selected_id()
            row = {
                "target": target, "name": name, "minutes": minutes,
                "priority": priority, "batch_size": batch_size, "types": types,
                "direction": "oldest" if self.direction.get() == "从旧到新" else "newest",
                "enabled": self.enabled.get(),
            }
            if index is None:
                self._channel_rows.append(row)
            else:
                original = self._channel_rows[index]
                row["target"] = original["target"]
                row["name"] = original.get("name") or name
                if original.get("peer_id"):
                    row["peer_id"] = original["peer_id"]
                self._channel_rows[index] = row
            save_channels(self._channel_rows, self.registry.monitor_path(account_id))
            self._refresh_channel_tree()
            self.app._log_account(account_id, f"[频道配置] 已保存 {row['target']}\n")
        except (ValueError, OSError) as exc:
            self.app.messagebox.showwarning("监控配置无效", str(exc), parent=self.win)

    def _update_selected(self) -> None:
        chosen = self.tree.selection()
        if self._selected_id() != DEFAULT_ACCOUNT and chosen:
            index = int(chosen[0])
            if index < len(self._channel_rows):
                row = self._channel_rows[index]
                self._upsert_channel(row["target"], row.get("name", ""), index=index)

    def _toggle_selected(self) -> None:
        chosen = self.tree.selection()
        if self._selected_id() == DEFAULT_ACCOUNT or not chosen:
            return
        row = self._channel_rows[int(chosen[0])]
        row["enabled"] = not row.get("enabled", True)
        save_channels(self._channel_rows, self.registry.monitor_path(self._selected_id()))
        self._refresh_channel_tree()

    def _remove_channel(self) -> None:
        chosen = self.tree.selection()
        if self._selected_id() == DEFAULT_ACCOUNT or not chosen:
            return
        self._channel_rows.pop(int(chosen[0]))
        save_channels(self._channel_rows, self.registry.monitor_path(self._selected_id()))
        self._refresh_channel_tree()

    def _dialog_selected(self, _event=None) -> None:
        chosen = self.dialog_tree.selection()
        if chosen:
            values = self.dialog_tree.item(chosen[0], "values")
            self.target.set(str(values[0]))
            self.channel_name.set(str(values[2]))

    def _refresh_dialog_tree(self) -> None:
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            # The main window may currently display B's dialogs.
            rows = getattr(self.app, "_default_dialog_rows",
                           getattr(self.app, "_dialog_rows", []))
        else:
            rows = self._dialog_cache.get(account_id, [])
        selected = self.dialog_tree.selection()
        previous = str(selected[0]) if selected else None
        self.dialog_tree.delete(*self.dialog_tree.get_children())
        visible = filter_dialogs(rows, self.dialog_keyword.get())
        for row in visible:
            if hasattr(row, "entity_id"):
                target, kind, title = str(row.entity_id), row.kind, row.title
            else:
                target, kind, title = str(row[0]), row[1], row[2]
            self.dialog_tree.insert("", "end", iid=target, values=(target, kind, title))
            if target == previous:
                self.dialog_tree.selection_set(target)
        self.dialog_counter.set(
            f"{len(visible)} / {len(rows)}"
            if account_id == DEFAULT_ACCOUNT or account_id in self._dialog_cache
            else "账号登录后加载"
        )

    def _ensure_dialogs(self, *, force: bool = False) -> None:
        """Fetch a connected account's joined dialogs once; switching reuses RAM."""
        from tgdl.channels import list_media_dialogs
        account_id = self._selected_id()
        if account_id == DEFAULT_ACCOUNT:
            if force:
                if self.app.client is not None:
                    # Always request through A's client, even if the main
                    # window is currently displaying account B.
                    self.app._run_task(self.app._load_dialogs(), None)
                else:
                    self.app.messagebox.showinfo(
                        "频道列表", "请先在主窗口登录原账号", parent=self.win
                    )
            self._refresh_dialog_tree()
            return
        if account_id in self._dialog_loading:
            return
        if not force and account_id in self._dialog_cache:
            self._refresh_dialog_tree()
            return
        if not force and account_id in self._dialog_retry_at:
            import time
            if time.monotonic() < self._dialog_retry_at[account_id]:
                return
        runtime = self.app._extra_runtimes.get(account_id)
        if runtime is None or runtime.client is None or not runtime.status.startswith("已连接"):
            if force:
                self.app.messagebox.showinfo(
                    "频道列表", "请先登录该 Telegram 账号", parent=self.win
                )
            self._refresh_dialog_tree()
            return
        self._dialog_loading.add(account_id)
        self.dialog_counter.set("正在加载...")

        async def fetch():
            context_token = current_account_id.set(account_id)
            try:
                return await list_media_dialogs(runtime.client)
            finally:
                current_account_id.reset(context_token)

        future = self.app.loop.submit(fetch())

        def finished(done):
            try:
                channels = done.result()
                error = None
            except Exception as exc:
                channels, error = None, str(exc)

            def apply():
                self._dialog_loading.discard(account_id)
                if channels is not None:
                    self._dialog_cache[account_id] = channels
                    self.app._log_account(
                        account_id, f"[频道列表] 已加载 {len(channels)} 个已加入的群组/频道\n"
                    )
                elif error is not None:
                    import time
                    self._dialog_retry_at[account_id] = time.monotonic() + 60
                    self.app._log_account(account_id, f"[频道列表] 加载失败: {error}\n")
                if self.win.winfo_exists() and self._selected_id() == account_id:
                    self._refresh_dialog_tree()
            self.app.root.after(0, apply)

        future.add_done_callback(finished)

    def _refresh_display_names(self) -> None:
        current_id = self._selected_id()
        refreshed = {
            f"{account_identity.label(a.id)}（{a.name}）": a.id
            for a in self.registry.accounts()
        }
        if refreshed != self._ids:
            self._ids = refreshed
            self.selector.configure(values=list(refreshed))
            for title, account_id in refreshed.items():
                if account_id == current_id:
                    self.choice.set(title)
                    break
            self.app._refresh_log_accounts()

    def _refresh_status(self) -> None:
        if not self.win.winfo_exists():
            return
        self._refresh_display_names()
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
                all_state.append(f"{account_identity.label(account.id)}: {runtime.active_count} 文件")
        self.status.set(
            f"当前：{selected_state}   |   其他账号："
            + ("；".join(all_state) if all_state else "无运行账号")
        )
        self._ensure_dialogs()
        self._refresh_channel_tree()
        self.win.after(1000, self._refresh_status)
