"""Transient in-memory QR login dialog for a secondary Telegram account."""
from __future__ import annotations

from datetime import datetime


class QrLoginWindow:
    def __init__(self, app, account_name: str, on_cancel):
        from PIL import ImageTk

        self.app = app
        self.ImageTk = ImageTk
        self.on_cancel = on_cancel
        tk, ttk = app.tk, app.ttk
        self.win = tk.Toplevel(app.root)
        self.win.title(f"Telegram 扫码登录：{account_name}")
        self.win.geometry("440x490")
        self.win.resizable(False, False)
        self.win.protocol("WM_DELETE_WINDOW", self.cancel)
        self._image = None
        self._closed = False
        ttk.Label(self.win, text=f"请用 {account_name} 当前已登录的 Telegram 扫码",
                  font=("Arial", 12, "bold")).pack(pady=(18, 8))
        ttk.Label(self.win, text="手机 Telegram → 设置 → 设备 → 连接桌面设备",
                  wraplength=400).pack()
        self.image_label = ttk.Label(self.win)
        self.image_label.pack(pady=12)
        self.info = ttk.Label(
            self.win, text="等待生成临时二维码……", wraplength=390,
        )
        self.info.pack(pady=3)
        ttk.Button(self.win, text="取消扫码登录", command=self.cancel).pack(pady=9)

    def on_qr(self, url: str, expires: datetime) -> None:
        # Called on the Telegram event loop. All Tk and Pillow UI work must
        # happen on the main thread. URLs are never written to disk/logs.
        self.app.root.after(0, lambda: self._display(url, expires))

    def _display(self, url: str, expires: datetime) -> None:
        if self._closed or not self.win.winfo_exists():
            return
        from tgdl.qr_auth import qr_image

        image = qr_image(url)
        self._image = self.ImageTk.PhotoImage(image)
        self.image_label.configure(image=self._image)
        self.info.configure(text="等待手机确认登录。二维码失效后会自动刷新。")

    def cancel(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.on_cancel()
        if self.win.winfo_exists():
            self.win.destroy()

    def done(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self.win.winfo_exists():
            self.win.destroy()
        self._image = None
