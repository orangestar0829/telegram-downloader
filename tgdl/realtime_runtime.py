"""Bind push ingestion to the existing GUI account/monitor lifecycle."""
from __future__ import annotations

import asyncio
import sqlite3

from tgdl.history import DownloadHistory


class RealtimeDispatch:
    """Loop-local readiness and wake-up signal, not a copy of readiness.

    A reconnect, permission failure or mode edit must gate the very next claim,
    even before the supervisor next checks its local configuration snapshot.
    """
    def __init__(self):
        self.service = None
        self.queued = asyncio.Event()

    def __contains__(self, peer):
        return self.service is not None and peer in self.service.ready_peers

    async def wait(self, delay):
        try:
            await asyncio.wait_for(self.queued.wait(), delay)
        except asyncio.TimeoutError:
            pass
        finally:
            self.queued.clear()


async def supervise(*, account_id, slots, ready, get_client, active, finished,
                    path, log, generation=lambda: None):
    # This interval checks only local lifecycle/config; no Telegram polling.
    from tgdl.realtime import RealtimeMonitor

    service = history = client = previous_generation = None

    async def close():
        nonlocal service, history
        ready.service = None
        try:
            if service is not None:
                await service.close()
        finally:
            service = None
            if history is not None:
                history.close()
                history = None

    try:
        while not finished():
            try:
                current_generation = generation()
                current_client = get_client()
                rows = slots.streams(account_id) if active() else ()
                if (service is not None and (not rows or current_client is not client
                                             or current_generation is not previous_generation)):
                    await close()
                if rows and current_client is not None:
                    if service is None:
                        history = DownloadHistory(path, busy_timeout_ms=25)
                        client, previous_generation = current_client, current_generation

                        def current(item, owner_generation=current_generation, owner_client=client):
                            return (active() and generation() is owner_generation and
                                    get_client() is owner_client and slots.matches(account_id, item))

                        service = RealtimeMonitor(client, history, account_id, log=log,
                                                  is_current=current, on_queued=ready.queued.set)
                        ready.service = service
                    service.update(rows)  # idempotent; also detects manual reconnect of this client
                await asyncio.sleep(0.2)
            except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
                await close()
                log(f"[实时监控] 监听暂不可用：{type(exc).__name__}；5 秒后重试")
                await asyncio.sleep(5)
    finally:
        await close()
