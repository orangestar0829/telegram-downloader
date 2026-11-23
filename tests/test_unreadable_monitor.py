"""Account-local suppression for definitive empty Telegram metadata replies."""
import asyncio
import sqlite3
import threading
from collections import deque
from contextlib import closing
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from telethon.errors import ChannelPrivateError, FloodWaitError
from telethon.tl.types import Channel, ChatPhotoEmpty, Message, MessageEmpty, MessageMediaPhoto, PeerChannel

from tgdl import gui, history as ledger, multi_runtime
from tgdl.accounts import Account, AccountRegistry
from tgdl.history import DownloadHistory, PendingMedia, UnreadableRetry
from tgdl.monitor import MonitorUnreadableSummary
from tgdl.realtime_runtime import RealtimeDispatch
from tgdl.slots import SlotCoordinator, make_plan

PEER = 345
MID = 77
JOB = PendingMedia(PEER, MID, "@photos", "Channel name", "photo", "20261123_77")
FIRST = 3600
SECOND = 6 * 3600
THIRD = 24 * 3600


def state(path, account="A", peer=PEER, mid=MID):
    with closing(sqlite3.connect(path)) as conn:
        return conn.execute(
            """SELECT misses, retry_after FROM account_unreadable_media
               WHERE account_id=? AND peer_id=? AND message_id=?""",
            (account, peer, mid),
        ).fetchone()


def claim(db, account, now, *, ttl=600):
    token = db.try_claim(account, PEER, MID, now=now, ttl=ttl)
    assert token is not None
    return token


def miss(db, account, now):
    return db.finish_unreadable_claim(account, PEER, MID, token=claim(db, account, now), now=now)


def test_retry_steps_persist_and_only_hide_this_account_after_fourth(tmp_path, monkeypatch):
    path = tmp_path / "history.db"
    clock = SimpleNamespace(now=1000.0)
    monkeypatch.setattr(ledger, "time", SimpleNamespace(time=lambda: clock.now))
    # Create the exact prior generic-backoff schema before opening the new DB.
    # It survives migration, but never seeds an unreadable streak.
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("""CREATE TABLE account_attempt_backoff (
            account_id TEXT NOT NULL, peer_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL, retry_after REAL NOT NULL,
            PRIMARY KEY (account_id,peer_id,message_id)
        )""")
        conn.execute("INSERT INTO account_attempt_backoff VALUES (?,?,?,?)",
                     ("A", PEER, MID, clock.now + 60))
    db = DownloadHistory(path)
    db.enqueue_media([JOB])
    assert db.try_claim("A", PEER, MID, now=clock.now) is None
    assert state(path) is None
    clock.now += 60
    assert miss(db, "A", clock.now) == UnreadableRetry(1, FIRST)
    assert db.pending_for_peer(PEER, account_id="A") == []
    assert db.pending_for_target(JOB.target, account_id="A") == []
    assert db.pending_for_peer(PEER, account_id="B") == [JOB]
    assert db.try_claim("A", PEER, MID, now=clock.now + FIRST - 1) is None
    db.close()

    db = DownloadHistory(path)
    try:
        clock.now += FIRST
        assert miss(db, "A", clock.now) == UnreadableRetry(2, SECOND)
        assert db.try_claim("A", PEER, MID, now=clock.now + SECOND - 1) is None
        clock.now += SECOND
        assert miss(db, "A", clock.now) == UnreadableRetry(3, THIRD)
        assert state(path) == (3, clock.now + THIRD)
        assert db.try_claim("A", PEER, MID, now=clock.now + THIRD - 1) is None
        db.close()
        db = DownloadHistory(path)
        assert db.pending_for_peer(PEER, account_id="B") == [JOB]
        clock.now += THIRD
        assert miss(db, "A", clock.now) == UnreadableRetry(4, None)
        clock.now += 1_000_000
        assert db.pending_for_target(JOB.target, account_id="A") == []
        assert db.pending_for_peer(PEER, account_id="A") == []
        assert db.try_claim("A", PEER, MID, now=clock.now) is None
        assert db.pending_count() == 1
        assert db.pending_for_target(JOB.target) == [JOB]  # diagnostics are global
        assert db.pending_for_target(JOB.target, account_id="B") == [JOB]
        assert db.pending_for_peer(PEER, account_id="B") == [JOB]
        assert state(path, "B") is None

        other_job = PendingMedia(PEER + 1, MID, "@different", "Other", "photo", JOB.base)
        db.enqueue_media([other_job])
        assert db.pending_for_peer(PEER + 1, account_id="A") == [other_job]

        assert miss(db, "B", clock.now) == UnreadableRetry(1, FIRST)
        clock.now += FIRST
        media = tmp_path / "saved.jpg"
        media.write_bytes(b"actual file")
        assert db.mark_claim_saved("B", PEER, MID, "photo", media, token=claim(db, "B", clock.now))
        assert db.contains(PEER, MID)
        assert media.read_bytes() == b"actual file"
        assert state(path, "A") is None and state(path, "B") is None
    finally:
        db.close()


def test_same_account_stale_generation_neither_resets_nor_increments(tmp_path):
    path = tmp_path / "history.db"
    db = DownloadHistory(path)
    other_connection = DownloadHistory(path)
    try:
        db.enqueue_media([JOB])
        assert miss(db, "A", 0) == UnreadableRetry(1, FIRST)
        stale = claim(db, "A", FIRST, ttl=1)
        current = claim(other_connection, "A", FIRST + 2)
        assert current != stale
        assert db.finish_unreadable_claim("A", PEER, MID, token=stale, now=FIRST + 2) is None
        assert not db.record_readable_message("A", PEER, MID, token=stale, now=FIRST + 2)
        assert other_connection.owns_claim("A", PEER, MID, token=current, now=FIRST + 2)
        assert state(path) == (1, FIRST)
        assert other_connection.finish_unreadable_claim("A", PEER, MID, token=current, now=FIRST + 2) == UnreadableRetry(2, SECOND)
    finally:
        db.close()
        other_connection.close()


def test_read_immediately_resets_before_later_payload_lease_expiry(tmp_path):
    path = tmp_path / "history.db"
    db = DownloadHistory(path)
    try:
        db.enqueue_media([JOB])
        assert miss(db, "A", 0) == UnreadableRetry(1, FIRST)
        old = claim(db, "A", FIRST, ttl=2)
        assert db.record_readable_message("A", PEER, MID, token=old, now=FIRST)
        assert state(path) is None
        assert db.owns_claim("A", PEER, MID, token=old, now=FIRST)
        # Simulate process death or an expired payload without a final release.
        assert miss(db, "A", FIRST + 2) == UnreadableRetry(1, FIRST)
    finally:
        db.close()


def test_summary_aggregates_by_peer_without_invites_or_message_identifiers():
    summary = MonitorUnreadableSummary()
    job = PendingMedia(PEER, MID, "https://t.me/+private-key", "Channel name", "photo", JOB.base)
    next_job = PendingMedia(PEER, MID + 1, "@another_alias", "Renamed", "photo", JOB.base)
    summary.add(job, UnreadableRetry(1, FIRST), now=1)
    assert not summary.drain(now=1.9)
    summary.add(next_job, UnreadableRetry(1, FIRST), now=2)
    assert not summary.drain(now=2.9)
    summary.add(job, UnreadableRetry(3, THIRD), now=3)
    assert not summary.drain(now=3.9)
    summary.add(job, UnreadableRetry(4, None), now=4)
    assert not summary.drain(now=8.9)
    lines = summary.drain(now=9)
    assert len(lines) == 1 and "Channel name" in lines[0]
    assert "第1次 2项，1小时后可重试" in lines[0]
    assert "第3次 1项，24小时后可重试" in lines[0]
    assert "连续4次 1项，已移出本账号自动队列" in lines[0]
    assert "private-key" not in lines[0] and "77" not in lines[0]
    assert not summary.drain(force=True)


def test_summary_does_not_starve_other_channels_or_delay_a_busy_one_forever():
    summary = MonitorUnreadableSummary()
    quiet_job = PendingMedia(PEER + 1, MID, "@different", "Quiet channel", "photo", JOB.base)
    summary.add(quiet_job, UnreadableRetry(2, SECOND), now=0)
    written = []
    for second in range(30):
        summary.add(JOB, UnreadableRetry(1, FIRST), now=second)
        written.extend(summary.drain(now=second))
    assert len(written) == 1 and "Quiet channel" in written[0]
    # The next miss arrives before the trailing quiet time, but the active
    # channel must emit by the 30-second cap anyway.
    summary.add(JOB, UnreadableRetry(1, FIRST), now=30)
    lines = summary.drain(now=30)
    assert len(lines) == 1 and "第1次 31项" in lines[0]
    summary.add(JOB, UnreadableRetry(1, FIRST), now=31)
    assert not summary.drain(now=31)
    lines = summary.drain(now=31, force=True)
    assert len(lines) == 1 and "第1次 1项" in lines[0]


async def wait_until(predicate, timeout=3):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.002)
    await asyncio.wait_for(poll(), timeout)


@pytest.mark.parametrize("account", ["default", "secondary"])
@pytest.mark.parametrize("mode", ["scheduled", "realtime"])
def test_monitor_workers_distinguish_empty_reply_rpc_failure_and_payload_failure(
    tmp_path, monkeypatch, capsys, account, mode,
):
    path = tmp_path / "tgdl_download_history.sqlite3"
    clock = SimpleNamespace(now=1000.0)
    monkeypatch.setattr(ledger, "time", SimpleNamespace(time=lambda: clock.now))
    channel = Channel(PEER, JOB.title, ChatPhotoEmpty(), None, megagroup=True)
    dated = Message(id=MID, peer_id=PeerChannel(PEER), date=datetime.now(timezone.utc),
                    message="", media=MessageMediaPhoto())
    empty = MessageEmpty(id=MID, peer_id=PeerChannel(PEER))
    failures = [OSError("network read failure"), FloodWaitError(request=None, capture=90)]
    if mode == "realtime":
        failures.append(ChannelPrivateError(request=None))
    responses = deque((None, *failures, dated, empty, None, empty, None))
    seen, payload_counts, summaries = [], [], []

    class Client:
        async def get_entity(self, peer):
            assert isinstance(peer, PeerChannel) and peer.channel_id == PEER
            return channel

        async def get_messages(self, entity, ids):
            assert entity is channel and ids == MID
            reply = responses.popleft()
            seen.append(reply)
            if isinstance(reply, Exception):
                raise reply
            return reply

        async def download_media(self, _message, **_kwargs):
            payload_counts.append(state(path, account))
            raise OSError("payload network failure")

    async def resolve(_client, _target):
        return channel

    async def ensure():
        pass

    row = {"target": JOB.target, "peer_id": PEER, "enabled": True, "types": "photo", "mode": mode}
    rows = [row]
    slots = SlotCoordinator()
    slots.configure(make_plan([(account, 1, rows)]))
    settings = SimpleNamespace(
        download_dir=tmp_path, temp_download_dir=None, media_speed_kib=0,
        media_concurrency=1, media_retries=0, chunk_concurrency=1,
    )
    dispatch = RealtimeDispatch()
    dispatch.service = SimpleNamespace(ready_peers={PEER})
    if account == "default":
        app = object.__new__(gui._App)
        app.client = Client()
        app.settings = settings
        app._monitored = rows
        app._monitor_download_stop = threading.Event()
        app._download_future = None
        app._monitor_progress_state = lambda *_: None
        app._update_download_progress = lambda *_: None
        app._log_download = lambda *_: None
        app._ensure_client = ensure
        app._slots = slots
        app._realtime_ready = dispatch
        monkeypatch.setattr(gui, "DownloadHistory", lambda: DownloadHistory(path))
        monkeypatch.setattr(gui, "resolve_channel", resolve)
        worker, stop = app._monitor_download_worker(1, 0, 1), app._monitor_download_stop.set
    else:
        runtime = multi_runtime.AccountMonitorRuntime(
            Account(account, "Secondary"), AccountRegistry(tmp_path), ask=None,
            on_log=lambda _, text: summaries.append(text), on_saved=lambda *_: None,
            on_progress=lambda *_: None,
        )
        runtime.client = Client()
        runtime._slots = slots
        runtime._realtime_ready = dispatch
        monkeypatch.setattr(runtime, "_channels", lambda: rows)
        monkeypatch.setattr(multi_runtime, "resolve_channel", resolve)
        monkeypatch.setattr(multi_runtime, "read_env_values", lambda _: {
            "media_concurrency": "1", "media_retries": "0",
            "chunk_concurrency": "1", "media_speed_kib": "0",
        })
        assert runtime.start_monitoring()
        runtime.peer_by_target[JOB.target] = PEER
        worker, stop = runtime._download_loop(settings), runtime.stop

    db = DownloadHistory(path)
    if mode == "realtime":
        db.start_realtime(account, PEER, "test", MID - 1)
        assert db.commit_realtime(account, PEER, "test", MID, [JOB]) == (1, MID)
    else:
        db.enqueue_media([JOB])

    async def scenario():
        task = asyncio.create_task(worker)
        try:
            expected = [1] * (1 + len(failures)) + [None, 1, 2, 3, 4]
            for i, misses in enumerate(expected, 1):
                if i > 1:
                    clock.now += THIRD + 1
                    dispatch.queued.set()
                await wait_until(lambda: len(seen) >= i and not db.active_claims(), timeout=4)
                recorded = state(path, account)
                assert (recorded[0] if recorded else None) == misses
                assert not db.contains(PEER, MID)
            # The dated success must clear the old streak before payload I/O,
            # not merely because a file was eventually recorded.
            assert payload_counts == [None]
            assert db.pending_count() == 1
            assert db.pending_for_peer(PEER, account_id=account) == []
            assert db.pending_for_peer(PEER, account_id="different") == [JOB]
        finally:
            stop()
            dispatch.queued.set()
            await asyncio.wait_for(task, 3)

    try:
        asyncio.run(scenario())
        log = capsys.readouterr().out if account == "default" else "\n".join(summaries)
        assert "第3次 1项，24小时后可重试" in log
        assert "连续4次 1项，已移出本账号自动队列" in log
    finally:
        db.close()
