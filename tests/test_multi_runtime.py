"""Secondary-account workers must respect monitoring eligibility and global claims."""
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from telethon.tl.types import Channel, Chat, ChatPhotoEmpty

from tgdl.accounts import Account, AccountRegistry
from tgdl.config import update_env
from tgdl.history import DownloadHistory, PendingMedia
from tgdl.monitor import save_channels
from tgdl import multi_runtime
from tgdl.account_context import current_account_id
from tgdl.account_identity import account_identity
from tgdl.gui import _QueueWriter
from tgdl.log_format import TimestampedLines
import queue


class FakeClient:
    def __init__(self, identity):
        self.identity = identity

    async def get_messages(self, entity, ids):
        return SimpleNamespace(
            id=ids, date=datetime(2026, 11, 22, tzinfo=timezone.utc),
        )


async def resolve(_client, target):
    return Channel(123, "Shared supergroup", ChatPhotoEmpty(), None, megagroup=True)


def create_runtime(registry, account_id, target, enabled):
    account = Account(account_id, account_id)
    update_env({
        "api_id": "111", "api_hash": "hash", "download_dir": str(registry.root),
        "media_concurrency": "1", "media_retries": "0",
        "chunk_concurrency": "1", "media_speed_kib": "0",
    }, registry.env_path(account.id))
    save_channels([{
        "target": target, "minutes": 5, "priority": 100,
        "batch_size": 100, "enabled": enabled, "types": "video",
        "direction": "newest", "name": "Shared",
    }], registry.monitor_path(account.id))
    runtime = multi_runtime.AccountMonitorRuntime(
        account, registry, ask=lambda _prompt, _secret: "",
        on_log=lambda _id, _line: None,
        on_saved=lambda _id, _line: None,
        on_progress=lambda *_args: None,
    )
    runtime.client = FakeClient(account_id)
    assert runtime.start_monitoring()
    runtime.peer_by_target[target] = 123
    return runtime


def test_two_accounts_take_different_files_across_target_aliases(tmp_path, monkeypatch):
    registry = AccountRegistry(tmp_path)
    account_a = create_runtime(registry, "acc-a", "@public", True)
    account_b = create_runtime(registry, "acc-b", "-100123", True)
    history = DownloadHistory(tmp_path / "tgdl_download_history.sqlite3")
    history.enqueue_media([
        PendingMedia(123, mid, "@public", "Shared", "video", str(mid))
        for mid in range(1, 9)
    ])
    history.close()
    seen = []

    async def fake_download(client, message, folder, base, stats, *, history, peer_id, **kwargs):
        seen.append((client.identity, message.id))
        await asyncio.sleep(0.04)
        assert history.mark_claim_saved(
            kwargs["account_id"], peer_id, message.id, "video", Path(base),
            token=kwargs["claim_token"],
        )
        kwargs["on_claim_committed"]()
        return Path(base)

    monkeypatch.setattr(multi_runtime, "resolve_channel", resolve)
    monkeypatch.setattr(multi_runtime, "classify_message", lambda _message: "video")
    monkeypatch.setattr(multi_runtime, "_download_with_retry", fake_download)
    settings = SimpleNamespace(download_dir=tmp_path, temp_download_dir=None,
                               media_speed_kib=0, media_concurrency=1,
                               media_retries=0, chunk_concurrency=1)

    async def scenario():
        a = asyncio.create_task(account_a._download_loop(settings))
        b = asyncio.create_task(account_b._download_loop(settings))
        try:
            for _ in range(100):
                db = DownloadHistory(tmp_path / "tgdl_download_history.sqlite3")
                count = db.pending_count()
                db.close()
                if count == 0:
                    break
                await asyncio.sleep(0.05)
            assert count == 0
        finally:
            account_a.stop()
            account_b.stop()
            await asyncio.wait_for(asyncio.gather(a, b), timeout=8)

    asyncio.run(scenario())
    assert sorted(mid for _account, mid in seen) == list(range(1, 9))
    assert {name for name, _mid in seen} == {"acc-a", "acc-b"}


def test_account_without_enabled_monitor_never_downloads(tmp_path, monkeypatch):
    registry = AccountRegistry(tmp_path)
    inactive = create_runtime(registry, "acc-b", "@same", False)
    history = DownloadHistory(tmp_path / "tgdl_download_history.sqlite3")
    history.enqueue_media([PendingMedia(123, 10, "@same", "Same", "photo", "10")])
    history.close()

    async def scenario():
        settings = SimpleNamespace(download_dir=tmp_path, temp_download_dir=None,
                                   media_speed_kib=0, media_concurrency=1,
                                   media_retries=0, chunk_concurrency=1)
        task = asyncio.create_task(inactive._download_loop(settings))
        await asyncio.sleep(0.6)
        inactive.stop()
        await asyncio.wait_for(task, timeout=3)
    asyncio.run(scenario())
    history = DownloadHistory(tmp_path / "tgdl_download_history.sqlite3")
    assert history.pending_count() == 1
    assert not history.active_claims()
    history.close()


def test_print_context_marks_secondary_account():
    queue_out = queue.Queue()
    writer = _QueueWriter(queue_out, TimestampedLines())
    token = current_account_id.set("acc-b")
    try:
        writer.write("download started\n")
    finally:
        current_account_id.reset(token)
    assert "[账号:" + account_identity.label("acc-b") + "]" in queue_out.get_nowait().text
    writer.write("legacy\n")
    assert "[账号:原账号（未登录）]" in queue_out.get_nowait().text


def test_account_tag_is_not_repeated_when_print_splits_newline():
    out = queue.Queue()
    writer = _QueueWriter(out, TimestampedLines())
    token = current_account_id.set("acc-b")
    try:
        writer.write("progress 100%")
        writer.write("\n")
    finally:
        current_account_id.reset(token)
    assert "".join(out.get_nowait().text for _ in range(2)).count("[账号:" + account_identity.label("acc-b") + "]") == 1



def test_secondary_transfer_refuses_a_basic_group_with_old_queue(tmp_path, monkeypatch):
    registry = AccountRegistry(tmp_path)
    runtime = create_runtime(registry, "acc-b", "-123", True)
    history = DownloadHistory(tmp_path / "tgdl_download_history.sqlite3")
    job = PendingMedia(-123, 10, "-123", "Basic group", "photo", "10")
    history.enqueue_media([job])
    token = history.try_claim(runtime.account.id, job.peer_id, job.message_id)
    assert token is not None
    messages_requested = []

    async def basic_group(_client, _target):
        return Chat(123, "Basic group", ChatPhotoEmpty(), 2, None, 1)

    async def get_messages(*_args, **_kwargs):
        messages_requested.append(True)
        raise AssertionError("should not read a basic-group message from another account")

    runtime.client.get_messages = get_messages
    monkeypatch.setattr(multi_runtime, "resolve_channel", basic_group)
    asyncio.run(runtime._transfer(history, {"target": "-123"}, job, None, None, 0, 1, token))

    assert not messages_requested
    assert not history.active_claims()
    assert history.pending_count() == 1  # remains available to the original account
    assert "-123" in runtime._blocked
    history.close()


def test_connected_secondary_monitor_stops_drains_and_cancels_without_logout(tmp_path, monkeypatch):
    from tgdl.config import Settings

    registry = AccountRegistry(tmp_path)
    runtime = create_runtime(registry, "acc-b", "@shared", True)
    runtime.stop_monitoring()  # The state of a newly authenticated account.
    connected_client = runtime.client
    source = tmp_path / "old-destination"
    destination = tmp_path / "new-destination"
    history = DownloadHistory(tmp_path / "tgdl_download_history.sqlite3")
    history.enqueue_media([PendingMedia(123, mid, "@shared", "Shared", "video", str(mid))
                           for mid in (1, 2)])
    monkeypatch.setattr(multi_runtime, "resolve_channel", resolve)
    monkeypatch.setattr(multi_runtime, "classify_message", lambda _message: "video")
    settings = Settings(api_id=1, api_hash="test", phone=None, download_dir=source,
                        media_concurrency=1, media_retries=0, chunk_concurrency=1)

    async def scenario():
        first_started = asyncio.Event()
        first_can_finish = asyncio.Event()
        second_started = asyncio.Event()
        second_cancelled = asyncio.Event()

        async def fake_download(client, message, folder, _base, _stats, *, history, peer_id, **kwargs):
            assert client is connected_client
            if message.id == 1:
                assert folder.is_relative_to(source)
                first_started.set()
                await first_can_finish.wait()
                saved = folder / "file.mp4"
                assert history.mark_claim_saved(
                    kwargs["account_id"], peer_id, message.id, "video", saved,
                    token=kwargs["claim_token"],
                )
                kwargs["on_claim_committed"]()
                return saved
            assert folder.is_relative_to(destination)
            second_started.set()
            try:
                await asyncio.Future()
            finally:
                second_cancelled.set()

        monkeypatch.setattr(multi_runtime, "_download_with_retry", fake_download)
        worker = asyncio.create_task(runtime._download_loop(settings))

        async def drained():
            while runtime.active_count:
                await asyncio.sleep(0.01)

        try:
            await asyncio.sleep(0)
            assert not history.active_claims()
            assert not first_started.is_set()
            assert runtime.start_monitoring()
            runtime.peer_by_target["@shared"] = 123
            await asyncio.wait_for(first_started.wait(), timeout=2)

            runtime.stop_monitoring()
            assert runtime.monitoring_draining
            assert not runtime.stop_event.is_set()
            assert not runtime.start_monitoring()
            first_can_finish.set()
            await asyncio.wait_for(drained(), timeout=2)
            assert history.contains(123, 1)
            assert history.pending_count() == 1
            assert not second_started.is_set()
            assert runtime.client is connected_client

            runtime.set_download_paths(destination, tmp_path / "new-staging")
            assert runtime.start_monitoring()
            runtime.peer_by_target["@shared"] = 123
            await asyncio.wait_for(second_started.wait(), timeout=2)
            runtime.cancel_transfers()
            await asyncio.wait_for(second_cancelled.wait(), timeout=2)
            await asyncio.wait_for(drained(), timeout=2)
            assert not runtime.monitoring
            assert not runtime.stop_event.is_set()
            assert runtime.client is connected_client
            assert history.pending_count() == 1
            assert not history.active_claims()
        finally:
            runtime.stop(cancel=True)
            await asyncio.wait_for(worker, timeout=2)

    try:
        asyncio.run(scenario())
    finally:
        history.close()
