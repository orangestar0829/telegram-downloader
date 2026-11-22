"""Monitor publication must be owned by the current durable lease generation."""
import asyncio
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from tgdl import downloader, history as ledger
from tgdl.history import DownloadHistory, PendingMedia

PEER = 100
MESSAGE = 12
BASE = "20261122_12"
PAYLOAD = b"full payload"


class FakeClient:
    def __init__(self, handoff=None):
        self.calls = 0
        self.handoff = handoff

    async def download_media(self, target, *, file, progress_callback):
        self.calls += 1
        path = Path(file)
        path.write_bytes(PAYLOAD)
        if self.handoff:
            self.handoff()
        return path


@pytest.fixture
def work(tmp_path, monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(ledger, "time", SimpleNamespace(time=lambda: clock.now))
    db = tmp_path / "history.sqlite3"
    folder = tmp_path / "media"
    folder.mkdir()
    first = DownloadHistory(db)
    later = DownloadHistory(db)
    first.enqueue_media([PendingMedia(PEER, MESSAGE, "@target", "target", "video", BASE)])
    token = first.try_claim("A", PEER, MESSAGE, ttl=1.0)
    assert token
    try:
        yield first, later, folder, clock, token, db
    finally:
        first.close()
        later.close()


def _download(client, history, folder, token, *, account="A", on_saved=None,
              on_claim_committed=None):
    msg = SimpleNamespace(id=MESSAGE, media=SimpleNamespace(
        document=SimpleNamespace(size=len(PAYLOAD), mime_type="video/mp4"),
    ))
    stats = downloader.DownloadStats()

    async def run():
        path = await downloader._download_with_retry(
            client, msg, folder, BASE, stats, skip_existing=True, retries=2,
            history=history, peer_id=PEER, account_id=account, claim_token=token,
            on_saved=on_saved, on_claim_committed=on_claim_committed,
        )
        return path, stats

    return asyncio.run(run())


def _downloaded_by(db):
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            "SELECT account_id, original_path FROM downloaded_media WHERE peer_id=? AND message_id=?",
            (PEER, MESSAGE),
        ).fetchone()
        return row


def _handoff(later, clock, account="B"):
    clock.now += 2.0
    token = later.try_claim(account, PEER, MESSAGE)
    assert token
    return token


@pytest.mark.parametrize("account", ["A", "B"])
def test_expired_completed_transfer_cannot_replace_new_attempt(work, account):
    first, later, folder, clock, token, db = work
    newer = []
    successful = []
    client = FakeClient(lambda: newer.append(_handoff(later, clock, account)))

    with pytest.raises(downloader.DownloadLeaseLost):
        _download(client, first, folder, token,
                  on_saved=successful.append, on_claim_committed=lambda: successful.append("A"))
    assert client.calls == 1
    assert successful == []
    assert later.owns_claim(account, PEER, MESSAGE, token=newer[0])
    assert later.pending_count() == 1
    assert _downloaded_by(db) is None
    assert list(folder.iterdir()) == []

    completed, stats = _download(FakeClient(), later, folder, newer[0], account=account)
    assert stats.saved == 1
    assert completed.read_bytes() == PAYLOAD
    assert _downloaded_by(db) == (account, str(completed))


def test_cross_volume_copy_is_fenced_after_awaiting(work, monkeypatch):
    first, later, folder, clock, token, db = work
    monkeypatch.setattr(downloader, "same_volume", lambda source, destination: False)
    sleep = asyncio.sleep
    newer = []

    async def takeover_while_copying(delay):
        if delay == 0 and not newer:
            newer.append(_handoff(later, clock))
        await sleep(delay)

    monkeypatch.setattr(downloader.asyncio, "sleep", takeover_while_copying)
    with pytest.raises(downloader.DownloadLeaseLost):
        _download(FakeClient(), first, folder, token)
    assert later.owns_claim("B", PEER, MESSAGE, token=newer[0])
    assert _downloaded_by(db) is None
    assert list(folder.iterdir()) == []


def test_stale_attempt_cannot_adopt_a_valid_existing_file(work, monkeypatch):
    first, later, folder, clock, token, db = work
    existing = folder / f"{BASE}.mp4"
    existing.write_bytes(PAYLOAD)
    newer = []

    def find_then_lose_lease(*args):
        newer.append(_handoff(later, clock))
        return existing

    monkeypatch.setattr(downloader, "_existing_media", find_then_lose_lease)
    client = FakeClient()
    with pytest.raises(downloader.DownloadLeaseLost):
        _download(client, first, folder, token)
    assert later.owns_claim("B", PEER, MESSAGE, token=newer[0])
    assert _downloaded_by(db) is None
    assert later.pending_count() == 1
    assert client.calls == 0
    assert existing.read_bytes() == PAYLOAD


def test_valid_existing_file_adoption_consumes_its_own_claim(work):
    first, later, folder, clock, token, db = work
    existing = folder / f"{BASE}.mp4"
    existing.write_bytes(PAYLOAD)
    client = FakeClient()
    committed = []
    result, stats = _download(
        client, first, folder, token,
        on_claim_committed=lambda: committed.append(_downloaded_by(db)),
    )
    assert result is None and stats.skipped == 1 and client.calls == 0
    assert not later.owns_claim("A", PEER, MESSAGE, token=token)
    assert later.pending_count() == 0
    assert committed == [("A", str(existing))]


def test_changed_existing_file_does_not_consume_the_valid_lease(work, monkeypatch):
    first, later, folder, clock, token, db = work
    existing = folder / f"{BASE}.mp4"
    existing.write_bytes(PAYLOAD)

    def candidate_changes_after_lookup(*args):
        existing.write_bytes(b"")
        return existing

    monkeypatch.setattr(downloader, "_existing_media", candidate_changes_after_lookup)
    client = FakeClient()
    result, stats = _download(client, first, folder, token)
    assert result.name == f"{BASE}_1.mp4"
    assert existing.read_bytes() == b""
    assert result.read_bytes() == PAYLOAD
    assert stats.saved == 1 and client.calls == 1
    assert _downloaded_by(db) == ("A", str(result))
