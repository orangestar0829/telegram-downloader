"""SSD staging budget concurrency and cancellation tests."""
import asyncio
from types import SimpleNamespace

import pytest

from tgdl import cache_budget


def test_capacity_queues_then_releases(tmp_path, monkeypatch):
    budget = cache_budget._CacheBudget(capacity=10, reserve=5)
    monkeypatch.setattr(cache_budget.shutil, "disk_usage", lambda _: SimpleNamespace(free=1000))

    async def run():
        await budget.acquire(tmp_path, 7)
        waiting = asyncio.create_task(budget.acquire(tmp_path, 6))
        await asyncio.sleep(0.02)
        assert not waiting.done()
        assert budget.active_bytes == 7
        await budget.release(7)
        await asyncio.wait_for(waiting, timeout=1)
        assert budget.active_bytes == 6
        await budget.release(6)
        assert budget.active_bytes == 0

    asyncio.run(run())


def test_waiter_cancel_does_not_consume_capacity(tmp_path, monkeypatch):
    budget = cache_budget._CacheBudget(capacity=10, reserve=5)
    monkeypatch.setattr(cache_budget.shutil, "disk_usage", lambda _: SimpleNamespace(free=1000))

    async def run():
        await budget.acquire(tmp_path, 8)
        first = asyncio.create_task(budget.acquire(tmp_path, 4))
        second = asyncio.create_task(budget.acquire(tmp_path, 3))
        await asyncio.sleep(0.02)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await budget.release(8)
        await asyncio.wait_for(second, timeout=1)
        assert budget.active_bytes == 3
        await budget.release(3)
        assert budget.active_bytes == 0

    asyncio.run(run())


def test_disk_reserve_waits_for_external_space(tmp_path, monkeypatch):
    budget = cache_budget._CacheBudget(capacity=20, reserve=10)
    free = [12]
    monkeypatch.setattr(cache_budget.shutil, "disk_usage", lambda _: SimpleNamespace(free=free[0]))
    monkeypatch.setattr(cache_budget, "SPACE_POLL_SECONDS", 0.02)

    async def run():
        waiting = asyncio.create_task(budget.acquire(tmp_path, 6))
        await asyncio.sleep(0.04)
        assert not waiting.done()
        assert budget.active_bytes == 0
        free[0] = 20
        await asyncio.wait_for(waiting, timeout=1)
        assert budget.active_bytes == 6
        await budget.release(6)

    asyncio.run(run())


def test_oversized_file_fails_without_queue(tmp_path):
    budget = cache_budget._CacheBudget(capacity=20, reserve=10)

    async def run():
        with pytest.raises(cache_budget.CacheFileTooLarge, match="超过 SSD 缓存上限"):
            await budget.acquire(tmp_path, 21)
        assert budget.active_bytes == 0

    asyncio.run(run())


def test_reservation_releases_after_exception(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_budget.shutil, "disk_usage", lambda _: SimpleNamespace(free=10**12))
    cache_budget._budgets.clear()

    async def run():
        with pytest.raises(RuntimeError, match="simulated"):
            async with cache_budget.staging_reservation(tmp_path, 100):
                raise RuntimeError("simulated")
        budget = cache_budget._budgets[cache_budget._volume_key(tmp_path)]
        assert budget.active_bytes == 0

    asyncio.run(run())
    cache_budget._budgets.clear()


def test_orphaned_temp_file_counts_against_capacity(tmp_path, monkeypatch):
    budget = cache_budget._CacheBudget(capacity=10, reserve=0)
    orphan = tmp_path / "tgdl-abc123_a"
    orphan.mkdir()
    (orphan / "data.mp4").write_bytes(b"x" * 9)
    monkeypatch.setattr(cache_budget.shutil, "disk_usage", lambda _: SimpleNamespace(free=100))
    monkeypatch.setattr(cache_budget, "SPACE_POLL_SECONDS", 0.01)

    async def run():
        task = asyncio.create_task(budget.acquire(tmp_path, 2))
        await asyncio.sleep(0.03)
        assert not task.done()
        (orphan / "data.mp4").unlink()
        orphan.rmdir()
        await asyncio.wait_for(task, timeout=1)
        assert budget.active_bytes == 2
        await budget.release(2)

    asyncio.run(run())


def test_lease_untracks_only_its_own_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(cache_budget.shutil, "disk_usage", lambda _: SimpleNamespace(free=10**12))
    cache_budget._budgets.clear()

    async def run():
        async with cache_budget.staging_reservation(tmp_path, 10) as lease1:
            async with cache_budget.staging_reservation(tmp_path, 10) as lease2:
                first = tmp_path / "tgdl-first"
                second = tmp_path / "tgdl-second"
                first.mkdir()
                second.mkdir()
                lease1.track(first)
                lease2.track(second)
                budget = lease1.budget
                assert first.resolve() in budget._owned_directories
                assert second.resolve() in budget._owned_directories
            assert first.resolve() in budget._owned_directories
            assert second.resolve() not in budget._owned_directories
        assert not budget._owned_directories
        assert budget.active_bytes == 0

    asyncio.run(run())
    cache_budget._budgets.clear()

def test_regular_media_under_staging_root_does_not_consume_cache(tmp_path, monkeypatch):
    budget = cache_budget._CacheBudget(capacity=10, reserve=0)
    for relative in ("tgdl-by-peer/123/videos", "tgdl-news/videos", "tgdl-abcdefgh/videos"):
        folder = tmp_path / relative
        folder.mkdir(parents=True)
        (folder / "video.mp4").write_bytes(b"x" * 17)
    monkeypatch.setattr(cache_budget.shutil, "disk_usage", lambda _: SimpleNamespace(free=100))

    async def run():
        await asyncio.wait_for(budget.acquire(tmp_path, 2), timeout=1)
        assert budget.active_bytes == 2
        await budget.release(2)

    asyncio.run(run())
