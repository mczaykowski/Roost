"""
P0-3 proof: production recovery discovers work from Postgres, not Redis inflight.

A Redis flush that drops inflight keys must not hide work Postgres still
remembers. Gated behind ROOST_TEST_POSTGRES_URL.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

import pytest

from roost.runtime.backends.redis import RedisInflightStore, RedisKeys
from roost.runtime.models import Snapshot, WorkItem
from roost.runtime.stores import RuntimeStores
from roost.runtime.swarm import RedisSwarm, SwarmConfig, _RedisSwarmRuntime

pg_url = os.environ.get("ROOST_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    not pg_url,
    reason="set ROOST_TEST_POSTGRES_URL to run Postgres-backed tests",
)


class _DummyEngine:
    engine_id = "dummy"

    async def init_snapshot(self, item):  # pragma: no cover
        raise NotImplementedError

    async def step(self, snapshot, item):  # pragma: no cover
        raise NotImplementedError


class _EmptyRedis:
    async def scan_iter(self, _pattern: str):
        if False:
            yield ""

    async def exists(self, _key: str) -> int:
        return 0

    async def aclose(self) -> None:
        return None


class _RecordingQueue:
    def __init__(self) -> None:
        self.enqueued: list[dict[str, Any]] = []

    async def enqueue(self, function: str, **kwargs: Any) -> str:
        self.enqueued.append({"function": function, **kwargs})
        return str(kwargs.get("work_id") or "")


class _EmptyInflight:
    def __init__(self) -> None:
        self.marks: list[str] = []

    async def get(self, work_id: str):
        del work_id
        return None

    async def mark(self, work_id, payload, ttl_seconds):
        del payload, ttl_seconds
        self.marks.append(work_id)

    async def clear(self, work_id):
        del work_id


@pytest.fixture
async def pool():
    from roost.runtime.backends.postgres import connect_postgres_pool

    p = await connect_postgres_pool(pg_url)  # type: ignore[arg-type]
    try:
        yield p
    finally:
        await p.close()


@pytest.fixture
async def migrated(pool):
    from roost.runtime.backends.postgres import apply_migrations

    apply_migrations(pg_url)  # type: ignore[arg-type]
    yield


def _bind_production_runtime(pool, *, stale_after_seconds: float = 0.0) -> _RedisSwarmRuntime:
    from roost.runtime.backends.postgres import build_postgres_durable_stores

    durable = build_postgres_durable_stores(pool)
    inflight = _EmptyInflight()
    runtime = _RedisSwarmRuntime.__new__(_RedisSwarmRuntime)
    runtime.config = SwarmConfig(
        redis_url="redis://localhost:6379/0",
        runtime_mode="production",
        postgres_url=pg_url,
        stale_after_seconds=stale_after_seconds,
        redis_prefix=f"roost-rec-{uuid.uuid4().hex[:8]}",
    )
    runtime.worker_id = "recovery-test"
    runtime._postgres_pool = pool
    runtime.keys = RedisKeys(prefix=runtime.config.redis_prefix)
    runtime.redis = _EmptyRedis()
    runtime.queue = _RecordingQueue()
    runtime.inflight = inflight
    runtime.artifacts = durable.artifacts
    runtime.workers = durable.workers
    runtime._activate_stores(
        RuntimeStores(
            work_items=durable.work_items,
            snapshots=durable.snapshots,
            leases=durable.leases,
            resources=durable.resources,
            inflight=inflight,
            control=durable.control,
        )
    )
    return runtime


async def _plant_running_work(runtime, *, work_id: str, finished: bool = False) -> None:
    item = WorkItem(work_id=work_id, engine="dummy")
    await runtime.work_items.put(item)
    snap = Snapshot(
        work_id=work_id,
        engine="dummy",
        version=1,
        step="s1",
        is_finished=finished,
        updated_at=time.time() - 120,
    )
    assert await runtime.snapshots.save(snap, expected_version=0)
    await runtime.control.set_state(work_id=work_id, engine="dummy", state="running", step="s1")


async def test_recover_orphans_discovers_postgres_work_without_inflight(migrated, pool):
    """Redis inflight flush must not hide work Postgres still remembers."""
    runtime = _bind_production_runtime(pool, stale_after_seconds=0.0)
    work_id = f"rec-{uuid.uuid4().hex[:8]}"
    await _plant_running_work(runtime, work_id=work_id)

    recovered = await runtime.recover_orphans_once()
    assert recovered >= 1
    assert any(job.get("work_id") == work_id for job in runtime.queue.enqueued)


async def test_recover_orphans_skips_active_lease(migrated, pool):
    runtime = _bind_production_runtime(pool, stale_after_seconds=0.0)
    work_id = f"live-{uuid.uuid4().hex[:8]}"
    await _plant_running_work(runtime, work_id=work_id)
    planted = await runtime.leases.try_acquire(work_id, "worker-live", ttl_seconds=300)
    assert planted is not None

    await runtime.recover_orphans_once()
    assert not any(job.get("work_id") == work_id for job in runtime.queue.enqueued)


async def test_recover_orphans_skips_finished_snapshot(migrated, pool):
    runtime = _bind_production_runtime(pool, stale_after_seconds=0.0)
    work_id = f"done-{uuid.uuid4().hex[:8]}"
    await _plant_running_work(runtime, work_id=work_id, finished=True)

    await runtime.recover_orphans_once()
    assert not any(job.get("work_id") == work_id for job in runtime.queue.enqueued)


async def test_production_store_types_do_not_swap_after_ensure(migrated, pool):
    """P2-4: durable store types are bound once and never swapped from Redis."""
    del pool
    from roost.runtime.backends.postgres import PostgresLeaseStore

    config = SwarmConfig(
        redis_url="redis://127.0.0.1:9/0",
        runtime_mode="production",
        postgres_url=pg_url,
        redis_prefix=f"roost-p24-{uuid.uuid4().hex[:8]}",
    )
    runtime = RedisSwarm(_DummyEngine(), config=config)
    try:
        assert runtime.stores is None
        await runtime._ensure_runtime_stores()
        assert runtime.stores is not None
        lease_type = type(runtime.stores.leases)
        assert issubclass(lease_type, PostgresLeaseStore)
        await runtime._ensure_runtime_stores()
        assert type(runtime.stores.leases) is lease_type
        assert isinstance(runtime.inflight, RedisInflightStore)
    finally:
        await runtime.close()
