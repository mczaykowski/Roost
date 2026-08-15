"""OPS-4: wait-only steps must not increment snapshot version.

A real observation still CAS-saves. Gated behind ROOST_TEST_POSTGRES_URL.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

import pytest

from roost.runtime.backends.redis import RedisKeys
from roost.runtime.models import Snapshot, WorkItem
from roost.runtime.stores import RuntimeStores
from roost.runtime.swarm import _RedisSwarmRuntime, SwarmConfig

pg_url = os.environ.get("ROOST_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    not pg_url,
    reason="set ROOST_TEST_POSTGRES_URL to run Postgres-backed tests",
)


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
    async def get(self, work_id: str):
        del work_id
        return None

    async def mark(self, work_id, payload, ttl_seconds):
        del work_id, payload, ttl_seconds

    async def clear(self, work_id):
        del work_id


class _WaitOnlyEngine:
    engine_id = "dummy"

    async def init_snapshot(self, item):  # pragma: no cover
        del item
        raise AssertionError("snapshot should already exist")

    async def step(self, snapshot, item):
        del item
        new = snapshot.model_copy()
        new.next_step_delay_seconds = 5.0
        return new


class _ObserveEngine:
    engine_id = "dummy"

    async def init_snapshot(self, item):  # pragma: no cover
        del item
        raise AssertionError("snapshot should already exist")

    async def step(self, snapshot, item):
        del item
        new = snapshot.model_copy()
        data = dict(snapshot.data)
        observations = list(data.get("observations") or [])
        observations.append({"ok": True, "observed_at": time.time()})
        data["observations"] = observations
        data["checks_completed"] = len(observations)
        new.data = data
        new.next_step_delay_seconds = 5.0
        return new


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


def _bind_production_runtime(pool) -> _RedisSwarmRuntime:
    from roost.runtime.backends.postgres import build_postgres_durable_stores

    durable = build_postgres_durable_stores(pool)
    inflight = _EmptyInflight()
    runtime = _RedisSwarmRuntime.__new__(_RedisSwarmRuntime)
    runtime.config = SwarmConfig(
        redis_url="redis://localhost:6379/0",
        runtime_mode="production",
        postgres_url=pg_url,
        redis_prefix=f"roost-wait-{uuid.uuid4().hex[:8]}",
        lease_ttl_seconds=60,
    )
    runtime.worker_id = "wait-only-test"
    runtime._postgres_pool = pool
    runtime.keys = RedisKeys(prefix=runtime.config.redis_prefix)
    runtime.redis = _EmptyRedis()
    runtime.queue = _RecordingQueue()
    runtime.inflight = inflight
    runtime.artifacts = None
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


async def _plant(runtime, *, work_id: str) -> Snapshot:
    item = WorkItem(work_id=work_id, engine="dummy")
    await runtime.work_items.put(item)
    snap = Snapshot(
        work_id=work_id,
        engine="dummy",
        version=1,
        step="check",
        data={"checks_completed": 1, "observations": [{"ok": True}]},
        updated_at=time.time() - 5,
    )
    assert await runtime.snapshots.save(snap, expected_version=0)
    await runtime.control.set_state(work_id=work_id, engine="dummy", state="running", step="check")
    loaded = await runtime.snapshots.load(work_id)
    assert loaded is not None
    return loaded


async def test_wait_only_step_does_not_increment_version(migrated, pool):
    runtime = _bind_production_runtime(pool)
    work_id = f"wait-{uuid.uuid4().hex[:8]}"
    planted = await _plant(runtime, work_id=work_id)

    result = await runtime._execute_one_step_impl(
        work_id=work_id,
        item=WorkItem(work_id=work_id, engine="dummy"),
        engine=_WaitOnlyEngine(),
    )
    assert result["info"] == "wait_only"
    latest = await runtime.snapshots.load(work_id)
    assert latest is not None
    assert latest.version == planted.version
    assert latest.data["checks_completed"] == 1
    assert runtime.queue.enqueued
    assert runtime.queue.enqueued[-1]["work_id"] == work_id


async def test_observation_step_still_increments_version(migrated, pool):
    runtime = _bind_production_runtime(pool)
    work_id = f"obs-{uuid.uuid4().hex[:8]}"
    planted = await _plant(runtime, work_id=work_id)

    result = await runtime._execute_one_step_impl(
        work_id=work_id,
        item=WorkItem(work_id=work_id, engine="dummy"),
        engine=_ObserveEngine(),
    )
    assert result["status"] == "success"
    assert result.get("info") != "wait_only"
    latest = await runtime.snapshots.load(work_id)
    assert latest is not None
    assert latest.version == planted.version + 1
    assert latest.data["checks_completed"] == 2
    assert len(latest.data["observations"]) == 2
