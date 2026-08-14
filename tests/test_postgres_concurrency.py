"""
P0-1 proof: concurrent store calls on production-mode stores must not crash.

Today every Postgres store shares one async connection, and psycopg3 async
connections are single-session — issuing two commands concurrently raises
``ProgrammingError: cannot execute commands: already executing``. The runtime
hits this because ``_execute_one_step_impl`` runs ``renew_loop`` (which calls
``leases.renew`` / ``resources.renew``) concurrently with the step body's
``snapshots.save`` / ``control.set_state`` on that same connection.

After the pool refactor, each store call resolves its own connection from a
shared pool, so concurrent calls no longer share a session.

These tests need a live Postgres. They are gated behind
``ROOST_TEST_POSTGRES_URL`` and skip when it is unset, so ``uv run pytest``
stays green in environments without Postgres (matching the existing CI test
job, which installs only the redis extra). The CI workflow that installs the
postgres extra sets the env var so these run there.
"""
from __future__ import annotations

import asyncio
import os
import uuid

import pytest

pg_url = os.environ.get("ROOST_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    not pg_url,
    reason="set ROOST_TEST_POSTGRES_URL to run Postgres-backed tests",
)


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
    """Ensure the schema exists for the duration of the test session."""
    from roost.runtime.backends.postgres import apply_migrations

    apply_migrations(pg_url)  # type: ignore[arg-type]
    yield


async def test_concurrent_renew_and_save_do_not_collide(migrated, pool):
    """
    The P0-1 bug: leases.renew and snapshots.save issued concurrently on
    production stores must both succeed. On the pre-fix shared-connection
    code this raises ``ProgrammingError: cannot execute commands: already
    executing``.
    """
    from roost.runtime.backends.postgres import (
        PostgresLeaseStore,
        PostgresSnapshotStore,
        PostgresWorkItemStore,
    )
    from roost.runtime.models import Snapshot, WorkItem

    work_id = f"conc-{uuid.uuid4().hex[:8]}"
    work_items = PostgresWorkItemStore(pool)
    leases = PostgresLeaseStore(pool)
    snapshots = PostgresSnapshotStore(pool)

    # FK target first: leases/snapshots reference roost_work_items.
    await work_items.put(WorkItem(work_id=work_id, engine="demo"))

    # Plant a lease that renew() can hit, and a snapshot row at version 1.
    planted = await leases.try_acquire(work_id, "worker-a", ttl_seconds=300)
    assert planted is not None
    seed = Snapshot(work_id=work_id, engine="demo", version=1, step="init")
    ok = await snapshots.save(seed, expected_version=0)
    assert ok

    # Now fire a renew and a save concurrently — the exact interleaving the
    # runtime produces (renew_loop ‖ step body). Both must succeed.
    renewed, saved = await asyncio.gather(
        leases.renew(planted, ttl_seconds=300),
        snapshots.save(seed.model_copy(update={"version": 2, "step": "s1"}), expected_version=1),
    )

    assert renewed is True
    assert saved is True

    latest = await snapshots.load(work_id)
    assert latest is not None and latest.version == 2


async def test_concurrent_lease_renew_and_resource_renew(migrated, pool):
    """
    Second concurrent pair the runtime issues: leases.renew and
    resources.renew from the same renew_loop. Both go through _borrow and
    must each check out their own connection.
    """
    from roost.runtime.backends.postgres import (
        PostgresLeaseStore,
        PostgresResourceStore,
        PostgresWorkItemStore,
    )
    from roost.runtime.models import WorkItem

    work_id = f"res-{uuid.uuid4().hex[:8]}"
    work_items = PostgresWorkItemStore(pool)
    leases = PostgresLeaseStore(pool)
    resources = PostgresResourceStore(pool)

    await work_items.put(WorkItem(work_id=work_id, engine="demo"))

    planted = await leases.try_acquire(work_id, "worker-b", ttl_seconds=300)
    assert planted is not None
    owner = f"{work_id}:worker-b:{planted.lease_id}"
    resource_keys = [f"{work_id}-res-A", f"{work_id}-res-B"]
    acquired = await resources.acquire(resources=resource_keys, owner_value=owner, ttl_seconds=300)
    assert acquired is True

    # Concurrent renew of lease + resources — mirrors renew_loop's body.
    lease_ok, res_ok = await asyncio.gather(
        leases.renew(planted, ttl_seconds=300),
        resources.renew(resources=resource_keys, owner_value=owner, ttl_seconds=300),
    )
    assert lease_ok is True
    assert res_ok is True
