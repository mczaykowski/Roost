"""
P0-2 proof: the durable writes of a step must be atomic.

Before the per-step transaction, each Postgres store committed independently
(``commit=True`` on every call). A failure between ``snapshots.save`` and
``control.set_state`` left durable state internally inconsistent — the
snapshot said ``done`` while the meta still said ``running``. For a system
whose pitch is "durable system of record," there was no transaction boundary
around its unit of durability.

After the refactor, the runtime opens one transaction
(``_step_transaction``) for the durable write window and passes that
connection to every store call. This test exercises that boundary directly
against the stores: writes within a passed connection commit together, and
an exception between writes rolls the whole thing back so meta and snapshot
can never disagree.

Gated behind ``ROOST_TEST_POSTGRES_URL`` (skips without a live Postgres).
"""
from __future__ import annotations

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
    from roost.runtime.backends.postgres import apply_migrations

    apply_migrations(pg_url)  # type: ignore[arg-type]
    yield


async def test_step_transaction_commits_together(migrated, pool):
    """Snapshot save + set_state on a shared connection commit together."""
    from roost.runtime.backends.postgres import PostgresControlPlaneStore, PostgresSnapshotStore
    from roost.runtime.models import Snapshot

    work_id = f"tx-ok-{uuid.uuid4().hex[:8]}"
    snapshots = PostgresSnapshotStore(pool)
    control = PostgresControlPlaneStore(pool)

    # Seed an item + meta so set_state has a row to update.
    from roost.runtime.models import WorkItem

    from roost.runtime.backends.postgres import PostgresWorkItemStore

    await PostgresWorkItemStore(pool).put(WorkItem(work_id=work_id, engine="demo"))
    await control.upsert_on_enqueue(WorkItem(work_id=work_id, engine="demo"), work_id)

    snap = Snapshot(work_id=work_id, engine="demo", version=1, step="init")
    assert await snapshots.save(snap, expected_version=0)

    conn = await pool.getconn()
    try:
        saved = await snapshots.save(
            snap.model_copy(update={"version": 2, "step": "s1", "is_finished": True, "finished_at": 1.0}),
            expected_version=1,
            conn=conn,
        )
        await control.set_state(
            work_id=work_id, engine="demo", state="done", step="s1", conn=conn
        )
        await conn.commit()
    finally:
        await pool.putconn(conn)

    assert saved is True
    latest = await snapshots.load(work_id)
    assert latest is not None and latest.version == 2 and latest.is_finished
    meta = await control.get_meta(work_id)
    assert meta is not None and meta["state"] == "done" and meta["step"] == "s1"


async def test_step_transaction_rolls_back_on_failure(migrated, pool):
    """
    The P0-2 bug: if a failure occurs after snapshots.save but before
    set_state, the snapshot write must NOT persist. Without a transaction
    boundary the save commits alone and durable state disagrees. With it,
    the rollback discards the save too.
    """
    from roost.runtime.backends.postgres import (
        PostgresControlPlaneStore,
        PostgresSnapshotStore,
        PostgresWorkItemStore,
    )
    from roost.runtime.models import Snapshot, WorkItem

    work_id = f"tx-rollback-{uuid.uuid4().hex[:8]}"
    snapshots = PostgresSnapshotStore(pool)
    control = PostgresControlPlaneStore(pool)
    work_items = PostgresWorkItemStore(pool)

    # Establish baseline: version 1 snapshot, meta queued.
    await work_items.put(WorkItem(work_id=work_id, engine="demo"))
    await control.upsert_on_enqueue(WorkItem(work_id=work_id, engine="demo"), work_id)
    snap = Snapshot(work_id=work_id, engine="demo", version=1, step="init")
    assert await snapshots.save(snap, expected_version=0)

    conn = await pool.getconn()
    with pytest.raises(RuntimeError, match="boom"):
        try:
            # Save version 2 inside the txn...
            await snapshots.save(
                snap.model_copy(update={"version": 2, "step": "s1", "is_finished": True}),
                expected_version=1,
                conn=conn,
            )
            # ...then fail before the matching set_state. This simulates a
            # crash between the durable writes.
            raise RuntimeError("boom")
        finally:
            # The runtime's _step_transaction calls rollback() on BaseException.
            await conn.rollback()
            await pool.putconn(conn)

    # Nothing committed: snapshot still at v1, meta still queued. They agree.
    latest = await snapshots.load(work_id)
    assert latest is not None and latest.version == 1 and not latest.is_finished
    meta = await control.get_meta(work_id)
    assert meta is not None and meta["state"] == "queued"
