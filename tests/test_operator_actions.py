"""
P1-3 proof: operator actions are written to roost_operator_actions.

The table shipped in 0001 with zero writers. Cancel/retry/dlq commands now
record a row after the action succeeds. Listing returns it.

Gated behind ``ROOST_TEST_POSTGRES_URL``.
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


async def test_record_cancel_shaped_row_and_list(migrated, pool):
    from roost.runtime.backends.postgres import PostgresOperatorActionStore

    store = PostgresOperatorActionStore(pool)
    work_id = f"op-{uuid.uuid4().hex[:8]}"
    recorded = await store.record(
        "cancel",
        work_id,
        "cli",
        {"reason": "test", "engine": "demo"},
    )

    assert recorded["action"] == "cancel"
    assert recorded["work_id"] == work_id
    assert recorded["actor"] == "cli"
    assert recorded["payload"]["reason"] == "test"
    assert recorded["id"] is not None

    rows = await store.list_for_work(work_id, limit=50)
    assert len(rows) == 1
    assert rows[0]["action"] == "cancel"
    assert rows[0]["actor"] == "cli"

    recent = await store.list_recent(limit=50)
    assert any(row["id"] == recorded["id"] for row in recent)
