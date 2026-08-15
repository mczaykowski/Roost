"""
P1-2 proof: link_child persists queryable children on roost_work_meta.

Production mode previously only wrote a work_child_linked event. get_meta
must return child_work_ids (Redis shape: list of dicts, most-recent-first,
capped) without scanning events.

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


async def _put_and_enqueue(pool, work_id: str):
    from roost.runtime.backends.postgres import (
        PostgresControlPlaneStore,
        PostgresWorkItemStore,
    )
    from roost.runtime.models import WorkItem

    item = WorkItem(work_id=work_id, engine="demo")
    await PostgresWorkItemStore(pool).put(item)
    await PostgresControlPlaneStore(pool).upsert_on_enqueue(item, work_id)
    return item


async def test_link_child_visible_on_get_meta_without_events(migrated, pool):
    from roost.runtime.backends.postgres import PostgresControlPlaneStore

    parent_id = f"parent-{uuid.uuid4().hex[:8]}"
    child_id = f"child-{uuid.uuid4().hex[:8]}"
    await _put_and_enqueue(pool, parent_id)
    await _put_and_enqueue(pool, child_id)

    control = PostgresControlPlaneStore(pool)
    await control.link_child(
        parent_work_id=parent_id,
        child_work_id=child_id,
        relation="spawned",
    )

    meta = await control.get_meta(parent_id)
    assert meta is not None
    children = meta["child_work_ids"]
    assert children == meta["children"]
    assert len(children) == 1
    assert children[0]["work_id"] == child_id
    assert children[0]["relation"] == "spawned"
    assert "at" in children[0]

    # Shape is on meta itself; callers must not need an event scan.
    events = await control.list_events(limit=50)
    linked = [event for event in events if event.get("kind") == "work_child_linked"]
    assert any(event.get("child_work_id") == child_id for event in linked)


async def test_link_child_most_recent_first_and_capped(migrated, pool):
    from roost.runtime.backends.postgres import PostgresControlPlaneStore

    parent_id = f"parent-{uuid.uuid4().hex[:8]}"
    await _put_and_enqueue(pool, parent_id)
    child_ids = []
    for _ in range(3):
        child_id = f"child-{uuid.uuid4().hex[:8]}"
        await _put_and_enqueue(pool, child_id)
        child_ids.append(child_id)

    control = PostgresControlPlaneStore(pool)
    for child_id in child_ids:
        await control.link_child(
            parent_work_id=parent_id,
            child_work_id=child_id,
            max_children=2,
        )

    meta = await control.get_meta(parent_id)
    assert meta is not None
    children = meta["child_work_ids"]
    assert [child["work_id"] for child in children] == [child_ids[2], child_ids[1]]


async def test_set_state_preserves_children(migrated, pool):
    from roost.runtime.backends.postgres import PostgresControlPlaneStore

    parent_id = f"parent-{uuid.uuid4().hex[:8]}"
    child_id = f"child-{uuid.uuid4().hex[:8]}"
    await _put_and_enqueue(pool, parent_id)
    await _put_and_enqueue(pool, child_id)

    control = PostgresControlPlaneStore(pool)
    await control.link_child(parent_work_id=parent_id, child_work_id=child_id)
    await control.set_state(work_id=parent_id, engine="demo", state="running", step="s1")

    meta = await control.get_meta(parent_id)
    assert meta is not None
    assert meta["state"] == "running"
    assert meta["child_work_ids"][0]["work_id"] == child_id
