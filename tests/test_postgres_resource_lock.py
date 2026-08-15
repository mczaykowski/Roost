"""
P2-1 proof: resource claims must be atomic under concurrency.

The previous acquire path was check-then-insert. Two workers could both see
no live claim and both insert. The replacement is a single INSERT ... ON
CONFLICT DO UPDATE ... WHERE expires_at <= now() OR same owner, RETURNING.
A missing RETURNING row means the claim is live under another owner.

Gated behind ``ROOST_TEST_POSTGRES_URL``.
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
    from roost.runtime.backends.postgres import apply_migrations

    apply_migrations(pg_url)  # type: ignore[arg-type]
    yield


async def test_concurrent_acquire_exactly_one_winner(migrated, pool):
    from roost.runtime.backends.postgres import PostgresResourceStore

    resources = PostgresResourceStore(pool)
    key = f"lock-{uuid.uuid4().hex[:8]}"

    first, second = await asyncio.gather(
        resources.acquire(resources=[key], owner_value="owner-a", ttl_seconds=300),
        resources.acquire(resources=[key], owner_value="owner-b", ttl_seconds=300),
    )

    assert sorted([first, second]) == [False, True]


async def test_same_owner_can_reacquire_and_renew(migrated, pool):
    from roost.runtime.backends.postgres import PostgresResourceStore

    resources = PostgresResourceStore(pool)
    key = f"lock-{uuid.uuid4().hex[:8]}"

    assert await resources.acquire(resources=[key], owner_value="owner-a", ttl_seconds=300)
    assert await resources.acquire(resources=[key], owner_value="owner-a", ttl_seconds=300)
    assert await resources.renew(resources=[key], owner_value="owner-a", ttl_seconds=300)
    assert not await resources.acquire(
        resources=[key], owner_value="owner-b", ttl_seconds=300
    )


async def test_expired_claim_can_be_taken_by_another_owner(migrated, pool):
    from roost.runtime.backends.postgres import PostgresResourceStore

    resources = PostgresResourceStore(pool)
    key = f"lock-{uuid.uuid4().hex[:8]}"

    assert await resources.acquire(resources=[key], owner_value="owner-a", ttl_seconds=0)

    conn = await pool.getconn()
    try:
        await conn.execute(
            """
            UPDATE roost_resource_claims
            SET expires_at = now() - interval '1 second'
            WHERE resource_key = %s
            """,
            (key,),
        )
        await conn.commit()
    finally:
        await pool.putconn(conn)

    assert await resources.acquire(resources=[key], owner_value="owner-b", ttl_seconds=300)
    assert not await resources.acquire(
        resources=[key], owner_value="owner-a", ttl_seconds=300
    )


async def test_multi_key_acquire_is_all_or_nothing(migrated, pool):
    from roost.runtime.backends.postgres import PostgresResourceStore

    resources = PostgresResourceStore(pool)
    suffix = uuid.uuid4().hex[:8]
    held = f"z-held-{suffix}"
    free = f"a-free-{suffix}"

    assert await resources.acquire(resources=[held], owner_value="owner-a", ttl_seconds=300)
    assert not await resources.acquire(
        resources=[free, held], owner_value="owner-b", ttl_seconds=300
    )
    # The failed multi-key attempt must not leave the free key claimed.
    assert await resources.acquire(resources=[free], owner_value="owner-c", ttl_seconds=300)
