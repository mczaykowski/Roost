"""Postgres durable storage support."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from .manager import Migration, apply_migrations, check_migrations, list_migrations
from .stores import (
    PostgresArtifactMetadataStore,
    PostgresControlPlaneStore,
    PostgresDurableStores,
    PostgresLeaseStore,
    PostgresOperatorActionStore,
    PostgresResourceStore,
    PostgresSnapshotStore,
    PostgresWorkerHeartbeatStore,
    PostgresWorkItemStore,
    build_postgres_durable_stores,
)


@asynccontextmanager
async def open_postgres_pool(conninfo: str, *, max_size: int = 4) -> AsyncIterator:
    """
    Open a short-lived connection pool for a one-shot command.

    CLI commands and the UI server are one-shot / per-request callers: they
    don't have the concurrency problem the runtime has, but they still need a
    pool because the stores now resolve connections per call. This opens a
    small pool, yields it, and closes it on exit. Use as::

        async with open_postgres_pool(url) as pool:
            control = PostgresControlPlaneStore(pool)
            ...
    """
    pool = await connect_postgres_pool(conninfo, max_size=max_size)
    try:
        yield pool
    finally:
        await pool.close()


async def connect_postgres_pool(conninfo: str, *, max_size: int = 4):
    """
    Open and return a connection pool (caller closes it).

    Use when a ``try/finally`` (rather than ``async with``) owns the lifecycle,
    which is how the CLI commands are structured. Pair the call with
    ``await pool.close()`` in the ``finally`` block.
    """
    from psycopg_pool import AsyncConnectionPool

    pool = AsyncConnectionPool(conninfo=conninfo, min_size=1, max_size=max_size, open=False)
    await pool.open()
    return pool


__all__ = [
    "Migration",
    "PostgresArtifactMetadataStore",
    "PostgresControlPlaneStore",
    "PostgresDurableStores",
    "PostgresLeaseStore",
    "PostgresOperatorActionStore",
    "PostgresResourceStore",
    "PostgresSnapshotStore",
    "PostgresWorkerHeartbeatStore",
    "PostgresWorkItemStore",
    "apply_migrations",
    "build_postgres_durable_stores",
    "check_migrations",
    "list_migrations",
    "open_postgres_pool",
]
