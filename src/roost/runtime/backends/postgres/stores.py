from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncIterator, Optional
import uuid

from roost.runtime.models import Artifact, Lease, Snapshot, WorkItem
from roost.runtime.stores import ControlPlaneStore, LeaseStore, ResourceStore, SnapshotStore, WorkItemStore


def _require_jsonb() -> Any:
    try:
        from psycopg.types.json import Jsonb
    except Exception as exc:
        raise RuntimeError(
            "Missing Postgres runtime dependency. Install with:\n"
            "  uv sync --extra postgres"
        ) from exc
    return Jsonb


def _ts(epoch_seconds: float | None) -> datetime | None:
    if epoch_seconds is None:
        return None
    return datetime.fromtimestamp(float(epoch_seconds), UTC)


def _epoch(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    return float(value)


def _child_work_ids(meta: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not meta:
        return []
    children = meta.get("child_work_ids")
    if children is None:
        children = meta.get("children")
    if not isinstance(children, list):
        return []
    return [child for child in children if isinstance(child, dict)]


class _ResourceAcquireConflict(Exception):
    """One key in a multi-key acquire lost; `_borrow` rolls the rest back."""


@asynccontextmanager
async def _borrow(pool: Any, conn: Any) -> AsyncIterator[tuple[Any, bool]]:
    """
    Resolve a connection for a single store call.

    When ``conn`` is provided it belongs to the caller's transaction: use it
    as-is and do NOT commit or close (the caller owns the transaction
    boundary). When ``conn`` is None, check one out of the pool, autocommit
    the call, and put it back. This is what keeps the concurrent ``renew_loop``
    and the step body from sharing a single Postgres session.
    """
    if conn is not None:
        yield conn, False
        return
    own = await pool.getconn()
    try:
        yield own, True
        await own.commit()
    except BaseException:
        await own.rollback()
        raise
    finally:
        await pool.putconn(own)


class PostgresWorkItemStore(WorkItemStore):
    def __init__(self, pool: Any):
        self._pool = pool

    async def put(self, item: WorkItem, ttl_seconds: int = 7 * 24 * 3600) -> None:
        del ttl_seconds
        await self._upsert(item, conn=None)

    async def get(self, work_id: str, *, conn: Any = None) -> Optional[WorkItem]:
        async with _borrow(self._pool, conn) as (c, _):
            row = await (await c.execute("SELECT raw FROM roost_work_items WHERE work_id = %s", (work_id,))).fetchone()
        if not row:
            return None
        return WorkItem.model_validate(row[0])

    async def get_or_claim_work_id(
        self, item: WorkItem, ttl_seconds: int = 7 * 24 * 3600, *, conn: Any = None
    ) -> str:
        del ttl_seconds
        if not item.idempotency_key:
            await self._upsert(item, conn=conn)
            return item.work_id

        async with _borrow(self._pool, conn) as (c, _):
            row = await (
                await c.execute(
                    """
                    INSERT INTO roost_work_items (
                      work_id, engine, payload, priority, resources, created_at,
                      deadline_at, idempotency_key, raw
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING work_id
                    """,
                    self._params(item),
                )
            ).fetchone()
            if row:
                work_id = str(row[0])
            else:
                existing = await (
                    await c.execute(
                        "SELECT work_id FROM roost_work_items WHERE idempotency_key = %s",
                        (item.idempotency_key,),
                    )
                ).fetchone()
                work_id = str(existing[0]) if existing else item.work_id
        return work_id

    async def _upsert(self, item: WorkItem, *, conn: Any = None) -> None:
        async with _borrow(self._pool, conn) as (c, _):
            await c.execute(
                """
                INSERT INTO roost_work_items (
                  work_id, engine, payload, priority, resources, created_at,
                  deadline_at, idempotency_key, raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (work_id) DO UPDATE SET
                  engine = EXCLUDED.engine,
                  payload = EXCLUDED.payload,
                  priority = EXCLUDED.priority,
                  resources = EXCLUDED.resources,
                  deadline_at = EXCLUDED.deadline_at,
                  idempotency_key = EXCLUDED.idempotency_key,
                  raw = EXCLUDED.raw
                """,
                self._params(item),
            )

    def _params(self, item: WorkItem) -> tuple[Any, ...]:
        Jsonb = _require_jsonb()
        raw = item.model_dump(mode="json")
        return (
            item.work_id,
            item.engine,
            Jsonb(item.payload),
            item.priority,
            list(item.resources),
            _ts(item.created_at),
            _ts(item.deadline_at),
            item.idempotency_key,
            Jsonb(raw),
        )


class PostgresSnapshotStore(SnapshotStore):
    def __init__(self, pool: Any):
        self._pool = pool

    async def load(self, work_id: str, *, conn: Any = None) -> Optional[Snapshot]:
        async with _borrow(self._pool, conn) as (c, _):
            row = await (
                await c.execute("SELECT raw FROM roost_snapshots WHERE work_id = %s", (work_id,))
            ).fetchone()
        if not row:
            return None
        return Snapshot.model_validate(row[0])

    async def save(
        self,
        snapshot: Snapshot,
        expected_version: int,
        ttl_seconds: int = 24 * 3600,
        *,
        conn: Any = None,
    ) -> bool:
        del ttl_seconds
        snapshot = snapshot.model_copy()
        snapshot.version = expected_version + 1
        snapshot.updated_at = datetime.now(UTC).timestamp()

        async with _borrow(self._pool, conn) as (c, _):
            if expected_version == 0:
                cursor = await c.execute(
                    """
                    INSERT INTO roost_snapshots (
                      work_id, engine, version, status, step, data, history, artifacts,
                      is_finished, next_step_delay_seconds, created_at, updated_at,
                      finished_at, failed_at, raw
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (work_id) DO NOTHING
                    """,
                    self._params(snapshot),
                )
            else:
                cursor = await c.execute(
                    """
                    UPDATE roost_snapshots SET
                      engine = %s,
                      version = %s,
                      status = %s,
                      step = %s,
                      data = %s,
                      history = %s,
                      artifacts = %s,
                      is_finished = %s,
                      next_step_delay_seconds = %s,
                      created_at = %s,
                      updated_at = %s,
                      finished_at = %s,
                      failed_at = %s,
                      raw = %s
                    WHERE work_id = %s
                      AND version = %s
                    """,
                    self._update_params(snapshot, expected_version),
                )
        return int(cursor.rowcount or 0) == 1

    def _params(self, snapshot: Snapshot) -> tuple[Any, ...]:
        Jsonb = _require_jsonb()
        raw = snapshot.model_dump(mode="json")
        artifacts = [artifact.model_dump(mode="json") for artifact in snapshot.artifacts]
        return (
            snapshot.work_id,
            snapshot.engine,
            snapshot.version,
            snapshot.status,
            snapshot.step,
            Jsonb(snapshot.data),
            Jsonb(snapshot.history),
            Jsonb(artifacts),
            snapshot.is_finished,
            snapshot.next_step_delay_seconds,
            _ts(snapshot.created_at),
            _ts(snapshot.updated_at),
            _ts(snapshot.finished_at),
            _ts(snapshot.failed_at),
            Jsonb(raw),
        )

    def _update_params(self, snapshot: Snapshot, expected_version: int) -> tuple[Any, ...]:
        params = self._params(snapshot)
        work_id = params[0]
        return (*params[1:], work_id, expected_version)


class PostgresArtifactMetadataStore:
    def __init__(self, pool: Any):
        self._pool = pool

    async def put(self, artifact: Artifact, *, conn: Any = None) -> None:
        Jsonb = _require_jsonb()
        raw = artifact.model_dump(mode="json")
        async with _borrow(self._pool, conn) as (c, _):
            await c.execute(
                """
                INSERT INTO roost_artifacts (
                  artifact_id, work_id, kind, uri, content_hash, metadata, raw
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (artifact_id) DO UPDATE SET
                  work_id = EXCLUDED.work_id,
                  kind = EXCLUDED.kind,
                  uri = EXCLUDED.uri,
                  content_hash = EXCLUDED.content_hash,
                  metadata = EXCLUDED.metadata,
                  raw = EXCLUDED.raw
                """,
                (
                    artifact.artifact_id,
                    artifact.work_id,
                    artifact.kind,
                    artifact.uri,
                    artifact.content_hash,
                    Jsonb(artifact.metadata),
                    Jsonb(raw),
                ),
            )

    async def get(self, artifact_id: str, *, conn: Any = None) -> Optional[Artifact]:
        async with _borrow(self._pool, conn) as (c, _):
            row = await (
                await c.execute("SELECT raw FROM roost_artifacts WHERE artifact_id = %s", (artifact_id,))
            ).fetchone()
        if not row:
            return None
        return Artifact.model_validate(row[0])

    async def list_for_work(self, work_id: str, *, limit: int = 50, conn: Any = None) -> list[Artifact]:
        async with _borrow(self._pool, conn) as (c, _):
            rows = await (
                await c.execute(
                    """
                    SELECT raw
                    FROM roost_artifacts
                    WHERE work_id = %s
                    ORDER BY created_at DESC, artifact_id DESC
                    LIMIT %s
                    """,
                    (work_id, max(0, int(limit))),
                )
            ).fetchall()
        return [Artifact.model_validate(row[0]) for row in rows]


class PostgresLeaseStore(LeaseStore):
    def __init__(self, pool: Any):
        self._pool = pool

    async def try_acquire(
        self, work_id: str, holder_id: str, ttl_seconds: int, *, conn: Any = None
    ) -> Optional[Lease]:
        lease_id = uuid.uuid4().hex
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        async with _borrow(self._pool, conn) as (c, _):
            row = await (
                await c.execute(
                    """
                    INSERT INTO roost_leases (work_id, holder_id, lease_id, expires_at, updated_at)
                    VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (work_id) DO UPDATE SET
                      holder_id = EXCLUDED.holder_id,
                      lease_id = EXCLUDED.lease_id,
                      expires_at = EXCLUDED.expires_at,
                      updated_at = now()
                    WHERE roost_leases.expires_at <= now()
                    RETURNING work_id, holder_id, lease_id, expires_at
                    """,
                    (work_id, holder_id, lease_id, expires_at),
                )
            ).fetchone()
        if not row:
            return None
        return Lease(
            work_id=str(row[0]),
            holder_id=str(row[1]),
            lease_id=str(row[2]),
            expires_at=float(_epoch(row[3]) or 0),
        )

    async def renew(self, lease: Lease, ttl_seconds: int, *, conn: Any = None) -> bool:
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        async with _borrow(self._pool, conn) as (c, _):
            cursor = await c.execute(
                """
                UPDATE roost_leases
                SET expires_at = %s, updated_at = now()
                WHERE work_id = %s
                  AND holder_id = %s
                  AND lease_id = %s
                  AND expires_at > now()
                """,
                (expires_at, lease.work_id, lease.holder_id, lease.lease_id),
            )
        return int(cursor.rowcount or 0) == 1

    async def release(self, lease: Lease, *, conn: Any = None) -> bool:
        async with _borrow(self._pool, conn) as (c, _):
            cursor = await c.execute(
                """
                DELETE FROM roost_leases
                WHERE work_id = %s
                  AND holder_id = %s
                  AND lease_id = %s
                """,
                (lease.work_id, lease.holder_id, lease.lease_id),
            )
        return int(cursor.rowcount or 0) == 1

    async def clear(self, work_id: str, *, conn: Any = None) -> int:
        async with _borrow(self._pool, conn) as (c, _):
            cursor = await c.execute("DELETE FROM roost_leases WHERE work_id = %s", (work_id,))
        return int(cursor.rowcount or 0)

    async def is_active(self, work_id: str, *, conn: Any = None) -> bool:
        async with _borrow(self._pool, conn) as (c, _):
            row = await (
                await c.execute(
                    "SELECT 1 FROM roost_leases WHERE work_id = %s AND expires_at > now()",
                    (work_id,),
                )
            ).fetchone()
        return bool(row)


class PostgresResourceStore(ResourceStore):
    def __init__(self, pool: Any):
        self._pool = pool

    async def acquire(
        self, *, resources: list[str], owner_value: str, ttl_seconds: int, conn: Any = None
    ) -> bool:
        keys = _resource_keys(resources)
        if not keys:
            return True

        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        try:
            async with _borrow(self._pool, conn) as (c, owned):
                if not owned:
                    await c.execute("SAVEPOINT roost_resource_acquire")
                try:
                    for key in keys:
                        row = await (
                            await c.execute(
                                """
                                INSERT INTO roost_resource_claims (
                                  resource_key, owner_value, expires_at, updated_at
                                )
                                VALUES (%s, %s, %s, now())
                                ON CONFLICT (resource_key) DO UPDATE SET
                                  owner_value = EXCLUDED.owner_value,
                                  expires_at = EXCLUDED.expires_at,
                                  updated_at = now()
                                WHERE roost_resource_claims.expires_at <= now()
                                   OR roost_resource_claims.owner_value
                                      = EXCLUDED.owner_value
                                RETURNING resource_key
                                """,
                                (key, owner_value, expires_at),
                            )
                        ).fetchone()
                        if not row:
                            raise _ResourceAcquireConflict
                except _ResourceAcquireConflict:
                    if not owned:
                        await c.execute("ROLLBACK TO SAVEPOINT roost_resource_acquire")
                        return False
                    raise
                if not owned:
                    await c.execute("RELEASE SAVEPOINT roost_resource_acquire")
        except _ResourceAcquireConflict:
            return False
        return True

    async def renew(self, *, resources: list[str], owner_value: str, ttl_seconds: int, conn: Any = None) -> bool:
        return await self.acquire(resources=resources, owner_value=owner_value, ttl_seconds=ttl_seconds, conn=conn)

    async def release(self, *, resources: list[str], owner_value: str, conn: Any = None) -> int:
        keys = _resource_keys(resources)
        if not keys:
            return 0
        async with _borrow(self._pool, conn) as (c, _):
            cursor = await c.execute(
                """
                DELETE FROM roost_resource_claims
                WHERE resource_key = ANY(%s)
                  AND owner_value = %s
                """,
                (keys, owner_value),
            )
        return int(cursor.rowcount or 0)

    async def clear(self, *, resources: list[str], conn: Any = None) -> int:
        keys = _resource_keys(resources)
        if not keys:
            return 0
        async with _borrow(self._pool, conn) as (c, _):
            cursor = await c.execute("DELETE FROM roost_resource_claims WHERE resource_key = ANY(%s)", (keys,))
        return int(cursor.rowcount or 0)


class PostgresControlPlaneStore(ControlPlaneStore):
    def __init__(self, pool: Any):
        self._pool = pool

    async def push_event(self, event: dict[str, Any], *, maxlen: Optional[int] = None, conn: Any = None) -> None:
        del maxlen
        Jsonb = _require_jsonb()
        async with _borrow(self._pool, conn) as (c, _):
            await c.execute(
                """
                INSERT INTO roost_events (kind, work_id, engine, payload)
                VALUES (%s, %s, %s, %s)
                """,
                (event.get("kind"), event.get("work_id"), event.get("engine"), Jsonb(event)),
            )

    async def list_events(self, *, limit: int = 50, conn: Any = None) -> list[dict[str, Any]]:
        async with _borrow(self._pool, conn) as (c, _):
            rows = await (
                await c.execute(
                    """
                    SELECT id, event_ts, payload
                    FROM roost_events
                    ORDER BY event_ts DESC, id DESC
                    LIMIT %s
                    """,
                    (max(0, int(limit)),),
                )
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row[2] or {})
            payload.setdefault("id", str(row[0]))
            payload.setdefault("ts", _epoch(row[1]))
            out.append(payload)
        return out

    async def upsert_on_enqueue(
        self, item: WorkItem, work_id: str, *, conn: Any = None
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        existing = await self.get_meta(work_id, conn=conn)
        meta = {
            "work_id": work_id,
            "engine": existing.get("engine") if existing else item.engine,
            "created_at": (
                existing.get("created_at") if existing else float(item.created_at or now.timestamp())
            ),
            "updated_at": now.timestamp(),
            "state": existing.get("state") if existing else "queued",
            "step": existing.get("step") if existing else "init",
            "child_work_ids": _child_work_ids(existing),
        }
        await self._upsert_meta(meta, conn=conn)
        await self.push_event(
            {
                "kind": "work_enqueued",
                "work_id": work_id,
                "engine": item.engine,
                "state": meta.get("state"),
                "step": meta.get("step"),
            },
            conn=conn,
        )
        return meta

    async def set_state(
        self,
        *,
        work_id: str,
        engine: str,
        state: str,
        step: Optional[str] = None,
        last_error: Optional[dict[str, Any]] = None,
        conn: Any = None,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        existing = await self.get_meta(work_id, conn=conn) or {}
        prev_state = str(existing.get("state") or "queued")
        prev_step = str(existing.get("step") or "")
        meta = {
            "work_id": work_id,
            "engine": existing.get("engine") or engine,
            "created_at": float(existing.get("created_at") or now.timestamp()),
            "updated_at": now.timestamp(),
            "state": state,
            "step": step if step is not None else str(existing.get("step") or "init"),
            "child_work_ids": _child_work_ids(existing),
        }
        if last_error is not None:
            meta["last_error"] = last_error
        elif existing.get("last_error") is not None:
            meta["last_error"] = existing["last_error"]
        await self._upsert_meta(meta, conn=conn)
        if prev_state != state or (step is not None and str(step) != prev_step):
            await self.push_event(
                {
                    "kind": "work_state_changed",
                    "work_id": work_id,
                    "engine": engine,
                    "prev_state": prev_state,
                    "state": state,
                    "prev_step": prev_step,
                    "step": meta.get("step"),
                    "last_error": last_error,
                },
                conn=conn,
            )
        return meta

    async def link_child(
        self,
        *,
        parent_work_id: str,
        child_work_id: str,
        relation: str = "child",
        max_children: int = 50,
        conn: Any = None,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        existing = await self.get_meta(parent_work_id, conn=conn) or {}
        children = _child_work_ids(existing)
        child_entry = {"work_id": child_work_id, "relation": relation, "at": now.timestamp()}
        children = [
            child
            for child in children
            if not (isinstance(child, dict) and child.get("work_id") == child_work_id)
        ]
        children.insert(0, child_entry)
        children = children[: max(1, int(max_children))]
        if existing.get("work_id"):
            existing["child_work_ids"] = children
            existing["updated_at"] = now.timestamp()
            await self._upsert_meta(existing, conn=conn)
        await self.push_event(
            {
                "kind": "work_child_linked",
                "work_id": parent_work_id,
                "child_work_id": child_work_id,
                "relation": relation,
            },
            conn=conn,
        )
        return await self.get_meta(parent_work_id, conn=conn) or {
            "work_id": parent_work_id,
            "child_work_ids": children,
        }

    async def get_meta(self, work_id: str, *, conn: Any = None) -> Optional[dict[str, Any]]:
        async with _borrow(self._pool, conn) as (c, _):
            row = await (
                await c.execute(
                    """
                    SELECT work_id, engine, state, step, created_at, updated_at,
                           last_error, children
                    FROM roost_work_meta
                    WHERE work_id = %s
                    """,
                    (work_id,),
                )
            ).fetchone()
        if not row:
            return None
        children = row[7] if isinstance(row[7], list) else []
        meta = {
            "work_id": row[0],
            "engine": row[1],
            "state": row[2],
            "step": row[3],
            "created_at": _epoch(row[4]),
            "updated_at": _epoch(row[5]),
            "children": children,
            "child_work_ids": children,
        }
        if row[6] is not None:
            meta["last_error"] = row[6]
        return meta

    async def list_work_ids(
        self, *, state: Optional[str], limit: int, offset: int, conn: Any = None
    ) -> list[str]:
        async with _borrow(self._pool, conn) as (c, _):
            if state:
                rows = await (
                    await c.execute(
                        """
                        SELECT work_id
                        FROM roost_work_meta
                        WHERE state = %s
                        ORDER BY updated_at DESC
                        LIMIT %s OFFSET %s
                        """,
                        (state, max(0, int(limit)), max(0, int(offset))),
                    )
                ).fetchall()
            else:
                rows = await (
                    await c.execute(
                        """
                        SELECT work_id
                        FROM roost_work_meta
                        ORDER BY updated_at DESC
                        LIMIT %s OFFSET %s
                        """,
                        (max(0, int(limit)), max(0, int(offset))),
                    )
                ).fetchall()
        return [str(row[0]) for row in rows]

    async def list_meta(
        self, *, state: Optional[str], limit: int = 20, offset: int = 0, conn: Any = None
    ) -> list[dict[str, Any]]:
        work_ids = await self.list_work_ids(state=state, limit=limit, offset=offset, conn=conn)
        out = []
        for work_id in work_ids:
            meta = await self.get_meta(work_id, conn=conn)
            if meta:
                out.append(meta)
        return out

    async def push_dlq(self, event: dict[str, Any], *, maxlen: int = 2000, conn: Any = None) -> None:
        del maxlen
        Jsonb = _require_jsonb()
        async with _borrow(self._pool, conn) as (c, _):
            await c.execute(
                """
                INSERT INTO roost_dlq (work_id, engine, step, last_error, payload)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    event.get("work_id"),
                    event.get("engine"),
                    event.get("step"),
                    Jsonb(event.get("last_error")) if event.get("last_error") is not None else None,
                    Jsonb(event),
                ),
            )
            await c.execute(
                """
                INSERT INTO roost_events (kind, work_id, engine, payload)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    "dlq_pushed",
                    event.get("work_id"),
                    event.get("engine"),
                    Jsonb(
                        {
                            "kind": "dlq_pushed",
                            "work_id": event.get("work_id"),
                            "engine": event.get("engine"),
                            "step": event.get("step"),
                            "last_error": event.get("last_error"),
                        }
                    ),
                ),
            )

    async def list_dlq(self, *, limit: int = 50, offset: int = 0, conn: Any = None) -> list[dict[str, Any]]:
        async with _borrow(self._pool, conn) as (c, _):
            rows = await (
                await c.execute(
                    """
                    SELECT id, work_id, engine, step, last_error, payload, created_at
                    FROM roost_dlq
                    WHERE acked_at IS NULL
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s OFFSET %s
                    """,
                    (max(0, int(limit)), max(0, int(offset))),
                )
            ).fetchall()
        return [self._dlq_row(row) for row in rows]

    async def get_dlq(self, index: int, *, conn: Any = None) -> Optional[dict[str, Any]]:
        rows = await self.list_dlq(limit=1, offset=index, conn=conn)
        return rows[0] if rows else None

    async def ack_dlq(self, index: int, *, conn: Any = None) -> bool:
        entry = await self.get_dlq(index, conn=conn)
        if not entry:
            return False
        async with _borrow(self._pool, conn) as (c, _):
            cursor = await c.execute(
                "UPDATE roost_dlq SET acked_at = now() WHERE id = %s AND acked_at IS NULL",
                (entry["id"],),
            )
        return int(cursor.rowcount or 0) == 1

    async def ack_dlq_work_id(self, work_id: str, *, conn: Any = None) -> int:
        async with _borrow(self._pool, conn) as (c, _):
            cursor = await c.execute(
                "UPDATE roost_dlq SET acked_at = now() WHERE work_id = %s AND acked_at IS NULL",
                (work_id,),
            )
        return int(cursor.rowcount or 0)

    async def _upsert_meta(self, meta: dict[str, Any], *, conn: Any = None) -> None:
        Jsonb = _require_jsonb()
        children = _child_work_ids(meta)
        async with _borrow(self._pool, conn) as (c, _):
            await c.execute(
                """
                INSERT INTO roost_work_meta (
                  work_id, engine, state, step, created_at, updated_at,
                  last_error, children
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (work_id) DO UPDATE SET
                  engine = EXCLUDED.engine,
                  state = EXCLUDED.state,
                  step = EXCLUDED.step,
                  updated_at = EXCLUDED.updated_at,
                  last_error = EXCLUDED.last_error,
                  children = EXCLUDED.children
                """,
                (
                    meta["work_id"],
                    meta["engine"],
                    meta["state"],
                    meta["step"],
                    _ts(meta["created_at"]),
                    _ts(meta["updated_at"]),
                    Jsonb(meta.get("last_error")) if meta.get("last_error") is not None else None,
                    Jsonb(children),
                ),
            )

    def _dlq_row(self, row: Any) -> dict[str, Any]:
        payload = dict(row[5] or {})
        payload.setdefault("id", row[0])
        payload.setdefault("work_id", row[1])
        payload.setdefault("engine", row[2])
        payload.setdefault("step", row[3])
        payload.setdefault("last_error", row[4])
        payload.setdefault("created_at", _epoch(row[6]))
        return payload


class PostgresOperatorActionStore:
    def __init__(self, pool: Any):
        self._pool = pool

    async def record(
        self,
        action: str,
        work_id: str | None,
        actor: str | None,
        payload: dict[str, Any] | None = None,
        *,
        conn: Any = None,
    ) -> dict[str, Any]:
        Jsonb = _require_jsonb()
        async with _borrow(self._pool, conn) as (c, _):
            row = await (
                await c.execute(
                    """
                    INSERT INTO roost_operator_actions (
                      action, work_id, actor, payload
                    )
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, action_ts, action, work_id, actor, payload
                    """,
                    (action, work_id, actor, Jsonb(payload or {})),
                )
            ).fetchone()
        return self._row(row)

    async def list_for_work(
        self, work_id: str, limit: int = 50, *, conn: Any = None
    ) -> list[dict[str, Any]]:
        async with _borrow(self._pool, conn) as (c, _):
            rows = await (
                await c.execute(
                    """
                    SELECT id, action_ts, action, work_id, actor, payload
                    FROM roost_operator_actions
                    WHERE work_id = %s
                    ORDER BY action_ts DESC, id DESC
                    LIMIT %s
                    """,
                    (work_id, max(0, int(limit))),
                )
            ).fetchall()
        return [self._row(row) for row in rows]

    async def list_recent(self, limit: int = 50, *, conn: Any = None) -> list[dict[str, Any]]:
        async with _borrow(self._pool, conn) as (c, _):
            rows = await (
                await c.execute(
                    """
                    SELECT id, action_ts, action, work_id, actor, payload
                    FROM roost_operator_actions
                    ORDER BY action_ts DESC, id DESC
                    LIMIT %s
                    """,
                    (max(0, int(limit)),),
                )
            ).fetchall()
        return [self._row(row) for row in rows]

    def _row(self, row: Any) -> dict[str, Any]:
        return {
            "id": row[0],
            "action_ts": _epoch(row[1]),
            "action": row[2],
            "work_id": row[3],
            "actor": row[4],
            "payload": row[5] or {},
        }


def annotate_worker_liveness(
    worker: dict[str, Any],
    *,
    now: float,
    stale_after_seconds: Optional[float] = None,
) -> dict[str, Any]:
    """Add age_seconds (and stale, when a window is given) to a heartbeat row."""
    last_seen = float(worker.get("last_seen_at") or 0.0)
    row = dict(worker)
    row["age_seconds"] = round(max(0.0, now - last_seen), 3)
    if stale_after_seconds is not None:
        row["stale"] = last_seen < (now - float(stale_after_seconds))
    return row


class PostgresWorkerHeartbeatStore:
    def __init__(self, pool: Any):
        self._pool = pool

    async def heartbeat(
        self,
        *,
        worker_id: str,
        engine_ids: list[str],
        queue_name: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        conn: Any = None,
    ) -> dict[str, Any]:
        Jsonb = _require_jsonb()
        async with _borrow(self._pool, conn) as (c, _):
            row = await (
                await c.execute(
                    """
                    INSERT INTO roost_worker_heartbeats (
                      worker_id, engine_ids, queue_name, last_seen_at, metadata
                    )
                    VALUES (%s, %s, %s, now(), %s)
                    ON CONFLICT (worker_id) DO UPDATE SET
                      engine_ids = EXCLUDED.engine_ids,
                      queue_name = EXCLUDED.queue_name,
                      last_seen_at = now(),
                      metadata = EXCLUDED.metadata
                    RETURNING worker_id, engine_ids, queue_name, last_seen_at, metadata
                    """,
                    (worker_id, sorted(set(engine_ids)), queue_name, Jsonb(metadata or {})),
                )
            ).fetchone()
        return self._row(row)

    async def list_workers(
        self, *, limit: int = 100, stale_after_seconds: Optional[int] = None, conn: Any = None
    ) -> list[dict[str, Any]]:
        async with _borrow(self._pool, conn) as (c, _):
            rows = await (
                await c.execute(
                    """
                    SELECT worker_id, engine_ids, queue_name, last_seen_at, metadata
                    FROM roost_worker_heartbeats
                    ORDER BY last_seen_at DESC, worker_id ASC
                    LIMIT %s
                    """,
                    (max(0, int(limit)),),
                )
            ).fetchall()
        workers = [self._row(row) for row in rows]
        now = datetime.now(UTC).timestamp()
        return [
            annotate_worker_liveness(
                worker, now=now, stale_after_seconds=stale_after_seconds
            )
            for worker in workers
        ]

    def _row(self, row: Any) -> dict[str, Any]:
        return {
            "worker_id": row[0],
            "engine_ids": list(row[1] or []),
            "queue_name": row[2],
            "last_seen_at": _epoch(row[3]),
            "metadata": row[4] or {},
        }


@dataclass(frozen=True)
class PostgresDurableStores:
    """
    Durable production-memory stores backed by one Postgres connection pool.

    This intentionally does not include queue or in-flight stores. Redis can
    still own movement, while Postgres owns the durable execution record.
    """

    work_items: PostgresWorkItemStore
    snapshots: PostgresSnapshotStore
    artifacts: PostgresArtifactMetadataStore
    leases: PostgresLeaseStore
    resources: PostgresResourceStore
    workers: PostgresWorkerHeartbeatStore
    control: PostgresControlPlaneStore


def build_postgres_durable_stores(pool: Any) -> PostgresDurableStores:
    """
    Build the durable stores from a Postgres connection pool.

    ``pool`` is anything with async ``getconn()`` / ``putconn()`` (e.g. a
    ``psycopg_pool.AsyncConnectionPool``). Stores do not own connections; each
    call either checks one out (autocommit) or uses a connection passed by the
    caller (participating in a caller's transaction).
    """
    return PostgresDurableStores(
        work_items=PostgresWorkItemStore(pool),
        snapshots=PostgresSnapshotStore(pool),
        artifacts=PostgresArtifactMetadataStore(pool),
        leases=PostgresLeaseStore(pool),
        resources=PostgresResourceStore(pool),
        workers=PostgresWorkerHeartbeatStore(pool),
        control=PostgresControlPlaneStore(pool),
    )


def _resource_keys(resources: list[str]) -> list[str]:
    return sorted({resource for resource in resources if resource})
