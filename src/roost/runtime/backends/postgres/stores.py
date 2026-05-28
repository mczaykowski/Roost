from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Optional
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


class PostgresWorkItemStore(WorkItemStore):
    def __init__(self, conn: Any, *, commit: bool = True):
        self.conn = conn
        self.commit = commit

    async def put(self, item: WorkItem, ttl_seconds: int = 7 * 24 * 3600) -> None:
        del ttl_seconds
        await self._upsert(item)

    async def get(self, work_id: str) -> Optional[WorkItem]:
        row = await (
            await self.conn.execute("SELECT raw FROM roost_work_items WHERE work_id = %s", (work_id,))
        ).fetchone()
        if not row:
            return None
        return WorkItem.model_validate(row[0])

    async def get_or_claim_work_id(self, item: WorkItem, ttl_seconds: int = 7 * 24 * 3600) -> str:
        del ttl_seconds
        if not item.idempotency_key:
            await self._upsert(item)
            return item.work_id

        row = await (
            await self.conn.execute(
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
        if self.commit:
            await self.conn.commit()
        if row:
            return str(row[0])

        existing = await (
            await self.conn.execute(
                "SELECT work_id FROM roost_work_items WHERE idempotency_key = %s",
                (item.idempotency_key,),
            )
        ).fetchone()
        return str(existing[0]) if existing else item.work_id

    async def _upsert(self, item: WorkItem) -> None:
        await self.conn.execute(
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
        if self.commit:
            await self.conn.commit()

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
    def __init__(self, conn: Any, *, commit: bool = True):
        self.conn = conn
        self.commit = commit

    async def load(self, work_id: str) -> Optional[Snapshot]:
        row = await (
            await self.conn.execute("SELECT raw FROM roost_snapshots WHERE work_id = %s", (work_id,))
        ).fetchone()
        if not row:
            return None
        return Snapshot.model_validate(row[0])

    async def save(self, snapshot: Snapshot, expected_version: int, ttl_seconds: int = 24 * 3600) -> bool:
        del ttl_seconds
        snapshot = snapshot.model_copy()
        snapshot.version = expected_version + 1
        snapshot.updated_at = datetime.now(UTC).timestamp()

        if expected_version == 0:
            cursor = await self.conn.execute(
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
            cursor = await self.conn.execute(
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
        if self.commit:
            await self.conn.commit()
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
    def __init__(self, conn: Any, *, commit: bool = True):
        self.conn = conn
        self.commit = commit

    async def put(self, artifact: Artifact) -> None:
        Jsonb = _require_jsonb()
        raw = artifact.model_dump(mode="json")
        await self.conn.execute(
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
        if self.commit:
            await self.conn.commit()

    async def get(self, artifact_id: str) -> Optional[Artifact]:
        row = await (
            await self.conn.execute("SELECT raw FROM roost_artifacts WHERE artifact_id = %s", (artifact_id,))
        ).fetchone()
        if not row:
            return None
        return Artifact.model_validate(row[0])

    async def list_for_work(self, work_id: str, *, limit: int = 50) -> list[Artifact]:
        rows = await (
            await self.conn.execute(
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
    def __init__(self, conn: Any, *, commit: bool = True):
        self.conn = conn
        self.commit = commit

    async def try_acquire(self, work_id: str, holder_id: str, ttl_seconds: int) -> Optional[Lease]:
        lease_id = uuid.uuid4().hex
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        row = await (
            await self.conn.execute(
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
        if self.commit:
            await self.conn.commit()
        if not row:
            return None
        return Lease(work_id=str(row[0]), holder_id=str(row[1]), lease_id=str(row[2]), expires_at=float(_epoch(row[3]) or 0))

    async def renew(self, lease: Lease, ttl_seconds: int) -> bool:
        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        cursor = await self.conn.execute(
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
        if self.commit:
            await self.conn.commit()
        return int(cursor.rowcount or 0) == 1

    async def release(self, lease: Lease) -> bool:
        cursor = await self.conn.execute(
            """
            DELETE FROM roost_leases
            WHERE work_id = %s
              AND holder_id = %s
              AND lease_id = %s
            """,
            (lease.work_id, lease.holder_id, lease.lease_id),
        )
        if self.commit:
            await self.conn.commit()
        return int(cursor.rowcount or 0) == 1

    async def clear(self, work_id: str) -> int:
        cursor = await self.conn.execute("DELETE FROM roost_leases WHERE work_id = %s", (work_id,))
        if self.commit:
            await self.conn.commit()
        return int(cursor.rowcount or 0)

    async def is_active(self, work_id: str) -> bool:
        row = await (
            await self.conn.execute(
                "SELECT 1 FROM roost_leases WHERE work_id = %s AND expires_at > now()",
                (work_id,),
            )
        ).fetchone()
        return bool(row)


class PostgresResourceStore(ResourceStore):
    def __init__(self, conn: Any, *, commit: bool = True):
        self.conn = conn
        self.commit = commit

    async def acquire(self, *, resources: list[str], owner_value: str, ttl_seconds: int) -> bool:
        keys = _resource_keys(resources)
        if not keys:
            return True

        await self._lock(keys)
        conflicts = await (
            await self.conn.execute(
                """
                SELECT resource_key
                FROM roost_resource_claims
                WHERE resource_key = ANY(%s)
                  AND owner_value <> %s
                  AND expires_at > now()
                """,
                (keys, owner_value),
            )
        ).fetchall()
        if conflicts:
            if self.commit:
                await self.conn.commit()
            return False

        expires_at = datetime.now(UTC) + timedelta(seconds=ttl_seconds)
        for key in keys:
            await self.conn.execute(
                """
                INSERT INTO roost_resource_claims (resource_key, owner_value, expires_at, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (resource_key) DO UPDATE SET
                  owner_value = EXCLUDED.owner_value,
                  expires_at = EXCLUDED.expires_at,
                  updated_at = now()
                """,
                (key, owner_value, expires_at),
            )
        if self.commit:
            await self.conn.commit()
        return True

    async def renew(self, *, resources: list[str], owner_value: str, ttl_seconds: int) -> bool:
        return await self.acquire(resources=resources, owner_value=owner_value, ttl_seconds=ttl_seconds)

    async def release(self, *, resources: list[str], owner_value: str) -> int:
        keys = _resource_keys(resources)
        if not keys:
            return 0
        cursor = await self.conn.execute(
            """
            DELETE FROM roost_resource_claims
            WHERE resource_key = ANY(%s)
              AND owner_value = %s
            """,
            (keys, owner_value),
        )
        if self.commit:
            await self.conn.commit()
        return int(cursor.rowcount or 0)

    async def clear(self, *, resources: list[str]) -> int:
        keys = _resource_keys(resources)
        if not keys:
            return 0
        cursor = await self.conn.execute("DELETE FROM roost_resource_claims WHERE resource_key = ANY(%s)", (keys,))
        if self.commit:
            await self.conn.commit()
        return int(cursor.rowcount or 0)

    async def _lock(self, resources: list[str]) -> None:
        for resource in resources:
            await self.conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (resource,))


class PostgresControlPlaneStore(ControlPlaneStore):
    def __init__(self, conn: Any, *, commit: bool = True):
        self.conn = conn
        self.commit = commit

    async def push_event(self, event: dict[str, Any], *, maxlen: Optional[int] = None) -> None:
        del maxlen
        Jsonb = _require_jsonb()
        await self.conn.execute(
            """
            INSERT INTO roost_events (kind, work_id, engine, payload)
            VALUES (%s, %s, %s, %s)
            """,
            (event.get("kind"), event.get("work_id"), event.get("engine"), Jsonb(event)),
        )
        if self.commit:
            await self.conn.commit()

    async def list_events(self, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = await (
            await self.conn.execute(
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

    async def upsert_on_enqueue(self, item: WorkItem, work_id: str) -> dict[str, Any]:
        now = datetime.now(UTC)
        existing = await self.get_meta(work_id)
        meta = {
            "work_id": work_id,
            "engine": existing.get("engine") if existing else item.engine,
            "created_at": existing.get("created_at") if existing else float(item.created_at or now.timestamp()),
            "updated_at": now.timestamp(),
            "state": existing.get("state") if existing else "queued",
            "step": existing.get("step") if existing else "init",
        }
        await self._upsert_meta(meta)
        await self.push_event(
            {
                "kind": "work_enqueued",
                "work_id": work_id,
                "engine": item.engine,
                "state": meta.get("state"),
                "step": meta.get("step"),
            }
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
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        existing = await self.get_meta(work_id) or {}
        prev_state = str(existing.get("state") or "queued")
        prev_step = str(existing.get("step") or "")
        meta = {
            "work_id": work_id,
            "engine": existing.get("engine") or engine,
            "created_at": float(existing.get("created_at") or now.timestamp()),
            "updated_at": now.timestamp(),
            "state": state,
            "step": step if step is not None else str(existing.get("step") or "init"),
        }
        if last_error is not None:
            meta["last_error"] = last_error
        elif existing.get("last_error") is not None:
            meta["last_error"] = existing["last_error"]
        await self._upsert_meta(meta)
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
                }
            )
        return meta

    async def link_child(
        self,
        *,
        parent_work_id: str,
        child_work_id: str,
        relation: str = "child",
        max_children: int = 50,
    ) -> dict[str, Any]:
        del max_children
        await self.push_event(
            {
                "kind": "work_child_linked",
                "work_id": parent_work_id,
                "child_work_id": child_work_id,
                "relation": relation,
            }
        )
        return await self.get_meta(parent_work_id) or {"work_id": parent_work_id}

    async def get_meta(self, work_id: str) -> Optional[dict[str, Any]]:
        row = await (
            await self.conn.execute(
                """
                SELECT work_id, engine, state, step, created_at, updated_at, last_error
                FROM roost_work_meta
                WHERE work_id = %s
                """,
                (work_id,),
            )
        ).fetchone()
        if not row:
            return None
        meta = {
            "work_id": row[0],
            "engine": row[1],
            "state": row[2],
            "step": row[3],
            "created_at": _epoch(row[4]),
            "updated_at": _epoch(row[5]),
        }
        if row[6] is not None:
            meta["last_error"] = row[6]
        return meta

    async def list_work_ids(self, *, state: Optional[str], limit: int, offset: int) -> list[str]:
        if state:
            rows = await (
                await self.conn.execute(
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
                await self.conn.execute(
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

    async def list_meta(self, *, state: Optional[str], limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        work_ids = await self.list_work_ids(state=state, limit=limit, offset=offset)
        out = []
        for work_id in work_ids:
            meta = await self.get_meta(work_id)
            if meta:
                out.append(meta)
        return out

    async def push_dlq(self, event: dict[str, Any], *, maxlen: int = 2000) -> None:
        del maxlen
        Jsonb = _require_jsonb()
        await self.conn.execute(
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
        if self.commit:
            await self.conn.commit()
        await self.push_event(
            {
                "kind": "dlq_pushed",
                "work_id": event.get("work_id"),
                "engine": event.get("engine"),
                "step": event.get("step"),
                "last_error": event.get("last_error"),
            }
        )

    async def list_dlq(self, *, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        rows = await (
            await self.conn.execute(
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

    async def get_dlq(self, index: int) -> Optional[dict[str, Any]]:
        rows = await self.list_dlq(limit=1, offset=index)
        return rows[0] if rows else None

    async def ack_dlq(self, index: int) -> bool:
        entry = await self.get_dlq(index)
        if not entry:
            return False
        cursor = await self.conn.execute(
            "UPDATE roost_dlq SET acked_at = now() WHERE id = %s AND acked_at IS NULL",
            (entry["id"],),
        )
        if self.commit:
            await self.conn.commit()
        return int(cursor.rowcount or 0) == 1

    async def ack_dlq_work_id(self, work_id: str) -> int:
        cursor = await self.conn.execute(
            "UPDATE roost_dlq SET acked_at = now() WHERE work_id = %s AND acked_at IS NULL",
            (work_id,),
        )
        if self.commit:
            await self.conn.commit()
        return int(cursor.rowcount or 0)

    async def _upsert_meta(self, meta: dict[str, Any]) -> None:
        Jsonb = _require_jsonb()
        await self.conn.execute(
            """
            INSERT INTO roost_work_meta (
              work_id, engine, state, step, created_at, updated_at, last_error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (work_id) DO UPDATE SET
              engine = EXCLUDED.engine,
              state = EXCLUDED.state,
              step = EXCLUDED.step,
              updated_at = EXCLUDED.updated_at,
              last_error = EXCLUDED.last_error
            """,
            (
                meta["work_id"],
                meta["engine"],
                meta["state"],
                meta["step"],
                _ts(meta["created_at"]),
                _ts(meta["updated_at"]),
                Jsonb(meta.get("last_error")) if meta.get("last_error") is not None else None,
            ),
        )
        if self.commit:
            await self.conn.commit()

    def _dlq_row(self, row: Any) -> dict[str, Any]:
        payload = dict(row[5] or {})
        payload.setdefault("id", row[0])
        payload.setdefault("work_id", row[1])
        payload.setdefault("engine", row[2])
        payload.setdefault("step", row[3])
        payload.setdefault("last_error", row[4])
        payload.setdefault("created_at", _epoch(row[6]))
        return payload


class PostgresWorkerHeartbeatStore:
    def __init__(self, conn: Any, *, commit: bool = True):
        self.conn = conn
        self.commit = commit

    async def heartbeat(
        self,
        *,
        worker_id: str,
        engine_ids: list[str],
        queue_name: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        Jsonb = _require_jsonb()
        row = await (
            await self.conn.execute(
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
        if self.commit:
            await self.conn.commit()
        return self._row(row)

    async def list_workers(self, *, limit: int = 100, stale_after_seconds: Optional[int] = None) -> list[dict[str, Any]]:
        rows = await (
            await self.conn.execute(
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
        if stale_after_seconds is None:
            return workers
        cutoff = datetime.now(UTC).timestamp() - stale_after_seconds
        return [dict(worker, stale=float(worker["last_seen_at"]) < cutoff) for worker in workers]

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
    Durable production-memory stores backed by one Postgres connection.

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


def build_postgres_durable_stores(conn: Any, *, commit: bool = True) -> PostgresDurableStores:
    return PostgresDurableStores(
        work_items=PostgresWorkItemStore(conn, commit=commit),
        snapshots=PostgresSnapshotStore(conn, commit=commit),
        artifacts=PostgresArtifactMetadataStore(conn, commit=commit),
        leases=PostgresLeaseStore(conn, commit=commit),
        resources=PostgresResourceStore(conn, commit=commit),
        workers=PostgresWorkerHeartbeatStore(conn, commit=commit),
        control=PostgresControlPlaneStore(conn, commit=commit),
    )


def _resource_keys(resources: list[str]) -> list[str]:
    return sorted({resource for resource in resources if resource})
