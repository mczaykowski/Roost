from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
import typing
import uuid
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Mapping, Optional

from redis import asyncio as aioredis
from saq import Queue, Worker

from roost.runtime.engine import Engine
from roost.runtime.models import WorkItem
from roost.runtime.config import resolve_trigger_path
from roost.runtime.backends.redis import (
    RedisControlPlane,
    RedisInflightStore,
    RedisKeys,
    RedisLeaseManager,
    RedisResourceManager,
    RedisSnapshotStore,
    RedisWorkItemStore,
)
from roost.runtime.stores import (
    ControlPlaneStore,
    InflightStore,
    LeaseStore,
    ResourceStore,
    RuntimeStores,
    SnapshotStore,
    WorkItemStore,
)
if typing.TYPE_CHECKING:
    from roost.runtime.config import RoostConfig

logger = logging.getLogger("roost.runtime")

_RECOVERABLE_STATES = ("running", "queued")
_TERMINAL_STATES = frozenset({"cancelled", "done", "failed"})
_RECOVERY_PAGE_SIZE = 200


def _observe(
    status: str,
    *,
    engine: str,
    retry: bool = False,
    dlq: bool = False,
    lease_contention: bool = False,
) -> None:
    from roost.runtime.metrics import (
        record_dlq,
        record_lease_contention,
        record_retry,
        record_step,
    )

    if lease_contention:
        record_lease_contention()
    if retry:
        record_retry()
    if dlq:
        record_dlq()
    record_step(engine or "unknown", status)


@dataclass(frozen=True)
class SwarmConfig:
    redis_url: str
    queue_name: str = "default"
    redis_prefix: str = "roost"
    runtime_mode: str = "simple"
    postgres_url: Optional[str] = None

    lease_ttl_seconds: int = 60
    inflight_ttl_seconds: int = 120
    worker_heartbeat_interval_seconds: float = 10.0
    recovery_interval_seconds: float = 2.0
    stale_after_seconds: float = 30.0
    resource_ttl_seconds: int = 60
    snapshot_ttl_seconds: int = 24 * 3600
    work_item_ttl_seconds: int = 7 * 24 * 3600

    job_retries: int = 5
    job_retry_delay_seconds: float = 1.0
    job_retry_backoff: bool | float = 60.0
    job_timeout_seconds: int = 120

    roost_config: Optional["RoostConfig"] = None


class _RedisSwarmRuntime:
    """
    Minimal persistent swarm runtime using Redis + SAQ.

    Semantics:
    - at-least-once job delivery
    - single-worker ownership via leases
    - optimistic snapshot persistence with versioning
    """

    def __init__(self, *, config: SwarmConfig, worker_id: Optional[str] = None):
        self.config = config
        self.worker_id = worker_id or f"roost-worker-{uuid.uuid4().hex[:8]}"

        self.redis = aioredis.from_url(config.redis_url, decode_responses=True)
        self.queue = Queue.from_url(config.redis_url, name=config.queue_name)
        self._postgres_pool: Any = None
        self._store_init_lock = asyncio.Lock()

        self.keys = RedisKeys(prefix=config.redis_prefix)
        # Movement is always Redis. Durable memory is Redis in simple mode and
        # Postgres in production — never swapped after the first bind.
        self.inflight: InflightStore = RedisInflightStore(self.redis, keys=self.keys)
        self.artifacts: Any = None
        self.workers: Any = None
        self.stores: Optional[RuntimeStores]
        if self._production_mode():
            # Durable bindings stay unset until the async pool opens. Do not
            # point leases/snapshots/control at Redis even briefly.
            self.stores = None
            self.leases: LeaseStore | None = None
            self.resources: ResourceStore | None = None
            self.work_items: WorkItemStore | None = None
            self.snapshots: SnapshotStore | None = None
            self.control: ControlPlaneStore | None = None
        else:
            self.stores = RuntimeStores(
                work_items=RedisWorkItemStore(self.redis, keys=self.keys),
                snapshots=RedisSnapshotStore(self.redis, keys=self.keys),
                leases=RedisLeaseManager(self.redis, keys=self.keys),
                resources=RedisResourceManager(self.redis, keys=self.keys),
                inflight=self.inflight,
                control=RedisControlPlane(self.redis, keys=self.keys),
            )
            self._activate_stores(self.stores)

    def _activate_stores(self, stores: RuntimeStores) -> None:
        self.stores = stores
        self.leases = stores.leases
        self.resources = stores.resources
        self.work_items = stores.work_items
        self.snapshots = stores.snapshots
        self.inflight = stores.inflight
        self.control = stores.control

    def _production_mode(self) -> bool:
        return self.config.runtime_mode == "production"

    async def _ensure_runtime_stores(self) -> None:
        """One-shot bind. Simple mode is ready in __init__; production opens the pool once."""
        if self.stores is not None:
            return

        async with self._store_init_lock:
            if self.stores is not None:
                return
            if not self._production_mode():
                return
            if not self.config.postgres_url:
                raise RuntimeError("Postgres URL is required when runtime_mode='production'")
            try:
                from psycopg_pool import AsyncConnectionPool

                from roost.runtime.backends.postgres import build_postgres_durable_stores
            except Exception as exc:
                raise RuntimeError(
                    "Missing Postgres runtime dependency. Install with:\n"
                    "  uv sync --extra postgres"
                ) from exc

            # One pool shared by every durable store. Each store call either
            # checks a connection out (autocommit) or uses a connection handed
            # to it by the caller (participating in a transaction). This is what
            # keeps the concurrent renew_loop and the step body from sharing a
            # single Postgres session (which would raise "already executing").
            pool = AsyncConnectionPool(
                conninfo=self.config.postgres_url,
                min_size=1,
                max_size=8,
                open=False,
            )
            await pool.open()
            self._postgres_pool = pool
            durable = build_postgres_durable_stores(pool)
            self.artifacts = durable.artifacts
            self.workers = durable.workers
            self._activate_stores(
                RuntimeStores(
                    work_items=durable.work_items,
                    snapshots=durable.snapshots,
                    leases=durable.leases,
                    resources=durable.resources,
                    inflight=self.inflight,
                    control=durable.control,
                )
            )

    @contextlib.asynccontextmanager
    async def _step_transaction(self) -> AsyncIterator[Any]:
        """
        One Postgres connection held for the durable write window of a step.

        Opens a connection out of the pool in non-autocommit mode so that all
        store calls passed ``conn=tx`` commit together — making the snapshot,
        the meta state, the events and any child links land atomically. The
        engine's ``step()`` runs BEFORE this is entered, so no connection is
        held across arbitrary user computation.
        """
        if not self._production_mode():
            # Simple mode stores (Redis) are not transactional; yield a sentinel
            # so the same call sites work in both modes.
            yield None
            return
        conn = await self._postgres_pool.getconn()
        try:
            yield conn
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise
        finally:
            await self._postgres_pool.putconn(conn)

    async def close(self) -> None:
        if self._postgres_pool is not None:
            await self._postgres_pool.close()
            self._postgres_pool = None
        await self.redis.aclose()

    def _scheduled_after(self, delay_seconds: float) -> int:
        if delay_seconds <= 0:
            return 0
        return int(time.time() + delay_seconds)

    def _work_id_from_inflight_key(self, key: str) -> Optional[str]:
        inflight_prefix = f"{self.keys.prefix}:inflight:"
        if not key.startswith(inflight_prefix):
            return None
        return key[len(inflight_prefix) :]

    def _jitter(self, delay_seconds: float) -> float:
        # +/- 50% jitter
        return max(0.0, delay_seconds * (0.5 + random.random()))

    def _backoff_seconds(self, attempt: int, *, base: float = 1.0, cap: float = 30.0) -> float:
        attempt = max(1, attempt)
        exp = base * (2 ** min(attempt - 1, 6))
        return min(cap, exp)

    def _job_timeout(self) -> int:
        return max(1, int(self.config.job_timeout_seconds))

    async def enqueue(self, item: WorkItem, delay_seconds: int = 0) -> str:
        await self._ensure_runtime_stores()
        work_id = await self.work_items.get_or_claim_work_id(item, ttl_seconds=self.config.work_item_ttl_seconds)
        await self.control.upsert_on_enqueue(item, work_id)
        await self.queue.enqueue(
            "work_step",
            work_id=work_id,
            scheduled=self._scheduled_after(delay_seconds),
            timeout=self._job_timeout(),
            retries=self.config.job_retries,
            retry_delay=self.config.job_retry_delay_seconds,
            retry_backoff=self.config.job_retry_backoff,
        )
        return work_id

    def _log_step_boundary(
        self,
        *,
        work_id: str,
        engine: str,
        step: Optional[str],
        attempt: int,
        status: str,
        version: Optional[int],
        started_at: float,
    ) -> None:
        logger.info(
            json.dumps(
                {
                    "work_id": work_id,
                    "engine": engine,
                    "step": step,
                    "attempt": attempt,
                    "status": status,
                    "version": version,
                    "duration_ms": int((time.perf_counter() - started_at) * 1000),
                },
                default=str,
            )
        )

    async def _execute_one_step_impl(
        self,
        *,
        work_id: str,
        item: WorkItem,
        engine: Engine,
        job_attempt: int = 1,
        job_id: str | None = None,
    ) -> Dict[str, Any]:
        started_at = time.perf_counter()
        status = "error"
        log_step: Optional[str] = None
        log_version: Optional[int] = None

        meta = await self.control.get_meta(work_id)
        if meta and meta.get("state") == "cancelled":
            _observe("cancelled", engine=item.engine)
            self._log_step_boundary(
                work_id=work_id,
                engine=item.engine,
                step=log_step,
                attempt=job_attempt,
                status="cancelled",
                version=log_version,
                started_at=started_at,
            )
            return {"status": "cancelled", "reason": "operator_cancelled", "job_id": job_id}

        lease = await self.leases.try_acquire(work_id, self.worker_id, ttl_seconds=self.config.lease_ttl_seconds)
        if not lease:
            await self.control.set_state(work_id=work_id, engine=item.engine, state="queued", step="lease_wait")
            delay = self._jitter(self._backoff_seconds(job_attempt, base=0.5, cap=10.0))
            await self.queue.enqueue(
                "work_step",
                work_id=work_id,
                scheduled=self._scheduled_after(delay),
                timeout=self._job_timeout(),
                retries=self.config.job_retries,
                retry_delay=self.config.job_retry_delay_seconds,
                retry_backoff=self.config.job_retry_backoff,
            )
            _observe("busy", engine=item.engine, lease_contention=True)
            self._log_step_boundary(
                work_id=work_id,
                engine=item.engine,
                step="lease_wait",
                attempt=job_attempt,
                status="busy",
                version=log_version,
                started_at=started_at,
            )
            return {"status": "busy", "reason": "lease_unavailable", "job_id": job_id}

        renew_stop = asyncio.Event()
        lease_lost = asyncio.Event()
        resource_owner_value = f"{work_id}:{lease.holder_id}:{lease.lease_id}"
        claimed_resources: list[str] = []

        async def renew_loop() -> None:
            interval = max(1, int(self.config.lease_ttl_seconds / 2))
            while not renew_stop.is_set():
                await asyncio.sleep(interval)
                if renew_stop.is_set():
                    return
                ok = await self.leases.renew(lease, ttl_seconds=self.config.lease_ttl_seconds)
                if not ok:
                    lease_lost.set()
                    return
                if claimed_resources:
                    await self.resources.renew(
                        resources=claimed_resources,
                        owner_value=resource_owner_value,
                        ttl_seconds=self.config.resource_ttl_seconds,
                    )

        renew_task = asyncio.create_task(renew_loop())

        try:
            meta = await self.control.get_meta(work_id)
            if meta and meta.get("state") == "cancelled":
                _observe("cancelled", engine=item.engine)
                status = "cancelled"
                return {"status": "cancelled", "reason": "operator_cancelled", "job_id": job_id}

            claimed_resources = list(item.resources or [])
            if claimed_resources:
                ok = await self.resources.acquire(
                    resources=claimed_resources,
                    owner_value=resource_owner_value,
                    ttl_seconds=self.config.resource_ttl_seconds,
                )
                if not ok:
                    await self.control.set_state(
                        work_id=work_id,
                        engine=item.engine,
                        state="queued",
                        step="resource_conflict",
                    )
                    delay = self._jitter(self._backoff_seconds(job_attempt, base=1.0, cap=20.0))
                    await self.queue.enqueue(
                        "work_step",
                        work_id=work_id,
                        scheduled=self._scheduled_after(delay),
                        timeout=self._job_timeout(),
                        retries=self.config.job_retries,
                        retry_delay=self.config.job_retry_delay_seconds,
                        retry_backoff=self.config.job_retry_backoff,
                    )
                    _observe("busy", engine=item.engine)
                    status = "busy"
                    return {
                        "status": "busy",
                        "reason": "resource_conflict",
                        "resources": claimed_resources,
                        "job_id": job_id,
                    }

            snapshot = await self.snapshots.load(work_id)
            if not snapshot:
                init = await engine.init_snapshot(item)
                init.work_id = work_id
                ok = await self.snapshots.save(init, expected_version=0, ttl_seconds=self.config.snapshot_ttl_seconds)
                snapshot = await self.snapshots.load(work_id) if not ok else init
                if not snapshot:
                    await self.control.set_state(work_id=work_id, engine=item.engine, state="failed", step="init_failed")
                    _observe("error", engine=item.engine)
                    status = "error"
                    return {"status": "error", "reason": "snapshot_init_failed"}

            if snapshot.is_finished:
                log_step = snapshot.step
                log_version = snapshot.version
                await self.control.set_state(
                    work_id=work_id,
                    engine=item.engine,
                    state="done",
                    step=snapshot.step,
                )
                _observe("success", engine=item.engine)
                status = "success"
                return {"status": "success", "info": "already_finished"}

            await self.control.set_state(work_id=work_id, engine=item.engine, state="running", step=snapshot.step)
            snapshot.status = "running"
            log_step = snapshot.step
            log_version = snapshot.version

            await self.inflight.mark(
                work_id,
                payload={
                    "worker_id": self.worker_id,
                    "lease_id": lease.lease_id,
                    "started_at": time.time(),
                    "snapshot_version": snapshot.version,
                    "step": snapshot.step,
                    "job_id": job_id,
                    "job_attempt": job_attempt,
                },
                ttl_seconds=self.config.inflight_ttl_seconds,
            )

            new_snapshot = await engine.step(snapshot, item)
            new_snapshot.work_id = work_id
            new_snapshot.status = "done" if new_snapshot.is_finished else "running"
            if new_snapshot.is_finished:
                new_snapshot.finished_at = time.time()
            log_step = new_snapshot.step
            log_version = new_snapshot.version

            meta = await self.control.get_meta(work_id)
            if meta and meta.get("state") == "cancelled":
                _observe("cancelled", engine=item.engine)
                status = "cancelled"
                return {"status": "cancelled", "reason": "operator_cancelled", "job_id": job_id}

            if lease_lost.is_set():
                # Do not persist a snapshot after losing the lease.
                await self.control.set_state(
                    work_id=work_id,
                    engine=item.engine,
                    state="queued",
                    step=snapshot.step,
                )
                delay = self._jitter(self._backoff_seconds(job_attempt, base=0.5, cap=10.0))
                await self.queue.enqueue(
                    "work_step",
                    work_id=work_id,
                    scheduled=self._scheduled_after(delay),
                    timeout=self._job_timeout(),
                    retries=self.config.job_retries,
                    retry_delay=self.config.job_retry_delay_seconds,
                    retry_backoff=self.config.job_retry_backoff,
                )
                _observe("retry", engine=item.engine, retry=True)
                status = "retry"
                return {"status": "retry", "reason": "lease_lost", "job_id": job_id}

            expected_version = snapshot.version

            # --- Durable write window: one transaction ---------------------
            # All of snapshot.save / set_state / events / artifacts / link_child
            # land atomically so meta and snapshot can never disagree after a
            # failure. engine.step() ran above, outside the transaction, so no
            # connection is held across arbitrary user computation. Redis
            # movement (queue.enqueue) happens after commit, per policy
            # agreement #3 (movement stays out of the memory transaction).
            async with self._step_transaction() as tx:
                ok = await self.snapshots.save(
                    new_snapshot,
                    expected_version=expected_version,
                    ttl_seconds=self.config.snapshot_ttl_seconds,
                    conn=tx,
                )
                if not ok:
                    latest = await self.snapshots.load(work_id, conn=tx)
                    if latest and (latest.version > expected_version or latest.is_finished):
                        await self.control.set_state(
                            work_id=work_id,
                            engine=item.engine,
                            state="running" if not latest.is_finished else "done",
                            step=latest.step,
                            conn=tx,
                        )
                        _observe("success", engine=item.engine)
                        status = "success"
                        log_step = latest.step
                        log_version = latest.version
                        return {"status": "success", "info": "won_race"}

                    # Lost the race but no newer finished snapshot: let the job
                    # retry. Nothing was written inside this txn.
                    _observe("retry", engine=item.engine, retry=True)
                    status = "retry"
                    return {"status": "retry", "reason": "version_conflict", "job_id": job_id}

                if self.artifacts:
                    for artifact in new_snapshot.artifacts:
                        await self.artifacts.put(artifact, conn=tx)

                trigger_children: list[WorkItem] = []
                if new_snapshot.is_finished:
                    trigger_children = await self._plan_triggers(
                        item=item, snapshot=new_snapshot, conn=tx
                    )

                await self.control.set_state(
                    work_id=work_id,
                    engine=item.engine,
                    state="done" if new_snapshot.is_finished else "running",
                    step=new_snapshot.step,
                    conn=tx,
                )
            # --- transaction committed ------------------------------------

            # Movement (Redis): enqueue the next step and any trigger children.
            # These happen after the durable write committed, so a crash here
            # leaves durable state consistent and the recovery scan re-enqueues.
            for child in trigger_children:
                await self.enqueue(child)

            if not new_snapshot.is_finished:
                delay = int(max(0, new_snapshot.next_step_delay_seconds))
                await self.queue.enqueue(
                    "work_step",
                    work_id=work_id,
                    scheduled=self._scheduled_after(delay),
                    timeout=self._job_timeout(),
                    retries=self.config.job_retries,
                    retry_delay=self.config.job_retry_delay_seconds,
                    retry_backoff=self.config.job_retry_backoff,
                )

            _observe("success", engine=item.engine)
            status = "success"
            log_version = expected_version + 1
            return {"status": "success", "step": new_snapshot.step, "finished": new_snapshot.is_finished}
        finally:
            renew_stop.set()
            renew_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renew_task
            await self.inflight.clear(work_id)
            if claimed_resources:
                await self.resources.release(resources=claimed_resources, owner_value=resource_owner_value)
            await self.leases.release(lease)
            self._log_step_boundary(
                work_id=work_id,
                engine=item.engine,
                step=log_step,
                attempt=job_attempt,
                status=status,
                version=log_version,
                started_at=started_at,
            )

    async def _best_effort_record_error(
        self,
        *,
        work_id: str,
        job_id: Optional[str],
        job_attempt: int,
        error: Exception,
        is_final: bool,
    ) -> None:
        last_error = {
            "job_id": job_id,
            "attempt": job_attempt,
            "type": error.__class__.__name__,
            "message": str(error),
            "at": time.time(),
        }
        engine_id = "unknown"
        step = "unknown"

        snap = await self.snapshots.load(work_id)
        if snap:
            engine_id = str(snap.engine or engine_id)
            step = str(snap.step or step)
        else:
            item = await self.work_items.get(work_id)
            if item:
                engine_id = str(item.engine or engine_id)
            else:
                meta = await self.control.get_meta(work_id)
                if meta and meta.get("engine"):
                    engine_id = str(meta["engine"])

        await self.control.set_state(
            work_id=work_id,
            engine=engine_id,
            state="failed" if is_final else "running",
            step=step,
            last_error=last_error,
        )
        if is_final:
            await self.control.push_dlq(
                {
                    "work_id": work_id,
                    "engine": engine_id,
                    "step": step,
                    "last_error": last_error,
                }
            )
            _observe("error", engine=engine_id, dlq=True)

    async def run_worker(self, *, concurrency: int = 10) -> None:
        await self._ensure_runtime_stores()

        async def work_step(ctx: Dict[str, Any], work_id: str, **_kwargs: Any) -> Dict[str, Any]:
            job = ctx.get("job")
            job_attempt = int(getattr(job, "attempts", 1) or 1)
            job_id = str(getattr(job, "id", "")) if job else None
            try:
                return await self._execute_one_step(work_id, job_attempt=job_attempt, job_id=job_id)
            except Exception as e:
                is_final = bool(job and getattr(job, "attempts", 0) >= getattr(job, "retries", 1))
                await self._best_effort_record_error(
                    work_id=work_id,
                    job_id=job_id,
                    job_attempt=job_attempt,
                    error=e,
                    is_final=is_final,
                )
                raise

        worker = Worker(self.queue, functions=[("work_step", work_step)], concurrency=concurrency)

        async def recovery_loop() -> None:
            while True:
                try:
                    await self.recover_orphans_once()
                finally:
                    await asyncio.sleep(self.config.recovery_interval_seconds)

        async def heartbeat_loop() -> None:
            while True:
                if self.workers:
                    await self.workers.heartbeat(
                        worker_id=self.worker_id,
                        engine_ids=self._worker_engine_ids(),
                        queue_name=self.config.queue_name,
                        metadata={
                            "runtime_mode": self.config.runtime_mode,
                            "redis_prefix": self.config.redis_prefix,
                            "concurrency": concurrency,
                        },
                    )
                await asyncio.sleep(max(1.0, self.config.worker_heartbeat_interval_seconds))

        recovery_task = asyncio.create_task(recovery_loop())
        heartbeat_task = asyncio.create_task(heartbeat_loop()) if self.workers else None
        try:
            await worker.start()
        finally:
            recovery_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await recovery_task
            if heartbeat_task:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

    async def recover_orphans_once(self) -> int:
        await self._ensure_runtime_stores()
        now = time.time()
        recovered = 0
        candidates: set[str] = set()

        # Primary discovery is the durable control plane (Postgres in production,
        # Redis in simple mode). Inflight keys are a fast-path hint only.
        for state in _RECOVERABLE_STATES:
            offset = 0
            while True:
                page = await self.control.list_work_ids(
                    state=state, limit=_RECOVERY_PAGE_SIZE, offset=offset
                )
                candidates.update(page)
                if len(page) < _RECOVERY_PAGE_SIZE:
                    break
                offset += _RECOVERY_PAGE_SIZE

        try:
            async for key in self.redis.scan_iter(f"{self.keys.prefix}:inflight:*"):
                work_id = self._work_id_from_inflight_key(str(key))
                if work_id:
                    candidates.add(work_id)
        except Exception as exc:
            # Inflight is a hint. Redis being down must not hide Postgres work.
            logger.warning("inflight hint scan failed: %s", exc)

        for work_id in candidates:
            if await self._recover_one(work_id, now=now):
                recovered += 1

        return recovered

    async def _recover_one(self, work_id: str, *, now: float) -> bool:
        meta = await self.control.get_meta(work_id)
        if meta and str(meta.get("state") or "") in _TERMINAL_STATES:
            return False

        if await self._lease_is_active(work_id):
            return False

        snap = await self.snapshots.load(work_id)
        if not snap or snap.is_finished:
            return False

        inflight = await self.inflight.get(work_id)
        timestamps: list[float] = []
        if snap.updated_at:
            timestamps.append(float(snap.updated_at))
        if meta and meta.get("updated_at") is not None:
            timestamps.append(float(meta["updated_at"]))
        if inflight and inflight.get("started_at") is not None:
            timestamps.append(float(inflight["started_at"]))
        if timestamps and (now - max(timestamps)) < self.config.stale_after_seconds:
            return False

        await self.queue.enqueue(
            "work_step",
            work_id=work_id,
            scheduled=0,
            timeout=self._job_timeout(),
            retries=self.config.job_retries,
            retry_delay=self.config.job_retry_delay_seconds,
            retry_backoff=self.config.job_retry_backoff,
        )
        # Bump durable updated_at so the next recovery tick does not storm.
        engine_id = str((meta or {}).get("engine") or snap.engine or "unknown")
        state = str((meta or {}).get("state") or "queued")
        step = str((meta or {}).get("step") or snap.step)
        await self.control.set_state(work_id=work_id, engine=engine_id, state=state, step=step)
        await self.inflight.mark(
            work_id,
            payload={
                "worker_id": self.worker_id,
                "started_at": now,
                "recovered": True,
                "step": step,
            },
            ttl_seconds=self.config.inflight_ttl_seconds,
        )
        return True

    async def _lease_is_active(self, work_id: str) -> bool:
        is_active = getattr(self.leases, "is_active", None)
        if is_active:
            return bool(await is_active(work_id))
        return bool(await self.redis.exists(self.keys.lease(work_id)))

    def _worker_engine_ids(self) -> list[str]:
        return []

    async def _plan_triggers(
        self, *, item: WorkItem, snapshot: Any, conn: Any
    ) -> list[WorkItem]:
        """
        Plan trigger fan-out inside the step transaction.

        Frozen contract: a trigger fires when ``condition`` is omitted, or when
        it names a single key that is truthy. Allowed forms are
        ``snapshot.data.<one_key>``, ``item.payload.<one_key>``, or a bare key
        looked up in ``snapshot.data``. Nested paths raise ValueError at plan
        time. ``payload_map`` copies matching keys under the same one-level rule.

        Durable parts (claim the child work id, record it as a child of this
        work, upsert its meta) run on the caller's connection so they commit
        atomically with the snapshot. Child items are returned so the caller
        can enqueue them (Redis movement) AFTER the transaction commits.
        """
        if not self.config.roost_config or not self.config.roost_config.triggers:
            return []

        planned: list[WorkItem] = []
        for trigger in self.config.roost_config.triggers:
            if trigger.on_engine_done != item.engine:
                continue

            if trigger.condition:
                val = resolve_trigger_path(
                    trigger.condition,
                    snapshot_data=snapshot.data,
                    item_payload=item.payload,
                    field="condition",
                )
                if not val:
                    continue

            payload = dict(item.payload)
            if trigger.payload_map:
                for target_key, source_path in trigger.payload_map.items():
                    val = resolve_trigger_path(
                        source_path,
                        snapshot_data=snapshot.data,
                        item_payload=item.payload,
                        field="payload_map",
                    )
                    if val is not None:
                        payload[target_key] = val

            new_item = WorkItem(
                work_id=uuid.uuid4().hex,
                engine=trigger.enqueue_engine,
                payload=payload,
                priority=item.priority,
                resources=item.resources,
            )

            child_id = await self.work_items.get_or_claim_work_id(
                new_item, ttl_seconds=self.config.work_item_ttl_seconds, conn=conn
            )
            new_item.work_id = child_id
            await self.control.upsert_on_enqueue(new_item, child_id, conn=conn)
            await self.control.link_child(
                parent_work_id=item.work_id, child_work_id=child_id, relation="trigger", conn=conn
            )
            planned.append(new_item)

        return planned


class RedisSwarm(_RedisSwarmRuntime):
    def __init__(self, engine: Engine, *, config: SwarmConfig, worker_id: Optional[str] = None):
        super().__init__(config=config, worker_id=worker_id)
        self.engine = engine

    def _worker_engine_ids(self) -> list[str]:
        return [self.engine.engine_id]

    async def _execute_one_step(
        self,
        work_id: str,
        *,
        job_attempt: int = 1,
        job_id: str | None = None,
    ) -> Dict[str, Any]:
        await self._ensure_runtime_stores()
        item = await self.work_items.get(work_id)
        if not item:
            await self.control.set_state(work_id=work_id, engine="unknown", state="failed", step="missing")
            await self.control.push_dlq(
                {
                    "work_id": work_id,
                    "engine": "unknown",
                    "step": "missing",
                    "last_error": {"type": "WorkItemMissing", "message": "work_item_missing"},
                }
            )
            return {"status": "error", "reason": "work_item_missing"}

        if item.engine != self.engine.engine_id:
            await self.control.set_state(
                work_id=work_id,
                engine=item.engine,
                state="failed",
                step="engine_mismatch",
                last_error={
                    "type": "EngineMismatch",
                    "message": f"expected={self.engine.engine_id} got={item.engine}",
                },
            )
            await self.control.push_dlq(
                {
                    "work_id": work_id,
                    "engine": item.engine,
                    "step": "engine_mismatch",
                    "last_error": {
                        "type": "EngineMismatch",
                        "message": f"expected={self.engine.engine_id} got={item.engine}",
                    },
                }
            )
            return {
                "status": "error",
                "reason": "engine_mismatch",
                "expected": self.engine.engine_id,
                "got": item.engine,
            }

        return await self._execute_one_step_impl(
            work_id=work_id,
            item=item,
            engine=self.engine,
            job_attempt=job_attempt,
            job_id=job_id,
        )


class RedisUniversalSwarm(_RedisSwarmRuntime):
    def __init__(self, engines: Mapping[str, Engine], *, config: SwarmConfig, worker_id: Optional[str] = None):
        super().__init__(config=config, worker_id=worker_id)
        self.engines: Dict[str, Engine] = dict(engines)

    def _worker_engine_ids(self) -> list[str]:
        return sorted(self.engines)

    async def _execute_one_step(
        self,
        work_id: str,
        *,
        job_attempt: int = 1,
        job_id: str | None = None,
    ) -> Dict[str, Any]:
        await self._ensure_runtime_stores()
        item = await self.work_items.get(work_id)
        if not item:
            await self.control.set_state(work_id=work_id, engine="unknown", state="failed", step="missing")
            await self.control.push_dlq(
                {
                    "work_id": work_id,
                    "engine": "unknown",
                    "step": "missing",
                    "last_error": {"type": "WorkItemMissing", "message": "work_item_missing"},
                }
            )
            return {"status": "error", "reason": "work_item_missing"}

        engine = self.engines.get(item.engine)
        if not engine:
            await self.control.set_state(
                work_id=work_id,
                engine=item.engine,
                state="failed",
                step="unknown_engine",
                last_error={"type": "UnknownEngine", "message": f"engine={item.engine}"},
            )
            await self.control.push_dlq(
                {
                    "work_id": work_id,
                    "engine": item.engine,
                    "step": "unknown_engine",
                    "last_error": {"type": "UnknownEngine", "message": f"engine={item.engine}"},
                }
            )
            return {"status": "error", "reason": "unknown_engine", "engine": item.engine}

        return await self._execute_one_step_impl(
            work_id=work_id,
            item=item,
            engine=engine,
            job_attempt=job_attempt,
            job_id=job_id,
        )
