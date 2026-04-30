from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
import typing
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from redis import asyncio as aioredis
from saq import Queue, Worker

from roost.runtime.engine import Engine
from roost.runtime.models import WorkItem
from roost.runtime.backends.redis import (
    RedisControlPlane,
    RedisInflightStore,
    RedisKeys,
    RedisLeaseManager,
    RedisResourceManager,
    RedisSnapshotStore,
    RedisWorkItemStore,
)
if typing.TYPE_CHECKING:
    from roost.runtime.config import RoostConfig


@dataclass(frozen=True)
class SwarmConfig:
    redis_url: str
    queue_name: str = "default"
    redis_prefix: str = "roost"

    lease_ttl_seconds: int = 60
    inflight_ttl_seconds: int = 120
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

        self.keys = RedisKeys(prefix=config.redis_prefix)
        self.leases = RedisLeaseManager(self.redis, keys=self.keys)
        self.resources = RedisResourceManager(self.redis, keys=self.keys)
        self.work_items = RedisWorkItemStore(self.redis, keys=self.keys)
        self.snapshots = RedisSnapshotStore(self.redis, keys=self.keys)
        self.inflight = RedisInflightStore(self.redis, keys=self.keys)
        self.control = RedisControlPlane(self.redis, keys=self.keys)

    async def close(self) -> None:
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

    async def _execute_one_step_impl(
        self,
        *,
        work_id: str,
        item: WorkItem,
        engine: Engine,
        job_attempt: int = 1,
        job_id: str | None = None,
    ) -> Dict[str, Any]:
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
            return {"status": "busy", "reason": "lease_unavailable", "job_id": job_id}

        renew_stop = asyncio.Event()
        resource_owner_value = f"{work_id}:{lease.holder_id}:{lease.lease_id}"
        claimed_resources: list[str] = []

        async def renew_loop() -> None:
            interval = max(1, int(self.config.lease_ttl_seconds / 2))
            while not renew_stop.is_set():
                await asyncio.sleep(interval)
                ok = await self.leases.renew(lease, ttl_seconds=self.config.lease_ttl_seconds)
                if not ok:
                    return
                if claimed_resources:
                    await self.resources.renew(
                        resources=claimed_resources,
                        owner_value=resource_owner_value,
                        ttl_seconds=self.config.resource_ttl_seconds,
                    )

        renew_task = asyncio.create_task(renew_loop())

        try:
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
                    return {"status": "error", "reason": "snapshot_init_failed"}

            if snapshot.is_finished:
                await self.control.set_state(
                    work_id=work_id,
                    engine=item.engine,
                    state="done",
                    step=snapshot.step,
                )
                return {"status": "success", "info": "already_finished"}

            await self.control.set_state(work_id=work_id, engine=item.engine, state="running", step=snapshot.step)
            snapshot.status = "running"

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

            expected_version = snapshot.version
            ok = await self.snapshots.save(
                new_snapshot,
                expected_version=expected_version,
                ttl_seconds=self.config.snapshot_ttl_seconds,
            )
            if not ok:
                latest = await self.snapshots.load(work_id)
                if latest and (latest.version > expected_version or latest.is_finished):
                    await self.control.set_state(
                        work_id=work_id,
                        engine=item.engine,
                        state="running" if not latest.is_finished else "done",
                        step=latest.step,
                    )
                    return {"status": "success", "info": "won_race"}

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
                return {"status": "retry", "reason": "version_conflict", "job_id": job_id}

            if new_snapshot.is_finished:
                await self._eval_triggers(work_id=work_id, item=item, snapshot=new_snapshot)

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

            await self.control.set_state(
                work_id=work_id,
                engine=item.engine,
                state="done" if new_snapshot.is_finished else "running",
                step=new_snapshot.step,
            )
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

    async def run_worker(self, *, concurrency: int = 10) -> None:
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

        recovery_task = asyncio.create_task(recovery_loop())
        try:
            await worker.start()
        finally:
            recovery_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await recovery_task

    async def recover_orphans_once(self) -> int:
        now = time.time()
        recovered = 0

        async for key in self.redis.scan_iter(f"{self.keys.prefix}:inflight:*"):
            raw = await self.redis.get(key)
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue

            started_at = float(payload.get("started_at", 0.0))
            if (now - started_at) < self.config.stale_after_seconds:
                continue

            key_str = str(key)
            work_id = self._work_id_from_inflight_key(key_str)
            if not work_id:
                continue

            # If the lease still exists, assume a slow worker is alive.
            if await self.redis.exists(self.keys.lease(work_id)):
                continue

            snap = await self.snapshots.load(work_id)
            if not snap or snap.is_finished:
                continue

            await self.queue.enqueue(
                "work_step",
                work_id=work_id,
                scheduled=0,
                timeout=self._job_timeout(),
                retries=self.config.job_retries,
                retry_delay=self.config.job_retry_delay_seconds,
                retry_backoff=self.config.job_retry_backoff,
            )
            recovered += 1

        return recovered

    async def _eval_triggers(self, *, work_id: str, item: WorkItem, snapshot: Any) -> None:
        if not self.config.roost_config or not self.config.roost_config.triggers:
            return

        for trigger in self.config.roost_config.triggers:
            if trigger.on_engine_done != item.engine:
                continue

            if trigger.condition:
                # Basic truthiness check for field path
                val = None
                if trigger.condition.startswith("snapshot.data."):
                    val = snapshot.data.get(trigger.condition[len("snapshot.data.") :])
                elif trigger.condition.startswith("item.payload."):
                    val = item.payload.get(trigger.condition[len("item.payload.") :])
                else:
                    val = snapshot.data.get(trigger.condition)
                
                if not val:
                    continue

            payload = dict(item.payload)
            if trigger.payload_map:
                for target_key, source_path in trigger.payload_map.items():
                    # Minimal path resolution: snapshot.data.field or item.payload.field
                    val = None
                    if source_path.startswith("snapshot.data."):
                        val = snapshot.data.get(source_path[len("snapshot.data.") :])
                    elif source_path.startswith("item.payload."):
                        val = item.payload.get(source_path[len("item.payload.") :])
                    else:
                        val = item.payload.get(source_path)
                    
                    if val is not None:
                        payload[target_key] = val

            new_item = WorkItem(
                work_id=uuid.uuid4().hex,
                engine=trigger.enqueue_engine,
                payload=payload,
                priority=item.priority,
                resources=item.resources,
            )
            
            child_id = await self.enqueue(new_item)
            await self.control.link_child(parent_work_id=work_id, child_work_id=child_id, relation="trigger")


class RedisSwarm(_RedisSwarmRuntime):
    def __init__(self, engine: Engine, *, config: SwarmConfig, worker_id: Optional[str] = None):
        super().__init__(config=config, worker_id=worker_id)
        self.engine = engine

    async def _execute_one_step(
        self,
        work_id: str,
        *,
        job_attempt: int = 1,
        job_id: str | None = None,
    ) -> Dict[str, Any]:
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

    async def _execute_one_step(
        self,
        work_id: str,
        *,
        job_attempt: int = 1,
        job_id: str | None = None,
    ) -> Dict[str, Any]:
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
