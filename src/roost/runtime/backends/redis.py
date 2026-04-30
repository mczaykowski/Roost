from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from redis import asyncio as aioredis

from roost.runtime.models import Lease, Snapshot, WorkItem


def _now() -> float:
    return time.time()


@dataclass(frozen=True)
class RedisKeys:
    prefix: str = "roost"

    def work_item(self, work_id: str) -> str:
        return f"{self.prefix}:work:{work_id}:item"

    def snapshot(self, work_id: str) -> str:
        return f"{self.prefix}:work:{work_id}:snapshot"

    def lease(self, work_id: str) -> str:
        return f"{self.prefix}:lease:{work_id}"

    def inflight(self, work_id: str) -> str:
        return f"{self.prefix}:inflight:{work_id}"

    def idem(self, idempotency_key: str) -> str:
        return f"{self.prefix}:idem:{idempotency_key}"

    def resource(self, resource: str) -> str:
        # resources can contain ":"; keep raw for debuggability
        return f"{self.prefix}:res:{resource}"

    def work_meta(self, work_id: str) -> str:
        return f"{self.prefix}:work:{work_id}:meta"

    def works_index(self) -> str:
        return f"{self.prefix}:works"

    def works_state_index(self, state: str) -> str:
        return f"{self.prefix}:works:state:{state}"

    def dlq(self) -> str:
        return f"{self.prefix}:dlq"

    def events(self) -> str:
        return f"{self.prefix}:events"


class RedisLeaseManager:
    def __init__(self, redis: aioredis.Redis, keys: Optional[RedisKeys] = None):
        self.redis = redis
        self.keys = keys or RedisKeys()

        self._release_script = self.redis.register_script(
            """
            local key = KEYS[1]
            local expected = ARGV[1]
            if redis.call("GET", key) == expected then
              return redis.call("DEL", key)
            end
            return 0
            """
        )
        self._renew_script = self.redis.register_script(
            """
            local key = KEYS[1]
            local expected = ARGV[1]
            local ttl = tonumber(ARGV[2])
            if redis.call("GET", key) == expected then
              return redis.call("EXPIRE", key, ttl)
            end
            return 0
            """
        )

    async def try_acquire(self, work_id: str, holder_id: str, ttl_seconds: int) -> Optional[Lease]:
        lease_id = uuid.uuid4().hex
        value = f"{holder_id}:{lease_id}"
        ok = await self.redis.set(self.keys.lease(work_id), value, ex=ttl_seconds, nx=True)
        if not ok:
            return None
        return Lease(work_id=work_id, holder_id=holder_id, lease_id=lease_id, expires_at=_now() + ttl_seconds)

    async def renew(self, lease: Lease, ttl_seconds: int) -> bool:
        expected = f"{lease.holder_id}:{lease.lease_id}"
        res = await self._renew_script(keys=[self.keys.lease(lease.work_id)], args=[expected, str(ttl_seconds)])
        return bool(res)

    async def release(self, lease: Lease) -> bool:
        expected = f"{lease.holder_id}:{lease.lease_id}"
        res = await self._release_script(keys=[self.keys.lease(lease.work_id)], args=[expected])
        return bool(res)


class RedisResourceManager:
    """
    Multi-resource claim system (best-effort isolation).

    Claims are TTL-based and are designed to fail open on worker crash (TTL expires).
    """

    def __init__(self, redis: aioredis.Redis, keys: Optional[RedisKeys] = None):
        self.redis = redis
        self.keys = keys or RedisKeys()

        self._acquire_script = self.redis.register_script(
            """
            local ttl = tonumber(ARGV[1])
            local value = ARGV[2]

            -- Check for conflicts
            for i = 1, #KEYS do
              local current = redis.call("GET", KEYS[i])
              if current and current ~= value then
                return 0
              end
            end

            -- Acquire (or refresh if already owned)
            for i = 1, #KEYS do
              redis.call("SET", KEYS[i], value, "EX", ttl)
            end
            return 1
            """
        )

        self._release_script = self.redis.register_script(
            """
            local value = ARGV[1]
            local released = 0
            for i = 1, #KEYS do
              if redis.call("GET", KEYS[i]) == value then
                released = released + redis.call("DEL", KEYS[i])
              end
            end
            return released
            """
        )

    async def acquire(self, *, resources: list[str], owner_value: str, ttl_seconds: int) -> bool:
        if not resources:
            return True
        keys = [self.keys.resource(r) for r in sorted(set(resources))]
        res = await self._acquire_script(keys=keys, args=[str(ttl_seconds), owner_value])
        return res == 1

    async def renew(self, *, resources: list[str], owner_value: str, ttl_seconds: int) -> bool:
        # renew is equivalent to acquire for existing-owner refresh
        return await self.acquire(resources=resources, owner_value=owner_value, ttl_seconds=ttl_seconds)

    async def release(self, *, resources: list[str], owner_value: str) -> int:
        if not resources:
            return 0
        keys = [self.keys.resource(r) for r in sorted(set(resources))]
        res = await self._release_script(keys=keys, args=[owner_value])
        return int(res or 0)


class RedisWorkItemStore:
    def __init__(self, redis: aioredis.Redis, keys: Optional[RedisKeys] = None):
        self.redis = redis
        self.keys = keys or RedisKeys()

    async def put(self, item: WorkItem, ttl_seconds: int = 7 * 24 * 3600) -> None:
        await self.redis.set(self.keys.work_item(item.work_id), item.model_dump_json(), ex=ttl_seconds)

    async def get(self, work_id: str) -> Optional[WorkItem]:
        raw = await self.redis.get(self.keys.work_item(work_id))
        if not raw:
            return None
        return WorkItem.model_validate_json(raw)

    async def get_or_claim_work_id(self, item: WorkItem, ttl_seconds: int = 7 * 24 * 3600) -> str:
        """
        If an idempotency_key is provided, ensure enqueue is de-duplicated.
        Returns the canonical work_id.
        """
        if not item.idempotency_key:
            await self.put(item, ttl_seconds=ttl_seconds)
            return item.work_id

        key = self.keys.idem(item.idempotency_key)
        ok = await self.redis.set(key, item.work_id, ex=ttl_seconds, nx=True)
        if ok:
            await self.put(item, ttl_seconds=ttl_seconds)
            return item.work_id

        existing = await self.redis.get(key)
        return str(existing) if existing else item.work_id


class RedisSnapshotStore:
    """
    Stores a single latest snapshot per work_id with optimistic versioning.

    Versioning is handled by the store to keep engines simple:
    - caller provides `expected_version`
    - store persists snapshot with `version = expected_version + 1`
    """

    def __init__(self, redis: aioredis.Redis, keys: Optional[RedisKeys] = None):
        self.redis = redis
        self.keys = keys or RedisKeys()

        self._cas_script = self.redis.register_script(
            """
            local key = KEYS[1]
            local expected = tonumber(ARGV[1])
            local new_json = ARGV[2]
            local ttl = tonumber(ARGV[3])

            local current = redis.call("GET", key)
            if not current then
              if expected ~= 0 then return 0 end
              redis.call("SET", key, new_json, "EX", ttl)
              return 1
            end

            local ok, decoded = pcall(cjson.decode, current)
            if not ok then return -1 end
            local current_version = tonumber(decoded["version"] or 0)
            if current_version ~= expected then
              return 0
            end

            redis.call("SET", key, new_json, "EX", ttl)
            return 1
            """
        )

    async def load(self, work_id: str) -> Optional[Snapshot]:
        raw = await self.redis.get(self.keys.snapshot(work_id))
        if not raw:
            return None
        return Snapshot.model_validate_json(raw)

    async def save(self, snapshot: Snapshot, expected_version: int, ttl_seconds: int = 24 * 3600) -> bool:
        snapshot = snapshot.model_copy()
        snapshot.version = expected_version + 1
        snapshot.updated_at = _now()
        res = await self._cas_script(
            keys=[self.keys.snapshot(snapshot.work_id)],
            args=[str(expected_version), snapshot.model_dump_json(), str(ttl_seconds)],
        )
        return res == 1


class RedisInflightStore:
    def __init__(self, redis: aioredis.Redis, keys: Optional[RedisKeys] = None):
        self.redis = redis
        self.keys = keys or RedisKeys()

    async def mark(self, work_id: str, payload: dict, ttl_seconds: int) -> None:
        await self.redis.set(self.keys.inflight(work_id), json.dumps(payload), ex=ttl_seconds)

    async def clear(self, work_id: str) -> None:
        await self.redis.delete(self.keys.inflight(work_id))

    async def get(self, work_id: str) -> Optional[dict]:
        raw = await self.redis.get(self.keys.inflight(work_id))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None


class RedisControlPlane:
    """
    Lightweight indexing + status store for operability.

    Tracks:
    - per-work metadata (`work:<id>:meta`)
    - global index (`works` zset)
    - per-state index (`works:state:<state>` zset)
    - a simple DLQ list (`dlq`)
    - an event stream (`events`) for observability
    """

    def __init__(self, redis: aioredis.Redis, keys: Optional[RedisKeys] = None, *, meta_ttl_seconds: int = 7 * 24 * 3600):
        self.redis = redis
        self.keys = keys or RedisKeys()
        self.meta_ttl_seconds = meta_ttl_seconds
        self.events_maxlen = 10_000

    async def push_event(self, event: dict, *, maxlen: Optional[int] = None) -> None:
        """
        Best-effort append to the events stream.

        Stored as a Redis Stream entry at `keys.events()` with fields:
          - ts: epoch seconds
          - json: JSON payload
        """
        try:
            payload = json.dumps(event, default=str)
            await self.redis.xadd(
                self.keys.events(),
                {"ts": str(_now()), "json": payload},
                maxlen=int(maxlen or self.events_maxlen),
                approximate=True,
            )
        except Exception:
            return

    async def list_events(self, *, limit: int = 50) -> list[dict]:
        """
        List recent events (newest-first).
        """
        if limit <= 0:
            return []
        out: list[dict] = []
        try:
            entries = await self.redis.xrevrange(self.keys.events(), max="+", min="-", count=int(limit))
        except Exception:
            return []
        for event_id, fields in entries:
            row = {"id": str(event_id)}
            if isinstance(fields, dict) and fields.get("ts") is not None:
                try:
                    row["ts"] = float(fields["ts"])
                except Exception:
                    row["ts"] = fields.get("ts")
            if isinstance(fields, dict) and fields.get("json"):
                try:
                    row.update(json.loads(fields["json"]))
                except Exception:
                    row["json"] = fields.get("json")
            else:
                row["fields"] = fields
            out.append(row)
        return out

    async def upsert_on_enqueue(self, item: WorkItem, work_id: str) -> dict:
        now = _now()
        key = self.keys.work_meta(work_id)
        raw = await self.redis.get(key)
        if raw:
            try:
                meta = json.loads(raw)
            except Exception:
                meta = {}
        else:
            meta = {}

        meta.setdefault("work_id", work_id)
        meta.setdefault("engine", item.engine)
        meta.setdefault("created_at", float(meta.get("created_at") or item.created_at or now))
        meta["updated_at"] = now
        meta.setdefault("state", "queued")
        meta.setdefault("step", "init")

        pipe = self.redis.pipeline()
        pipe.set(key, json.dumps(meta), ex=self.meta_ttl_seconds)
        pipe.zadd(self.keys.works_index(), {work_id: meta["updated_at"]})
        pipe.zadd(self.keys.works_state_index(meta["state"]), {work_id: meta["updated_at"]})
        await pipe.execute()
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
        last_error: Optional[dict] = None,
    ) -> dict:
        """Set the primary execution state for a work item."""
        now = _now()
        key = self.keys.work_meta(work_id)
        raw = await self.redis.get(key)
        meta = {}
        if raw:
            try:
                meta = json.loads(raw)
            except Exception:
                meta = {}

        prev_state = str(meta.get("state") or "queued")
        prev_step = str(meta.get("step") or "")
        meta.setdefault("work_id", work_id)
        meta.setdefault("engine", engine)
        meta.setdefault("created_at", float(meta.get("created_at") or now))
        meta["updated_at"] = now
        meta["state"] = state
        if step is not None:
            meta["step"] = step
        if last_error is not None:
            meta["last_error"] = last_error

        pipe = self.redis.pipeline()
        pipe.set(key, json.dumps(meta), ex=self.meta_ttl_seconds)
        pipe.zadd(self.keys.works_index(), {work_id: meta["updated_at"]})
        pipe.zadd(self.keys.works_state_index(state), {work_id: meta["updated_at"]})
        if prev_state != state:
            pipe.zrem(self.keys.works_state_index(prev_state), work_id)
        await pipe.execute()
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
    ) -> dict:
        """
        Record a lightweight relationship in the parent meta.

        Stored as `child_work_ids` list (most-recent-first) with capped length.
        """
        now = _now()
        key = self.keys.work_meta(parent_work_id)
        raw = await self.redis.get(key)
        meta: dict = {}
        if raw:
            try:
                meta = json.loads(raw)
            except Exception:
                meta = {}

        children = meta.get("child_work_ids")
        if not isinstance(children, list):
            children = []
        child_entry = {"work_id": child_work_id, "relation": relation, "at": now}
        children = [c for c in children if not (isinstance(c, dict) and c.get("work_id") == child_work_id)]
        children.insert(0, child_entry)
        meta["child_work_ids"] = children[: max(1, int(max_children))]
        meta["updated_at"] = now

        pipe = self.redis.pipeline()
        pipe.set(key, json.dumps(meta), ex=self.meta_ttl_seconds)
        pipe.zadd(self.keys.works_index(), {parent_work_id: meta["updated_at"]})
        await pipe.execute()
        await self.push_event(
            {
                "kind": "work_child_linked",
                "work_id": parent_work_id,
                "child_work_id": child_work_id,
                "relation": relation,
            }
        )
        return meta

    async def get_meta(self, work_id: str) -> Optional[dict]:
        raw = await self.redis.get(self.keys.work_meta(work_id))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    async def list_work_ids(self, *, state: Optional[str], limit: int, offset: int) -> list[str]:
        zkey = self.keys.works_state_index(state) if state else self.keys.works_index()
        start = max(0, offset)
        end = start + max(0, limit) - 1
        if limit <= 0:
            return []
        return [str(x) for x in await self.redis.zrevrange(zkey, start, end)]

    async def list_meta(self, *, state: Optional[str], limit: int = 20, offset: int = 0) -> list[dict]:
        work_ids = await self.list_work_ids(state=state, limit=limit, offset=offset)
        if not work_ids:
            return []
        keys = [self.keys.work_meta(wid) for wid in work_ids]
        raws = await self.redis.mget(keys)
        out: list[dict] = []
        for wid, raw in zip(work_ids, raws, strict=False):
            if not raw:
                out.append({"work_id": wid})
                continue
            try:
                out.append(json.loads(raw))
            except Exception:
                out.append({"work_id": wid})
        return out

    async def push_dlq(self, event: dict, *, maxlen: int = 2000) -> None:
        payload = json.dumps(event)
        pipe = self.redis.pipeline()
        pipe.lpush(self.keys.dlq(), payload)
        pipe.ltrim(self.keys.dlq(), 0, maxlen - 1)
        await pipe.execute()
        await self.push_event(
            {
                "kind": "dlq_pushed",
                "work_id": event.get("work_id"),
                "engine": event.get("engine"),
                "step": event.get("step"),
                "event_kind": event.get("kind"),
                "last_error": event.get("last_error"),
            }
        )

    async def list_dlq(self, *, limit: int = 50, offset: int = 0) -> list[dict]:
        start = max(0, offset)
        end = start + max(0, limit) - 1
        raws = await self.redis.lrange(self.keys.dlq(), start, end)
        out: list[dict] = []
        for raw in raws:
            try:
                out.append(json.loads(raw))
            except Exception:
                out.append({"raw": raw})
        return out

    async def get_dlq(self, index: int) -> Optional[dict]:
        if index < 0:
            return None
        raw = await self.redis.lindex(self.keys.dlq(), index)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return {"raw": raw}

    async def ack_dlq(self, index: int) -> bool:
        """
        Best-effort acknowledge of a DLQ entry by index.

        Uses LSET+LREM to remove one element without reading/rewriting the whole list.
        """
        if index < 0:
            return False
        tombstone = f"__ack__:{uuid.uuid4().hex}"
        try:
            await self.redis.lset(self.keys.dlq(), index, tombstone)
        except Exception:
            return False
        removed = int(await self.redis.lrem(self.keys.dlq(), 1, tombstone))
        return removed == 1
