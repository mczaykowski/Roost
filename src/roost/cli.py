from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Optional

from roost.runtime.config import (
    DEFAULT_QUEUE,
    DEFAULT_REDIS_PREFIX,
    DEFAULT_REDIS_URL,
    DEFAULT_RUNTIME_MODE,
    DEFAULT_WORKSPACE_MODE,
    load_roost_config,
    resolve_config_relative_path,
    resolve_roost_config_path,
)
from roost.runtime.models import WorkItem
from roost.runtime.namespacing import apply_namespace, resolve_artifact_root, resolve_workspace_root
from roost.runtime.registry import EngineRegistry


def _missing_redis_deps() -> list[str]:
    missing = []
    for module in ("redis", "saq"):
        try:
            __import__(module)
        except Exception:
            missing.append(module)
    return missing


def _missing_postgres_deps() -> list[str]:
    missing = []
    try:
        __import__("psycopg")
    except Exception:
        missing.append("psycopg")
    return missing


def _require_redis_deps() -> None:
    missing = _missing_redis_deps()
    if missing:
        raise SystemExit(
            "Missing Redis runtime dependencies. Install with:\n"
            "  uv sync --extra redis\n"
            f"Missing: {', '.join(missing)}"
        )


async def _record_operator_action(
    postgres: Any,
    *,
    action: str,
    work_id: str | None,
    actor: str = "cli",
    payload: dict[str, Any] | None = None,
) -> None:
    """Best-effort audit write; never fails the operator command."""
    if postgres is None:
        return
    try:
        from roost.runtime.backends.postgres import PostgresOperatorActionStore

        await PostgresOperatorActionStore(postgres).record(
            action, work_id, actor, payload or {}
        )
    except Exception:
        logging.getLogger("roost").exception(
            "failed to record operator action %s for work_id=%s", action, work_id
        )


def _require_postgres_runtime(args: argparse.Namespace) -> None:
    if not getattr(args, "postgres_url", None):
        raise SystemExit("Postgres URL is required for production mode")
    missing = _missing_postgres_deps()
    if missing:
        raise SystemExit(
            "Missing Postgres runtime dependencies. Install with:\n"
            "  uv sync --extra postgres\n"
            f"Missing: {', '.join(missing)}"
        )


def _json_loads(value: str) -> dict[str, Any]:
    try:
        out = json.loads(value)
    except Exception as exc:
        raise SystemExit(f"Invalid JSON: {exc}") from exc
    if not isinstance(out, dict):
        raise SystemExit("JSON payload must be an object")
    return out


def _choose(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _apply_runtime_config(args: argparse.Namespace) -> argparse.Namespace:
    repo_path = getattr(args, "repo_path", ".")
    config_path, explicit = resolve_roost_config_path(
        repo_path=repo_path,
        cli_path=getattr(args, "config", None),
    )
    config = load_roost_config(config_path, explicit=explicit)
    args.config_path = config_path
    args.roost_config = config

    if hasattr(args, "runtime_mode"):
        args.runtime_mode = _choose(args.runtime_mode, config.runtime.mode if config else None, DEFAULT_RUNTIME_MODE)
    if hasattr(args, "postgres_url"):
        args.postgres_url = _choose(
            args.postgres_url,
            os.getenv("ROOST_POSTGRES_URL"),
            config.postgres.url if config else None,
        )
    if hasattr(args, "redis_url"):
        args.redis_url = _choose(
            args.redis_url,
            os.getenv("ROOST_REDIS_URL"),
            config.redis.url if config else None,
            DEFAULT_REDIS_URL,
        )
    if hasattr(args, "queue"):
        args.queue = _choose(args.queue, os.getenv("ROOST_QUEUE"), config.redis.queue if config else None, DEFAULT_QUEUE)
    if hasattr(args, "redis_prefix"):
        args.redis_prefix = _choose(
            args.redis_prefix,
            os.getenv("ROOST_REDIS_PREFIX"),
            config.redis.prefix if config else None,
            DEFAULT_REDIS_PREFIX,
        )
    if hasattr(args, "namespace"):
        args.namespace = _choose(args.namespace, os.getenv("ROOST_NAMESPACE"), config.redis.namespace if config else None)
    if hasattr(args, "engines"):
        args.engines = _choose(args.engines, config.worker.engines if config else None, "watchlist")
    if hasattr(args, "concurrency"):
        args.concurrency = _choose(args.concurrency, config.worker.concurrency if config else None, 4)
    if hasattr(args, "timeout"):
        args.timeout = _choose(args.timeout, config.worker.timeout_seconds if config else None, 120)
    if hasattr(args, "retries"):
        args.retries = _choose(args.retries, config.worker.retries if config else None, 5)
    if hasattr(args, "lease_ttl"):
        args.lease_ttl = _choose(args.lease_ttl, config.worker.lease_ttl_seconds if config else None, 60)
    if hasattr(args, "workspace_root"):
        configured = resolve_config_relative_path(
            config.worker.workspace_root if config else None,
            config_path=config_path,
            repo_path=repo_path,
        )
        args.workspace_root = _choose(args.workspace_root, os.getenv("ROOST_WORKSPACE_ROOT"), configured)
    if hasattr(args, "workspace_mode"):
        args.workspace_mode = _choose(
            args.workspace_mode,
            os.getenv("ROOST_WORKSPACE_MODE"),
            config.worker.workspace_mode if config else None,
            DEFAULT_WORKSPACE_MODE,
        )
    if hasattr(args, "artifact_root"):
        configured = resolve_config_relative_path(
            config.artifacts.root if config else None,
            config_path=config_path,
            repo_path=repo_path,
        )
        args.artifact_root = _choose(args.artifact_root, os.getenv("ROOST_ARTIFACT_ROOT"), configured)
    return args


def _roost_toml_template(args: argparse.Namespace) -> str:
    namespace = f'namespace = "{args.namespace}"\n' if args.namespace else "# namespace = \"dev\"\n"
    postgres_url = f'url = "{args.postgres_url}"\n' if args.postgres_url else '# url = "postgresql://localhost/roost"\n'
    return f"""# Roost runtime configuration.
# CLI flags override environment variables; environment variables override this file.

[runtime]
mode = "{args.runtime_mode}"

[redis]
url = "{args.redis_url}"
queue = "{args.queue}"
prefix = "{args.redis_prefix}"
{namespace}
[postgres]
{postgres_url}

[worker]
engines = "{args.engines}"
concurrency = {args.concurrency}
timeout_seconds = {args.timeout}
retries = {args.retries}
lease_ttl_seconds = {args.lease_ttl}
workspace_root = "{args.workspace_root}"
workspace_mode = "{args.workspace_mode}"

[artifacts]
root = "{args.artifact_root}"

# [[triggers]]
# on_engine_done = "watchlist"
# enqueue_engine = "demo"
"""


def _cmd_init(args: argparse.Namespace) -> None:
    path = os.path.abspath(args.path)
    if os.path.exists(path) and not args.force:
        raise SystemExit(f"{path} already exists. Re-run with --force to overwrite it.")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(_roost_toml_template(args))
    print(f"Created {path}")
    print("Next: uv run roost doctor --config " + path)


def _cmd_engines(_args: argparse.Namespace) -> None:
    registry = EngineRegistry.from_entry_points()
    for info in registry.info():
        print(f"{info.engine_id}\t{info.source}")


def _cmd_enqueue(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()

    from redis import asyncio as aioredis
    from saq import Queue

    from roost.runtime.backends.redis import RedisControlPlane, RedisKeys, RedisWorkItemStore

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        postgres = None
        queue = Queue.from_url(args.redis_url, name=args.queue)
        try:
            if args.runtime_mode == "production":
                _require_postgres_runtime(args)

                from roost.runtime.backends.postgres import (
                    PostgresControlPlaneStore,
                    PostgresWorkItemStore,
                    connect_postgres_pool,
                )

                postgres = await connect_postgres_pool(args.postgres_url)
                store = PostgresWorkItemStore(postgres)
                control = PostgresControlPlaneStore(postgres)
            else:
                store = RedisWorkItemStore(redis, keys=keys)
                control = RedisControlPlane(redis, keys=keys)

            payload = _json_loads(args.payload)
            work_id = args.work_id or uuid.uuid4().hex
            item = WorkItem(
                work_id=work_id,
                engine=args.engine,
                payload=payload,
                priority=args.priority,
                resources=list(args.resource or []),
                idempotency_key=args.idempotency_key,
            )
            canonical_id = await store.get_or_claim_work_id(item)
            await control.upsert_on_enqueue(item, canonical_id)
            await queue.enqueue(
                "work_step",
                work_id=canonical_id,
                scheduled=int(time.time() + args.delay_seconds) if args.delay_seconds else 0,
                timeout=args.timeout,
                retries=args.retries,
            )
            print(canonical_id)
        finally:
            if postgres:
                await postgres.close()
            await redis.aclose()

    asyncio.run(run())


def _cmd_worker(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()

    from roost.runtime.swarm import RedisSwarm, RedisUniversalSwarm, SwarmConfig

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        workspace_root = resolve_workspace_root(
            repo_path=args.repo_path,
            workspace_root=args.workspace_root,
            namespace=args.namespace,
        )
        artifact_root = resolve_artifact_root(
            repo_path=args.repo_path,
            artifact_root=args.artifact_root,
            namespace=args.namespace,
        )

        registry = EngineRegistry.from_entry_points()
        selected = registry.engine_ids() if args.engines == "all" else [
            e.strip() for e in args.engines.split(",") if e.strip()
        ]
        if not selected:
            raise SystemExit("No engines selected")

        engine_kwargs = {
            "repo_path": os.path.abspath(args.repo_path),
            "redis_url": args.redis_url,
            "redis_prefix": redis_prefix,
            "workspace_root": workspace_root,
            "workspace_mode": args.workspace_mode,
            "artifact_root": artifact_root,
        }
        engines = {engine_id: registry.create(engine_id, **engine_kwargs) for engine_id in selected}
        config = SwarmConfig(
            redis_url=args.redis_url,
            queue_name=args.queue,
            redis_prefix=redis_prefix,
            runtime_mode=args.runtime_mode,
            postgres_url=args.postgres_url,
            lease_ttl_seconds=args.lease_ttl,
            job_timeout_seconds=args.timeout,
            job_retries=args.retries,
            roost_config=args.roost_config,
        )
        swarm = (
            RedisSwarm(next(iter(engines.values())), config=config)
            if len(engines) == 1
            else RedisUniversalSwarm(engines, config=config)
        )
        try:
            await swarm.run_worker(concurrency=args.concurrency)
        finally:
            await swarm.close()

    asyncio.run(run())


def _cmd_status(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()

    from redis import asyncio as aioredis

    from roost.runtime.backends.redis import RedisControlPlane, RedisKeys, RedisSnapshotStore, RedisWorkItemStore

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        postgres = None
        try:
            if args.runtime_mode == "production":
                _require_postgres_runtime(args)
                from roost.runtime.backends.postgres import (
                    PostgresControlPlaneStore,
                    PostgresSnapshotStore,
                    PostgresWorkItemStore,
                    connect_postgres_pool,
                )

                postgres = await connect_postgres_pool(args.postgres_url)
                control = PostgresControlPlaneStore(postgres)
                snapshots = PostgresSnapshotStore(postgres)
                items = PostgresWorkItemStore(postgres)
            else:
                control = RedisControlPlane(redis, keys=keys)
                snapshots = RedisSnapshotStore(redis, keys=keys)
                items = RedisWorkItemStore(redis, keys=keys)

            item = await items.get(args.work_id)
            snapshot = await snapshots.load(args.work_id)
            out = {
                "meta": await control.get_meta(args.work_id),
                "item": item.model_dump(mode="json") if item else None,
                "snapshot": snapshot.model_dump(mode="json") if snapshot else None,
            }
            print(json.dumps(out, indent=2, sort_keys=True, default=str))
        finally:
            if postgres:
                await postgres.close()
            await redis.aclose()

    asyncio.run(run())


def _cmd_inspect(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()

    from redis import asyncio as aioredis

    from roost.runtime.backends.redis import (
        RedisControlPlane,
        RedisInflightStore,
        RedisKeys,
        RedisSnapshotStore,
        RedisWorkItemStore,
    )

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        postgres = None
        try:
            if args.runtime_mode == "production":
                _require_postgres_runtime(args)
                from roost.runtime.backends.postgres import (
                    PostgresControlPlaneStore,
                    PostgresLeaseStore,
                    PostgresSnapshotStore,
                    PostgresWorkItemStore,
                    connect_postgres_pool,
                )

                postgres = await connect_postgres_pool(args.postgres_url)
                control = PostgresControlPlaneStore(postgres)
                snapshots = PostgresSnapshotStore(postgres)
                items = PostgresWorkItemStore(postgres)
                lease_detail: int | bool = await PostgresLeaseStore(postgres).is_active(args.work_id)
                lease_key = "lease_active"
            else:
                control = RedisControlPlane(redis, keys=keys)
                snapshots = RedisSnapshotStore(redis, keys=keys)
                items = RedisWorkItemStore(redis, keys=keys)
                lease_detail = await redis.ttl(keys.lease(args.work_id))
                lease_key = "lease_ttl_seconds"
            inflight = RedisInflightStore(redis, keys=keys)
            out = {
                "meta": await control.get_meta(args.work_id),
                "item": (await items.get(args.work_id)),
                "snapshot": (await snapshots.load(args.work_id)),
                "inflight": await inflight.get(args.work_id),
                lease_key: lease_detail,
            }
            out["item"] = out["item"].model_dump(mode="json") if out["item"] else None
            out["snapshot"] = out["snapshot"].model_dump(mode="json") if out["snapshot"] else None
            print(json.dumps(out, indent=2, sort_keys=True, default=str))
        finally:
            if postgres:
                await postgres.close()
            await redis.aclose()

    asyncio.run(run())


def _cmd_retry(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()

    from redis import asyncio as aioredis
    from saq import Queue

    from roost.runtime.backends.redis import RedisControlPlane, RedisInflightStore, RedisKeys, RedisWorkItemStore

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        queue = Queue.from_url(args.redis_url, name=args.queue)
        postgres = None
        try:
            if args.runtime_mode == "production":
                _require_postgres_runtime(args)
                from roost.runtime.backends.postgres import (
                    PostgresControlPlaneStore,
                    PostgresWorkItemStore,
                    connect_postgres_pool,
                )

                postgres = await connect_postgres_pool(args.postgres_url)
                items = PostgresWorkItemStore(postgres)
                control = PostgresControlPlaneStore(postgres)
            else:
                items = RedisWorkItemStore(redis, keys=keys)
                control = RedisControlPlane(redis, keys=keys)

            item = await items.get(args.work_id)
            if not item:
                raise SystemExit(f"Work item not found: {args.work_id}")
            await RedisInflightStore(redis, keys=keys).clear(args.work_id)
            await control.set_state(work_id=args.work_id, engine=item.engine, state="queued", step=args.step or "retry")
            await queue.enqueue(
                "work_step",
                work_id=args.work_id,
                scheduled=int(time.time() + args.delay_seconds) if args.delay_seconds else 0,
                timeout=args.timeout,
                retries=args.retries,
            )
            await control.push_event(
                {
                    "kind": "work_retry_requested",
                    "work_id": args.work_id,
                    "engine": item.engine,
                    "delay_seconds": args.delay_seconds,
                }
            )
            await _record_operator_action(
                postgres,
                action="retry",
                work_id=args.work_id,
                payload={"engine": item.engine, "delay_seconds": args.delay_seconds},
            )
            print(json.dumps({"work_id": args.work_id, "state": "queued"}, indent=2, sort_keys=True))
        finally:
            if postgres:
                await postgres.close()
            await redis.aclose()

    asyncio.run(run())


def _cmd_cancel(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()

    from redis import asyncio as aioredis

    from roost.runtime.backends.redis import (
        RedisControlPlane,
        RedisInflightStore,
        RedisKeys,
        RedisLeaseManager,
        RedisResourceManager,
        RedisWorkItemStore,
    )

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        postgres = None
        try:
            if args.runtime_mode == "production":
                _require_postgres_runtime(args)
                from roost.runtime.backends.postgres import (
                    PostgresControlPlaneStore,
                    PostgresLeaseStore,
                    PostgresResourceStore,
                    PostgresWorkItemStore,
                    connect_postgres_pool,
                )

                postgres = await connect_postgres_pool(args.postgres_url)
                items = PostgresWorkItemStore(postgres)
                control = PostgresControlPlaneStore(postgres)
                lease_store = PostgresLeaseStore(postgres)
                resource_store = PostgresResourceStore(postgres)
            else:
                items = RedisWorkItemStore(redis, keys=keys)
                control = RedisControlPlane(redis, keys=keys)
                lease_store = RedisLeaseManager(redis, keys=keys)
                resource_store = RedisResourceManager(redis, keys=keys)

            item = await items.get(args.work_id)
            if not item:
                raise SystemExit(f"Work item not found: {args.work_id}")
            await RedisInflightStore(redis, keys=keys).clear(args.work_id)
            leases_cleared = await lease_store.clear(args.work_id)
            resources_cleared = await resource_store.clear(resources=item.resources)
            meta = await control.set_state(
                work_id=args.work_id,
                engine=item.engine,
                state="cancelled",
                step=args.reason or "cancelled",
            )
            await control.push_event(
                {
                    "kind": "work_cancelled",
                    "work_id": args.work_id,
                    "engine": item.engine,
                    "reason": args.reason,
                    "leases_cleared": leases_cleared,
                    "resources_cleared": resources_cleared,
                }
            )
            await _record_operator_action(
                postgres,
                action="cancel",
                work_id=args.work_id,
                payload={
                    "engine": item.engine,
                    "reason": args.reason,
                    "leases_cleared": leases_cleared,
                    "resources_cleared": resources_cleared,
                },
            )
            print(json.dumps(meta, indent=2, sort_keys=True, default=str))
        finally:
            if postgres:
                await postgres.close()
            await redis.aclose()

    asyncio.run(run())


def _cmd_dlq(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()

    from redis import asyncio as aioredis
    from saq import Queue

    from roost.runtime.backends.redis import RedisControlPlane, RedisInflightStore, RedisKeys, RedisWorkItemStore

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        postgres = None
        try:
            if args.runtime_mode == "production":
                _require_postgres_runtime(args)
                from roost.runtime.backends.postgres import (
                    PostgresControlPlaneStore,
                    PostgresWorkItemStore,
                    connect_postgres_pool,
                )

                postgres = await connect_postgres_pool(args.postgres_url)
                control = PostgresControlPlaneStore(postgres)
                items = PostgresWorkItemStore(postgres)
            else:
                control = RedisControlPlane(redis, keys=keys)
                items = RedisWorkItemStore(redis, keys=keys)

            if args.dlq_cmd == "list":
                rows = await control.list_dlq(limit=args.limit, offset=args.offset)
                print(json.dumps(rows, indent=2, sort_keys=True, default=str))
                return

            entry = await control.get_dlq(args.index)
            if not entry:
                raise SystemExit(f"DLQ entry not found at index {args.index}")
            work_id = str(entry.get("work_id") or "")
            if not work_id:
                raise SystemExit(f"DLQ entry at index {args.index} does not include a work_id")

            if args.dlq_cmd == "ack":
                ok = await control.ack_dlq(args.index)
                if ok:
                    await _record_operator_action(
                        postgres,
                        action="dlq_ack",
                        work_id=work_id,
                        payload={"index": args.index},
                    )
                print(json.dumps({"acked": ok, "index": args.index}, indent=2, sort_keys=True))
                return

            item = await items.get(work_id)
            if not item:
                raise SystemExit(f"Work item not found: {work_id}")
            queue = Queue.from_url(args.redis_url, name=args.queue)
            await RedisInflightStore(redis, keys=keys).clear(work_id)
            await control.set_state(work_id=work_id, engine=item.engine, state="queued", step=args.step or "dlq_replay")
            await queue.enqueue(
                "work_step",
                work_id=work_id,
                scheduled=int(time.time() + args.delay_seconds) if args.delay_seconds else 0,
                timeout=args.timeout,
                retries=args.retries,
            )
            acked = await control.ack_dlq(args.index) if args.ack else False
            await control.push_event(
                {
                    "kind": "dlq_replay_requested",
                    "work_id": work_id,
                    "engine": item.engine,
                    "index": args.index,
                    "acked": acked,
                }
            )
            await _record_operator_action(
                postgres,
                action="dlq_replay",
                work_id=work_id,
                payload={"engine": item.engine, "index": args.index, "acked": acked},
            )
            print(json.dumps({"work_id": work_id, "state": "queued", "acked": acked}, indent=2, sort_keys=True))
        finally:
            if postgres:
                await postgres.close()
            await redis.aclose()

    asyncio.run(run())


def _cmd_list(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()

    from redis import asyncio as aioredis

    from roost.runtime.backends.redis import RedisControlPlane, RedisKeys

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        postgres = None
        try:
            if args.runtime_mode == "production":
                _require_postgres_runtime(args)

                from roost.runtime.backends.postgres import PostgresControlPlaneStore, connect_postgres_pool

                postgres = await connect_postgres_pool(args.postgres_url)
                control = PostgresControlPlaneStore(postgres)
            else:
                control = RedisControlPlane(redis, keys=keys)

            rows = await control.list_meta(
                state=args.state,
                limit=args.limit,
                offset=args.offset,
            )
            print(json.dumps(rows, indent=2, sort_keys=True, default=str))
        finally:
            if postgres:
                await postgres.close()
            await redis.aclose()

    asyncio.run(run())


def _cmd_events(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()

    from redis import asyncio as aioredis

    from roost.runtime.backends.redis import RedisControlPlane, RedisKeys

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        postgres = None
        try:
            if args.runtime_mode == "production":
                _require_postgres_runtime(args)

                from roost.runtime.backends.postgres import PostgresControlPlaneStore, connect_postgres_pool

                postgres = await connect_postgres_pool(args.postgres_url)
                control = PostgresControlPlaneStore(postgres)
            else:
                control = RedisControlPlane(redis, keys=keys)

            events = await control.list_events(limit=args.limit)
            print(json.dumps(events, indent=2, sort_keys=True, default=str))
        finally:
            if postgres:
                await postgres.close()
            await redis.aclose()

    asyncio.run(run())


def _cmd_workers(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)

    async def run() -> None:
        if args.runtime_mode != "production":
            print(
                json.dumps(
                    {
                        "runtime_mode": args.runtime_mode,
                        "stale_after_seconds": args.stale_after,
                        "rows": [],
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return

        _require_postgres_runtime(args)

        from roost.runtime.backends.postgres import (
            PostgresWorkerHeartbeatStore,
            open_postgres_pool,
        )

        async with open_postgres_pool(args.postgres_url) as postgres:
            rows = await PostgresWorkerHeartbeatStore(postgres).list_workers(
                limit=args.limit,
                stale_after_seconds=args.stale_after,
            )
        print(
            json.dumps(
                {
                    "runtime_mode": args.runtime_mode,
                    "stale_after_seconds": args.stale_after,
                    "rows": rows,
                },
                indent=2,
                sort_keys=True,
                default=str,
            )
        )

    asyncio.run(run())


def _cmd_actions(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)

    async def run() -> None:
        if args.runtime_mode != "production":
            print(
                json.dumps(
                    {"runtime_mode": args.runtime_mode, "rows": []},
                    indent=2,
                    sort_keys=True,
                )
            )
            return

        _require_postgres_runtime(args)

        from roost.runtime.backends.postgres import (
            PostgresOperatorActionStore,
            open_postgres_pool,
        )

        async with open_postgres_pool(args.postgres_url) as postgres:
            store = PostgresOperatorActionStore(postgres)
            if args.work_id:
                rows = await store.list_for_work(args.work_id, limit=args.limit)
            else:
                rows = await store.list_recent(limit=args.limit)
        print(
            json.dumps(
                {"runtime_mode": args.runtime_mode, "rows": rows},
                indent=2,
                sort_keys=True,
                default=str,
            )
        )

    asyncio.run(run())


def _cmd_workspace_path(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    from roost.runtime.workspaces import WorkspaceManager, WorkspaceSpec

    root = resolve_workspace_root(
        repo_path=args.repo_path,
        workspace_root=args.workspace_root,
        namespace=args.namespace,
    )
    manager = WorkspaceManager(
        WorkspaceSpec(base_repo_path=os.path.abspath(args.repo_path), root_dir=root, mode=args.workspace_mode)
    )
    print(manager.workspace_path(args.work_id))


def _cmd_artifact_show(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    from roost.runtime.artifacts import FileArtifactStore

    root = resolve_artifact_root(
        repo_path=args.repo_path,
        artifact_root=args.artifact_root,
        namespace=args.namespace,
    )
    store = FileArtifactStore(root_dir=root)
    content = store.read_bytes(args.artifact_id, ext=args.ext)
    if content is None:
        raise SystemExit(f"Artifact not found: {args.artifact_id}")
    print(content.decode(args.encoding, errors="replace"))


def _cmd_ui(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)
    _require_redis_deps()
    if args.runtime_mode == "production":
        _require_postgres_runtime(args)

    from roost.ui.server import config_from_args, run_console

    run_console(host=args.host, port=args.port, config=config_from_args(args))


def _cmd_migrate(args: argparse.Namespace) -> None:
    _apply_runtime_config(args)

    from roost.runtime.backends.postgres import apply_migrations, list_migrations

    if args.plan:
        rows = [
            {
                "version": migration.version,
                "name": migration.name,
                "checksum": migration.checksum,
            }
            for migration in list_migrations()
        ]
        print(json.dumps(rows, indent=2, sort_keys=True))
        return

    if not args.postgres_url:
        raise SystemExit(
            "Postgres URL is required. Pass --postgres-url, set ROOST_POSTGRES_URL, "
            "or add [postgres].url to roost.toml."
        )

    try:
        rows = apply_migrations(args.postgres_url)
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(rows, indent=2, sort_keys=True, default=str))


def _nearest_existing_parent(path: str) -> str:
    current = os.path.abspath(path)
    while not os.path.exists(current):
        parent = os.path.dirname(current)
        if parent == current:
            return current
        current = parent
    return current


def _doctor_line(status: str, label: str, detail: str = "") -> None:
    suffix = f" - {detail}" if detail else ""
    print(f"{status:<4} {label}{suffix}")


def _cmd_doctor(args: argparse.Namespace) -> None:
    failures = 0
    warnings = 0
    print("Roost doctor")

    try:
        _apply_runtime_config(args)
        if args.config_path:
            _doctor_line("OK", "config", args.config_path)
        else:
            warnings += 1
            _doctor_line("WARN", "config", "no roost.toml found; using CLI/env/defaults")
    except Exception as exc:
        _doctor_line("FAIL", "config", str(exc))
        raise SystemExit(1) from exc

    try:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        _doctor_line("OK", "namespace", redis_prefix)
    except Exception as exc:
        failures += 1
        _doctor_line("FAIL", "namespace", str(exc))
        redis_prefix = args.redis_prefix

    missing = _missing_redis_deps()
    if missing:
        failures += 1
        _doctor_line("FAIL", "redis dependencies", f"missing: {', '.join(missing)}")
    else:
        _doctor_line("OK", "redis dependencies", "redis and saq installed")

        async def ping() -> None:
            from redis import asyncio as aioredis

            redis = aioredis.from_url(args.redis_url, decode_responses=True)
            try:
                await redis.ping()
            finally:
                await redis.aclose()

        try:
            asyncio.run(ping())
            _doctor_line("OK", "redis connection", args.redis_url)
        except Exception as exc:
            failures += 1
            _doctor_line("FAIL", "redis connection", f"{args.redis_url} ({exc})")

    runtime_mode = getattr(args, "runtime_mode", "simple") or "simple"
    _doctor_line("OK", "runtime mode", runtime_mode)
    if runtime_mode == "production" or getattr(args, "postgres_url", None):
        if not args.postgres_url:
            failures += 1
            _doctor_line("FAIL", "postgres url", "required for production mode")
        else:
            missing_postgres = _missing_postgres_deps()
            if missing_postgres:
                failures += 1
                _doctor_line("FAIL", "postgres dependencies", f"missing: {', '.join(missing_postgres)}")
            else:
                _doctor_line("OK", "postgres dependencies", "psycopg installed")
                try:
                    import psycopg
                    from roost.runtime.backends.postgres import check_migrations

                    with psycopg.connect(args.postgres_url, connect_timeout=3) as conn:
                        conn.execute("SELECT 1")
                    _doctor_line("OK", "postgres connection", args.postgres_url)

                    migration_rows = check_migrations(args.postgres_url)
                    not_applied = [row for row in migration_rows if row.get("state") != "applied"]
                    if not_applied:
                        failures += 1
                        details = ", ".join(
                            f"{row['version']}:{row['state']}" for row in not_applied
                        )
                        _doctor_line("FAIL", "postgres migrations", f"{details}; run roost migrate")
                    else:
                        versions = ", ".join(str(row["version"]) for row in migration_rows)
                        _doctor_line("OK", "postgres migrations", versions)
                except Exception as exc:
                    failures += 1
                    _doctor_line("FAIL", "postgres connection", f"{args.postgres_url} ({exc})")

    registry = EngineRegistry.from_entry_points()
    available = registry.engine_ids()
    selected = available if args.engines == "all" else [e.strip() for e in args.engines.split(",") if e.strip()]
    unknown = [engine for engine in selected if engine not in available]
    if unknown:
        failures += 1
        _doctor_line("FAIL", "engines", f"unknown: {', '.join(unknown)}; available: {', '.join(available)}")
    else:
        _doctor_line("OK", "engines", ", ".join(selected) if selected else "(none)")

    artifact_root = resolve_artifact_root(
        repo_path=args.repo_path,
        artifact_root=args.artifact_root,
        namespace=args.namespace,
    )
    artifact_parent = _nearest_existing_parent(artifact_root)
    if os.path.isdir(artifact_root):
        writable = os.access(artifact_root, os.W_OK)
        if writable:
            _doctor_line("OK", "artifacts", artifact_root)
        else:
            failures += 1
            _doctor_line("FAIL", "artifacts", f"{artifact_root} is not writable")
    elif os.access(artifact_parent, os.W_OK):
        _doctor_line("OK", "artifacts", f"{artifact_root} can be created")
    else:
        failures += 1
        _doctor_line("FAIL", "artifacts", f"parent is not writable: {artifact_parent}")

    workspace_root = resolve_workspace_root(
        repo_path=args.repo_path,
        workspace_root=args.workspace_root,
        namespace=args.namespace,
    )
    workspace_parent = _nearest_existing_parent(workspace_root)
    if args.workspace_mode not in {"worktree", "clone"}:
        failures += 1
        _doctor_line("FAIL", "workspace mode", args.workspace_mode)
    elif os.path.isdir(workspace_root):
        _doctor_line("OK", "workspace", f"{workspace_root} ({args.workspace_mode})")
    elif os.access(workspace_parent, os.W_OK):
        _doctor_line("OK", "workspace", f"{workspace_root} can be created ({args.workspace_mode})")
    else:
        failures += 1
        _doctor_line("FAIL", "workspace", f"parent is not writable: {workspace_parent}")

    _doctor_line("OK", "queue", args.queue)
    from roost.runtime.metrics import enabled as metrics_enabled

    if metrics_enabled():
        _doctor_line("OK", "metrics", "enabled")
    else:
        warnings += 1
        _doctor_line("WARN", "metrics", "install roost-runtime[metrics]")
    if failures:
        print(f"\n{failures} check(s) failed.")
        raise SystemExit(1)
    if warnings:
        print(f"\nPassed with {warnings} warning(s).")
    else:
        print("\nAll checks passed.")


def _add_config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", help="Path to roost.toml")


def _add_redis_args(parser: argparse.ArgumentParser, *, production: bool = False) -> None:
    _add_config_arg(parser)
    parser.add_argument("--redis-url")
    parser.add_argument("--queue")
    parser.add_argument("--redis-prefix")
    parser.add_argument("--namespace")
    if production:
        parser.add_argument("--runtime-mode", choices=["simple", "production"])
        parser.add_argument("--postgres-url")


def _add_postgres_args(parser: argparse.ArgumentParser) -> None:
    _add_config_arg(parser)
    parser.add_argument("--runtime-mode", choices=["simple", "production"])
    parser.add_argument("--postgres-url")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Roost durable runtime for agent step-machines")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init", help="Create a minimal roost.toml")
    p.add_argument("--path", default="roost.toml")
    p.add_argument("--force", action="store_true")
    p.add_argument("--runtime-mode", choices=["simple", "production"], default=DEFAULT_RUNTIME_MODE)
    p.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    p.add_argument("--postgres-url")
    p.add_argument("--queue", default=DEFAULT_QUEUE)
    p.add_argument("--redis-prefix", default=DEFAULT_REDIS_PREFIX)
    p.add_argument("--namespace")
    p.add_argument("--engines", default="watchlist")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--lease-ttl", type=int, default=60)
    p.add_argument("--workspace-root", default=".roost/workspaces")
    p.add_argument("--workspace-mode", choices=["worktree", "clone"], default=DEFAULT_WORKSPACE_MODE)
    p.add_argument("--artifact-root", default=".roost/artifacts")
    p.set_defaults(fn=_cmd_init)

    p = sub.add_parser("engines", help="List installed engine entry points")
    p.set_defaults(fn=_cmd_engines)

    p = sub.add_parser("enqueue", help="Enqueue a work item")
    _add_redis_args(p, production=True)
    p.add_argument("--engine", required=True)
    p.add_argument("--payload", required=True, help="JSON object")
    p.add_argument("--work-id")
    p.add_argument("--idempotency-key")
    p.add_argument("--priority", type=int, default=0)
    p.add_argument("--resource", action="append")
    p.add_argument("--delay-seconds", type=int, default=0)
    p.add_argument("--timeout", type=int)
    p.add_argument("--retries", type=int)
    p.set_defaults(fn=_cmd_enqueue)

    p = sub.add_parser("worker", help="Run a worker")
    _add_redis_args(p, production=True)
    p.add_argument("--repo-path", default=".")
    p.add_argument("--engines", help="Comma-separated engine ids, or all")
    p.add_argument("--concurrency", type=int)
    p.add_argument("--timeout", type=int)
    p.add_argument("--retries", type=int)
    p.add_argument("--lease-ttl", type=int)
    p.add_argument("--workspace-root")
    p.add_argument("--workspace-mode", choices=["worktree", "clone"])
    p.add_argument("--artifact-root")
    p.set_defaults(fn=_cmd_worker)

    p = sub.add_parser("status", help="Show work item metadata and snapshot")
    _add_redis_args(p, production=True)
    p.add_argument("work_id")
    p.set_defaults(fn=_cmd_status)

    p = sub.add_parser("inspect", help="Show work item, snapshot, inflight, and lease details")
    _add_redis_args(p, production=True)
    p.add_argument("work_id")
    p.set_defaults(fn=_cmd_inspect)

    p = sub.add_parser("retry", help="Re-enqueue existing work")
    _add_redis_args(p, production=True)
    p.add_argument("work_id")
    p.add_argument("--delay-seconds", type=int, default=0)
    p.add_argument("--timeout", type=int)
    p.add_argument("--retries", type=int)
    p.add_argument("--step")
    p.set_defaults(fn=_cmd_retry)

    p = sub.add_parser("cancel", help="Mark work as cancelled and clear local ownership markers")
    _add_redis_args(p, production=True)
    p.add_argument("work_id")
    p.add_argument("--reason")
    p.set_defaults(fn=_cmd_cancel)

    p = sub.add_parser("dlq", help="List, re-enqueue from latest snapshot, or acknowledge dead-lettered work")
    dlq_sub = p.add_subparsers(dest="dlq_cmd", required=True)

    dlq_list = dlq_sub.add_parser("list", help="List dead-letter entries")
    _add_redis_args(dlq_list, production=True)
    dlq_list.add_argument("--limit", type=int, default=50)
    dlq_list.add_argument("--offset", type=int, default=0)
    dlq_list.set_defaults(fn=_cmd_dlq)

    dlq_replay = dlq_sub.add_parser("replay", help="Re-enqueue a dead-letter entry from its latest snapshot")
    _add_redis_args(dlq_replay, production=True)
    dlq_replay.add_argument("index", type=int)
    dlq_replay.add_argument("--ack", action="store_true", help="Remove the DLQ entry after enqueueing")
    dlq_replay.add_argument("--delay-seconds", type=int, default=0)
    dlq_replay.add_argument("--timeout", type=int)
    dlq_replay.add_argument("--retries", type=int)
    dlq_replay.add_argument("--step")
    dlq_replay.set_defaults(fn=_cmd_dlq)

    dlq_ack = dlq_sub.add_parser("ack", help="Remove a dead-letter entry by index")
    _add_redis_args(dlq_ack, production=True)
    dlq_ack.add_argument("index", type=int)
    dlq_ack.set_defaults(fn=_cmd_dlq)

    p = sub.add_parser("list", help="List recent work metadata")
    _add_redis_args(p, production=True)
    p.add_argument("--state")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--offset", type=int, default=0)
    p.set_defaults(fn=_cmd_list)

    p = sub.add_parser("events", help="List recent runtime events")
    _add_redis_args(p, production=True)
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(fn=_cmd_events)

    p = sub.add_parser("workers", help="List worker heartbeats")
    _add_postgres_args(p)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--stale-after", type=int, default=30)
    p.set_defaults(fn=_cmd_workers)

    p = sub.add_parser("actions", help="List operator action records")
    _add_postgres_args(p)
    p.add_argument("--work-id")
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(fn=_cmd_actions)

    p = sub.add_parser("workspace-path", help="Print the isolated workspace path for a work id")
    _add_config_arg(p)
    p.add_argument("work_id")
    p.add_argument("--repo-path", default=".")
    p.add_argument("--workspace-root")
    p.add_argument("--workspace-mode", choices=["worktree", "clone"])
    p.add_argument("--namespace")
    p.set_defaults(fn=_cmd_workspace_path)

    p = sub.add_parser("artifact-show", help="Print a content-addressed artifact")
    _add_config_arg(p)
    p.add_argument("artifact_id")
    p.add_argument("--ext", default="bin")
    p.add_argument("--encoding", default="utf-8")
    p.add_argument("--repo-path", default=".")
    p.add_argument("--artifact-root")
    p.add_argument("--namespace")
    p.set_defaults(fn=_cmd_artifact_show)

    p = sub.add_parser("ui", help="Run the local Roost Console")
    _add_redis_args(p, production=True)
    p.add_argument("--host", default=os.getenv("ROOST_UI_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("ROOST_UI_PORT", "8766")))
    p.add_argument("--repo-path", default=".")
    p.add_argument("--artifact-root")
    p.set_defaults(fn=_cmd_ui)

    p = sub.add_parser("migrate", help="Apply Postgres durable-state migrations")
    _add_postgres_args(p)
    p.add_argument("--repo-path", default=".")
    p.add_argument("--plan", action="store_true", help="List packaged migrations without connecting to Postgres")
    p.set_defaults(fn=_cmd_migrate)

    p = sub.add_parser("doctor", help="Check local Roost runtime configuration")
    _add_redis_args(p)
    p.add_argument("--repo-path", default=".")
    p.add_argument("--runtime-mode", choices=["simple", "production"])
    p.add_argument("--postgres-url")
    p.add_argument("--engines")
    p.add_argument("--workspace-root")
    p.add_argument("--workspace-mode", choices=["worktree", "clone"])
    p.add_argument("--artifact-root")
    p.set_defaults(fn=_cmd_doctor)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
