from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
import uuid
from typing import Any, Optional

from roost.runtime.models import WorkItem
from roost.runtime.namespacing import apply_namespace, resolve_artifact_root, resolve_workspace_root
from roost.runtime.registry import EngineRegistry


def _require_redis_deps() -> None:
    missing = []
    for module in ("redis", "saq"):
        try:
            __import__(module)
        except Exception:
            missing.append(module)
    if missing:
        raise SystemExit(
            "Missing Redis runtime dependencies. Install with:\n"
            "  uv sync --extra redis\n"
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


def _cmd_engines(_args: argparse.Namespace) -> None:
    registry = EngineRegistry.from_entry_points()
    for info in registry.info():
        print(f"{info.engine_id}\t{info.source}")


def _cmd_enqueue(args: argparse.Namespace) -> None:
    _require_redis_deps()

    from redis import asyncio as aioredis
    from saq import Queue

    from roost.runtime.backends.redis import RedisControlPlane, RedisKeys, RedisWorkItemStore

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        queue = Queue.from_url(args.redis_url, name=args.queue)
        try:
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
            store = RedisWorkItemStore(redis, keys=keys)
            control = RedisControlPlane(redis, keys=keys)
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
            await redis.aclose()

    asyncio.run(run())


def _cmd_worker(args: argparse.Namespace) -> None:
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
            lease_ttl_seconds=args.lease_ttl,
            job_timeout_seconds=args.timeout,
            job_retries=args.retries,
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
    _require_redis_deps()

    from redis import asyncio as aioredis

    from roost.runtime.backends.redis import RedisControlPlane, RedisKeys, RedisSnapshotStore, RedisWorkItemStore

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        try:
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
            await redis.aclose()

    asyncio.run(run())


def _cmd_list(args: argparse.Namespace) -> None:
    _require_redis_deps()

    from redis import asyncio as aioredis

    from roost.runtime.backends.redis import RedisControlPlane, RedisKeys

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        try:
            rows = await RedisControlPlane(redis, keys=keys).list_meta(
                state=args.state,
                limit=args.limit,
                offset=args.offset,
            )
            print(json.dumps(rows, indent=2, sort_keys=True, default=str))
        finally:
            await redis.aclose()

    asyncio.run(run())


def _cmd_events(args: argparse.Namespace) -> None:
    _require_redis_deps()

    from redis import asyncio as aioredis

    from roost.runtime.backends.redis import RedisControlPlane, RedisKeys

    async def run() -> None:
        redis_prefix = apply_namespace(args.redis_prefix, args.namespace)
        keys = RedisKeys(prefix=redis_prefix)
        redis = aioredis.from_url(args.redis_url, decode_responses=True)
        try:
            events = await RedisControlPlane(redis, keys=keys).list_events(limit=args.limit)
            print(json.dumps(events, indent=2, sort_keys=True, default=str))
        finally:
            await redis.aclose()

    asyncio.run(run())


def _cmd_workspace_path(args: argparse.Namespace) -> None:
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


def _add_redis_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--redis-url", default=os.getenv("ROOST_REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--queue", default=os.getenv("ROOST_QUEUE", "default"))
    parser.add_argument("--redis-prefix", default=os.getenv("ROOST_REDIS_PREFIX", "roost"))
    parser.add_argument("--namespace", default=os.getenv("ROOST_NAMESPACE"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Roost durable runtime for agent step-machines")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("engines", help="List installed engine entry points")
    p.set_defaults(fn=_cmd_engines)

    p = sub.add_parser("enqueue", help="Enqueue a work item")
    _add_redis_args(p)
    p.add_argument("--engine", required=True)
    p.add_argument("--payload", required=True, help="JSON object")
    p.add_argument("--work-id")
    p.add_argument("--idempotency-key")
    p.add_argument("--priority", type=int, default=0)
    p.add_argument("--resource", action="append")
    p.add_argument("--delay-seconds", type=int, default=0)
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--retries", type=int, default=5)
    p.set_defaults(fn=_cmd_enqueue)

    p = sub.add_parser("worker", help="Run a Redis-backed worker")
    _add_redis_args(p)
    p.add_argument("--repo-path", default=".")
    p.add_argument("--engines", default="demo", help="Comma-separated engine ids, or all")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument("--retries", type=int, default=5)
    p.add_argument("--lease-ttl", type=int, default=60)
    p.add_argument("--workspace-root")
    p.add_argument("--workspace-mode", choices=["worktree", "clone"], default="worktree")
    p.add_argument("--artifact-root")
    p.set_defaults(fn=_cmd_worker)

    p = sub.add_parser("status", help="Show work item metadata and snapshot")
    _add_redis_args(p)
    p.add_argument("work_id")
    p.set_defaults(fn=_cmd_status)

    p = sub.add_parser("list", help="List recent work metadata")
    _add_redis_args(p)
    p.add_argument("--state")
    p.add_argument("--limit", type=int, default=20)
    p.add_argument("--offset", type=int, default=0)
    p.set_defaults(fn=_cmd_list)

    p = sub.add_parser("events", help="List recent runtime events")
    _add_redis_args(p)
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(fn=_cmd_events)

    p = sub.add_parser("workspace-path", help="Print the isolated workspace path for a work id")
    p.add_argument("work_id")
    p.add_argument("--repo-path", default=".")
    p.add_argument("--workspace-root")
    p.add_argument("--workspace-mode", choices=["worktree", "clone"], default="worktree")
    p.add_argument("--namespace")
    p.set_defaults(fn=_cmd_workspace_path)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
