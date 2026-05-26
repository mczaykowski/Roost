from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from redis import asyncio as aioredis

from roost.runtime.artifacts import FileArtifactStore
from roost.runtime.backends.redis import RedisControlPlane, RedisKeys, RedisSnapshotStore, RedisWorkItemStore
from roost.runtime.namespacing import apply_namespace, resolve_artifact_root


@dataclass(frozen=True)
class ConsoleConfig:
    redis_url: str
    redis_prefix: str
    queue_name: str
    repo_path: str
    artifact_root: str
    namespace: str | None = None


def run_console(*, host: str, port: int, config: ConsoleConfig) -> None:
    handler = _build_handler(config)
    server = ThreadingHTTPServer((host, port), handler)
    print(f"Roost Console: http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _build_handler(config: ConsoleConfig) -> type[BaseHTTPRequestHandler]:
    static_root = resources.files("roost.ui.static")
    repo_assets = Path(config.repo_path).resolve() / "assets"

    class ConsoleHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = unquote(parsed.path)
            query = parse_qs(parsed.query)

            try:
                if path == "/":
                    self._send_static(static_root / "index.html")
                elif path.startswith("/static/"):
                    self._send_static(static_root / path.removeprefix("/static/"))
                elif path.startswith("/assets/"):
                    self._send_static(repo_assets / path.removeprefix("/assets/"))
                elif path == "/api/summary":
                    self._send_json(asyncio.run(_summary(config)))
                elif path == "/api/work":
                    limit = _int_query(query, "limit", 80)
                    state = _str_query(query, "state")
                    self._send_json(asyncio.run(_work_list(config, state=state, limit=limit)))
                elif path.startswith("/api/work/"):
                    work_id = path.removeprefix("/api/work/")
                    self._send_json(asyncio.run(_work_detail(config, work_id=work_id)))
                elif path == "/api/events":
                    limit = _int_query(query, "limit", 80)
                    self._send_json(asyncio.run(_events(config, limit=limit)))
                elif path == "/api/failed":
                    limit = _int_query(query, "limit", 50)
                    self._send_json(asyncio.run(_failed(config, limit=limit)))
                elif path.startswith("/api/artifacts/"):
                    artifact_id = path.removeprefix("/api/artifacts/")
                    ext = _str_query(query, "ext") or "json"
                    self._send_json(_artifact(config, artifact_id=artifact_id, ext=ext))
                else:
                    self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            except FileNotFoundError:
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception as exc:
                self._send_json({"error": f"{exc.__class__.__name__}: {exc}"}, status=500)

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _send_static(self, ref: Any) -> None:
            content = ref.read_bytes()
            content_type = mimetypes.guess_type(str(ref))[0] or "application/octet-stream"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def _send_json(self, payload: Any, *, status: int = 200) -> None:
            content = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

    return ConsoleHandler


async def _with_redis(config: ConsoleConfig):
    redis_prefix = apply_namespace(config.redis_prefix, config.namespace)
    keys = RedisKeys(prefix=redis_prefix)
    redis = aioredis.from_url(config.redis_url, decode_responses=True)
    return redis, keys


async def _summary(config: ConsoleConfig) -> dict[str, Any]:
    redis, keys = await _with_redis(config)
    try:
        control = RedisControlPlane(redis, keys=keys)
        rows = await control.list_meta(state=None, limit=500, offset=0)
        states = {"queued": 0, "running": 0, "done": 0, "failed": 0, "cancelled": 0}
        for row in rows:
            state = str(row.get("state") or "queued")
            states[state] = states.get(state, 0) + 1
        return {
            "redis_url": config.redis_url,
            "queue": config.queue_name,
            "prefix": apply_namespace(config.redis_prefix, config.namespace),
            "total": len(rows),
            "states": states,
            "updated_at": time.time(),
        }
    finally:
        await redis.aclose()


async def _work_list(config: ConsoleConfig, *, state: str | None, limit: int) -> dict[str, Any]:
    redis, keys = await _with_redis(config)
    try:
        control = RedisControlPlane(redis, keys=keys)
        items = RedisWorkItemStore(redis, keys=keys)
        snapshots = RedisSnapshotStore(redis, keys=keys)
        rows = await control.list_meta(
            state=state or None,
            limit=max(1, min(limit, 200)),
            offset=0,
        )
        enriched = []
        for row in rows:
            work_id = str(row.get("work_id") or "")
            item = await items.get(work_id) if work_id else None
            snapshot = await snapshots.load(work_id) if work_id else None
            data = snapshot.data if snapshot else {}
            is_finished = snapshot.is_finished if snapshot else False
            enriched.append(
                {
                    **row,
                    "resources": item.resources if item else [],
                    "payload": item.payload if item else {},
                    "snapshot_version": snapshot.version if snapshot else None,
                    "is_finished": is_finished,
                    "next_run_at": None if is_finished else data.get("next_check_after"),
                    "artifacts_count": len(snapshot.artifacts) if snapshot else 0,
                    "observations_count": len(data.get("observations") or []),
                }
            )
        return {"rows": enriched}
    finally:
        await redis.aclose()


async def _work_detail(config: ConsoleConfig, *, work_id: str) -> dict[str, Any]:
    redis, keys = await _with_redis(config)
    try:
        control = RedisControlPlane(redis, keys=keys)
        items = RedisWorkItemStore(redis, keys=keys)
        snapshots = RedisSnapshotStore(redis, keys=keys)
        item = await items.get(work_id)
        snapshot = await snapshots.load(work_id)
        return {
            "meta": await control.get_meta(work_id),
            "item": item.model_dump(mode="json") if item else None,
            "snapshot": snapshot.model_dump(mode="json") if snapshot else None,
        }
    finally:
        await redis.aclose()


async def _events(config: ConsoleConfig, *, limit: int) -> dict[str, Any]:
    redis, keys = await _with_redis(config)
    try:
        events = await RedisControlPlane(redis, keys=keys).list_events(limit=max(1, min(limit, 200)))
        return {"rows": events}
    finally:
        await redis.aclose()


async def _failed(config: ConsoleConfig, *, limit: int) -> dict[str, Any]:
    redis, keys = await _with_redis(config)
    try:
        control = RedisControlPlane(redis, keys=keys)
        rows = await control.list_meta(state="failed", limit=max(1, min(limit, 200)), offset=0)
        dlq = await control.list_dlq(limit=max(1, min(limit, 200)), offset=0)
        return {"rows": rows, "dlq": dlq}
    finally:
        await redis.aclose()


def _artifact(config: ConsoleConfig, *, artifact_id: str, ext: str) -> dict[str, Any]:
    store = FileArtifactStore(root_dir=config.artifact_root)
    content = store.read_bytes(artifact_id, ext=ext)
    if content is None:
        return {"artifact_id": artifact_id, "content": None, "error": "Artifact not found"}
    text = content.decode("utf-8", errors="replace")
    parsed: Any = None
    if ext == "json":
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None
    return {
        "artifact_id": artifact_id,
        "ext": ext,
        "metadata": store.load_meta(artifact_id),
        "content": parsed if parsed is not None else text,
    }


def _int_query(query: dict[str, list[str]], key: str, default: int) -> int:
    try:
        return int((query.get(key) or [default])[0])
    except Exception:
        return default


def _str_query(query: dict[str, list[str]], key: str) -> str | None:
    value = (query.get(key) or [None])[0]
    return str(value) if value else None


def config_from_args(args: Any) -> ConsoleConfig:
    return ConsoleConfig(
        redis_url=args.redis_url,
        redis_prefix=args.redis_prefix,
        queue_name=args.queue,
        repo_path=os.path.abspath(args.repo_path),
        artifact_root=resolve_artifact_root(
            repo_path=args.repo_path,
            artifact_root=args.artifact_root,
            namespace=args.namespace,
        ),
        namespace=args.namespace,
    )
