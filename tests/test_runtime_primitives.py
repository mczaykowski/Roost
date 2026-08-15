from __future__ import annotations

import os
import re

from roost.runtime.artifacts import FileArtifactStore
from roost.runtime.backends.redis import RedisKeys
from roost.runtime.backends.redis import (
    RedisControlPlane,
    RedisInflightStore,
    RedisLeaseManager,
    RedisResourceManager,
    RedisSnapshotStore,
    RedisWorkItemStore,
)
from roost.runtime.models import Snapshot, WorkItem
from roost.runtime.registry import EngineRegistry
from roost.runtime.stores import (
    ControlPlaneStore,
    InflightStore,
    LeaseStore,
    ResourceStore,
    SnapshotStore,
    WorkItemStore,
)
from roost.runtime.swarm import RedisSwarm, SwarmConfig, _RedisSwarmRuntime
from roost.runtime.workspaces import WorkspaceManager, WorkspaceSpec


class _DummyEngine:
    engine_id = "dummy"

    async def init_snapshot(self, item):  # pragma: no cover
        raise NotImplementedError

    async def step(self, snapshot, item):  # pragma: no cover
        raise NotImplementedError


def test_registry_from_factories_creates_engine():
    registry = EngineRegistry.from_factories({"dummy": lambda **_kwargs: _DummyEngine()})
    assert registry.create("dummy").engine_id == "dummy"


def test_work_id_from_inflight_key_handles_colons():
    swarm = RedisSwarm.__new__(RedisSwarm)
    swarm.keys = RedisKeys(prefix="roost")
    assert swarm._work_id_from_inflight_key("roost:inflight:abc:def") == "abc:def"


def test_file_artifact_store_put_and_path(tmp_path):
    store = FileArtifactStore(root_dir=str(tmp_path))
    artifact = store.put_bytes(work_id="W-1", kind="patch", content=b"hello", ext="patch")
    path = store.get_path(artifact.artifact_id, ext="patch")
    assert path.endswith(f"{artifact.artifact_id}.patch")
    assert store.read_bytes(artifact.artifact_id, ext="patch") == b"hello"


def test_redis_stores_satisfy_runtime_store_protocols():
    assert issubclass(RedisWorkItemStore, WorkItemStore)
    assert issubclass(RedisSnapshotStore, SnapshotStore)
    assert issubclass(RedisLeaseManager, LeaseStore)
    assert issubclass(RedisResourceManager, ResourceStore)
    assert issubclass(RedisInflightStore, InflightStore)
    assert issubclass(RedisControlPlane, ControlPlaneStore)


def test_workspace_path_is_stable_and_safe(tmp_path):
    manager = WorkspaceManager(
        WorkspaceSpec(
            base_repo_path=str(tmp_path / "base"),
            root_dir=str(tmp_path / "root"),
            mode="worktree",
        )
    )

    path = manager.workspace_path("A:B/C")
    leaf = os.path.basename(path)
    assert ":" not in leaf
    assert "/" not in leaf and "\\" not in leaf
    assert leaf.startswith("A-B-C-")
    assert re.fullmatch(r"[A-Za-z0-9._-]+-[0-9a-f]{10}", leaf)


async def test_runtime_skips_operator_cancelled_work_before_lease():
    class Control:
        async def get_meta(self, work_id):
            return {"work_id": work_id, "state": "cancelled"}

    runtime = _RedisSwarmRuntime.__new__(_RedisSwarmRuntime)
    runtime.control = Control()

    result = await runtime._execute_one_step_impl(
        work_id="work-1",
        item=WorkItem(work_id="work-1", engine="dummy"),
        engine=_DummyEngine(),
    )

    assert result == {"status": "cancelled", "reason": "operator_cancelled", "job_id": None}


async def test_redis_store_methods_accept_conn_none():
    """Simple-mode stores must ignore conn= so the shared step txn call sites work."""
    from unittest.mock import AsyncMock, MagicMock

    redis = MagicMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.register_script = MagicMock(return_value=AsyncMock(return_value=1))
    redis.xadd = AsyncMock()
    pipe = MagicMock()
    pipe.set = MagicMock(return_value=pipe)
    pipe.zadd = MagicMock(return_value=pipe)
    pipe.zrem = MagicMock(return_value=pipe)
    pipe.execute = AsyncMock(return_value=[])
    redis.pipeline = MagicMock(return_value=pipe)

    snapshots = RedisSnapshotStore(redis)
    snap = Snapshot(work_id="w1", engine="dummy", version=1)
    assert await snapshots.save(snap, expected_version=0, conn=None) is True
    assert await snapshots.load("w1", conn=None) is None

    control = RedisControlPlane(redis)
    item = WorkItem(work_id="w1", engine="dummy")
    await control.set_state(work_id="w1", engine="dummy", state="queued", conn=None)
    await control.upsert_on_enqueue(item, "w1", conn=None)
    await control.link_child(parent_work_id="w1", child_work_id="c1", conn=None)

    work_items = RedisWorkItemStore(redis)
    claimed = await work_items.get_or_claim_work_id(item, conn=None)
    assert claimed == "w1"


def test_production_init_does_not_bind_redis_memory_stores():
    runtime = RedisSwarm(
        _DummyEngine(),
        config=SwarmConfig(
            redis_url="redis://127.0.0.1:9/0",
            runtime_mode="production",
            postgres_url="postgresql://roost:roost@127.0.0.1:9/roost",
        ),
    )
    assert runtime.stores is None
    assert runtime.leases is None
    assert runtime.snapshots is None
    assert runtime.control is None
    assert runtime.work_items is None
    assert isinstance(runtime.inflight, RedisInflightStore)
