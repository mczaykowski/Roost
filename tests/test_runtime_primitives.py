from __future__ import annotations

import os
import re

from roost.runtime.artifacts import FileArtifactStore
from roost.runtime.backends.redis import RedisKeys
from roost.runtime.registry import EngineRegistry
from roost.runtime.swarm import RedisSwarm
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
