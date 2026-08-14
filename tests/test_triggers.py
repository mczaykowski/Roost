"""Frozen trigger DSL: one-level truthy keys only."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from roost.runtime.config import RoostConfig, TriggerConfig
from roost.runtime.models import Snapshot, WorkItem
from roost.runtime.swarm import SwarmConfig, _RedisSwarmRuntime


class _WorkItems:
    async def get_or_claim_work_id(self, item, ttl_seconds=0, *, conn=None):
        del ttl_seconds, conn
        return item.work_id


class _Control:
    async def upsert_on_enqueue(self, item, work_id, *, conn=None):
        del item, work_id, conn
        return {}

    async def link_child(
        self, *, parent_work_id, child_work_id, relation="child", conn=None, max_children=50
    ):
        del parent_work_id, child_work_id, relation, conn, max_children
        return {}


def _runtime_with_triggers(triggers: list[TriggerConfig]) -> _RedisSwarmRuntime:
    runtime = _RedisSwarmRuntime.__new__(_RedisSwarmRuntime)
    runtime.config = SwarmConfig(
        redis_url="redis://localhost:6379/0",
        roost_config=RoostConfig(triggers=triggers),
    )
    runtime.work_items = _WorkItems()
    runtime.control = _Control()
    return runtime


async def test_truthy_snapshot_key_fires():
    runtime = _runtime_with_triggers(
        [
            TriggerConfig(
                on_engine_done="parent",
                enqueue_engine="child",
                condition="snapshot.data.ready",
            )
        ]
    )
    item = WorkItem(work_id="p1", engine="parent")
    snap = Snapshot(work_id="p1", engine="parent", is_finished=True, data={"ready": True})
    planned = await runtime._plan_triggers(item=item, snapshot=snap, conn=None)
    assert len(planned) == 1
    assert planned[0].engine == "child"


async def test_missing_or_falsey_key_does_not_fire():
    runtime = _runtime_with_triggers(
        [
            TriggerConfig(
                on_engine_done="parent",
                enqueue_engine="child",
                condition="ready",
            )
        ]
    )
    item = WorkItem(work_id="p1", engine="parent")
    missing = Snapshot(work_id="p1", engine="parent", is_finished=True, data={})
    assert await runtime._plan_triggers(item=item, snapshot=missing, conn=None) == []

    falsey = Snapshot(work_id="p1", engine="parent", is_finished=True, data={"ready": False})
    assert await runtime._plan_triggers(item=item, snapshot=falsey, conn=None) == []


async def test_omitted_condition_always_fires():
    runtime = _runtime_with_triggers(
        [TriggerConfig(on_engine_done="parent", enqueue_engine="child")]
    )
    item = WorkItem(work_id="p1", engine="parent")
    snap = Snapshot(work_id="p1", engine="parent", is_finished=True, data={})
    planned = await runtime._plan_triggers(item=item, snapshot=snap, conn=None)
    assert len(planned) == 1


def test_nested_path_rejected_at_config_load():
    with pytest.raises(ValidationError, match="single-level"):
        TriggerConfig(
            on_engine_done="parent",
            enqueue_engine="child",
            condition="snapshot.data.foo.bar",
        )


async def test_nested_path_rejected_at_plan_time():
    trigger = TriggerConfig.model_construct(
        on_engine_done="parent",
        enqueue_engine="child",
        condition="snapshot.data.foo.bar",
        payload_map=None,
    )
    runtime = _runtime_with_triggers([])
    runtime.config.roost_config.triggers.append(trigger)
    item = WorkItem(work_id="p1", engine="parent")
    snap = Snapshot(work_id="p1", engine="parent", is_finished=True, data={"foo": {"bar": True}})
    with pytest.raises(ValueError, match="single-level"):
        await runtime._plan_triggers(item=item, snapshot=snap, conn=None)
