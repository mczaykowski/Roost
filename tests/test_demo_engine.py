from __future__ import annotations

import pytest

from roost.engines.demo.engine import DemoEngine
from roost.runtime.models import WorkItem


@pytest.mark.asyncio
async def test_demo_engine_advances_until_done():
    engine = DemoEngine()
    item = WorkItem(work_id="demo-1", engine="demo", payload={"count_to": 2})

    snapshot = await engine.init_snapshot(item)
    assert snapshot.step == "count"
    assert snapshot.is_finished is False

    snapshot = await engine.step(snapshot, item)
    assert snapshot.data["count"] == 1
    assert snapshot.is_finished is False

    snapshot = await engine.step(snapshot, item)
    assert snapshot.data["count"] == 2
    assert snapshot.step == "done"
    assert snapshot.is_finished is True
