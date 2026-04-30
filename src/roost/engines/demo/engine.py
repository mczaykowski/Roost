from __future__ import annotations

import time
from typing import Any

from roost.runtime.models import Snapshot, WorkItem


class DemoEngine:
    """
    Tiny durable engine used by the README and tests.

    Payload:
      {
        "message": "hello",
        "count_to": 3
      }

    Each call to step() increments one counter and persists progress in the
    snapshot. Kill a worker between steps and another worker can continue from
    the latest saved snapshot.
    """

    engine_id = "demo"

    async def init_snapshot(self, item: WorkItem) -> Snapshot:
        payload = dict(item.payload or {})
        count_to = max(1, int(payload.get("count_to", 3)))
        return Snapshot(
            work_id=item.work_id,
            engine=self.engine_id,
            step="count",
            data={
                "message": str(payload.get("message") or "hello from Roost"),
                "count_to": count_to,
                "count": 0,
                "events": [],
            },
            is_finished=False,
            next_step_delay_seconds=0.0,
        )

    async def step(self, snapshot: Snapshot, item: WorkItem) -> Snapshot:
        data = dict(snapshot.data)
        count = int(data.get("count") or 0) + 1
        count_to = max(1, int(data.get("count_to") or 1))

        events = list(data.get("events") or [])
        events.append({"count": count, "at": time.time()})
        data["count"] = count
        data["events"] = events

        new_snapshot = snapshot.model_copy()
        new_snapshot.data = data
        if count >= count_to:
            new_snapshot.step = "done"
            new_snapshot.is_finished = True
            new_snapshot.finished_at = time.time()
            new_snapshot.next_step_delay_seconds = 0.0
        else:
            new_snapshot.step = "count"
            new_snapshot.is_finished = False
            new_snapshot.next_step_delay_seconds = float(item.payload.get("delay_seconds", 0.0) or 0.0)
        return new_snapshot


def build_engine(**_kwargs: Any) -> DemoEngine:
    return DemoEngine()
