from __future__ import annotations

from typing import Protocol

from roost.runtime.models import Snapshot, WorkItem


class Engine(Protocol):
    """
    Minimal engine contract.

    The runtime owns scheduling, leases, retries, persistence, and observability.
    Engines own snapshot transitions.
    """

    engine_id: str

    async def init_snapshot(self, item: WorkItem) -> Snapshot: ...

    async def step(self, snapshot: Snapshot, item: WorkItem) -> Snapshot: ...

