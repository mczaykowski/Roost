from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol, runtime_checkable

from roost.runtime.models import Lease, Snapshot, WorkItem


@runtime_checkable
class WorkItemStore(Protocol):
    async def put(self, item: WorkItem, ttl_seconds: int = 7 * 24 * 3600) -> None: ...

    async def get(self, work_id: str) -> Optional[WorkItem]: ...

    async def get_or_claim_work_id(
        self, item: WorkItem, ttl_seconds: int = 7 * 24 * 3600, *, conn: Any = None
    ) -> str: ...


@runtime_checkable
class SnapshotStore(Protocol):
    async def load(self, work_id: str, *, conn: Any = None) -> Optional[Snapshot]: ...

    async def save(
        self,
        snapshot: Snapshot,
        expected_version: int,
        ttl_seconds: int = 24 * 3600,
        *,
        conn: Any = None,
    ) -> bool: ...


@runtime_checkable
class LeaseStore(Protocol):
    async def try_acquire(self, work_id: str, holder_id: str, ttl_seconds: int) -> Optional[Lease]: ...

    async def renew(self, lease: Lease, ttl_seconds: int) -> bool: ...

    async def release(self, lease: Lease) -> bool: ...

    async def clear(self, work_id: str) -> int: ...


@runtime_checkable
class ResourceStore(Protocol):
    async def acquire(self, *, resources: list[str], owner_value: str, ttl_seconds: int) -> bool: ...

    async def renew(self, *, resources: list[str], owner_value: str, ttl_seconds: int) -> bool: ...

    async def release(self, *, resources: list[str], owner_value: str) -> int: ...

    async def clear(self, *, resources: list[str]) -> int: ...


@runtime_checkable
class InflightStore(Protocol):
    async def mark(self, work_id: str, payload: dict[str, Any], ttl_seconds: int) -> None: ...

    async def clear(self, work_id: str) -> None: ...

    async def get(self, work_id: str) -> Optional[dict[str, Any]]: ...


@runtime_checkable
class ControlPlaneStore(Protocol):
    async def push_event(self, event: dict[str, Any], *, maxlen: Optional[int] = None) -> None: ...

    async def list_events(self, *, limit: int = 50) -> list[dict[str, Any]]: ...

    async def upsert_on_enqueue(
        self, item: WorkItem, work_id: str, *, conn: Any = None
    ) -> dict[str, Any]: ...

    async def set_state(
        self,
        *,
        work_id: str,
        engine: str,
        state: str,
        step: Optional[str] = None,
        last_error: Optional[dict[str, Any]] = None,
        conn: Any = None,
    ) -> dict[str, Any]: ...

    async def link_child(
        self,
        *,
        parent_work_id: str,
        child_work_id: str,
        relation: str = "child",
        max_children: int = 50,
        conn: Any = None,
    ) -> dict[str, Any]: ...

    async def get_meta(self, work_id: str) -> Optional[dict[str, Any]]: ...

    async def list_work_ids(self, *, state: Optional[str], limit: int, offset: int) -> list[str]: ...

    async def list_meta(self, *, state: Optional[str], limit: int = 20, offset: int = 0) -> list[dict[str, Any]]: ...

    async def push_dlq(self, event: dict[str, Any], *, maxlen: int = 2000) -> None: ...

    async def list_dlq(self, *, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]: ...

    async def get_dlq(self, index: int) -> Optional[dict[str, Any]]: ...

    async def ack_dlq(self, index: int) -> bool: ...

    async def ack_dlq_work_id(self, work_id: str) -> int: ...


@dataclass(frozen=True)
class RuntimeStores:
    work_items: WorkItemStore
    snapshots: SnapshotStore
    leases: LeaseStore
    resources: ResourceStore
    inflight: InflightStore
    control: ControlPlaneStore
