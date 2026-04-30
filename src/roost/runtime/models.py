from __future__ import annotations

import time
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


ArtifactKind = Literal["patch", "log", "report", "binary", "json", "other"]


class Artifact(BaseModel):
    """A content-addressed or URI-addressed output produced by an engine."""

    artifact_id: str
    work_id: str
    kind: ArtifactKind = "other"

    uri: Optional[str] = None
    content_hash: Optional[str] = None

    metadata: Dict[str, Any] = Field(default_factory=dict)


class WorkItem(BaseModel):
    """
    A durable unit of work for the swarm.

    Engines define the meaning of `payload`, while the runtime standardizes scheduling, leases, and retries.
    """

    work_id: str
    engine: str = "demo"
    payload: Dict[str, Any] = Field(default_factory=dict)

    priority: int = 0
    resources: List[str] = Field(default_factory=list)

    created_at: float = Field(default_factory=time.time)
    deadline_at: Optional[float] = None

    idempotency_key: Optional[str] = None


class Snapshot(BaseModel):
    """A replayable, versioned snapshot for preemptible execution."""

    work_id: str
    engine: str = "demo"

    status: Literal["queued", "running", "done", "failed", "cancelled"] = "queued"
    version: int = 1
    step: str = "init"
    data: Dict[str, Any] = Field(default_factory=dict)
    history: List[str] = Field(default_factory=list)
    artifacts: List[Artifact] = Field(default_factory=list)

    is_finished: bool = False
    next_step_delay_seconds: float = 0.0

    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    finished_at: Optional[float] = None
    failed_at: Optional[float] = None


class Lease(BaseModel):
    """A time-bound claim for executing a work item."""

    work_id: str
    holder_id: str
    lease_id: str
    expires_at: float
