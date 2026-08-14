from __future__ import annotations

import os
import tomllib
from typing import Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_QUEUE = "default"
DEFAULT_REDIS_PREFIX = "roost"
DEFAULT_WORKSPACE_MODE = "worktree"
DEFAULT_RUNTIME_MODE = "simple"

_SNAPSHOT_DATA_PREFIX = "snapshot.data."
_ITEM_PAYLOAD_PREFIX = "item.payload."


def validate_trigger_path(path: str, *, field: str = "condition") -> None:
    """
    Freeze the trigger path contract to a single key lookup.

    Allowed forms: ``snapshot.data.<key>``, ``item.payload.<key>``, or a bare
    key (looked up in ``snapshot.data``). Nested paths are rejected.
    """
    if not path:
        raise ValueError(f"Trigger {field} must be a non-empty path")
    if path.startswith(_SNAPSHOT_DATA_PREFIX):
        rest = path[len(_SNAPSHOT_DATA_PREFIX) :]
    elif path.startswith(_ITEM_PAYLOAD_PREFIX):
        rest = path[len(_ITEM_PAYLOAD_PREFIX) :]
    else:
        rest = path
    if not rest or "." in rest:
        raise ValueError(
            f"Trigger {field} {path!r} is not a single-level key. "
            "Allowed forms: snapshot.data.<key>, item.payload.<key>, or a bare key"
        )


def resolve_trigger_path(
    path: str, *, snapshot_data: dict, item_payload: dict, field: str = "condition"
) -> object:
    """Resolve a frozen one-level trigger path. Raises ValueError if nested."""
    validate_trigger_path(path, field=field)
    if path.startswith(_SNAPSHOT_DATA_PREFIX):
        return snapshot_data.get(path[len(_SNAPSHOT_DATA_PREFIX) :])
    if path.startswith(_ITEM_PAYLOAD_PREFIX):
        return item_payload.get(path[len(_ITEM_PAYLOAD_PREFIX) :])
    return snapshot_data.get(path)


class TriggerConfig(BaseModel):
    """
    Fan-out rule: when ``on_engine_done`` finishes, enqueue ``enqueue_engine``.

    A trigger fires when ``condition`` is omitted, or when it names a single key
    that is truthy. Allowed condition (and ``payload_map`` source) forms:

    - ``snapshot.data.<one_key>``
    - ``item.payload.<one_key>``
    - a bare key, looked up in ``snapshot.data``

    Nested paths (extra dots after the prefix, e.g. ``snapshot.data.foo.bar``)
    are rejected at config-load and plan time. ``payload_map`` copies matching
    keys under the same one-level rule.
    """

    model_config = ConfigDict(extra="forbid")

    on_engine_done: str
    enqueue_engine: str
    condition: Optional[str] = None
    payload_map: Optional[dict[str, str]] = None

    @field_validator("condition")
    @classmethod
    def _condition_single_level(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            validate_trigger_path(value, field="condition")
        return value

    @field_validator("payload_map")
    @classmethod
    def _payload_map_single_level(
        cls, value: Optional[dict[str, str]]
    ) -> Optional[dict[str, str]]:
        if value:
            for source in value.values():
                validate_trigger_path(source, field="payload_map")
        return value


class RedisRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = DEFAULT_REDIS_URL
    queue: str = DEFAULT_QUEUE
    prefix: str = DEFAULT_REDIS_PREFIX
    namespace: Optional[str] = None


class RuntimeModeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["simple", "production"] = DEFAULT_RUNTIME_MODE


class PostgresRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: Optional[str] = None


class WorkerRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    engines: str = "watchlist"
    concurrency: int = 4
    timeout_seconds: int = 120
    retries: int = 5
    lease_ttl_seconds: int = 60
    stale_after_seconds: float = 30.0
    recovery_interval_seconds: float = 2.0
    heartbeat_interval_seconds: float = 10.0
    workspace_root: Optional[str] = None
    workspace_mode: Literal["worktree", "clone"] = DEFAULT_WORKSPACE_MODE


class ArtifactRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    root: Optional[str] = None


class RoostConfig(BaseModel):
    """
    Optional runtime config.

    Triggers let the swarm enqueue follow-up work when an engine finishes, without
    hard-coding vertical behavior into the runtime.
    """

    model_config = ConfigDict(extra="forbid")

    runtime: RuntimeModeConfig = Field(default_factory=RuntimeModeConfig)
    redis: RedisRuntimeConfig = Field(default_factory=RedisRuntimeConfig)
    postgres: PostgresRuntimeConfig = Field(default_factory=PostgresRuntimeConfig)
    worker: WorkerRuntimeConfig = Field(default_factory=WorkerRuntimeConfig)
    artifacts: ArtifactRuntimeConfig = Field(default_factory=ArtifactRuntimeConfig)
    triggers: list[TriggerConfig] = Field(default_factory=list)


def resolve_roost_config_path(*, repo_path: str, cli_path: Optional[str]) -> Tuple[Optional[str], bool]:
    if cli_path:
        return os.path.abspath(cli_path), True
    env_path = os.getenv("ROOST_CONFIG")
    if env_path:
        return os.path.abspath(env_path), True
    candidate = os.path.join(os.path.abspath(repo_path), "roost.toml")
    if os.path.exists(candidate):
        return candidate, False
    return None, False


def load_roost_config(path: Optional[str], *, explicit: bool) -> Optional[RoostConfig]:
    if not path:
        return None
    if not os.path.exists(path):
        if explicit:
            raise FileNotFoundError(path)
        return None
    with open(path, "rb") as f:
        data = tomllib.load(f)
    try:
        return RoostConfig.model_validate(data)
    except ValidationError as exc:
        raise ValueError(f"Invalid Roost config ({path}): {exc}") from exc


def resolve_config_relative_path(path: Optional[str], *, config_path: Optional[str], repo_path: str) -> Optional[str]:
    if not path:
        return None
    if os.path.isabs(path):
        return path
    base = os.path.dirname(os.path.abspath(config_path)) if config_path else os.path.abspath(repo_path)
    return os.path.abspath(os.path.join(base, path))
