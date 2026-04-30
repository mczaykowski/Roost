from __future__ import annotations

import os
import tomllib
from typing import Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class TriggerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    on_engine_done: str
    enqueue_engine: str
    condition: Optional[str] = None
    payload_map: Optional[dict[str, str]] = None


class RoostConfig(BaseModel):
    """
    Optional runtime config.

    Triggers let the swarm enqueue follow-up work when an engine finishes, without
    hard-coding vertical behavior into the runtime.
    """

    model_config = ConfigDict(extra="forbid")

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
