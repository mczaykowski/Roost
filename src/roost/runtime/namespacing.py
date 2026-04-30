from __future__ import annotations

import os
import re
from typing import Optional

from roost.runtime.workspaces import default_workspace_root


_SAFE = re.compile(r"[^a-zA-Z0-9._-]+")
_WS = re.compile(r"\s+")


def normalize_namespace(value: str) -> str:
    """
    Convert a user namespace like "acme/project-1" into a stable key component.

    Rules:
    - trim whitespace
    - "/" becomes ":" (so it reads like a path in redis keys/queue names)
    - spaces become "-"
    - remaining unsafe characters are replaced with "-"
    """
    raw = (value or "").strip()
    if not raw:
        raise ValueError("namespace must be non-empty")
    parts = [p.strip() for p in re.split(r"[:/]+", raw) if p.strip()]
    cleaned: list[str] = []
    for part in parts:
        part = _WS.sub("-", part)
        part = _SAFE.sub("-", part)
        part = part.strip("-")
        if part:
            cleaned.append(part)
    ns = ":".join(cleaned).strip(":")
    if not ns:
        raise ValueError("namespace normalizes to empty")
    return ns


def apply_namespace(base: str, namespace: Optional[str]) -> str:
    base = str(base or "").strip() or "roost"
    if not namespace:
        return base
    ns = normalize_namespace(namespace)
    return f"{base}:{ns}"


def default_artifact_root(*, repo_path: str) -> str:
    return os.path.join(os.path.abspath(repo_path), ".roost", "artifacts")


def resolve_workspace_root(*, repo_path: str, workspace_root: Optional[str], namespace: Optional[str]) -> str:
    root = workspace_root or default_workspace_root(repo_path=os.path.abspath(repo_path))
    if not namespace:
        return os.path.abspath(root)
    return os.path.abspath(os.path.join(root, normalize_namespace(namespace)))


def resolve_artifact_root(*, repo_path: str, artifact_root: Optional[str], namespace: Optional[str]) -> str:
    root = artifact_root or os.getenv("ROOST_ARTIFACT_ROOT") or default_artifact_root(repo_path=repo_path)
    if not namespace:
        return os.path.abspath(root)
    return os.path.abspath(os.path.join(root, normalize_namespace(namespace)))
