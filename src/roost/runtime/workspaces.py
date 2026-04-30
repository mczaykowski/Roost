from __future__ import annotations

import hashlib
import os
import re
import subprocess
from dataclasses import dataclass
from typing import Literal

WorkspaceMode = Literal["worktree", "clone"]


class WorkspaceError(RuntimeError):
    pass


def _run_git(*args: str, cwd: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=cwd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if proc.returncode != 0:
        raise WorkspaceError(f"git {' '.join(args)} failed in {cwd}: {proc.stdout.strip()}")
    return proc.stdout.strip()


def _safe_dirname(work_id: str, *, max_slug: int = 48) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", (work_id or "work").strip()).strip("-") or "work"
    slug = slug[:max_slug]
    suffix = hashlib.sha1(work_id.encode("utf-8")).hexdigest()[:10]
    return f"{slug}-{suffix}"


@dataclass(frozen=True)
class WorkspaceSpec:
    base_repo_path: str
    root_dir: str
    mode: WorkspaceMode = "worktree"


class WorkspaceManager:
    """
    Per-work, isolated workspaces.

    The default mode uses `git worktree` so many workspaces share a single object database.
    Falls back to `clone` when `worktree` is unavailable.
    """

    def __init__(self, spec: WorkspaceSpec):
        self.spec = spec
        self.base_repo_path = os.path.abspath(spec.base_repo_path)
        self.root_dir = os.path.abspath(spec.root_dir)

    def workspace_path(self, work_id: str) -> str:
        return os.path.join(self.root_dir, _safe_dirname(work_id))

    def ensure(self, work_id: str) -> str:
        os.makedirs(self.root_dir, exist_ok=True)
        dest = self.workspace_path(work_id)
        if os.path.exists(os.path.join(dest, ".git")):
            return dest

        if os.path.exists(dest):
            raise WorkspaceError(f"Workspace path exists but is not a git repo: {dest}")

        if self.spec.mode == "worktree":
            try:
                return self._ensure_worktree(dest)
            except WorkspaceError:
                return self._ensure_clone(dest)

        if self.spec.mode == "clone":
            return self._ensure_clone(dest)

        raise WorkspaceError(f"Unknown workspace mode: {self.spec.mode}")

    def _ensure_git_base(self) -> None:
        os.makedirs(self.base_repo_path, exist_ok=True)
        try:
            _run_git("rev-parse", "--is-inside-work-tree", cwd=self.base_repo_path)
            return
        except WorkspaceError:
            pass

        _run_git("init", cwd=self.base_repo_path)
        readme = os.path.join(self.base_repo_path, "README.md")
        if not os.path.exists(readme):
            with open(readme, "w", encoding="utf-8") as f:
                f.write("# Roost Workspace Template\n")

        _run_git("add", "README.md", cwd=self.base_repo_path)
        # This can fail if git user.name/email are not configured; keep the error helpful.
        _run_git(
            "-c",
            "user.email=roost@local",
            "-c",
            "user.name=Roost",
            "commit",
            "-m",
            "Initial commit: workspace template",
            cwd=self.base_repo_path,
        )

    def _ensure_worktree(self, dest: str) -> str:
        self._ensure_git_base()
        _run_git("worktree", "add", "--detach", dest, "HEAD", cwd=self.base_repo_path)
        return dest

    def _ensure_clone(self, dest: str) -> str:
        self._ensure_git_base()
        parent = os.path.dirname(dest)
        os.makedirs(parent, exist_ok=True)
        _run_git("clone", "--shared", self.base_repo_path, dest, cwd=parent)
        return dest


def default_workspace_root(*, repo_path: str) -> str:
    return os.path.join(os.path.abspath(repo_path), ".roost", "workspaces")


def workspace_spec_from_env(*, repo_path: str) -> WorkspaceSpec:
    root = os.getenv("ROOST_WORKSPACE_ROOT") or default_workspace_root(repo_path=repo_path)
    mode = os.getenv("ROOST_WORKSPACE_MODE", "worktree").strip().lower()
    if mode not in ("worktree", "clone"):
        mode = "worktree"
    mode_typed: WorkspaceMode = "clone" if mode == "clone" else "worktree"
    return WorkspaceSpec(base_repo_path=repo_path, root_dir=root, mode=mode_typed)
