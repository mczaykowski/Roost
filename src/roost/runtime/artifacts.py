from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Optional, Protocol

from roost.runtime.models import Artifact, ArtifactKind


class ArtifactStore(Protocol):
    def put_bytes(
        self,
        *,
        work_id: str,
        kind: ArtifactKind,
        content: bytes,
        ext: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> Artifact: ...

    def get_path(self, artifact_id: str, *, ext: Optional[str] = None) -> str: ...


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@dataclass(frozen=True)
class FileArtifactStore:
    """
    Content-addressed artifact store on disk.

    Layout:
      <root>/<aa>/<bb>/<artifact_id>.<ext>
      <root>/<aa>/<bb>/<artifact_id>.json  (metadata)
    """

    root_dir: str

    def _dir_for(self, artifact_id: str) -> str:
        a = artifact_id[:2] or "00"
        b = artifact_id[2:4] or "00"
        return os.path.join(self.root_dir, a, b)

    def get_path(self, artifact_id: str, *, ext: Optional[str] = None) -> str:
        ext_final = (ext or "bin").lstrip(".")
        return os.path.join(self._dir_for(artifact_id), f"{artifact_id}.{ext_final}")

    def _meta_path(self, artifact_id: str) -> str:
        return os.path.join(self._dir_for(artifact_id), f"{artifact_id}.json")

    def load_meta(self, artifact_id: str) -> Optional[dict]:
        path = self._meta_path(artifact_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def read_bytes(self, artifact_id: str, *, ext: Optional[str] = None) -> Optional[bytes]:
        path = self.get_path(artifact_id, ext=ext)
        if not os.path.exists(path):
            return None
        try:
            with open(path, "rb") as f:
                return f.read()
        except Exception:
            return None

    def put_bytes(
        self,
        *,
        work_id: str,
        kind: ArtifactKind,
        content: bytes,
        ext: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> Artifact:
        artifact_id = _sha256(content)
        os.makedirs(self._dir_for(artifact_id), exist_ok=True)

        ext_final = (ext or ("patch" if kind == "patch" else "bin")).lstrip(".")
        path = self.get_path(artifact_id, ext=ext_final)
        meta_path = self._meta_path(artifact_id)

        # Atomic-ish write: create if missing, otherwise treat as deduped.
        if not os.path.exists(path):
            tmp = f"{path}.tmp.{os.getpid()}"
            with open(tmp, "wb") as f:
                f.write(content)
            os.replace(tmp, path)

        meta = {
            "artifact_id": artifact_id,
            "work_id": work_id,
            "kind": kind,
            "content_hash": artifact_id,
            "ext": ext_final,
            "metadata": metadata or {},
        }
        if not os.path.exists(meta_path):
            tmpm = f"{meta_path}.tmp.{os.getpid()}"
            with open(tmpm, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2, sort_keys=True, default=str)
            os.replace(tmpm, meta_path)

        return Artifact(
            artifact_id=artifact_id,
            work_id=work_id,
            kind=kind,
            uri=None,
            content_hash=artifact_id,
            metadata=dict(metadata or {}),
        )
