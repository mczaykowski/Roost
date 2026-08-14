from __future__ import annotations

import hashlib
import json
import re
import time
from typing import Any
from urllib import error, request

from roost.runtime.artifacts import FileArtifactStore
from roost.runtime.models import Snapshot, WorkItem


_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)


class WatchlistEngine:
    """
    Small crash-safe URL watchlist engine.

    Payload:
      {
        "url": "https://example.com",
        "claim": "Example Domain is reachable",
        "checks_required": 3,
        "delay_seconds": 5
      }

    Each step fetches the URL, records an observation in the snapshot, and either
    schedules another check or writes a final JSON evidence report artifact.
    """

    engine_id = "watchlist"

    def __init__(self, *, artifact_root: str | None = None, timeout_seconds: float = 8.0):
        self.artifacts = FileArtifactStore(root_dir=artifact_root or ".roost/artifacts")
        self.timeout_seconds = timeout_seconds

    async def init_snapshot(self, item: WorkItem) -> Snapshot:
        payload = dict(item.payload or {})
        url = str(payload.get("url") or "https://example.com").strip()
        checks_required = max(1, int(payload.get("checks_required", 3)))
        delay_seconds = max(0.0, float(payload.get("delay_seconds", 5.0)))

        return Snapshot(
            work_id=item.work_id,
            engine=self.engine_id,
            step="check",
            data={
                "url": url,
                "claim": str(payload.get("claim") or "URL is reachable"),
                "checks_required": checks_required,
                "checks_completed": 0,
                "delay_seconds": delay_seconds,
                "observations": [],
            },
            is_finished=False,
            next_step_delay_seconds=0.0,
        )

    async def step(self, snapshot: Snapshot, item: WorkItem) -> Snapshot:
        data = dict(snapshot.data)
        now = time.time()
        next_check_after = float(data.get("next_check_after") or 0.0)
        if next_check_after and now < next_check_after:
            # Wait is movement (next_step_delay_seconds). next_check_after stays
            # on the last saved observation so recovery does not check early.
            new_snapshot = snapshot.model_copy()
            new_snapshot.step = "check"
            new_snapshot.is_finished = False
            new_snapshot.next_step_delay_seconds = max(0.0, next_check_after - now)
            return new_snapshot

        observations = list(data.get("observations") or [])
        observation = self._observe_url(str(data["url"]))
        observations.append(observation)

        checks_completed = len(observations)
        checks_required = max(1, int(data.get("checks_required") or 1))
        data["observations"] = observations
        data["checks_completed"] = checks_completed
        data["last_observed_at"] = observation["observed_at"]

        new_snapshot = snapshot.model_copy()
        new_snapshot.data = data

        if checks_completed >= checks_required:
            report = self._build_report(item=item, data=data)
            content = json.dumps(report, indent=2, sort_keys=True).encode("utf-8")
            artifact = self.artifacts.put_bytes(
                work_id=item.work_id,
                kind="json",
                content=content,
                ext="json",
                metadata={
                    "engine": self.engine_id,
                    "url": data["url"],
                    "verdict": report["verdict"],
                    "checks_completed": checks_completed,
                },
            )
            new_snapshot.step = "done"
            new_snapshot.is_finished = True
            new_snapshot.finished_at = time.time()
            new_snapshot.next_step_delay_seconds = 0.0
            new_snapshot.artifacts = [*new_snapshot.artifacts, artifact]
            new_snapshot.data = {**data, "verdict": report["verdict"]}
            return new_snapshot

        delay_seconds = max(0.0, float(data.get("delay_seconds") or 0.0))
        new_snapshot.step = "check"
        new_snapshot.is_finished = False
        new_snapshot.next_step_delay_seconds = delay_seconds
        new_snapshot.data = {
            **data,
            "next_check_after": time.time() + delay_seconds,
        }
        return new_snapshot

    def _observe_url(self, url: str) -> dict[str, Any]:
        started = time.time()
        req = request.Request(url, headers={"User-Agent": "RoostWatchlist/0.1"})
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                body = resp.read(128_000)
                text = body.decode("utf-8", errors="replace")
                title_match = _TITLE_RE.search(text)
                title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else ""
                return {
                    "ok": 200 <= int(resp.status) < 400,
                    "status": int(resp.status),
                    "url": url,
                    "final_url": resp.geturl(),
                    "title": title[:160],
                    "bytes_sampled": len(body),
                    "body_sha256": hashlib.sha256(body).hexdigest(),
                    "elapsed_ms": int((time.time() - started) * 1000),
                    "observed_at": time.time(),
                }
        except error.HTTPError as exc:
            return {
                "ok": False,
                "status": int(exc.code),
                "url": url,
                "error": exc.reason,
                "elapsed_ms": int((time.time() - started) * 1000),
                "observed_at": time.time(),
            }
        except Exception as exc:
            return {
                "ok": False,
                "status": None,
                "url": url,
                "error": f"{exc.__class__.__name__}: {exc}",
                "elapsed_ms": int((time.time() - started) * 1000),
                "observed_at": time.time(),
            }

    def _build_report(self, *, item: WorkItem, data: dict[str, Any]) -> dict[str, Any]:
        observations = list(data.get("observations") or [])
        ok_count = sum(1 for obs in observations if obs.get("ok"))
        verdict = "reachable" if ok_count == len(observations) else "unstable"
        if ok_count == 0:
            verdict = "unreachable"

        return {
            "work_id": item.work_id,
            "engine": self.engine_id,
            "url": data["url"],
            "claim": data.get("claim"),
            "verdict": verdict,
            "checks_required": int(data.get("checks_required") or len(observations)),
            "checks_completed": len(observations),
            "ok_count": ok_count,
            "observations": observations,
            "reported_at": time.time(),
        }


def build_engine(**kwargs: Any) -> WatchlistEngine:
    return WatchlistEngine(artifact_root=kwargs.get("artifact_root"))
