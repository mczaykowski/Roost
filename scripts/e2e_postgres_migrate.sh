#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_PORT="${ROOST_E2E_POSTGRES_PORT:-55432}"
POSTGRES_NAME="roost-e2e-postgres-$$"
POSTGRES_DB="roost"
POSTGRES_USER="roost"
POSTGRES_PASSWORD="roost"
POSTGRES_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"

cleanup() {
  docker rm -f "${POSTGRES_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

roost() {
  uv run --extra postgres roost "$@"
}

wait_for_postgres() {
  for _ in {1..60}; do
    if docker exec "${POSTGRES_NAME}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "Postgres did not become ready" >&2
  return 1
}

cd "${ROOT_DIR}"

echo "Starting Postgres on ${POSTGRES_URL}"
docker run \
  -d \
  --rm \
  -p "${POSTGRES_PORT}:5432" \
  --name "${POSTGRES_NAME}" \
  -e POSTGRES_DB="${POSTGRES_DB}" \
  -e POSTGRES_USER="${POSTGRES_USER}" \
  -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
  postgres:16 >/dev/null

wait_for_postgres

echo "Migration plan"
roost migrate --plan

echo "Applying migrations"
FIRST_RUN="$(roost migrate --postgres-url "${POSTGRES_URL}")"
printf '%s\n' "${FIRST_RUN}"

echo "Re-applying migrations to verify idempotency"
SECOND_RUN="$(roost migrate --postgres-url "${POSTGRES_URL}")"
printf '%s\n' "${SECOND_RUN}"

POSTGRES_URL="${POSTGRES_URL}" uv run --extra postgres python - <<'PY'
import os
import psycopg

expected = {
    "roost_schema_migrations",
    "roost_work_items",
    "roost_work_meta",
    "roost_snapshots",
    "roost_artifacts",
    "roost_leases",
    "roost_resource_claims",
    "roost_events",
    "roost_dlq",
    "roost_worker_heartbeats",
    "roost_operator_actions",
}

with psycopg.connect(os.environ["POSTGRES_URL"]) as conn:
    rows = conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'roost_%'
        """
    ).fetchall()

actual = {row[0] for row in rows}
missing = sorted(expected - actual)
if missing:
    raise SystemExit(f"Missing tables: {', '.join(missing)}")

print(f"Verified {len(expected)} Roost tables")
PY

POSTGRES_URL="${POSTGRES_URL}" uv run --extra postgres python - <<'PY'
import asyncio
import os

from roost.runtime.backends.postgres import (
    PostgresArtifactMetadataStore,
    PostgresControlPlaneStore,
    PostgresLeaseStore,
    PostgresResourceStore,
    PostgresSnapshotStore,
    PostgresWorkerHeartbeatStore,
    PostgresWorkItemStore,
    open_postgres_pool,
)
from roost.runtime.models import Artifact, Snapshot, WorkItem


async def main() -> None:
    async with open_postgres_pool(os.environ["POSTGRES_URL"]) as pool:
        items = PostgresWorkItemStore(pool)
        snapshots = PostgresSnapshotStore(pool)
        artifacts = PostgresArtifactMetadataStore(pool)
        leases = PostgresLeaseStore(pool)
        resources = PostgresResourceStore(pool)
        workers = PostgresWorkerHeartbeatStore(pool)
        control = PostgresControlPlaneStore(pool)

        item = WorkItem(
            work_id="postgres-e2e-1",
            engine="watchlist",
            payload={"url": "https://example.com"},
            resources=["domain:example.com"],
            idempotency_key="postgres-e2e-key",
        )
        claimed = await items.get_or_claim_work_id(item)
        duplicate = await items.get_or_claim_work_id(item.model_copy(update={"work_id": "postgres-e2e-duplicate"}))
        loaded = await items.get("postgres-e2e-1")

        if claimed != "postgres-e2e-1":
            raise SystemExit(f"unexpected claimed id: {claimed}")
        if duplicate != "postgres-e2e-1":
            raise SystemExit(f"unexpected duplicate id: {duplicate}")
        if not loaded or loaded.payload["url"] != "https://example.com":
            raise SystemExit("work item did not round-trip")

        first = Snapshot(
            work_id="postgres-e2e-1",
            engine="watchlist",
            step="observe",
            data={"checks_completed": 1},
        )
        if not await snapshots.save(first, expected_version=0):
            raise SystemExit("initial snapshot save failed")
        if await snapshots.save(first, expected_version=0):
            raise SystemExit("stale snapshot save unexpectedly succeeded")

        second = first.model_copy(update={"step": "done", "is_finished": True})
        if not await snapshots.save(second, expected_version=1):
            raise SystemExit("snapshot update failed")

        loaded_snapshot = await snapshots.load("postgres-e2e-1")
        if not loaded_snapshot or loaded_snapshot.version != 2 or loaded_snapshot.step != "done":
            raise SystemExit("snapshot did not round-trip")

        lease = await leases.try_acquire(item.work_id, "worker-a", 30)
        if lease is None:
            raise SystemExit("lease acquire failed")
        if await leases.try_acquire(item.work_id, "worker-b", 30):
            raise SystemExit("conflicting lease unexpectedly succeeded")
        if not await leases.renew(lease, 30):
            raise SystemExit("lease renew failed")
        if not await leases.release(lease):
            raise SystemExit("lease release failed")
        if not await leases.try_acquire(item.work_id, "worker-b", 30):
            raise SystemExit("lease acquire after release failed")

        if not await resources.acquire(resources=item.resources, owner_value="worker-a", ttl_seconds=30):
            raise SystemExit("resource acquire failed")
        if await resources.acquire(resources=item.resources, owner_value="worker-b", ttl_seconds=30):
            raise SystemExit("conflicting resource acquire unexpectedly succeeded")
        if not await resources.renew(resources=item.resources, owner_value="worker-a", ttl_seconds=30):
            raise SystemExit("resource renew failed")
        if await resources.release(resources=item.resources, owner_value="worker-a") != 1:
            raise SystemExit("resource release failed")
        if not await resources.acquire(resources=item.resources, owner_value="worker-b", ttl_seconds=30):
            raise SystemExit("resource acquire after release failed")

        artifact = Artifact(
            artifact_id="postgres-e2e-artifact",
            work_id=item.work_id,
            kind="json",
            uri="s3://example/roost/postgres-e2e-artifact.json",
            content_hash="sha256:example",
            metadata={"bytes": 42},
        )
        await artifacts.put(artifact)
        loaded_artifact = await artifacts.get(artifact.artifact_id)
        work_artifacts = await artifacts.list_for_work(item.work_id)
        if not loaded_artifact or loaded_artifact.uri != artifact.uri:
            raise SystemExit("artifact metadata did not round-trip")
        if [a.artifact_id for a in work_artifacts] != [artifact.artifact_id]:
            raise SystemExit("artifact metadata list did not round-trip")

        worker = await workers.heartbeat(
            worker_id="worker-a",
            engine_ids=[item.engine],
            queue_name="default",
            metadata={"pid": 123},
        )
        if worker["worker_id"] != "worker-a" or worker["engine_ids"] != [item.engine]:
            raise SystemExit("worker heartbeat did not round-trip")
        worker_list = await workers.list_workers(stale_after_seconds=60)
        if not worker_list or worker_list[0]["stale"]:
            raise SystemExit("worker heartbeat list did not round-trip")

        await control.upsert_on_enqueue(item, item.work_id)
        meta = await control.set_state(
            work_id=item.work_id,
            engine=item.engine,
            state="failed",
            step="observe",
            last_error={"type": "ExampleError", "message": "boom"},
        )
        if meta["state"] != "failed":
            raise SystemExit("metadata state did not round-trip")

        await control.push_dlq(
            {
                "work_id": item.work_id,
                "engine": item.engine,
                "step": "observe",
                "last_error": {"type": "ExampleError", "message": "boom"},
            }
        )
        dlq = await control.list_dlq(limit=10, offset=0)
        if not dlq or dlq[0]["work_id"] != item.work_id:
            raise SystemExit("DLQ did not round-trip")
        if not await control.ack_dlq(0):
            raise SystemExit("DLQ ack failed")
        if await control.list_dlq(limit=10, offset=0):
            raise SystemExit("DLQ ack did not hide entry")

        events = await control.list_events(limit=10)
        kinds = {event.get("kind") for event in events}
        if "work_state_changed" not in kinds or "dlq_pushed" not in kinds:
            raise SystemExit(f"missing expected events: {kinds}")

asyncio.run(main())
print("Verified Postgres work item, snapshot, artifact, lease, resource, worker, and control-plane stores")
PY
