#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REDIS_PORT="${ROOST_E2E_REDIS_PORT:-6392}"
HTTP_PORT="${ROOST_E2E_HTTP_PORT:-8777}"
POSTGRES_PORT="${ROOST_E2E_POSTGRES_PORT:-55442}"
REDIS_NAME="roost-e2e-prod-redis-$$"
POSTGRES_NAME="roost-e2e-prod-postgres-$$"
REDIS_URL="redis://localhost:${REDIS_PORT}/0"
POSTGRES_DB="roost"
POSTGRES_USER="roost"
POSTGRES_PASSWORD="roost"
POSTGRES_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
REDIS_PREFIX="roost-e2e-prod-$$"
ARTIFACT_ROOT="$(mktemp -d /tmp/roost-e2e-prod-artifacts.XXXXXX)"
HTTP_LOG="$(mktemp /tmp/roost-e2e-prod-http.XXXXXX)"
WORKER_LOG="$(mktemp /tmp/roost-e2e-prod-worker.XXXXXX)"
HTTP_PID=""
WORKER_PID=""

cleanup() {
  if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
    kill "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${HTTP_PID}" ]] && kill -0 "${HTTP_PID}" 2>/dev/null; then
    kill "${HTTP_PID}" 2>/dev/null || true
    wait "${HTTP_PID}" 2>/dev/null || true
  fi
  docker rm -f "${REDIS_NAME}" >/dev/null 2>&1 || true
  docker rm -f "${POSTGRES_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

roost() {
  uv run --extra redis --extra postgres roost "$@"
}

json_get() {
  python3 -c "import json,sys; d=json.load(sys.stdin); ${1}"
}

wait_for_redis() {
  for _ in {1..30}; do
    if docker exec "${REDIS_NAME}" redis-cli ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "Redis did not become ready" >&2
  return 1
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

status_json() {
  roost status \
    --runtime-mode production \
    --postgres-url "${POSTGRES_URL}" \
    --redis-url "${REDIS_URL}" \
    --redis-prefix "${REDIS_PREFIX}" \
    "$1"
}

wait_for_first_observation() {
  local work_id="$1"
  for _ in {1..30}; do
    local status checks
    status="$(status_json "${work_id}")"
    checks="$(printf '%s' "${status}" | json_get 'print((((d.get("snapshot") or {}).get("data") or {}).get("checks_completed")) or 0)')"
    if [[ "${checks}" -ge 1 ]]; then
      printf '%s' "${status}"
      return 0
    fi
    sleep 0.5
  done
  echo "Timed out waiting for first observation" >&2
  return 1
}

wait_for_done() {
  local work_id="$1"
  for _ in {1..60}; do
    local status state
    status="$(status_json "${work_id}")"
    state="$(printf '%s' "${status}" | json_get 'print(((d.get("meta") or {}).get("state")) or "")')"
    if [[ "${state}" == "done" ]]; then
      printf '%s' "${status}"
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for work to finish" >&2
  return 1
}

start_worker() {
  roost worker \
    --runtime-mode production \
    --postgres-url "${POSTGRES_URL}" \
    --redis-url "${REDIS_URL}" \
    --redis-prefix "${REDIS_PREFIX}" \
    --engines watchlist \
    --concurrency 1 \
    --artifact-root "${ARTIFACT_ROOT}" \
    --repo-path "${ROOT_DIR}" \
    >"${WORKER_LOG}" 2>&1 &
  WORKER_PID="$!"
}

cd "${ROOT_DIR}"

echo "Starting Redis on ${REDIS_URL}"
docker run -d --rm -p "${REDIS_PORT}:6379" --name "${REDIS_NAME}" redis:7 >/dev/null
wait_for_redis

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

echo "Verifying doctor catches missing migrations"
if roost doctor \
  --runtime-mode production \
  --postgres-url "${POSTGRES_URL}" \
  --redis-url "${REDIS_URL}" \
  --redis-prefix "${REDIS_PREFIX}" \
  --engines watchlist \
  --repo-path "${ROOT_DIR}" \
  --artifact-root "${ARTIFACT_ROOT}" \
  >/dev/null 2>&1; then
  echo "Doctor unexpectedly passed before migrations" >&2
  exit 1
fi

echo "Applying Postgres migrations"
roost migrate --postgres-url "${POSTGRES_URL}" >/dev/null

echo "Checking production runtime configuration"
roost doctor \
  --runtime-mode production \
  --postgres-url "${POSTGRES_URL}" \
  --redis-url "${REDIS_URL}" \
  --redis-prefix "${REDIS_PREFIX}" \
  --engines watchlist \
  --repo-path "${ROOT_DIR}" \
  --artifact-root "${ARTIFACT_ROOT}" \
  >/dev/null

echo "Serving ${ROOT_DIR} at http://127.0.0.1:${HTTP_PORT}/"
python3 -m http.server "${HTTP_PORT}" --bind 127.0.0.1 >"${HTTP_LOG}" 2>&1 &
HTTP_PID="$!"

echo "Starting production-mode Roost watchlist worker"
start_worker

WORK_ID="$(
  roost enqueue \
    --runtime-mode production \
    --postgres-url "${POSTGRES_URL}" \
    --redis-url "${REDIS_URL}" \
    --redis-prefix "${REDIS_PREFIX}" \
    --engine watchlist \
    --resource domain:localhost \
    --payload "{\"url\":\"http://127.0.0.1:${HTTP_PORT}/README.md\",\"claim\":\"Local Roost README is reachable after production-mode restart\",\"checks_required\":4,\"delay_seconds\":3}"
)"

echo "Enqueued work: ${WORK_ID}"
echo "Waiting for at least one Postgres-backed snapshot..."
FIRST_STATUS="$(wait_for_first_observation "${WORK_ID}")"
FIRST_CHECKS="$(printf '%s' "${FIRST_STATUS}" | json_get 'print(d["snapshot"]["data"]["checks_completed"])')"
echo "Snapshot persisted with ${FIRST_CHECKS} observation(s). Killing worker."

kill "${WORKER_PID}" 2>/dev/null || true
wait "${WORKER_PID}" 2>/dev/null || true
WORKER_PID=""

echo "Restarting worker to resume from Postgres snapshot"
start_worker

FINAL_STATUS="$(wait_for_done "${WORK_ID}")"
ARTIFACT_ID="$(printf '%s' "${FINAL_STATUS}" | json_get 'print(d["snapshot"]["artifacts"][0]["artifact_id"])')"
FINAL_CHECKS="$(printf '%s' "${FINAL_STATUS}" | json_get 'print(d["snapshot"]["data"]["checks_completed"])')"
VERDICT="$(printf '%s' "${FINAL_STATUS}" | json_get 'print(d["snapshot"]["data"]["verdict"])')"

INSPECT_JSON="$(
  roost inspect \
    --runtime-mode production \
    --postgres-url "${POSTGRES_URL}" \
    --redis-url "${REDIS_URL}" \
    --redis-prefix "${REDIS_PREFIX}" \
    "${WORK_ID}"
)"
LIST_JSON="$(
  roost list \
    --runtime-mode production \
    --postgres-url "${POSTGRES_URL}" \
    --redis-url "${REDIS_URL}" \
    --redis-prefix "${REDIS_PREFIX}" \
    --state done \
    --limit 10
)"
EVENTS_JSON="$(
  roost events \
    --runtime-mode production \
    --postgres-url "${POSTGRES_URL}" \
    --redis-url "${REDIS_URL}" \
    --redis-prefix "${REDIS_PREFIX}" \
    --limit 20
)"

INSPECT_STATE="$(printf '%s' "${INSPECT_JSON}" | json_get 'print(d["meta"]["state"])')"
LIST_MATCHES="$(printf '%s' "${LIST_JSON}" | WORK_ID="${WORK_ID}" python3 -c 'import json,os,sys; rows=json.load(sys.stdin); print(sum(1 for row in rows if row.get("work_id") == os.environ["WORK_ID"]))')"
EVENT_MATCHES="$(printf '%s' "${EVENTS_JSON}" | WORK_ID="${WORK_ID}" python3 -c 'import json,os,sys; rows=json.load(sys.stdin); print(sum(1 for row in rows if row.get("work_id") == os.environ["WORK_ID"]))')"

if [[ "${INSPECT_STATE}" != "done" ]]; then
  echo "Production inspect did not read done state" >&2
  exit 1
fi
if [[ "${LIST_MATCHES}" -lt 1 ]]; then
  echo "Production list did not include completed work" >&2
  exit 1
fi
if [[ "${EVENT_MATCHES}" -lt 1 ]]; then
  echo "Production events did not include work events" >&2
  exit 1
fi

POSTGRES_URL="${POSTGRES_URL}" WORK_ID="${WORK_ID}" ARTIFACT_ID="${ARTIFACT_ID}" uv run --extra postgres python - <<'PY'
import os
import psycopg

with psycopg.connect(os.environ["POSTGRES_URL"]) as conn:
    artifact = conn.execute(
        "SELECT artifact_id FROM roost_artifacts WHERE work_id = %s",
        (os.environ["WORK_ID"],),
    ).fetchone()
    worker = conn.execute("SELECT worker_id FROM roost_worker_heartbeats LIMIT 1").fetchone()

if not artifact or artifact[0] != os.environ["ARTIFACT_ID"]:
    raise SystemExit("Postgres artifact metadata was not recorded")
if not worker:
    raise SystemExit("Postgres worker heartbeat was not recorded")
PY

echo "Final status: ${VERDICT} after ${FINAL_CHECKS} persisted observations"
echo "Production inspect/list/events read from Postgres"
echo "Artifact metadata and worker heartbeat recorded in Postgres"
echo "Artifact: ${ARTIFACT_ID}"
echo
roost artifact-show "${ARTIFACT_ID}" --ext json --artifact-root "${ARTIFACT_ROOT}"
