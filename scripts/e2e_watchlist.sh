#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REDIS_PORT="${ROOST_E2E_REDIS_PORT:-6381}"
HTTP_PORT="${ROOST_E2E_HTTP_PORT:-8765}"
REDIS_NAME="roost-e2e-redis-$$"
REDIS_URL="redis://localhost:${REDIS_PORT}/0"
REDIS_PREFIX="roost-e2e-$$"
ARTIFACT_ROOT="$(mktemp -d /tmp/roost-e2e-artifacts.XXXXXX)"
HTTP_LOG="$(mktemp /tmp/roost-e2e-http.XXXXXX)"
WORKER_LOG="$(mktemp /tmp/roost-e2e-worker.XXXXXX)"
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
}
trap cleanup EXIT

roost() {
  uv run --extra redis roost "$@"
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

wait_for_first_observation() {
  local work_id="$1"
  for _ in {1..30}; do
    local status checks
    status="$(roost status --redis-url "${REDIS_URL}" --redis-prefix "${REDIS_PREFIX}" "${work_id}")"
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
    status="$(roost status --redis-url "${REDIS_URL}" --redis-prefix "${REDIS_PREFIX}" "${work_id}")"
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

echo "Serving ${ROOT_DIR} at http://127.0.0.1:${HTTP_PORT}/"
python3 -m http.server "${HTTP_PORT}" --bind 127.0.0.1 >"${HTTP_LOG}" 2>&1 &
HTTP_PID="$!"

echo "Starting Roost watchlist worker"
start_worker

WORK_ID="$(
  roost enqueue \
    --redis-url "${REDIS_URL}" \
    --redis-prefix "${REDIS_PREFIX}" \
    --engine watchlist \
    --resource domain:localhost \
    --payload "{\"url\":\"http://127.0.0.1:${HTTP_PORT}/README.md\",\"claim\":\"Local Roost README is reachable after restart\",\"checks_required\":4,\"delay_seconds\":3}"
)"

echo "Enqueued work: ${WORK_ID}"
echo "Waiting for at least one persisted observation..."
FIRST_STATUS="$(wait_for_first_observation "${WORK_ID}")"
FIRST_CHECKS="$(printf '%s' "${FIRST_STATUS}" | json_get 'print(d["snapshot"]["data"]["checks_completed"])')"
echo "Snapshot persisted with ${FIRST_CHECKS} observation(s). Killing worker."

kill "${WORKER_PID}" 2>/dev/null || true
wait "${WORKER_PID}" 2>/dev/null || true
WORKER_PID=""

echo "Restarting worker to resume from saved snapshot"
start_worker

FINAL_STATUS="$(wait_for_done "${WORK_ID}")"
ARTIFACT_ID="$(printf '%s' "${FINAL_STATUS}" | json_get 'print(d["snapshot"]["artifacts"][0]["artifact_id"])')"
FINAL_CHECKS="$(printf '%s' "${FINAL_STATUS}" | json_get 'print(d["snapshot"]["data"]["checks_completed"])')"
VERDICT="$(printf '%s' "${FINAL_STATUS}" | json_get 'print(d["snapshot"]["data"]["verdict"])')"

echo "Final status: ${VERDICT} after ${FINAL_CHECKS} persisted observations"
echo "Artifact: ${ARTIFACT_ID}"
echo
roost artifact-show "${ARTIFACT_ID}" --ext json --artifact-root "${ARTIFACT_ROOT}"
