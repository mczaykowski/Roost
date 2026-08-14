#!/usr/bin/env bash
# Honest production drill: watchlist engine, mid-step SIGKILL while the lease is
# held, Redis FLUSHALL, then a replacement worker must resume from Postgres.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REDIS_PORT="${ROOST_DRILL_REDIS_PORT:-6394}"
HTTP_PORT="${ROOST_DRILL_HTTP_PORT:-8779}"
POSTGRES_PORT="${ROOST_DRILL_POSTGRES_PORT:-55445}"
REDIS_NAME="roost-drill-redis-$$"
POSTGRES_NAME="roost-drill-pg-$$"
REDIS_URL="redis://localhost:${REDIS_PORT}/0"
POSTGRES_URL="postgresql://roost:roost@localhost:${POSTGRES_PORT}/roost"
REDIS_PREFIX="roost-drill-$$"
ARTIFACT_ROOT="$(mktemp -d /tmp/roost-drill-artifacts.XXXXXX)"
HTTP_LOG="$(mktemp /tmp/roost-drill-http.XXXXXX)"
WORKER_A_LOG="$(mktemp /tmp/roost-drill-worker-a.XXXXXX)"
WORKER_B_LOG="$(mktemp /tmp/roost-drill-worker-b.XXXXXX)"
WORKER_C_LOG="$(mktemp /tmp/roost-drill-worker-c.XXXXXX)"
HTTP_PID=""
WORKER_A_PID=""
WORKER_B_PID=""
WORKER_C_PID=""
LEASE_TTL="${ROOST_DRILL_LEASE_TTL:-8}"
STALE_AFTER="${ROOST_DRILL_STALE_AFTER:-2}"
RECOVERY_INTERVAL="${ROOST_DRILL_RECOVERY_INTERVAL:-1}"
HEARTBEAT_INTERVAL="${ROOST_DRILL_HEARTBEAT_INTERVAL:-2}"
HTTP_SLEEP="${ROOST_DRILL_HTTP_SLEEP:-5}"
CHECKS_REQUIRED=3
DELAY_SECONDS=0

cleanup() {
  for pid in "${WORKER_A_PID}" "${WORKER_B_PID}" "${WORKER_C_PID}" "${HTTP_PID}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill -9 "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    fi
  done
  docker rm -f "${REDIS_NAME}" >/dev/null 2>&1 || true
  docker rm -f "${POSTGRES_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

roost() {
  uv run --extra redis --extra postgres roost "$@"
}

common=(
  --runtime-mode production
  --postgres-url "${POSTGRES_URL}"
  --redis-url "${REDIS_URL}"
  --redis-prefix "${REDIS_PREFIX}"
)

json_get() {
  python3 -c "import json,sys; d=json.load(sys.stdin); ${1}"
}

wait_for_redis() {
  for _ in {1..40}; do
    if docker exec "${REDIS_NAME}" redis-cli ping >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "Redis did not become ready" >&2
  return 1
}

wait_for_postgres() {
  for _ in {1..60}; do
    if docker exec "${POSTGRES_NAME}" pg_isready -U roost -d roost >/dev/null 2>&1; then
      if uv run --extra postgres python - "${POSTGRES_URL}" <<'PY' >/dev/null 2>&1
import sys, psycopg
with psycopg.connect(sys.argv[1], connect_timeout=2) as conn:
    conn.execute("SELECT 1")
PY
      then
        return 0
      fi
    fi
    sleep 0.25
  done
  echo "Postgres did not become ready" >&2
  return 1
}

status_json() {
  roost status "${common[@]}" "$1"
}

pg() {
  uv run --extra postgres python - "$POSTGRES_URL" "$@" <<'PY'
import json, sys, psycopg
url, kind = sys.argv[1], sys.argv[2]
work_id = sys.argv[3] if len(sys.argv) > 3 else None
with psycopg.connect(url) as conn:
    if kind == "meta":
        row = conn.execute(
            "SELECT state, step, updated_at FROM roost_work_meta WHERE work_id = %s",
            (work_id,),
        ).fetchone()
        print(json.dumps({"state": row[0] if row else None, "step": row[1] if row else None}, default=str))
    elif kind == "snap":
        row = conn.execute(
            """
            SELECT version, step, is_finished, data->>'checks_completed', data->>'verdict'
            FROM roost_snapshots WHERE work_id = %s
            """,
            (work_id,),
        ).fetchone()
        print(json.dumps({
            "version": row[0] if row else None,
            "step": row[1] if row else None,
            "is_finished": row[2] if row else None,
            "checks_completed": int(row[3] or 0) if row else 0,
            "verdict": row[4] if row else None,
        }, default=str))
    elif kind == "lease":
        row = conn.execute(
            "SELECT holder_id, expires_at > now() FROM roost_leases WHERE work_id = %s",
            (work_id,),
        ).fetchone()
        print(json.dumps({"holder": row[0] if row else None, "active": bool(row and row[1])}))
    elif kind == "queue":
        n = conn.execute("SELECT count(*) FROM roost_work_meta").fetchone()[0]
        print(n)
PY
}

wait_for_checks() {
  local work_id="$1"
  local want="$2"
  local seconds="${3:-45}"
  local i
  for ((i = 0; i < seconds * 2; i++)); do
    local checks
    checks="$(pg snap "${work_id}" | json_get 'print(d.get("checks_completed") or 0)')"
    if [[ "${checks}" -ge "${want}" ]]; then
      echo "${checks}"
      return 0
    fi
    sleep 0.5
  done
  echo "Timed out waiting for ${want} check(s); last=$(pg snap "${work_id}")" >&2
  return 1
}

wait_for_done() {
  local work_id="$1"
  for _ in {1..90}; do
    local state
    state="$(pg meta "${work_id}" | json_get 'print(d.get("state") or "")')"
    if [[ "${state}" == "done" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for done; meta=$(pg meta "${work_id}") snap=$(pg snap "${work_id}")" >&2
  return 1
}

wait_for_active_lease() {
  local work_id="$1"
  local seconds="${2:-30}"
  local i
  for ((i = 0; i < seconds * 10; i++)); do
    local lease
    lease="$(pg lease "${work_id}")"
    local active
    active="$(printf '%s' "${lease}" | json_get 'print(d.get("active"))')"
    if [[ "${active}" == "True" ]]; then
      printf '%s' "${lease}"
      return 0
    fi
    sleep 0.1
  done
  echo "Timed out waiting for active lease; last=$(pg lease "${work_id}")" >&2
  return 1
}

start_worker() {
  local log="$1"
  roost worker \
    "${common[@]}" \
    --engines watchlist \
    --concurrency 1 \
    --lease-ttl "${LEASE_TTL}" \
    --stale-after "${STALE_AFTER}" \
    --recovery-interval "${RECOVERY_INTERVAL}" \
    --heartbeat-interval "${HEARTBEAT_INTERVAL}" \
    --log-level info \
    --artifact-root "${ARTIFACT_ROOT}" \
    --repo-path "${ROOT_DIR}" \
    >"${log}" 2>&1 &
  echo $!
}

holder_pid() {
  local holder="$1"
  roost workers --runtime-mode production --postgres-url "${POSTGRES_URL}" --limit 20 --stale-after 60 \
    | HOLDER="${holder}" python3 -c '
import json, os, sys
holder = os.environ["HOLDER"]
d = json.load(sys.stdin)
for row in d.get("rows") or []:
    if row.get("worker_id") == holder:
        pid = (row.get("metadata") or {}).get("pid")
        print(pid or "")
        break
'
}

phase() {
  echo
  echo "=== $* ==="
}

cd "${ROOT_DIR}"

phase "Start isolated Redis + Postgres"
docker run -d --rm -p "${REDIS_PORT}:6379" --name "${REDIS_NAME}" redis:7 >/dev/null
wait_for_redis
docker run -d --rm -p "${POSTGRES_PORT}:5432" --name "${POSTGRES_NAME}" \
  -e POSTGRES_DB=roost -e POSTGRES_USER=roost -e POSTGRES_PASSWORD=roost \
  postgres:16 >/dev/null
wait_for_postgres

phase "Migrate + doctor"
roost migrate --postgres-url "${POSTGRES_URL}"
roost doctor "${common[@]}" --engines watchlist --repo-path "${ROOT_DIR}" --artifact-root "${ARTIFACT_ROOT}" || true

phase "Start blocking HTTP so engine.step() holds the lease"
python3 - "${HTTP_PORT}" "${HTTP_SLEEP}" >"${HTTP_LOG}" 2>&1 <<'PY' &
import sys, time
from http.server import BaseHTTPRequestHandler, HTTPServer

port = int(sys.argv[1])
sleep_s = float(sys.argv[2])

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(sleep_s)
        body = b"<html><title>ok</title></html>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass

HTTPServer(("127.0.0.1", port), Handler).serve_forever()
PY
HTTP_PID="$!"

phase "Start two-worker fleet"
WORKER_A_PID="$(start_worker "${WORKER_A_LOG}")"
WORKER_B_PID="$(start_worker "${WORKER_B_LOG}")"
echo "worker A pid=${WORKER_A_PID}  worker B pid=${WORKER_B_PID}"
sleep 2

WORK_ID="$(
  roost enqueue \
    "${common[@]}" \
    --engine watchlist \
    --payload "{\"url\":\"http://127.0.0.1:${HTTP_PORT}/slow\",\"claim\":\"reachable through mid-step crash and Redis flush\",\"checks_required\":${CHECKS_REQUIRED},\"delay_seconds\":${DELAY_SECONDS}}"
)"
echo "enqueued ${WORK_ID}"

phase "Wait until Postgres shows an active lease, then SIGKILL that holder"
LEASE="$(wait_for_active_lease "${WORK_ID}" 30)"
echo "lease=${LEASE} snap=$(pg snap "${WORK_ID}")"
CHECKS_BEFORE="$(pg snap "${WORK_ID}" | json_get 'print(d.get("checks_completed") or 0)')"
HOLDER="$(printf '%s' "${LEASE}" | json_get 'print(d.get("holder") or "")')"
KILL_PID="$(holder_pid "${HOLDER}")"
if [[ -z "${KILL_PID}" ]]; then
  echo "Could not map lease holder ${HOLDER} to a pid; falling back to worker A" >&2
  KILL_PID="${WORKER_A_PID}"
fi
echo "SIGKILL holder=${HOLDER} pid=${KILL_PID} checks_before=${CHECKS_BEFORE}"
kill -9 "${KILL_PID}" 2>/dev/null || true
wait "${KILL_PID}" 2>/dev/null || true
if [[ "${KILL_PID}" == "${WORKER_A_PID}" ]]; then
  WORKER_A_PID=""
elif [[ "${KILL_PID}" == "${WORKER_B_PID}" ]]; then
  WORKER_B_PID=""
fi

phase "Survivor resumes from latest saved snapshot (lease TTL=${LEASE_TTL}s); do not wait for done yet"
echo "-- workers after kill --"
roost workers --runtime-mode production --postgres-url "${POSTGRES_URL}" --limit 20 --stale-after 15 | python3 -c '
import json, sys
d = json.load(sys.stdin)
for r in d.get("rows") or []:
    print("%s stale=%s age_seconds=%s" % (r.get("worker_id"), r.get("stale"), r.get("age_seconds")))
'
RESUME_WANT=$((CHECKS_BEFORE + 1))
if [[ "${RESUME_WANT}" -lt 1 ]]; then
  RESUME_WANT=1
fi
CHECKS_AFTER_RESUME="$(wait_for_checks "${WORK_ID}" "${RESUME_WANT}" 60)"
echo "checks=${CHECKS_AFTER_RESUME} meta=$(pg meta "${WORK_ID}") snap=$(pg snap "${WORK_ID}")"
if [[ "${CHECKS_AFTER_RESUME}" -lt "${CHECKS_BEFORE}" ]]; then
  echo "FAIL: checks went backwards (${CHECKS_AFTER_RESUME} < ${CHECKS_BEFORE})" >&2
  exit 1
fi

phase "Kill remaining workers, FLUSHALL Redis, recover with --stale-after ${STALE_AFTER}"
for pid in "${WORKER_A_PID}" "${WORKER_B_PID}"; do
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill -9 "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
done
WORKER_A_PID=""
WORKER_B_PID=""
docker exec "${REDIS_NAME}" redis-cli FLUSHALL
INFLIGHT="$(docker exec "${REDIS_NAME}" redis-cli KEYS '*')"
echo "redis keys after flush: ${INFLIGHT:-<none>}"
echo "postgres still has: meta=$(pg meta "${WORK_ID}") snap=$(pg snap "${WORK_ID}")"

WORKER_C_PID="$(start_worker "${WORKER_C_LOG}")"
echo "worker C pid=${WORKER_C_PID}"

phase "Replacement worker must recover from Postgres and finish"
wait_for_done "${WORK_ID}"
echo "meta=$(pg meta "${WORK_ID}") snap=$(pg snap "${WORK_ID}")"

phase "Operator inspection after recovery"
INSPECT="$(roost inspect "${common[@]}" "${WORK_ID}")"
echo "$INSPECT" | python3 -c '
import json,sys
d=json.load(sys.stdin)
m=d.get("meta") or {}
s=d.get("snapshot") or {}
data=s.get("data") or {}
print("inspect state", m.get("state"))
print("inspect step", m.get("step"))
print("snapshot version", s.get("version"), "finished", s.get("is_finished"))
print("checks", data.get("checks_completed"), "verdict", data.get("verdict"))
print("observations", len(data.get("observations") or []))
print("artifacts", len(s.get("artifacts") or []))
'
LIST_N="$(roost list "${common[@]}" --state done --limit 10 | WID="${WORK_ID}" python3 -c 'import json,os,sys; rows=json.load(sys.stdin); print(sum(1 for r in rows if r.get("work_id")==os.environ["WID"]))')"
EVENTS="$(roost events "${common[@]}" --limit 80)"
EVENTS_N="$(printf '%s' "${EVENTS}" | WID="${WORK_ID}" python3 -c 'import json,os,sys; rows=json.load(sys.stdin); print(sum(1 for r in rows if r.get("work_id")==os.environ["WID"]))')"
RECOVERED_N="$(printf '%s' "${EVENTS}" | WID="${WORK_ID}" python3 -c 'import json,os,sys; rows=json.load(sys.stdin); print(sum(1 for r in rows if r.get("work_id")==os.environ["WID"] and r.get("kind")=="work_recovered"))')"
echo "list done matches=${LIST_N} event matches=${EVENTS_N} work_recovered=${RECOVERED_N}"
echo "-- events (kinds) --"
printf '%s' "${EVENTS}" | python3 -c 'import json,sys; rows=json.load(sys.stdin); print(", ".join((r.get("kind") or "?") for r in rows[:16]))'

SNAP="$(pg snap "${WORK_ID}")"
CHECKS="$(printf '%s' "${SNAP}" | json_get 'print(d["checks_completed"])')"
FINISHED="$(printf '%s' "${SNAP}" | json_get 'print(d["is_finished"])')"
VERDICT="$(printf '%s' "${SNAP}" | json_get 'print(d.get("verdict") or "")')"
VERSION="$(printf '%s' "${SNAP}" | json_get 'print(d.get("version") or 0)')"
STATE="$(pg meta "${WORK_ID}" | json_get 'print(d["state"])')"

fail=0
if [[ "${STATE}" != "done" ]]; then
  echo "FAIL: meta state is ${STATE}, want done" >&2
  fail=1
fi
if [[ "${FINISHED}" != "True" ]]; then
  echo "FAIL: snapshot not finished" >&2
  fail=1
fi
if [[ "${CHECKS}" -lt "${CHECKS_REQUIRED}" ]]; then
  echo "FAIL: checks_completed=${CHECKS} want >= ${CHECKS_REQUIRED}" >&2
  fail=1
fi
if [[ "${VERSION}" -gt 10 ]]; then
  echo "FAIL: snapshot version ${VERSION} looks like a wait-only version storm" >&2
  fail=1
fi
if [[ "${LIST_N}" -lt 1 ]]; then
  echo "FAIL: list --state done missed the work" >&2
  fail=1
fi
if [[ "${EVENTS_N}" -lt 1 ]]; then
  echo "FAIL: events missed the work" >&2
  fail=1
fi
if [[ "${RECOVERED_N}" -lt 1 ]]; then
  echo "FAIL: missing work_recovered event after Redis flush" >&2
  fail=1
fi

echo
echo "worker A log (last 15):"
tail -n 15 "${WORKER_A_LOG}" || true
echo "worker C log (last 20):"
tail -n 20 "${WORKER_C_LOG}" || true

if ! grep -q '"work_id"' "${WORKER_C_LOG}"; then
  echo "FAIL: worker C log has no JSON step lines (OPS-1 logging)" >&2
  fail=1
fi

if [[ "${fail}" -ne 0 ]]; then
  echo
  echo "DRILL FAILED"
  exit 1
fi

echo
echo "DRILL PASSED"
echo "work_id=${WORK_ID} verdict=${VERDICT} checks=${CHECKS} version=${VERSION}"
echo "Survived: mid-step SIGKILL of the lease holder, full Redis flush, replacement resume from Postgres."
