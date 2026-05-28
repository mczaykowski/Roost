const state = {
  view: "work",
  filter: "",
  search: "",
  workRows: [],
  workerRows: [],
  detail: null,
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

function prettyTime(epochSeconds) {
  if (!epochSeconds) return "-";
  const seconds = Math.max(0, Math.floor(Date.now() / 1000 - Number(epochSeconds)));
  if (seconds < 5) return "now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}

function futureTime(epochSeconds) {
  if (!epochSeconds) return "-";
  const seconds = Math.ceil(Number(epochSeconds) - Date.now() / 1000);
  if (seconds <= 0) return "now";
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.ceil(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.ceil(minutes / 60);
  return `${hours}h`;
}

function stateLabel(value) {
  if (value === "queued") return "Waiting";
  if (!value) return "Waiting";
  return value.charAt(0).toUpperCase() + value.slice(1);
}

function pill(value) {
  const stateName = value || "queued";
  return `<span class="state-pill state-${stateName}">${stateLabel(stateName)}</span>`;
}

function shortId(value) {
  if (!value) return "-";
  return value.length > 18 ? `${value.slice(0, 12)}...${value.slice(-4)}` : value;
}

function json(value) {
  return JSON.stringify(value ?? {}, null, 2);
}

async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

async function postJson(url, payload = {}) {
  const res = await fetch(url, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `${res.status} ${res.statusText}`);
  return data;
}

function setConnection(main, sub) {
  $("#connectionLabel").innerHTML = `
    <span class="connection-main">${main}</span>
    <span class="connection-sub">${sub || "&nbsp;"}</span>
  `;
}

async function refresh() {
  try {
    const [summary, work, events, failed] = await Promise.all([
      getJson("/api/summary"),
      getJson(`/api/work?limit=120&state=${encodeURIComponent(state.filter)}`),
      getJson("/api/events?limit=80"),
      getJson("/api/failed?limit=80"),
    ]);
    renderSummary(summary);
    state.workRows = work.rows || [];
    renderWork();
    renderEvents(events.rows || []);
    renderFailed(failed.rows || [], failed.dlq || []);
    if (summary.runtime_mode === "production") {
      const workers = await getJson("/api/workers?limit=120&stale_after=30");
      state.workerRows = workers.rows || [];
    } else {
      state.workerRows = [];
    }
    renderWorkers(summary.runtime_mode || "simple");
  } catch (err) {
    setConnection("Offline", err.message);
  }
}

function renderSummary(summary) {
  const mode = summary.runtime_mode === "production" ? "production" : "simple";
  setConnection(`${summary.queue || "default"} / ${mode}`, summary.prefix || "roost");
  $("#statTotal").textContent = summary.total ?? 0;
  $("#statRunning").textContent = summary.states?.running ?? 0;
  $("#statQueued").textContent = summary.states?.queued ?? 0;
  $("#statDone").textContent = summary.states?.done ?? 0;
  $("#statFailed").textContent = summary.states?.failed ?? 0;
}

function matchesSearch(row) {
  const term = state.search.trim().toLowerCase();
  if (!term) return true;
  return [row.work_id, row.engine, row.step, row.state, JSON.stringify(row)]
    .join(" ")
    .toLowerCase()
    .includes(term);
}

function renderWork() {
  const rows = state.workRows.filter(matchesSearch);
  if (!rows.length) {
    $("#workRows").innerHTML = `
      <tr class="empty-row">
        <td colspan="7">
          <div class="empty">No work found.</div>
        </td>
      </tr>
    `;
    return;
  }
  $("#workRows").innerHTML = rows
    .map((row) => {
      const output = outputLabel(row);
      const resources = resourceLabel(row.resources || []);
      return `
        <tr>
          <td>${pill(row.state)}</td>
          <td>
            <button class="row-button" data-work="${row.work_id}">
              <span class="cell-main mono">${shortId(row.work_id)}</span>
              <span class="cell-meta">${resources}</span>
            </button>
          </td>
          <td><span class="cell-main">${row.engine || "-"}</span></td>
          <td>
            <span class="cell-main">${row.step || "-"}</span>
            <span class="cell-meta">v${row.snapshot_version || "-"}</span>
          </td>
          <td><span class="cell-main">${prettyTime(row.updated_at)}</span></td>
          <td><span class="cell-main">${row.is_finished ? "-" : futureTime(row.next_run_at)}</span></td>
          <td>${output}</td>
        </tr>
      `;
    })
    .join("");
  $$("[data-work]").forEach((button) => {
    button.addEventListener("click", () => openDetail(button.dataset.work));
  });
}

function resourceLabel(resources) {
  if (!resources.length) return "No resource";
  if (resources.length === 1) return resources[0];
  return `${resources[0]} +${resources.length - 1}`;
}

function outputLabel(row) {
  if (row.last_error) return `<span class="output output-error">Error</span>`;
  if (row.artifacts_count > 0) return `<span class="output">JSON</span>`;
  if (row.state === "done") return `<span class="output">Ready</span>`;
  if (row.observations_count > 0) return `<span class="output">${row.observations_count} saved</span>`;
  return `<span class="output output-muted">-</span>`;
}

function renderEvents(rows) {
  if (!rows.length) {
    $("#eventRows").innerHTML = `<div class="empty">No events yet.</div>`;
    return;
  }
  $("#eventRows").innerHTML = rows
    .map(
      (row) => `
        <div class="event-row">
          <span>${prettyTime(row.ts)}</span>
          <span>${row.engine || "-"}</span>
          <span>${eventText(row)}</span>
          <span class="mono">${shortId(row.work_id || "")}</span>
        </div>
      `,
    )
    .join("");
}

function renderWorkers(runtimeMode) {
  if (runtimeMode !== "production") {
    $("#workerRows").innerHTML = `
      <tr class="empty-row">
        <td colspan="6">
          <div class="empty">Worker heartbeats are available in production mode.</div>
        </td>
      </tr>
    `;
    return;
  }
  const rows = state.workerRows;
  if (!rows.length) {
    $("#workerRows").innerHTML = `
      <tr class="empty-row">
        <td colspan="6">
          <div class="empty">No workers found.</div>
        </td>
      </tr>
    `;
    return;
  }
  $("#workerRows").innerHTML = rows
    .map((row) => {
      const metadata = row.metadata || {};
      const engines = (row.engine_ids || []).join(", ") || "-";
      return `
        <tr>
          <td>${workerPill(row)}</td>
          <td>
            <span class="cell-main mono">${shortId(row.worker_id)}</span>
            <span class="cell-meta">${metadata.concurrency ? `${metadata.concurrency} slots` : "worker"}</span>
          </td>
          <td>${engines}</td>
          <td>${row.queue_name || "-"}</td>
          <td>${prettyTime(row.last_seen_at)}</td>
          <td>${metadata.runtime_mode || "production"}</td>
        </tr>
      `;
    })
    .join("");
}

function workerPill(row) {
  return row.stale
    ? `<span class="state-pill state-failed">Stale</span>`
    : `<span class="state-pill state-done">Active</span>`;
}

function eventText(row) {
  if (row.kind === "work_state_changed") return `${stateLabel(row.prev_state)} to ${stateLabel(row.state)}`;
  if (row.kind === "work_enqueued") return "Work added";
  if (row.kind === "dlq_pushed") return "Moved to failed work";
  if (row.kind === "work_retry_requested") return "Retry requested";
  if (row.kind === "work_cancelled") return "Cancelled";
  if (row.kind === "dlq_replay_requested") return "DLQ replay requested";
  return row.kind || "Runtime event";
}

function renderFailed(rows, dlq) {
  const combined = [
    ...rows.map((row) => ({...row, source: "failed"})),
    ...dlq.map((row) => ({...row, source: "dlq"})),
  ];
  if (!combined.length) {
    $("#failedRows").innerHTML = `
      <tr class="empty-row">
        <td colspan="5">
          <div class="empty">No failed work.</div>
        </td>
      </tr>
    `;
    return;
  }
  $("#failedRows").innerHTML = combined
    .map((row) => {
      const error = row.last_error?.message || row.last_error?.type || "-";
      const action =
        row.source === "dlq"
          ? `
            <div class="row-actions">
              <button class="action-button" data-dlq-replay="${row.index}">Replay</button>
              <button class="action-button action-muted" data-dlq-ack="${row.index}">Ack</button>
            </div>
          `
          : `<button class="action-button" data-retry="${row.work_id}">Retry</button>`;
      return `
        <tr>
          <td class="mono">${shortId(row.work_id || "")}</td>
          <td>${row.engine || "-"}</td>
          <td>${row.step || "-"}</td>
          <td>${error}</td>
          <td>${action}</td>
        </tr>
      `;
    })
    .join("");
  bindActionButtons();
}

async function openDetail(workId) {
  const detail = await getJson(`/api/work/${encodeURIComponent(workId)}`);
  state.detail = detail;
  $("#detailTitle").textContent = shortId(workId);
  $("#detailMeta").textContent = [
    detail.meta?.state ? stateLabel(detail.meta.state) : null,
    detail.meta?.engine,
    detail.meta?.step,
  ]
    .filter(Boolean)
    .join(" / ");
  $("#payloadJson").textContent = json(detail.item?.payload || {});
  $("#snapshotJson").textContent = json(detail.snapshot || {});
  renderDetailActions(detail);
  renderArtifacts(detail.snapshot?.artifacts || []);
  $("#detailDrawer").classList.add("is-open");
  $("#detailDrawer").setAttribute("aria-hidden", "false");
}

function renderDetailActions(detail, message = "") {
  const workId = detail.item?.work_id || detail.meta?.work_id;
  const status = detail.meta?.state || detail.snapshot?.status || "queued";
  const canCancel = ["queued", "running"].includes(status);
  const canRetry = ["failed", "cancelled", "done"].includes(status);
  const buttons = [];

  if (canRetry) {
    buttons.push(`<button class="action-button" data-retry="${workId}">Retry</button>`);
  }
  if (canCancel) {
    buttons.push(`<button class="action-button action-danger" data-cancel="${workId}">Cancel</button>`);
  }

  $("#detailActions").innerHTML = `
    <div class="action-strip">
      ${buttons.length ? buttons.join("") : `<span class="action-note">No action needed.</span>`}
      <span class="action-message">${message}</span>
    </div>
  `;
  bindActionButtons();
}

function setDetailMessage(message) {
  if (!state.detail) return;
  renderDetailActions(state.detail, message);
}

async function runWorkAction(kind, workId) {
  if (!workId) return;
  try {
    if (kind === "retry") {
      await postJson(`/api/work/${encodeURIComponent(workId)}/retry`, {});
      setDetailMessage("Retry queued.");
    } else if (kind === "cancel") {
      await postJson(`/api/work/${encodeURIComponent(workId)}/cancel`, {reason: "ui_cancelled"});
      setDetailMessage("Cancelled.");
    }
    await refresh();
    if (state.detail?.item?.work_id === workId || state.detail?.meta?.work_id === workId) {
      await openDetail(workId);
    }
  } catch (err) {
    setDetailMessage(err.message);
  }
}

async function runDlqAction(kind, index) {
  try {
    if (kind === "replay") {
      await postJson(`/api/dlq/${encodeURIComponent(index)}/replay`, {ack: true});
    } else {
      await postJson(`/api/dlq/${encodeURIComponent(index)}/ack`, {});
    }
    await refresh();
  } catch (err) {
    setConnection("Action failed", err.message);
  }
}

function bindActionButtons() {
  $$("[data-retry]").forEach((button) => {
    button.onclick = () => runWorkAction("retry", button.dataset.retry);
  });
  $$("[data-cancel]").forEach((button) => {
    button.onclick = () => runWorkAction("cancel", button.dataset.cancel);
  });
  $$("[data-dlq-replay]").forEach((button) => {
    button.onclick = () => runDlqAction("replay", button.dataset.dlqReplay);
  });
  $$("[data-dlq-ack]").forEach((button) => {
    button.onclick = () => runDlqAction("ack", button.dataset.dlqAck);
  });
}

function renderArtifacts(artifacts) {
  if (!artifacts.length) {
    $("#artifactList").innerHTML = `<div class="empty">No output yet.</div>`;
    return;
  }
  $("#artifactList").innerHTML = artifacts
    .map(
      (artifact) => `
        <div class="artifact-card">
          <div class="mono">${shortId(artifact.artifact_id)}</div>
          <div>${artifact.kind || "output"}</div>
          <button data-artifact="${artifact.artifact_id}">Show output</button>
          <pre id="artifact-${artifact.artifact_id}" hidden></pre>
        </div>
      `,
    )
    .join("");
  $$("[data-artifact]").forEach((button) => {
    button.addEventListener("click", async () => {
      const data = await getJson(`/api/artifacts/${button.dataset.artifact}?ext=json`);
      const pre = $(`#artifact-${button.dataset.artifact}`);
      pre.hidden = false;
      pre.textContent = json(data.content || data);
    });
  });
}

function setView(nextView) {
  state.view = nextView;
  $$(".nav-item").forEach((item) => item.classList.toggle("is-active", item.dataset.view === nextView));
  ["work", "workers", "events", "failed"].forEach((name) => {
    $(`#${name}View`).classList.toggle("is-visible", name === nextView);
  });
}

$$(".nav-item").forEach((button) => button.addEventListener("click", () => setView(button.dataset.view)));
$$(".filter").forEach((button) => {
  button.addEventListener("click", () => {
    state.filter = button.dataset.state || "";
    $$(".filter").forEach((item) => item.classList.toggle("is-active", item === button));
    refresh();
  });
});
$("#searchInput").addEventListener("input", (event) => {
  state.search = event.target.value;
  renderWork();
});
$$("[data-close]").forEach((node) => {
  node.addEventListener("click", () => {
    $("#detailDrawer").classList.remove("is-open");
    $("#detailDrawer").setAttribute("aria-hidden", "true");
  });
});

refresh();
setInterval(refresh, 3000);
