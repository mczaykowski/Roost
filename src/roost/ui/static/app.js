const state = {
  view: "work",
  filter: "",
  search: "",
  workRows: [],
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
  } catch (err) {
    setConnection("Offline", err.message);
  }
}

function renderSummary(summary) {
  setConnection(summary.queue || "default", summary.prefix || "roost");
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

function eventText(row) {
  if (row.kind === "work_state_changed") return `${stateLabel(row.prev_state)} to ${stateLabel(row.state)}`;
  if (row.kind === "work_enqueued") return "Work added";
  if (row.kind === "dlq_pushed") return "Moved to failed work";
  return row.kind || "Runtime event";
}

function renderFailed(rows, dlq) {
  const combined = rows.length ? rows : dlq;
  if (!combined.length) {
    $("#failedRows").innerHTML = `
      <tr class="empty-row">
        <td colspan="4">
          <div class="empty">No failed work.</div>
        </td>
      </tr>
    `;
    return;
  }
  $("#failedRows").innerHTML = combined
    .map((row) => {
      const error = row.last_error?.message || row.last_error?.type || "-";
      return `
        <tr>
          <td class="mono">${shortId(row.work_id || "")}</td>
          <td>${row.engine || "-"}</td>
          <td>${row.step || "-"}</td>
          <td>${error}</td>
        </tr>
      `;
    })
    .join("");
}

async function openDetail(workId) {
  const detail = await getJson(`/api/work/${encodeURIComponent(workId)}`);
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
  renderArtifacts(detail.snapshot?.artifacts || []);
  $("#detailDrawer").classList.add("is-open");
  $("#detailDrawer").setAttribute("aria-hidden", "false");
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
  ["work", "events", "failed"].forEach((name) => {
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
