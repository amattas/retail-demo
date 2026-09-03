/* Agent Operations — live approval funnel from the Eventhouse agent_actions table */

const api = (path) => fetch(path).then(async (r) => {
  if (!r.ok) {
    const d = await r.json().catch(() => ({}));
    throw new Error(d.detail || `${r.status} ${r.statusText}`);
  }
  return r.json();
});

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

const money = (n) => "$" + Math.round(Number(n) || 0).toLocaleString();
const num = (n) => (Number(n) || 0).toLocaleString();

function fmtTime(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

/* ---------- KPI cards ---------- */
function renderKpis(k) {
  const cards = [
    { label: "Actions proposed", value: num(k.total), sub: "by all agents", cls: "" },
    { label: "Approved", value: num(k.approved), sub: "human-confirmed", cls: "kpi-approved" },
    { label: "Pending review", value: num(k.pending), sub: "awaiting approval", cls: "kpi-pending" },
    { label: "Dismissed", value: num(k.dismissed), sub: "rejected by operator", cls: "kpi-dismissed" },
    { label: "Reorders approved", value: num(k.approved_reorders), sub: `${num(k.approved_reorder_units)} units`, cls: "kpi-approved" },
    { label: "Churn LTV addressed", value: money(k.addressed_ltv), sub: `${num(k.approved_campaigns)} campaign(s)`, cls: "kpi-approved" },
  ];
  document.getElementById("ops-kpis").innerHTML = cards.map((c) =>
    `<div class="kpi ${c.cls}">
       <div class="kpi-value">${c.value}</div>
       <div class="kpi-label">${c.label}</div>
       <div class="kpi-sub">${c.sub}</div>
     </div>`).join("");
}

/* ---------- timeline (last hour, per-minute stacked bars) ---------- */
const STATUS_ORDER = ["drafted", "approved", "dismissed"];
function renderTimeline(rows) {
  const el = document.getElementById("ops-timeline");
  if (!rows || !rows.length) {
    el.innerHTML = `<div class="empty">No agent activity in the last hour yet — ask an action question on the Dashboard and approve a recommendation.</div>`;
    return;
  }
  // group by minute bucket
  const byBin = new Map();
  let max = 0;
  rows.forEach((r) => {
    const b = r.action_ts;
    if (!byBin.has(b)) byBin.set(b, { drafted: 0, approved: 0, dismissed: 0 });
    byBin.get(b)[r.action_status] = (byBin.get(b)[r.action_status] || 0) + r.events;
  });
  const bins = [...byBin.entries()].sort((a, b) => new Date(a[0]) - new Date(b[0]));
  bins.forEach(([, v]) => { max = Math.max(max, v.drafted + v.approved + v.dismissed); });
  el.innerHTML = bins.map(([ts, v]) => {
    const total = v.drafted + v.approved + v.dismissed;
    const h = max ? Math.round((total / max) * 100) : 0;
    const seg = STATUS_ORDER.map((s) =>
      v[s] ? `<div class="bar-seg seg-${s}" style="flex:${v[s]}"></div>` : "").join("");
    return `<div class="bar-col" title="${fmtTime(ts)} · ${total} event(s)">
              <div class="bar" style="height:${h}%">${seg}</div>
            </div>`;
  }).join("");
}

/* ---------- by-agent funnel ---------- */
function renderByAgent(rows) {
  const el = document.getElementById("ops-byagent");
  if (!rows || !rows.length) { el.innerHTML = `<div class="empty">No actions yet.</div>`; return; }
  const agents = {};
  rows.forEach((r) => {
    agents[r.agent] = agents[r.agent] || { drafted: 0, approved: 0, dismissed: 0 };
    agents[r.agent][r.action_status] = r.n;
  });
  el.innerHTML = Object.entries(agents).map(([agent, v]) => {
    const total = (v.drafted || 0) + (v.approved || 0) + (v.dismissed || 0);
    const pct = (x) => (total ? (x / total) * 100 : 0);
    return `<div class="agent-row">
        <div class="agent-name">${escapeHtml(agent)}<span class="agent-total">${total}</span></div>
        <div class="agent-bar">
          <div class="seg-approved" style="width:${pct(v.approved || 0)}%"></div>
          <div class="seg-drafted" style="width:${pct(v.drafted || 0)}%"></div>
          <div class="seg-dismissed" style="width:${pct(v.dismissed || 0)}%"></div>
        </div>
        <div class="agent-legend">
          <span>${v.approved || 0} approved</span> ·
          <span>${v.drafted || 0} pending</span> ·
          <span>${v.dismissed || 0} dismissed</span>
        </div>
      </div>`;
  }).join("");
}

/* ---------- recent feed ---------- */
function renderFeed(rows) {
  const el = document.getElementById("ops-feed");
  if (!rows || !rows.length) {
    el.innerHTML = `<tr><td colspan="6" class="empty">No events yet.</td></tr>`;
    return;
  }
  el.innerHTML = rows.map((r) =>
    `<tr>
       <td class="mono">${fmtTime(r.action_ts)}</td>
       <td>${escapeHtml(r.agent)}</td>
       <td>${escapeHtml(r.action_kind)}</td>
       <td><span class="pill pill-${escapeHtml(r.action_status)}">${escapeHtml(r.action_status)}</span></td>
       <td>${escapeHtml(r.action_title || "")}</td>
       <td class="muted-cell">${escapeHtml(r.action_impact || "")}</td>
     </tr>`).join("");
}

/* ---------- refresh loop ---------- */
async function refresh() {
  const upd = document.getElementById("ops-updated");
  try {
    const d = await api("/api/actions/dashboard");
    if (d.enabled === false) {
      upd.textContent = d.error ? "unavailable" : "logging disabled";
      return;
    }
    renderKpis(d.kpis || {});
    renderTimeline(d.timeline || []);
    renderByAgent(d.byAgent || []);
    renderFeed(d.recent || []);
    upd.textContent = (d.mode === "replay" ? "synthetic replay · " : "updated ") +
      new Date().toLocaleTimeString();
  } catch (err) {
    upd.textContent = "error: " + err.message;
  }
}

refresh();
setInterval(refresh, 5000);
