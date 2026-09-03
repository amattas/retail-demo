/* Retail Intelligence — Dashboard page (Power BI report + chat) */

const api = (path, opts) => fetch(path, opts).then(async (r) => {
  if (!r.ok) {
    const detail = await r.json().catch(() => ({}));
    throw new Error(detail.detail || `${r.status} ${r.statusText}`);
  }
  return r.json();
});

/* ---------- Power BI embed ---------- */
function renderReplayDashboard(data) {
  const status = document.getElementById("report-status");
  status.textContent = "replay · synthetic";
  document.getElementById("scenario-headline").textContent = data.headline;
  const kpis = (data.kpis || []).map((k) =>
    `<div class="mock-kpi">
       <div class="mock-kpi-label">${escapeHtml(k.label)}</div>
       <div class="mock-kpi-value">${escapeHtml(k.value)}</div>
       <div class="mock-kpi-delta">${escapeHtml(k.delta)}</div>
     </div>`).join("");
  const rows = (data.stores || []).map((s) =>
    `<tr><td>${escapeHtml(s.store)}</td><td>${escapeHtml(s.market)}</td>
      <td>${escapeHtml(s.velocity)}</td><td>${escapeHtml(s.weeks_cover)}</td>
      <td><span class="decision-pill">${escapeHtml(s.recommendation)}</span></td></tr>`
  ).join("");
  document.getElementById("report").innerHTML =
    `<div class="mock-report">
       <div class="mock-report-title">
         <div><strong>${escapeHtml(data.brand)}</strong><span>${escapeHtml(data.period)}</span></div>
         <span class="synthetic-badge">Synthetic data</span>
       </div>
       <div class="mock-kpis">${kpis}</div>
       <div class="mock-insight">
         <span>Growth signal</span>
         <strong>Momentum Runner +38.0%</strong>
         <small>inside Footwear -6.2%</small>
       </div>
       <div class="mock-table-title">Supply-aware store opportunity</div>
       <table class="rec-table"><thead><tr><th>Store</th><th>Market</th>
         <th>Units/day</th><th>Weeks cover</th><th>Decision</th></tr></thead>
         <tbody>${rows}</tbody></table>
       <div class="mock-footnote">All names, values, entities, and decisions are fictional.</div>
     </div>`;
}

async function embedReport() {
  const status = document.getElementById("report-status");
  try {
    status.textContent = "loading…";
    const embed = await api("/api/embed");
    const models = window["powerbi-client"].models;
    const config = {
      type: "report",
      tokenType: models.TokenType.Aad,
      accessToken: embed.accessToken,
      embedUrl: embed.embedUrl,
      id: embed.reportId,
      settings: {
        panes: { filters: { visible: false } },
        background: models.BackgroundType.Transparent,
      },
    };
    const container = document.getElementById("report");
    window.powerbi.reset(container);
    const report = window.powerbi.embed(container, config);
    report.off("loaded");
    report.on("loaded", () => (status.textContent = "ready"));
    report.off("error");
    report.on("error", () => (status.textContent = "error"));
    // AAD tokens last ~60-75 min; refresh well before expiry.
    setInterval(async () => {
      try {
        const fresh = await api("/api/embed");
        await report.setAccessToken(fresh.accessToken);
      } catch (_) { /* ignore transient refresh errors */ }
    }, 50 * 60 * 1000);
  } catch (err) {
    status.textContent = "failed";
    document.getElementById("report").innerHTML =
      `<div style="padding:16px;color:var(--muted)">Power BI embed failed: ${err.message}</div>`;
  }
}

/* ---------- Chat ---------- */
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

// Minimal inline markdown: **bold** only.
function mdInline(s) {
  return escapeHtml(s).replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
}

function addMessage(text, cls, who) {
  const wrap = document.getElementById("messages");
  const div = document.createElement("div");
  div.className = `msg ${cls}`;
  if (who) {
    const w = document.createElement("div");
    w.className = "who";
    w.textContent = who;
    div.appendChild(w);
  }
  if (cls.includes("bot") && !cls.includes("loading")) {
    const body = document.createElement("span");
    body.innerHTML = mdInline(text);
    div.appendChild(body);
  } else {
    div.appendChild(document.createTextNode(text));
  }
  wrap.appendChild(div);
  wrap.scrollTop = wrap.scrollHeight;
  return div;
}

/* ---------- follow-up suggestion (cross-agent hand-off) ---------- */
function renderFollowUp(followUp) {
  if (!followUp || !followUp.message) return;
  const wrap = document.getElementById("messages");
  const div = document.createElement("div");
  div.className = "followup";
  const hint = document.createElement("span");
  hint.className = "followup-hint";
  hint.textContent = "Suggested next step";
  const chip = document.createElement("button");
  chip.type = "button";
  chip.className = "chip followup-chip";
  chip.textContent = followUp.label || followUp.message;
  chip.title = followUp.message;
  chip.addEventListener("click", () => sendChat(followUp.message));
  div.appendChild(hint);
  div.appendChild(chip);
  wrap.appendChild(div);
  wrap.scrollTop = wrap.scrollHeight;
}

/* ---------- Recommendation / action card ---------- */
async function setProposalStatus(id, status, btnRow) {
  try {
    await api(`/api/proposals/${id}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status }),
    });
    btnRow.innerHTML =
      `<span class="action-status ${status}">${status === "approved" ? "✓ approved" : "✕ dismissed"}</span>`;
  } catch (err) {
    btnRow.innerHTML = `<span class="action-status err">${escapeHtml(err.message)}</span>`;
  }
}

function renderRecommendation(rec) {
  const wrap = document.getElementById("messages");
  const card = document.createElement("div");
  card.className = "rec-card";

  card.innerHTML =
    `<div class="rec-head">
       <span class="rec-title">${escapeHtml(rec.title)}</span>
       <span class="rec-impact">${escapeHtml(rec.impact || "")}</span>
     </div>
     <div class="rec-summary">${escapeHtml(rec.summary || "")}</div>`;

  // Evidence table
  if (rec.evidence && rec.evidence.length && rec.columns) {
    const tbl = document.createElement("table");
    tbl.className = "rec-table";
    const head = rec.columns.map((c) => `<th>${escapeHtml(c[1])}</th>`).join("");
    const body = rec.evidence.map((row) =>
      "<tr>" + rec.columns.map((c) => {
        const v = row[c[0]];
        const cls = v === "UNCOVERED" ? ' class="hot"' : "";
        return `<td${cls}>${escapeHtml(v == null ? "" : v)}</td>`;
      }).join("") + "</tr>").join("");
    tbl.innerHTML = `<thead><tr>${head}</tr></thead><tbody>${body}</tbody>`;
    card.appendChild(tbl);
  }

  // Actions (drafted proposals)
  if (rec.actions && rec.actions.length) {
    const al = document.createElement("div");
    al.className = "rec-actions";
    al.innerHTML = `<div class="rec-actions-label">Drafted actions (approval required)</div>`;
    rec.actions.forEach((p) => {
      const item = document.createElement("div");
      item.className = "action-item";
      item.innerHTML =
        `<div class="action-text">
           <div class="action-title">${escapeHtml(p.title)}</div>
           <div class="action-detail">${escapeHtml(p.detail || "")}${p.impact ? ` · <span class="action-impact">${escapeHtml(p.impact)}</span>` : ""}</div>
         </div>`;
      const btns = document.createElement("div");
      btns.className = "action-btns";
      const approve = document.createElement("button");
      approve.className = "btn-approve";
      approve.textContent = "Approve";
      approve.addEventListener("click", () => setProposalStatus(p.id, "approved", btns));
      const dismiss = document.createElement("button");
      dismiss.className = "btn-dismiss";
      dismiss.textContent = "Dismiss";
      dismiss.addEventListener("click", () => setProposalStatus(p.id, "dismissed", btns));
      btns.appendChild(approve);
      btns.appendChild(dismiss);
      item.appendChild(btns);
      al.appendChild(item);
    });
    card.appendChild(al);
  }

  wrap.appendChild(card);
  wrap.scrollTop = wrap.scrollHeight;
}

const ROUTE_LABEL = {
  "ontology": "answered via ontology",
  "data-agent": "answered via semantic model",
  "inventory-agent": "Inventory Agent",
  "retention-agent": "Retention Agent",
  "merchandising-agent": "Merchandising Agent",
};

/* ---------- transparency trace ("how I reached this") ---------- */
function renderTrace(trace) {
  const wrap = document.getElementById("messages");
  const det = document.createElement("details");
  det.className = "trace";
  const steps = (trace.steps || [])
    .map((s) => `<li>${mdInline(s)}</li>`).join("");
  const tables = (trace.tables && trace.tables.length)
    ? `<div class="trace-tables">Tables: ${trace.tables
        .map((t) => `<code>${escapeHtml(t)}</code>`).join(" ")}</div>`
    : "";
  const fb = trace.fellBack
    ? `<span class="trace-fb">fallback</span>` : "";

  // Reconstructed graph path the ontology query traversed.
  const rels = (trace.relationships && trace.relationships.length)
    ? `<div class="trace-block"><div class="trace-label">Graph path traversed</div>${
        trace.relationships.map((r) =>
          `<div class="trace-rel">${escapeHtml(r)}</div>`).join("")}</div>`
    : (trace.entities && trace.entities.length)
      ? `<div class="trace-block"><div class="trace-label">Entities touched</div>${
          trace.entities.map((e) =>
            `<code>${escapeHtml(e)}</code>`).join(" ")}</div>`
      : "";

  // The actual call we made to the backend service (DAX is hidden by Fabric,
  // but we always show the exact MCP tool + arguments we sent).
  const call = trace.call
    ? `<div class="trace-block"><div class="trace-label">Call made</div>` +
      `<pre class="trace-code">${escapeHtml(
        trace.call.tool || trace.call.endpoint || "")}(${escapeHtml(
        JSON.stringify(trace.call.arguments || {}, null, 0))})</pre>` +
      `<div class="trace-sub">${escapeHtml(trace.call.endpoint || "")}</div></div>`
    : "";

  // Actual queries the app itself generated and ran (agent DAX).
  const queries = (trace.queries && trace.queries.length)
    ? `<div class="trace-block"><div class="trace-label">Queries executed</div>${
        trace.queries.map((q) =>
          `<div class="trace-q"><span class="trace-qlabel">${
            escapeHtml(q.label || "")} · ${escapeHtml(q.lang || "")}</span>` +
          `<pre class="trace-code">${escapeHtml(q.text || "")}</pre></div>`).join("")}</div>`
    : "";

  const preview = trace.rawPreview
    ? `<div class="trace-block"><div class="trace-label">Result preview</div>` +
      `<pre class="trace-code">${escapeHtml(trace.rawPreview)}</pre></div>`
    : "";

  const note = trace.note
    ? `<div class="trace-note">${escapeHtml(trace.note)}</div>` : "";

  det.innerHTML =
    `<summary><span class="trace-src">${escapeHtml(trace.source || "")}</span>${fb}
       <span class="trace-toggle">how I reached this</span></summary>
     <div class="trace-body">
       ${trace.router ? `<div class="trace-why"><strong>Router:</strong> ${escapeHtml(trace.router)}</div>` : ""}
       ${trace.decision ? `<div class="trace-why"><strong>Why this source:</strong> ${escapeHtml(trace.decision)}</div>` : ""}
       <ol class="trace-steps">${steps}</ol>
       ${rels}
       ${call}
       ${queries}
       ${preview}
       ${tables}
       ${note}
     </div>`;
  wrap.appendChild(det);
  wrap.scrollTop = wrap.scrollHeight;
}

/* ---------- router toggle (LLM vs keyword) ---------- */
let ROUTER_MODE = "llm";
function initRouterToggle() {
  const toggle = document.getElementById("router-toggle");
  if (!toggle) return;
  toggle.querySelectorAll(".rt-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      ROUTER_MODE = btn.dataset.router;
      toggle.querySelectorAll(".rt-btn").forEach((b) =>
        b.classList.toggle("active", b === btn));
    });
  });
}

async function sendChat(message) {
  addMessage(message, "user");
  const sendBtn = document.getElementById("send-btn");
  sendBtn.disabled = true;
  const route = document.getElementById("chat-route");
  if (route) route.textContent = "";
  const loading = addMessage("thinking…", "bot loading", "Assistant");
  try {
    const res = await api("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message, surface: "auto", router: ROUTER_MODE }),
    });
    loading.remove();
    addMessage(res.answer, "bot", "Assistant");
    if (res.trace) renderTrace(res.trace);
    if (res.recommendation) renderRecommendation(res.recommendation);
    if (res.followUp) renderFollowUp(res.followUp);
    if (route && res.routedTo) {
      route.textContent = ROUTE_LABEL[res.routedTo] || res.routedTo;
    }
  } catch (err) {
    loading.remove();
    addMessage(err.message, "bot err", "error");
  } finally {
    sendBtn.disabled = false;
  }
}

/* ---------- starter question chips ---------- */
const CHIPS = [
  "Which product families are growing inside declining categories?",
  "Is Momentum Runner growth broad or concentrated?",
  "Which stores carry Momentum Runner and how are they supplied?",
  "Which stores have traffic growth but declining conversion?",
  "What should we do to protect Momentum Runner growth without creating stockouts?",
];

function renderChips() {
  const bar = document.getElementById("chips");
  bar.innerHTML = "";
  CHIPS.forEach((q) => {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "chip";
    chip.textContent = q;
    chip.addEventListener("click", () => sendChat(q));
    bar.appendChild(chip);
  });
}

/* ---------- wiring ---------- */
async function init() {
  document.getElementById("chat-form").addEventListener("submit", (e) => {
    e.preventDefault();
    const input = document.getElementById("chat-input");
    const msg = input.value.trim();
    if (!msg) return;
    input.value = "";
    sendChat(msg);
  });

  try {
    const c = await api("/api/config");
    document.getElementById("meta").textContent =
      `${c.brand || "fictional retail"} · ${c.mode} · semantic model · ontology`;
    if (c.mode === "replay") {
      renderReplayDashboard(await api("/api/demo/dashboard"));
    } else {
      embedReport();
    }
  } catch (err) {
    document.getElementById("report-status").textContent = "configuration error";
  }
  renderChips();
  initRouterToggle();
  addMessage(
    "Welcome to the synthetic Aster & Pine scenario. Start with the signal, " +
    "use the ontology to understand the connected stores and inventory, then " +
    "ask for a supply-aware action plan.", "bot", "Assistant");
}

document.addEventListener("DOMContentLoaded", init);
