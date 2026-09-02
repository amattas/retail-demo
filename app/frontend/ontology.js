/* Ontology Explorer page — graph + entity detail */

const api = (path, opts) => fetch(path, opts).then(async (r) => {
  if (!r.ok) {
    const detail = await r.json().catch(() => ({}));
    throw new Error(detail.detail || `${r.status} ${r.statusText}`);
  }
  return r.json();
});

const NODE_COLORS = { entity: "#7c5cff" };

async function renderGraph() {
  let graph;
  try {
    graph = await api("/api/ontology/graph");
  } catch (err) {
    document.getElementById("cy").innerHTML =
      `<div style="padding:16px;color:var(--muted)">Ontology graph failed: ${err.message}</div>`;
    return;
  }
  const elements = [];
  graph.nodes.forEach((n) =>
    elements.push({ data: { id: n.id, label: n.label,
      props: n.property_count, ts: n.timeseries_count } })
  );
  graph.edges.forEach((e, i) =>
    elements.push({ data: { id: `e${i}`, source: e.source, target: e.target,
      label: e.label, kind: e.kind } })
  );

  const cy = cytoscape({
    container: document.getElementById("cy"),
    elements,
    style: [
      { selector: "node", style: {
        "background-color": NODE_COLORS.entity,
        "label": "data(label)", "color": "#e7eaf0", "font-size": 11,
        "text-valign": "center", "text-halign": "center",
        "text-outline-width": 2, "text-outline-color": "#171a21",
        "width": 36, "height": 36, "border-width": 2, "border-color": "#1f232c",
      }},
      { selector: "node:selected", style: {
        "border-color": "#4ea1ff", "border-width": 3,
        "background-color": "#4ea1ff",
      }},
      { selector: "edge", style: {
        "width": 1.5, "line-color": "#3a4356",
        "target-arrow-color": "#3a4356", "target-arrow-shape": "triangle",
        "curve-style": "bezier", "label": "data(label)",
        "font-size": 8, "color": "#9aa3b2",
        "text-rotation": "autorotate", "text-background-color": "#171a21",
        "text-background-opacity": 0.85, "text-background-padding": 2,
      }},
      { selector: 'edge[kind = "eventhouse"]', style: {
        "line-color": "#ffb24e", "target-arrow-color": "#ffb24e",
        "line-style": "dashed",
      }},
    ],
    layout: { name: "cose", animate: true, padding: 30, nodeRepulsion: 9000,
      idealEdgeLength: 110, nodeOverlap: 20 },
  });

  cy.on("tap", "node", (evt) => showEntity(evt.target.id()));
}

async function showEntity(name) {
  const box = document.getElementById("entity-detail");
  box.innerHTML = `<p class="hint">Loading ${name}…</p>`;
  try {
    const e = await api(`/api/ontology/entity?name=${encodeURIComponent(name)}`);
    const props = (e.properties || []).map((p) =>
      `<div class="prop"><span>${p.name}</span><span class="pt">${p.valueType}</span></div>`
    ).join("");
    const ts = (e.timeseriesProperties || []).slice(0, 40).map((p) =>
      `<div class="prop"><span>${p.name}</span><span class="pt">${p.valueType}</span></div>`
    ).join("");
    box.innerHTML = `
      <h3>${e.name || name}</h3>
      <div class="tag">${e.namespace || ""} · ${(e.properties||[]).length} properties ·
        ${(e.timeseriesProperties||[]).length} telemetry signals</div>
      ${props ? `<div class="prop-group"><h4>Properties</h4>${props}</div>` : ""}
      ${ts ? `<div class="prop-group"><h4>Telemetry (first 40)</h4>${ts}</div>` : ""}`;
  } catch (err) {
    box.innerHTML = `<p class="hint">Could not load ${name}: ${err.message}</p>`;
  }
}

function init() {
  api("/api/config").then((c) => {
    document.getElementById("meta").textContent =
      `workspace ${c.workspaceId.slice(0, 8)}… · data agent · ontology`;
  }).catch(() => {});
  renderGraph();
}

document.addEventListener("DOMContentLoaded", init);
