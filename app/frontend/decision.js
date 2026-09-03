const api = (path, opts) => fetch(path, opts).then(async (r) => {
  if (!r.ok) {
    const detail = await r.json().catch(() => ({}));
    throw new Error(detail.detail || `${r.status} ${r.statusText}`);
  }
  return r.json();
});

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

function list(items) {
  return `<ul>${items.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>`;
}

function renderDecision(d) {
  const transfer = d.recommendation.transfer[0];
  const override = d.override
    ? `<div class="override-result">
         <strong>Override recorded</strong>
         <span>${escapeHtml(d.override.excludedCandidate)} excluded:
           ${escapeHtml(d.override.reason)}</span>
         <span>Replacement sources: <strong>${escapeHtml(d.override.replacementCandidates.join(", "))}</strong></span>
       </div>`
    : `<form id="override-form" class="override-form">
         <label for="override-reason">Human override</label>
         <span>Exclude Store J and recalculate from the remaining source candidates.</span>
         <input id="override-reason" required
           placeholder="Reason, e.g. local event requires safety stock" />
         <button type="submit">Apply override</button>
       </form>`;
  document.getElementById("decision-canvas").innerHTML =
    `<div class="decision-stage signal">
       <span>1 · Signal</span><strong>${escapeHtml(d.signal.title)}</strong>
       <p>${escapeHtml(d.signal.detail)} · ${escapeHtml(d.signal.period)}</p>
     </div>
     <div class="decision-stage">
       <span>2 · Diagnosis</span><strong>Where growth and risk intersect</strong>
       ${list(d.diagnosis)}
     </div>
     <div class="decision-stage constraint">
       <span>3 · Constraint</span><strong>Respect certified grain</strong>
       <p>${escapeHtml(d.constraint)}</p>
     </div>
     <div class="decision-stage recommendation">
       <span>4 · Recommendation</span><strong>${escapeHtml(d.recommendation.expectedValue)}</strong>
       <div class="decision-actions">
         <div><b>Activate</b>${escapeHtml(d.recommendation.activate.join(", "))}</div>
         <div><b>Replenish</b>${escapeHtml(d.recommendation.replenish.join(", "))}</div>
         <div><b>Transfer</b>${escapeHtml(d.effectiveTransferSources.join(", "))} → ${escapeHtml(transfer.to.join(", "))}
           · ${escapeHtml(transfer.units)} units</div>
       </div>
     </div>
     <div class="decision-stage override">${override}</div>
     <div class="decision-stage package">
       <span>6 · Action package</span><strong>Draft-only governed outputs</strong>
       ${list(d.actionPackage)}
     </div>`;
  const form = document.getElementById("override-form");
  if (form) {
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      const reason = document.getElementById("override-reason").value.trim();
      const updated = await api("/api/demo/decision/override", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({reason}),
      });
      renderDecision(updated);
    });
  }
}

api("/api/demo/decision")
  .then(renderDecision)
  .catch((err) => {
    document.getElementById("decision-canvas").innerHTML =
      `<div class="empty">Could not load the decision: ${escapeHtml(err.message)}</div>`;
  });
