const $ = (selector) => document.querySelector(selector);

const beats = [
  {
    title: "Morning signal",
    coach: [
      "Dana starts with one decision-worthy signal, not a wall of dashboards.",
      "Open the signal",
      "The performance waterfall explains why the regional decline hides a winner.",
    ],
  },
  {
    title: "Test concentration",
    coach: [
      "Growth is broad enough to matter, but coverage determines where to act.",
      "Test territory-wide activation",
      "The evidence contradicts a blanket promotion and reveals a reserved-inventory guardrail.",
    ],
  },
  {
    title: "Shape the decision",
    coach: [
      "The recommendation is selective: hold, replenish, activate, and transfer.",
      "Review recommendation",
      "Dana can override Store J and the transfer plan recalculates from Stores H and I.",
    ],
  },
  {
    title: "Build the package",
    coach: [
      "The decision becomes operational artifacts, not another chat answer.",
      "Build the package",
      "A transfer list, activation brief, and governed decision log are created.",
    ],
  },
  {
    title: "Send for review",
    coach: [
      "The story closes with accountable review and a resolved Decision Canvas.",
      "Send for review",
      "Owners receive the package and the morning signal is resolved.",
    ],
  },
];

let state;
let beat = 0;
let signalOpened = false;
let contradictionShown = false;
let recommendationShown = false;
let decision;

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}

function post(path, body) {
  return request(path, { method: "POST", body: JSON.stringify(body) });
}

function renderScope() {
  const scope = state.scope;
  $("#scope-bar").innerHTML = `
    <span><strong>Territory</strong> ${scope.territory}</span>
    <span><strong>Sales</strong> ${scope.salesPeriod}</span>
    <span><strong>Comparison</strong> ${scope.comparisonPeriod}</span>
    <span><strong>Inventory</strong> ${scope.inventorySnapshot}</span>
    <span class="certified-pill">${scope.mode}</span>
  `;
}

function renderBeatNav() {
  $("#beat-nav").innerHTML = beats.map((item, index) => `
    <li class="${index === beat ? "active" : ""} ${index < beat ? "complete" : ""}">
      <button ${index > beat ? "disabled" : ""} data-beat="${index}">
        <span>${index < beat ? "✓" : index + 1}</span>${item.title}
      </button>
    </li>
  `).join("");
  document.querySelectorAll("[data-beat]").forEach((button) => {
    button.addEventListener("click", () => {
      beat = Number(button.dataset.beat);
      render();
    });
  });
}

function waterfall() {
  return `<div class="waterfall">
    ${state.waterfall.map((item) => {
      const width = Math.max(8, Math.min(100, Math.abs(item.value) * 2.1));
      const positive = item.value >= 0;
      return `<div class="waterfall-row">
        <span>${item.label}</span>
        <div class="waterfall-track">
          <i class="${positive ? "positive" : "negative"}" style="width:${width}%"></i>
        </div>
        <strong>${positive ? "+" : ""}${item.value}%</strong>
      </div>`;
    }).join("")}
  </div>`;
}

function beatOne() {
  if (!signalOpened) {
    return `<button class="signal-card" id="open-signal">
      <span class="signal-badge">Morning signal</span>
      <strong>${state.signal.title}</strong>
      <span>${state.signal.summary}</span>
      <small>Click to explain the decline →</small>
    </button>`;
  }
  return `
    <div class="section-heading">
      <div><p class="eyebrow">What changed</p><h2>Performance waterfall</h2></div>
      <span class="status-pill warning">Needs a decision</span>
    </div>
    ${waterfall()}
    <div class="insight-callout">
      <strong>The category decline is real, but it is not uniform.</strong>
      Momentum Runner is growing 38% while outlet performance pulls the region down.
      The next question is whether that growth is concentrated enough to ignore.
    </div>`;
}

function matrix() {
  return `<div class="store-matrix">
    ${state.stores.map((store) => `
      <article class="store-tile ${store.quadrant.includes("low coverage") ? "at-risk" : store.quadrant.includes("Weak") ? "source" : "activate"}">
        <header><strong>${store.store}</strong><span>${store.market}</span></header>
        <div><b>${store.velocity}</b><small>weekly velocity</small></div>
        <div><b>${store.weeks_cover}</b><small>weeks cover</small></div>
        <footer>${store.recommendation}</footer>
      </article>
    `).join("")}
  </div>`;
}

function beatTwo() {
  return `
    <div class="section-heading">
      <div><p class="eyebrow">Concentration test</p><h2>Velocity × coverage across ten stores</h2></div>
      <span class="status-pill success">${state.concentration.storesUp} of ${state.concentration.storesTotal} stores growing</span>
    </div>
    <p class="lead">${state.concentration.storesAbove50} stores are growing above 50%. The signal is broad, but the inventory position is not.</p>
    ${matrix()}
    ${contradictionShown ? `
      <div class="contradiction">
        <p class="eyebrow">Contradiction found</p>
        <h3>Do not activate the whole territory.</h3>
        <p>Stores A-C have strong velocity but less than 1.5 weeks of cover. Promotion would accelerate avoidable stockouts.</p>
        <div class="guardrail"><strong>Grain guardrail:</strong> reserved inventory is validated at location × style × size × snapshot date before it can be moved.</div>
      </div>` : ""}`;
}

function beatThree() {
  const overridden = Boolean(decision?.override);
  const sources = decision?.effectiveTransferSources || ["Store H", "Store I", "Store J"];
  return `
    <div class="section-heading">
      <div><p class="eyebrow">Selective recommendation</p><h2>Protect demand before promoting it</h2></div>
      <span class="status-pill ${overridden ? "success" : "warning"}">${overridden ? "Override applied" : "Dana review required"}</span>
    </div>
    <div class="recommendation-grid">
      <article><span>Hold + replenish</span><strong>Stores A-C</strong><p>Protect core sizes and the next receipt window.</p></article>
      <article><span>Activate</span><strong>Stores D-G</strong><p>Strong velocity with enough coverage to support a local push.</p></article>
      <article><span>Transfer</span><strong>${sources.join(", ")}</strong><p>Move compatible style-size inventory into Stores A-C.</p></article>
    </div>
    ${overridden ? `
      <div class="override-result"><strong>Human context changed the plan.</strong> Store J is excluded for a local community event. Transfer quantities now recalculate from Stores H and I.</div>
    ` : `
      <button class="inline-action" id="open-override">Dana knows something the data does not: exclude Store J →</button>
    `}`;
}

function packageArtifact(title, description, rows) {
  return `<article class="artifact-card">
    <span class="artifact-icon">✓</span><div><h3>${title}</h3><p>${description}</p>${rows}</div>
  </article>`;
}

function packageContent() {
  if (state.packageStatus === "not built") {
    return `<div class="empty-package"><span>3</span><h2>Decision artifacts are ready to build</h2><p>The evidence, guardrails, recommendation, and Dana's override will travel together.</p></div>`;
  }
  return `<div class="package-stack">
    ${packageArtifact("Transfer review list", "420 compatible units from Stores H and I into Stores A-C.", `<small>Grain: location × style × size · Snapshot: ${state.scope.inventorySnapshot}</small>`)}
    ${packageArtifact("Activation brief", "Activate Stores D-G. Explicitly exclude A-C for low coverage and J for local context.", "<small>Owner: Regional activation lead</small>")}
    ${packageArtifact("Decision log", "Captures sources, periods, grains, rationale, override, owners, and review date.", `<small>${state.scope.salesPeriod} · Review due in 2 business days</small>`)}
  </div>`;
}

function beatFour() {
  return `<div class="section-heading"><div><p class="eyebrow">Operational handoff</p><h2>Build a reviewable action package</h2></div><span class="status-pill ${state.packageStatus === "built" ? "success" : "neutral"}">${state.packageStatus}</span></div>${packageContent()}`;
}

function beatFive() {
  const sent = state.reviewStatus === "sent for review";
  return `<div class="section-heading"><div><p class="eyebrow">Decision Canvas</p><h2>${sent ? "Decision sent and signal resolved" : "Ready for accountable review"}</h2></div><span class="status-pill ${sent ? "success" : "warning"}">${sent ? "Complete" : "Ready"}</span></div>
    <div class="decision-summary ${sent ? "complete" : ""}">
      <div><span>Decision</span><strong>Selective Momentum Runner activation and inventory protection</strong></div>
      <div><span>Human override</span><strong>Store J excluded; sourcing recalculated from H and I</strong></div>
      <div><span>Package</span><strong>Transfer list · Activation brief · Decision log</strong></div>
      <div><span>Signal</span><strong>${sent ? "Resolved — monitoring continues" : "Active — awaiting review"}</strong></div>
      ${sent ? `<p class="completion-message">Dana turned a hidden winner into a governed plan without exposing real customer data or allowing an agent to act autonomously.</p>` : ""}
    </div>`;
}

function updateCoach() {
  const item = beats[beat];
  $("#coach-title").textContent = item.title;
  [$("#coach-say"), $("#coach-click"), $("#coach-result")].forEach((node, index) => {
    node.textContent = item.coach[index];
  });
}

function configureActions() {
  const primary = $("#primary-action");
  const secondary = $("#secondary-action");
  secondary.classList.add("hidden");
  primary.disabled = false;

  const configs = [
    [signalOpened ? "Continue to concentration" : "Open the signal", signalOpened ? nextBeat : openSignal],
    [contradictionShown ? "Review recommendation" : "Test territory-wide activation", contradictionShown ? nextBeat : showContradiction],
    [decision?.override ? "Continue to package" : "Apply Dana's override", decision?.override ? nextBeat : openOverride],
    [state.packageStatus === "built" ? "Continue to review" : "Build the package", state.packageStatus === "built" ? nextBeat : buildPackage],
    [state.reviewStatus === "sent for review" ? "Review completed decision" : "Send for review", state.reviewStatus === "sent for review" ? () => {} : sendForReview],
  ];
  primary.textContent = configs[beat][0];
  primary.onclick = configs[beat][1];
  if (beat === 4 && state.reviewStatus === "sent for review") primary.disabled = true;
  $("#action-hint").textContent = beat === 2 && !decision?.override
    ? "The override reason is required and remains in the decision log."
    : "Each action advances the same five-beat story.";
}

function render() {
  renderScope();
  renderBeatNav();
  $("#progress-label").textContent = `Beat ${beat + 1} of 5`;
  $("#progress-fill").style.width = `${(beat + 1) * 20}%`;
  $("#beat-content").innerHTML = [beatOne, beatTwo, beatThree, beatFour, beatFive][beat]();
  $("#evidence-content").innerHTML = `
    <p><strong>Evidence:</strong> governed semantic metrics, ontology relationships, aligned inventory snapshot, and deterministic action rules.</p>
    <p><strong>Fabric IQ:</strong> performance and inventory grounding.</p>
    <p><strong>Foundry IQ:</strong> recommendation orchestration and policy checks.</p>
    <p><strong>Work IQ:</strong> fictional owner and review context.</p>
    <p><strong>Web IQ:</strong> optional public-market context; not required for the decision.</p>`;
  updateCoach();
  configureActions();
  $("#open-signal")?.addEventListener("click", openSignal);
  $("#open-override")?.addEventListener("click", openOverride);
}

function openSignal() {
  signalOpened = true;
  render();
}

function showContradiction() {
  contradictionShown = true;
  render();
}

function openOverride() {
  $("#override-dialog").showModal();
}

function nextBeat() {
  beat = Math.min(4, beat + 1);
  render();
}

async function buildPackage() {
  state = await post("/api/demo/studio/package", { status: "built" });
  render();
}

async function sendForReview() {
  state = await post("/api/demo/studio/package", { status: "sent for review" });
  state = await post("/api/demo/studio/signal", { status: "resolved" });
  render();
}

$("#override-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  decision = await post("/api/demo/decision/override", {
    reason: $("#override-reason").value,
  });
  $("#override-dialog").close();
  render();
});
$("#cancel-override").addEventListener("click", () => $("#override-dialog").close());

async function init() {
  [state, decision] = await Promise.all([
    request("/api/demo/studio"),
    request("/api/demo/decision"),
  ]);
  render();
}

init().catch((error) => {
  $("#beat-content").innerHTML = `<p class="error">${error.message}</p>`;
});
