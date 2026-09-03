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

const QUESTIONS = [
  "Which product families are growing inside declining categories?",
  "Is Momentum Runner growth broad or concentrated?",
  "Which stores carry Momentum Runner and how are they supplied?",
  "Which stores have traffic growth but declining conversion?",
  "What are total synthetic net sales for the current fiscal period?",
];

let studio;

function renderSignal() {
  const signal = studio.signal;
  const resolved = signal.status === "resolved";
  document.getElementById("signal-card").innerHTML =
    `<div>
       <span class="signal-status ${resolved ? "resolved" : "active"}">${escapeHtml(signal.status)}</span>
       <strong>${escapeHtml(signal.title)}</strong>
       <p>${escapeHtml(signal.detail)}</p>
       <b>${escapeHtml(signal.impact)}</b>
     </div>
     <button id="signal-toggle">${resolved ? "Reopen signal" : "Resolve signal"}</button>`;
  document.getElementById("signal-toggle").addEventListener("click", async () => {
    studio = await api("/api/demo/studio/signal", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({status: resolved ? "active" : "resolved"}),
    });
    renderAll();
  });
}

function renderStages() {
  const completed = new Set(studio.completed || []);
  document.getElementById("iq-stages").innerHTML = studio.stages.map((stage, i) =>
    `<article class="iq-card ${completed.has(stage.id) ? "complete" : ""}">
       <div class="iq-number">0${i + 1}</div>
       <span>${escapeHtml(stage.name)}</span>
       <strong>${escapeHtml(stage.promise)}</strong>
       <p>${escapeHtml(stage.prompt)}</p>
       <button data-stage="${escapeHtml(stage.id)}">
         ${completed.has(stage.id) ? "Run again" : `Invoke ${escapeHtml(stage.name)}`}
       </button>
     </article>`).join("");
  document.querySelectorAll("[data-stage]").forEach((button) => {
    button.addEventListener("click", () => runStage(button.dataset.stage));
  });
}

async function runStage(stageId) {
  const result = await api(`/api/demo/studio/iq/${stageId}`, {method: "POST"});
  studio = result.studio;
  const stage = result.stage;
  document.getElementById("stage-output").innerHTML =
    `<span class="output-eyebrow">${escapeHtml(stage.name)}</span>
     <h3>${escapeHtml(stage.answer)}</h3>
     <div class="source-list">${stage.sources.map((s) =>
       `<span>${escapeHtml(s)}</span>`).join("")}</div>
     <p><strong>Next:</strong> ${escapeHtml(stage.next)}</p>`;
  renderAll();
}

function renderQuestions() {
  document.getElementById("question-list").innerHTML = QUESTIONS.map((q, i) =>
    `<button data-question="${i}">${escapeHtml(q)}</button>`).join("");
  document.querySelectorAll("[data-question]").forEach((button) => {
    button.addEventListener("click", async () => {
      const question = QUESTIONS[Number(button.dataset.question)];
      const box = document.getElementById("question-answer");
      box.textContent = "Asking the synthetic Fabric Data Agent…";
      const result = await api("/api/chat", {
        method: "POST", headers: {"Content-Type": "application/json"},
        body: JSON.stringify({message: question, surface: "auto", router: "llm"}),
      });
      box.innerHTML =
        `<span>${escapeHtml(result.routedTo)}</span>
         <strong>${escapeHtml(question)}</strong>
         <p>${escapeHtml(result.answer)}</p>`;
    });
  });
}

async function triggerAgents() {
  const result = await api("/api/chat", {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({
      message: "What should we do to protect Momentum Runner growth without creating stockouts?",
      surface: "auto", router: "llm",
    }),
  });
  const output = document.getElementById("agent-output");
  output.innerHTML =
    `<span>Action agents triggered</span>
     <strong>${escapeHtml(result.recommendation.title)}</strong>
     <p>${escapeHtml(result.recommendation.summary)}</p>
     <div class="source-list">${result.recommendation.actions.map((a) =>
       `<span>${escapeHtml(a.title)}</span>`).join("")}</div>`;
}

async function setDecision(status) {
  studio = await api("/api/demo/studio/decision", {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({status}),
  });
  renderAll();
}

function renderAll() {
  renderSignal();
  renderStages();
  document.getElementById("decision-state").textContent = studio.decisionStatus;
}

async function init() {
  studio = await api("/api/demo/studio");
  renderAll();
  renderQuestions();
  document.getElementById("trigger-agents").addEventListener("click", triggerAgents);
  document.getElementById("approve-decision").addEventListener(
    "click", () => setDecision("approved"));
  document.getElementById("dismiss-decision").addEventListener(
    "click", () => setDecision("dismissed"));
}

init().catch((err) => {
  document.getElementById("stage-output").textContent = err.message;
});
