// Renders a markdown document (data-src on #doc) into styled HTML, turns
// ```mermaid fenced blocks into rendered diagrams, and builds a sticky TOC.
function slugify(text) {
  return text.toLowerCase().trim()
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-");
}

function buildToc(root) {
  const toc = document.getElementById("doc-toc");
  if (!toc) return;
  const heads = root.querySelectorAll("h2, h3");
  if (!heads.length) { toc.style.display = "none"; return; }
  const seen = {};
  const list = document.createElement("ul");
  heads.forEach((h) => {
    let id = slugify(h.textContent);
    if (seen[id]) { seen[id]++; id = id + "-" + seen[id]; } else { seen[id] = 1; }
    h.id = id;
    const li = document.createElement("li");
    li.className = "toc-" + h.tagName.toLowerCase();
    const a = document.createElement("a");
    a.href = "#" + id;
    a.textContent = h.textContent;
    li.appendChild(a);
    list.appendChild(li);
  });
  toc.innerHTML = "<div class='toc-title'>On this page</div>";
  toc.appendChild(list);
}

async function renderDoc() {
  const el = document.getElementById("doc");
  const src = el.dataset.src;
  try {
    const res = await fetch(src, { cache: "no-store" });
    if (!res.ok) throw new Error("HTTP " + res.status);
    const md = await res.text();
    el.innerHTML = marked.parse(md);

    // Convert marked's mermaid code blocks into <pre class="mermaid">.
    el.querySelectorAll("code.language-mermaid").forEach((code) => {
      const pre = code.closest("pre");
      const div = document.createElement("pre");
      div.className = "mermaid";
      div.textContent = code.textContent;
      pre.replaceWith(div);
    });

    if (window.mermaid) {
      mermaid.initialize({
        startOnLoad: false,
        securityLevel: "loose",
        theme: "base",
        flowchart: { htmlLabels: true, curve: "basis", nodeSpacing: 55, rankSpacing: 70, padding: 14 },
        themeVariables: {
          fontFamily: "Segoe UI, system-ui, sans-serif",
          fontSize: "15px",
          lineColor: "#8b94a6",
          textColor: "#e7eaf0",
          clusterBkg: "#13161c",
          clusterBorder: "#2a2f3a",
          edgeLabelBackground: "#1b2230",
          tertiaryColor: "#171a21"
        }
      });
      try { await mermaid.run({ querySelector: ".mermaid" }); } catch (e) { /* diagram parse */ }
    }
    buildToc(el);
  } catch (e) {
    el.innerHTML = "<p class='doc-error'>Could not load content (" + e.message + ").</p>";
  }
}

document.addEventListener("DOMContentLoaded", renderDoc);
