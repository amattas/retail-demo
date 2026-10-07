"""Publication and evidence boundaries for living documentation."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
DESIGN = DOCS / "design"


def _nav_paths(value: object) -> set[str]:
    if isinstance(value, str):
        return {value}
    if isinstance(value, list):
        return set().union(*(_nav_paths(item) for item in value))
    if isinstance(value, dict):
        return set().union(*(_nav_paths(item) for item in value.values()))
    return set()


def test_public_source_contains_living_owners_not_raw_drafts() -> None:
    config = tomllib.loads((ROOT / "zensical.toml").read_text(encoding="utf-8"))
    project = config["project"]
    assert project["docs_dir"] == "docs"
    assert "exclude_docs" not in project
    assert "draft_docs" not in project
    assert {path.name for path in DOCS.iterdir() if path.is_file()} == {
        "index.md",
        "agentic-action-framework.md",
    }
    assert not (DOCS / "superpowers").exists()
    assert not list(DOCS.rglob("*.html"))
    assert not list(DOCS.rglob("*.excalidraw"))
    pointer = (DOCS / "agentic-action-framework.md").read_text(encoding="utf-8")
    assert "decision-experience.md" in pointer
    assert len(pointer.splitlines()) < 20
    paths = _nav_paths(project["nav"])
    for path in paths:
        assert (DOCS / path).is_file(), path
    assert "design/specifications/modules/ml-ai/decision-experience.md" in paths
    assert "design/specifications/modules/ml-ai/routing.md" in paths
    assert "guides/sample-retail-questions.md" in paths


def test_documentation_relative_file_links_resolve() -> None:
    pages = [
        *DOCS.rglob("*.md"),
        ROOT / "README.md",
        ROOT / "utility" / "README.md",
        ROOT / "deploy" / "README.md",
        ROOT / "STATUS.md",
    ]
    failures = []
    for page in pages:
        text = page.read_text(encoding="utf-8")
        text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
        for link in re.findall(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+[^)]*)?\)", text):
            parsed = urlsplit(link)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            path = unquote(parsed.path)
            target = (
                ROOT / path.lstrip("/")
                if path.startswith("/")
                else page.parent / path
            )
            if not target.exists():
                failures.append(f"{page.relative_to(ROOT)} -> {link}")
    assert not failures, "\n".join(failures)


def test_traceability_has_unique_rows_for_owned_requirements_and_threats() -> None:
    requirements = set()
    for page in (DESIGN / "requirements").rglob("*.md"):
        if page.name in {"traceability.md", "backlog.md", "README.md"}:
            continue
        requirements.update(
            re.findall(
                r"^### (REQ-[A-Z]+-\d+)\b",
                page.read_text(encoding="utf-8"),
                re.MULTILINE,
            )
        )
    threat_text = (DESIGN / "security" / "threat-model.md").read_text(
        encoding="utf-8"
    )
    threats = set(re.findall(r"^\| `(THREAT-\d+)`", threat_text, re.MULTILINE))
    ledger = (DESIGN / "requirements" / "traceability.md").read_text(
        encoding="utf-8"
    )
    rows = re.findall(
        r"^\| `((?:REQ-[A-Z]+|THREAT)-\d+)` \| [^|]+ \| `([^`]+)`",
        ledger,
        re.MULTILINE,
    )
    ids = [identifier for identifier, _ in rows]
    assert len(ids) == len(set(ids))
    assert set(ids) == requirements | threats
    states = {"proposed", "accepted", "implemented", "verified", "blocked", "retired"}
    assert all(state in states for _, state in rows)
    controls = (DESIGN / "security" / "controls.md").read_text(encoding="utf-8")
    owned_controls = set(re.findall(r"^\| `(SEC-\d+)`", controls, re.MULTILINE))
    assert set(re.findall(r"`(SEC-\d+)`", threat_text)) <= owned_controls
    assert "THREAT-009" in threats
    assert dict(rows)["REQ-MLAI-005"] == "proposed"
    for page in (DESIGN / "requirements" / "modules").glob("*/backlog.md"):
        text = page.read_text(encoding="utf-8")
        assert "## Open" in text, page
        assert "## Settled — do not reopen" in text, page


def test_entry_points_do_not_claim_old_counts_or_current_live_completion() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "42-table Direct Lake semantic model" in readme
    assert "six required ML" in readme
    framework = (
        DESIGN / "specifications" / "modules" / "deployment" / "framework.md"
    ).read_text(encoding="utf-8")
    assert "`5+0`, `30+2`, and `40+2`" in framework
    status = (ROOT / "STATUS.md").read_text(encoding="utf-8")
    assert "7/7 complete" not in status
    assert "No fresh live deployments were performed" in status
