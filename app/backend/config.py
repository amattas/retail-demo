"""Runtime configuration for the Ontology Explorer app.

Deployment-specific values are supplied through environment variables so the
repository does not contain tenant or customer environment metadata.
"""

from __future__ import annotations

import os
from pathlib import Path

# Demo mode. "auto" uses replay whenever the minimum live Fabric configuration
# is absent, so a fresh clone starts as a complete synthetic experience.
DEMO_MODE = os.getenv("RETAIL_DEMO_MODE", "auto").strip().lower()

# Fabric workspace that hosts the semantic model, report, data agent and ontology.
WORKSPACE_ID = os.getenv("RETAIL_WORKSPACE_ID", "")

# The user-created Fabric Data Agent (sits on top of the semantic model).
DATA_AGENT_ID = os.getenv("RETAIL_DATA_AGENT_ID", "")

# The Fabric ontology item.
ONTOLOGY_ITEM_ID = os.getenv("RETAIL_ONTOLOGY_ITEM_ID", "")

LIVE_CONFIGURED = bool(WORKSPACE_ID and DATA_AGENT_ID and ONTOLOGY_ITEM_ID)
REPLAY_MODE = DEMO_MODE == "replay" or (
    DEMO_MODE == "auto" and not LIVE_CONFIGURED
)

# Minimal "lite" ontology (Store + receipt_created + stockout_detected). It is
# bounded to live event tables so it answers real-time, store-level sales-velocity
# questions reliably where the full ontology 500s on unbounded Eventhouse scans.
ONTOLOGY_LITE_ITEM_ID = os.getenv("RETAIL_ONTOLOGY_LITE_ITEM_ID", "")

# Power BI report to embed. If left blank the backend resolves it by name.
REPORT_ID = os.getenv("RETAIL_REPORT_ID", "")
REPORT_NAME = os.getenv("RETAIL_REPORT_NAME", "retail_model")

# Semantic model / dataset behind the report. Used by the action agents to read
# real rows via the Power BI ``executeQueries`` (DAX) API. If blank it is
# resolved from the report at runtime.
DATASET_ID = os.getenv("RETAIL_DATASET_ID", "")

# Eventhouse (KQL) that backs the real-time layer. Approved/drafted agent actions
# are logged here (table ``agent_actions``) so they appear on a streaming
# dashboard — closing the loop from recommendation to action inside Fabric.
EVENTHOUSE_CLUSTER = os.getenv(
    "RETAIL_EVENTHOUSE_CLUSTER",
    "",
)
EVENTHOUSE_DB = os.getenv("RETAIL_EVENTHOUSE_DB", "retail_eventhouse")
EVENTHOUSE_TABLE = os.getenv("RETAIL_EVENTHOUSE_TABLE", "agent_actions")
# Set RETAIL_EVENTHOUSE_LOG=0 to disable Eventhouse write-back (e.g. offline demo).
EVENTHOUSE_LOG = bool(EVENTHOUSE_CLUSTER) and os.getenv("RETAIL_EVENTHOUSE_LOG", "1") != "0"

# Azure resource scopes.
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
COGNITIVE_SCOPE = "https://cognitiveservices.azure.com/.default"

# Azure OpenAI — powers the LLM intent router that chooses between the semantic
# model (Data Agent), the ontology graph, or an operations agent. Auth is keyless
# (AAD via az login) by default; set RETAIL_AOAI_KEY to use an API key instead.
# Set RETAIL_LLM_ROUTER=0 to force the deterministic keyword router.
AOAI_ENDPOINT = os.getenv("RETAIL_AOAI_ENDPOINT", "").rstrip("/")
AOAI_DEPLOYMENT = os.getenv("RETAIL_AOAI_DEPLOYMENT", "gpt-4o-mini")
AOAI_API_VERSION = os.getenv("RETAIL_AOAI_API_VERSION", "2024-08-01-preview")
AOAI_KEY = os.getenv("RETAIL_AOAI_KEY", "")
LLM_ROUTER = os.getenv("RETAIL_LLM_ROUTER", "1") != "0"

# Which LLM provider powers the intent router. "azure-openai" (default) uses the
# gpt-4o-mini deployment above. "anthropic" uses Claude via the Anthropic API —
# set RETAIL_ANTHROPIC_API_KEY (and optionally RETAIL_ANTHROPIC_MODEL). NOTE: the
# router is the ONLY model we control; the Ontology MCP and Fabric Data Agent do
# their own NL→query translation with Fabric's internal model, which cannot be
# swapped to Claude.
LLM_PROVIDER = os.getenv("RETAIL_LLM_PROVIDER", "azure-openai").strip().lower()
ANTHROPIC_API_KEY = os.getenv("RETAIL_ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("RETAIL_ANTHROPIC_MODEL", "claude-sonnet-4-5")
ANTHROPIC_VERSION = os.getenv("RETAIL_ANTHROPIC_VERSION", "2023-06-01")
ANTHROPIC_ENDPOINT = os.getenv(
    "RETAIL_ANTHROPIC_ENDPOINT", "https://api.anthropic.com/v1/messages"
)

# Ontology MCP call budget (seconds). The Fabric ontology translator answers
# single-entity / 1-hop enrichment lookups in ~20-30s but internally 500s on
# deep multi-hop traversals after ~100s. We give valid 1-hop traversals room to
# finish, then fall the orchestrator back to the Data Agent.
ONTOLOGY_TIMEOUT = int(os.getenv("RETAIL_ONTOLOGY_TIMEOUT", "115"))

# The ontology NL→graph translator is nondeterministic: the *same* question
# succeeds ~2/3 of the time and otherwise emits a transient graph-query syntax
# error (BadRequest / "Variable ... not defined"). A simple re-ask almost always
# clears it, so we retry a couple of times before falling back to the Data Agent.
# Total attempts = 1 + RETAIL_ONTOLOGY_RETRIES.
ONTOLOGY_RETRIES = int(os.getenv("RETAIL_ONTOLOGY_RETRIES", "2"))

# Base hosts.
FABRIC_HOST = "https://api.fabric.microsoft.com"
POWERBI_HOST = "https://api.powerbi.com"

# Repo paths (used to derive ontology relationship edges from the source notebook).
REPO_ROOT = Path(__file__).resolve().parents[2]
ONTOLOGY_NOTEBOOK = (
    REPO_ROOT / "fabric" / "lakehouse" / "30-create-ontology.ipynb"
)

# MCP endpoint URLs.
DATA_AGENT_MCP_URL = (
    f"{FABRIC_HOST}/v1/mcp/workspaces/{WORKSPACE_ID}/dataagents/{DATA_AGENT_ID}/agent"
)
ONTOLOGY_MCP_URL = (
    f"{FABRIC_HOST}/v1/mcp/dataPlane/workspaces/{WORKSPACE_ID}"
    f"/items/{ONTOLOGY_ITEM_ID}/ontologyEndpoint"
)
ONTOLOGY_LITE_MCP_URL = (
    f"{FABRIC_HOST}/v1/mcp/dataPlane/workspaces/{WORKSPACE_ID}"
    f"/items/{ONTOLOGY_LITE_ITEM_ID}/ontologyEndpoint"
)
