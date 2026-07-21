# %% [markdown]
# # Stream clickstream events
# Part of the retail-demo setup utility. Runs the deterministic synthetic
# **clickstream** generator inside Fabric and pushes events to the
# `clickstream_eventstream` **custom endpoint** using its Event Hub-compatible
# connection string — exactly the way an external application integrates with
# Fabric. The Eventstream lands the events in the `clickstream_eventhouse` KQL
# database (`clickstream_events` table). No Spark Kusto connector, no direct
# Eventhouse write: the Eventstream stays in the architecture.
#
# Each event is shaped as::
#
#     { "event_id", "customer_id", "event_timestamp",
#       "event_type": "page_view | product_view | cart_add | search",
#       "detail": { "page_url", "product_id", "search_terms" } }
#
# `customer_id` is drawn from `1..customer_count` to match `dim_customers.ID`
# (contiguous), and `product_id` from `1..product_count` to match
# `dim_products.ID`. The counts are read from the Silver dims when available so
# events carry valid foreign keys.
#
# This is the optional **live driver**, not part of the ordered batch setup. It
# paces itself to a target rate (default 10,000,000 events/day ≈ 116/sec) and
# stops after `duration_seconds` or `max_events`, whichever comes first. Set both
# to 0 to run until you interrupt the cell.
#
# The notebook is self-contained (no engine cell); it inlines the
# `retail_setup.clickstream` generator so it needs no package install beyond
# `azure-eventhub`. The custom-endpoint connection string is **auto-resolved**
# from the Fabric REST API using the notebook's own identity — no secret to
# paste. Set `connection_string` only to override (e.g. target another stream).

# %%
# ruff: noqa: F821, E402  (Fabric-injected globals; imports live in notebook cells)
# azure-eventhub is not always preinstalled on Fabric; ensure it (driver-only)
# before sending. This is the same client an external application would use
# against the Eventstream. Idempotent: installs only when missing.
import importlib.util
import subprocess
import sys

if importlib.util.find_spec("azure.eventhub") is None:
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet", "azure-eventhub"]
    )

# %% [parameters]
# Fabric parameters — override per run via the pipeline/parameterization.
#
# connection_string is auto-resolved from the Fabric REST API (see the resolver
# cell) when left blank — the notebook uses its own identity, so no secret is
# stored. Set it explicitly only to override the target (it must be an Event
# Hub-compatible connection string with an embedded EntityPath).
connection_string = ""         # blank = auto-resolve via Fabric API; else override
eventstream_name = "clickstream_eventstream"  # Eventstream to publish into
source_name = ""               # custom-endpoint source name; blank = first CustomEndpoint
eventhub_name = ""             # only needed if an override conn string omits EntityPath

rate = 116.0                   # events/second (10M/day ~= 116). 0 -> use daily_target
daily_target = 10_000_000      # target events/day when rate == 0
duration_seconds = 600         # 0 = run until interrupted; >0 = stop after N seconds
max_events = 0                 # 0 = unlimited; >0 = stop after N events

seed = 42                      # deterministic RNG seed
customer_count = 50_000        # fallback; overridden by dim_customers count when readable
product_count = 5_000          # fallback; overridden by dim_products count when readable
batch_size = 500               # events per send batch
partition_by_customer = False  # route each customer to a fixed partition (preserves order)

# %%
# Resolve dimension ID ranges from the Silver dims (written by the setup
# notebooks) so emitted customer_id / product_id are valid foreign keys. Falls
# back to the parameter values above if the dims are not present.
def _param(value: str, default: str) -> str:
    return default if len(value) > 1 and value[0] == value[1] == "{" else value


LAKEHOUSE_NAME = _param("{{LAKEHOUSE_NAME}}", "retail_lakehouse")
SILVER_DB = _param("{{SILVER_DB}}", "ag")


def _count(table: str, default: int) -> int:
    try:
        return spark.table(f"{LAKEHOUSE_NAME}.{SILVER_DB}.{table}").count()  # noqa: F821
    except Exception as exc:  # noqa: BLE001 - dims optional; default on any read error
        print(f"  {table} not found ({exc}); using parameter default {default}")
        return default


customer_count = _count("dim_customers", customer_count)
product_count = _count("dim_products", product_count)
print(f"ranges: customers={customer_count} products={product_count}")

# %%
# Auto-resolve the Eventstream custom-endpoint connection string via the Fabric
# REST API, using the notebook's own identity — no secret is stored in the
# notebook. This is exactly how an external application would fetch its endpoint
# credential. Skipped when `connection_string` is set as an override.
import json as _json
import urllib.request as _urlreq

_FABRIC_API = "https://api.fabric.microsoft.com/v1"


def _fabric_token() -> str:
    # "pbi" audience issues a token accepted by the Fabric REST API.
    import notebookutils  # Fabric runtime  # noqa: F821

    return notebookutils.credentials.getToken("pbi")


def _current_workspace_id() -> str:
    import notebookutils  # noqa: F821

    ctx = getattr(notebookutils.runtime, "context", {}) or {}
    for key in ("currentWorkspaceId", "workspaceId"):
        if ctx.get(key):
            return str(ctx[key])
    import mssparkutils  # legacy alias fallback  # noqa: F821

    return str(mssparkutils.env.getWorkspaceId())


def _fabric_get(path: str, token: str) -> dict:
    req = _urlreq.Request(  # noqa: S310 - fixed trusted Fabric API host
        f"{_FABRIC_API}/{path}", headers={"Authorization": f"Bearer {token}"}
    )
    with _urlreq.urlopen(req) as resp:  # noqa: S310
        return _json.loads(resp.read().decode("utf-8"))


def resolve_connection_string(eventstream_name: str, source_name: str = "") -> str:
    """Return the Event Hub-compatible connection string for the Eventstream's
    custom-endpoint source (with EntityPath embedded), resolved via Fabric REST."""
    token = _fabric_token()
    workspace_id = _current_workspace_id()

    streams = _fabric_get(f"workspaces/{workspace_id}/eventstreams", token).get("value", [])
    match = next((s for s in streams if s.get("displayName") == eventstream_name), None)
    if match is None:
        available = [s.get("displayName") for s in streams]
        raise RuntimeError(
            f"Eventstream {eventstream_name!r} not found in workspace {workspace_id}. "
            f"Available: {available}"
        )
    es_id = match["id"]

    topology = _fabric_get(
        f"workspaces/{workspace_id}/eventstreams/{es_id}/topology", token
    )
    sources = topology.get("sources", [])
    if source_name:
        src = next((s for s in sources if s.get("name") == source_name), None)
    else:
        src = next((s for s in sources if s.get("type") == "CustomEndpoint"), None)
    if src is None:
        found = [(s.get("name"), s.get("type")) for s in sources]
        raise RuntimeError(
            f"No custom-endpoint source on {eventstream_name!r}; sources: {found}"
        )

    conn = _fabric_get(
        f"workspaces/{workspace_id}/eventstreams/{es_id}/sources/{src['id']}/connection",
        token,
    )
    cs = conn.get("accessKeys", {}).get("primaryConnectionString")
    if not cs:
        raise RuntimeError(
            "connection endpoint returned no primaryConnectionString "
            f"(keys: {list(conn.get('accessKeys', {}))})"
        )
    return cs


if not connection_string:
    connection_string = resolve_connection_string(eventstream_name, source_name)
    print(f"resolved connection string for {eventstream_name!r} via Fabric API")
else:
    print("using connection_string parameter override")

# %% [clickstream]

# %%
# Build the generator config and push events to the Eventstream custom endpoint.
config = GeneratorConfig(
    daily_target=daily_target,
    rate=rate if rate and rate > 0 else None,
    customer_count=customer_count,
    product_count=product_count,
    seed=seed,
    batch_size=batch_size,
    max_events=max_events,
    duration_seconds=duration_seconds,
    partition_by_customer=partition_by_customer,
)


def log(msg: str) -> None:
    print(msg)


sink = EventHubSink(
    connection_string,
    eventhub_name=eventhub_name or None,
    partition_by_customer=partition_by_customer,
)
stats = run(config, sink, logger=log)
print(
    f"done: {stats.events_sent:,} events in {stats.elapsed_seconds:.1f}s "
    f"({stats.effective_rate:.0f}/sec)"
)
