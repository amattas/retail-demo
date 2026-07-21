# Clickstream generator + Fabric Eventstream → Eventhouse

Synthetic **clickstream** events generated in Python, pushed to a Fabric
**Eventstream** custom endpoint, and landed in the `clickstream_eventhouse`
**Eventhouse**. Designed to sustain **10,000,000 events/day** (~116 events/sec)
with headroom to burst much higher.

## Event shape

```json
{
  "event_id": "3eb13b90-4668-4257-bdd6-40fb06671ad1",
  "customer_id": 14629,
  "event_timestamp": "2026-07-20T22:26:03.539123+00:00",
  "event_type": "page_view | product_view | cart_add | search",
  "detail": {
    "page_url": "/product/1144",
    "product_id": 1144,
    "search_terms": null
  }
}
```

- `customer_id` maps to `dim_customers.ID` — a contiguous `1..customer_count`
  range in the historical generator, so events carry valid foreign keys. Use
  `--customers-file` to draw from exact exported IDs instead of a range.
- `product_id` maps to `dim_products.ID` and is present only for `product_view`
  and `cart_add`. `search_terms` is present only for `search`. All three
  `detail` keys are always present (`null` when not applicable).
- Deterministic for a given `--seed` (including `event_id`), matching the
  repo's deterministic-generation convention.

## Run the generator

Install the optional Event Hub client (adds `azure-eventhub`):

```powershell
cd utility
python -m pip install -e ".[clickstream]"
```

Preview events without sending (no connection required):

```powershell
python -m retail_setup.clickstream --dry-run --max-events 5
```

Stream into the Eventstream custom endpoint at 10M/day:

```powershell
$env:CLICKSTREAM_EVENTHUB_CONNECTION_STRING = "<custom-endpoint-connection-string>"
python -m retail_setup.clickstream --customer-count 50000 --product-count 5000
# or the installed console script:
retail-clickstream --customer-count 50000 --product-count 5000
```

Get the connection string from the Fabric portal: open the clickstream
Eventstream → the custom endpoint source → **Event Hub** tab →
**Connection string-primary key**.

Useful flags: `--rate <events/sec>` (overrides `--daily-target`),
`--batch-size`, `--max-events`, `--duration`, `--partition-by-customer`
(routes each `customer_id` to a fixed Event Hub partition to preserve
per-customer order; events are still batched per partition, so throughput is
unaffected).

## Run inside Fabric (notebook)

The same generator ships as a Fabric notebook, **clickstream-generator**, so it
can run inside the workspace without a local machine. It inlines this module and
pushes to the Eventstream custom endpoint via `azure-eventhub` — the identical
integration an external application uses, so the Eventstream stays in the
architecture (no direct Eventhouse write).

- **Source:** `utility/notebooks/templates/driver-06-clickstream.py`
  → built into `utility/notebooks/clickstream-generator.ipynb` by
  `python scripts/build_notebooks.py` (the `# %% [clickstream]` cell inlines
  `generator.py`; run `--check` in CI to guard drift).
- **Deploy:** rendered by `retail-setup render` and staged into the **Streaming**
  workspace folder by the deploy's `stream` notebook group (part of
  `retail-setup deploy`, alongside `stream-events`). It is a manual long-running
  driver, not part of the ordered setup pipeline.
- **Run:** open the notebook in Fabric and **Run All** — no secret to paste. The
  notebook auto-resolves the custom-endpoint connection string from the Fabric
  REST API using its own identity: it looks up `eventstream_name`
  (`clickstream_eventstream` by default) in the current workspace, finds the
  `CustomEndpoint` source, and reads its `primaryConnectionString` (with
  `EntityPath` embedded). `customer_id` / `product_id` ranges are read from the
  Silver `dim_customers` / `dim_products` when present, else fall back to the
  `customer_count` / `product_count` parameters. Tune `rate`,
  `duration_seconds`, `max_events`, `batch_size`, and `partition_by_customer`
  per run. Set the `connection_string` parameter only to override the target
  (e.g. a different stream); it bypasses auto-resolution.
- **Compute (small capacities):** the deploy creates a secondary, non-default
  Spark pool `retail_realtime_pool` (1–6 Small nodes) **and** a Fabric
  Environment `retail_realtime` bound to it (see the `spark` block in
  `deploy/config/deploy.yml`). They exist because the workspace default pool
  `retail_setup_pool` is sized for F64 (max 10 nodes) and is rejected on an F8
  capacity, whose Spark node-count ceiling is 6 — so setup-pool sessions fail
  with `SparkSettingsInvalidNodeCount`. A notebook can't attach to a bare custom
  pool, so select the **Environment** `retail_realtime` in the notebook's
  environment picker before **Run All**; it routes the session onto the 6-node
  pool without changing the workspace default. The generator is pure-Python and
  needs only a single node.

  The pool and Environment item are provisioned by Terraform
  (`spark_realtime_*` variables), but the `fabric_environment` resource can't
  bind the pool or publish Spark settings — `deploy.scripts.configure_environment`
  does the bind + publish via the Fabric REST API after `terraform apply` (a
  step in `retail-setup deploy`).

The auto-resolution uses these Fabric REST endpoints under the notebook
identity: `GET /v1/workspaces/{ws}/eventstreams`,
`.../eventstreams/{id}/topology`, and
`.../eventstreams/{id}/sources/{sourceId}/connection`. The identity running the
notebook needs read access to the Eventstream (contributor on the workspace).

## Infrastructure (Terraform)

The real-time path is provisioned by `deploy/terraform/clickstream.tf`
(templates in `deploy/terraform/clickstream/`) and is **opt-in** via
`clickstream_enabled` (enabled for `dev`, disabled for `test`/`prod`):

- `fabric_eventhouse.clickstream` — the `clickstream_eventhouse` Eventhouse.
- `fabric_kql_database.clickstream` — a KQL database whose `DatabaseSchema.kql`
  defines the `clickstream_events` table (schema contract):
  `event_id:string, customer_id:long, event_timestamp:datetime,
  event_type:string, detail:dynamic`.
- `fabric_eventstream.clickstream` — a **CustomEndpoint** source routed to an
  **Eventhouse ProcessedIngestion** destination (columns matched by name; no
  named ingestion mapping required).

Deploy with the standard flow (`retail-setup deploy --env dev`), which runs the
Terraform in `deploy/terraform`. Enablement and item names come from
`deploy/config/deploy.yml` (`clickstream:` block) and are rendered into
`deploy/terraform/environments/<env>.tfvars` by `deploy_config.render_tfvars`.

10M/day sits comfortably within the Eventstream **Low** throughput tier; raise
the tier in the portal if you drive substantially higher rates.

## Troubleshooting

**The generator reports success but no rows appear in the Eventhouse.** The
generator only writes to the Event Hub-compatible custom endpoint; it cannot
observe the downstream Eventstream. If the **Eventhouse destination node is
paused**, events are accepted and **buffered** at the endpoint but never
ingested — so sends "succeed" while the table stays empty. The destination
typically pauses when the **Fabric capacity is paused** (for example, an
overnight auto-pause).

Check the node status and resume it (source *and* destination must be
`Running`):

```powershell
$t = az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv
$ws = "<workspace-id>"; $es = "<eventstream-id>"
$topo = Invoke-RestMethod -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/eventstreams/$es/topology" -Headers @{Authorization="Bearer $t"}
$topo.sources + $topo.destinations | ForEach-Object { "$($_.name): $($_.status)" }

# Resume the paused destination (WhenLastStopped replays buffered events):
$dest = "<destination-node-id>"
Invoke-WebRequest -Method Post -Headers @{Authorization="Bearer $t"; "Content-Type"="application/json"} `
  -Uri "https://api.fabric.microsoft.com/v1/workspaces/$ws/eventstreams/$es/destinations/$dest/resume" `
  -Body '{"startType":"WhenLastStopped"}'
```

Resuming with `WhenLastStopped` replays the buffered events, so nothing sent
during the pause is lost (within the endpoint's retention window). Also confirm
the capacity is active before deploying or streaming
(`az fabric capacity resume ...`).

**The Eventstream "Test result" preview shows "No data to preview."** That pane
is a *live* sampler of events flowing through the stream at that moment — it is
not a view of the destination table. Run the generator while the preview is open
and click **Refresh**, or query the Eventhouse directly
(`clickstream_events | count`).

**An "Information"-level schema-mismatch notice on the destination.** Expected
before any data has flowed: the source schema has not been sampled yet, so the
editor cannot confirm the columns match. `ProcessedIngestion` maps by column
name, and the generator's fields already match `clickstream_events`, so no
mapper operator is required. The notice clears once events flow.
