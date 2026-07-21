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
