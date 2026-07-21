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
(uses `customer_id` as the partition key to preserve per-customer order).

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
