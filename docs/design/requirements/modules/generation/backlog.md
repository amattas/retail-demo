# Generation backlog

## Open

### ENH-009 - Selectable retail scenario presets {#enh-009}

- **Priority / effort:** Idea / M
- **Outcome:** A presenter can select named scenarios (for example a supplier
  short-ship week, a regional storm, a holiday rush) that drive the existing
  deterministic customer, lead-time, promotion, weather, holiday and incident
  models.
- **Acceptance:** Scenario presets preserve determinism and move documented KPI
  targets in predictable directions.
- **Current boundary:** The underlying models exist (`REQ-GEN-005` to
  `REQ-GEN-012`); their parameters live in `calibration.py` and the profiles,
  not in selectable presets.

### Generation known limitations

- Online order hours follow one UTC curve, not each customer's local time.
- Holiday closures and lifts are per store class, not per store, and the
  calendar is US-only.
- Average Product Price and inventory valuation use the anchor-date list
  price, not the weekly shelf price.
- Overflow shipment legs take the shared truck pool; if they outnumber it a
  leg can only take the earliest-free truck.
- Inventory throughput used by feasibility (1 billion store-SKU days per
  node-hour) is a planning figure, not a measurement.

## Settled — do not reopen

- Generation follows store-local time and Silver stores true UTC;
  `dim_stores.TimeZone` and `fact_receipts.local_hour` carry the local view.
- Dimensions depend on the seed and the pinned `anchor_date`, never on the run
  date.
- No separate price-history table: sale prices are on the line facts, catalog
  prices on `dim_products`.
- Thresholds live once in `calibration.py`; KQL rules, notebooks and TMDL keep
  literal values that contract tests pin to it.
- Voided sales leave no record (no receipt and no payment), which keeps the
  payments-to-receipts relationship.

- Generation is Spark-native and deterministic.
- Lakehouse Delta tables replace local DuckDB/parquet as the active contract.
- New data columns use `snake_case`; legacy TMDL-bound exceptions remain
  documented until migrated.
- Shared business invariants are enforced by
  `utility/src/retail_setup/generation/invariants.py`: no sales while closed,
  no pre-launch sales, no same-day returns, product lifecycle dates present,
  and validated (non-degenerate) profile controls.
