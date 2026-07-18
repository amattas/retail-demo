# Power BI backlog

## Open

### IMP-009 - Fix current-state and time-slice KPI semantics {#imp-009}

- **Priority / effort:** P2 / M
- **Outcome:** State folding, status normalization, date relationships, grain,
  weighting, labels, and technical-field visibility agree across KQL and DAX.
- **Acceptance:** Automated query/model tests prove labels, slicers, state, and
  source grain.
- **Progress:** Status normalization and technical-field-visibility defects are
  resolved and guarded: the payment-anomaly KQL function filtered on lowercase
  `"declined"` (never matched the uppercase source enum), now fixed to
  `"DECLINED"`; every orphan raw `event_date` column (`fact_payments`,
  `fact_promotions`, `fact_receipt_lines` — none of which have a `dim_date`
  relationship) is now hidden to remove duplicate/ambiguous date slicers.
  `tests/scripts/test_kpi_status_semantics.py` pins canonical UPPERCASE status
  literals across KQL and DAX and generically asserts that any `event_date`
  column that is not a `dim_date` relationship key stays hidden. Roll-up
  "average" KPIs are now volume-weighted: `Avg Store Basket`,
  `Avg Zone Dwell`, and `Avg Truck Dwell Minutes` recompute from base totals
  (weighted by receipts/customers/trucks) instead of naively averaging stored
  per-row averages, and the stored `avg_*` columns no longer auto-sum
  (`tests/scripts/test_kpi_aggregation_weighting.py`). The pandas
  `__index_level_0__` write artifact exposed and summable on 11 fact tables is
  now hidden and non-summable
  (`tests/scripts/test_semantic_model_technical_fields.py`). State folding and
  date-relationship/grain reconciliation for the visible key columns remain
  open.

### ENH-002 - Make dynamic pricing the flagship closed-loop action story {#enh-002}

- **Priority / effort:** Idea / L
- **Outcome:** Governed approve/reject writeback, current recommendation state,
  observed impact, and audit history form one reusable action pattern.

### ENH-006 - Improve usability, accessibility, and localization {#enh-006}

- **Priority / effort:** Idea / M
- **Outcome:** Latest-period defaults, drillthrough controls, alt text,
  keyboard/mobile support, contrast, culture, currency, and forecast confidence
  are intentionally designed.

## Settled — do not reopen

- The active semantic model is Direct Lake, not a documented KQL/Lakehouse
  hybrid.
- Curated explicit measures are preferred over implicit aggregation.
- Optional ML tables require explicit support and deployment status.
