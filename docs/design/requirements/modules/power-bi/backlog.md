# Power BI backlog

## Open

### IMP-009 - Fix current-state and time-slice KPI semantics {#imp-009}

- **Priority / effort:** P2 / M
- **Outcome:** State folding, status normalization, date relationships, grain,
  weighting, labels, and technical-field visibility agree across KQL and DAX.
- **Acceptance:** Automated query/model tests prove labels, slicers, state, and
  source grain.
- **Progress:** Status normalization and one technical-field-visibility defect
  are resolved and guarded: the payment-anomaly KQL function filtered on
  lowercase `"declined"` (never matched the uppercase source enum), now fixed to
  `"DECLINED"`; the orphan raw `event_date` column on `fact_payments` (no
  `dim_date` relationship, duplicates `Event Timestamp`) is now hidden.
  `tests/scripts/test_kpi_status_semantics.py` pins canonical UPPERCASE status
  literals across KQL and DAX and the visibility fix. State folding, date
  relationships, grain, and weighting reconciliation remain open.

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
