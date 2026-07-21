# Power BI backlog

## Open

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
- Current-state and time-slice KPIs use overwrite snapshots, producer-aligned
  labels, `dim_date` slicing, volume-weighted rollups, and hidden
  non-aggregatable technical fields.
- Optional ML tables require explicit support and deployment status.
