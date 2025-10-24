# AGENTS.md — Lakehouse

Spec for Lakehouse structures and medallion flows.

Bronze → Silver mapping:
- Flatten envelope and payload; standardize timestamps; cast to schema
- Deduplicate via `trace_id`/`receipt_id` where applicable
- Enforce referential integrity against master dimensions

Silver → Gold transformations:
- Sales rollups, inventory SCD snapshots, logistics durations, attribution windows

Shortcuts/External tables:
- Optionally create shortcuts to KQL exported parquet for hybrid analysis

Pipelines:
- Orchestrate with `pipelines` component; schedule micro-batch compaction

