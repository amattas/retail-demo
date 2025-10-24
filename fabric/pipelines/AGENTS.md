# AGENTS.md — Pipelines

Orchestration spec for Lakehouse transforms and maintenance.

Requirements:
- Idempotent tasks with checkpointing
- Parameterized by date/store/region for backfills
- Alerting on failures to `rules` channels

Tasks:
- Bronze JSON → Silver typed
- Silver dedup/constraints
- Gold aggregates refresh
- Optimize + Vacuum cadence

