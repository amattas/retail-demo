# CLAUDE.md — Project Overview

You are assisting with building Microsoft Fabric Real-Time Intelligence assets for a retail demo powered by a synthetic data generator. Use the folder-level `CLAUDE.md` files in `fabric/*` for focused guidance per asset.

Immediate Priorities:
1) Wire Eventstream to Azure Event Hubs (`retail-events`) and route to KQL + Lakehouse Bronze
2) Define KQL DB tables/mappings and materialized views for core KPIs
3) Create initial Querysets and Real-Time Dashboards
4) Stand up Lakehouse Silver tables aligned to `datagen` fact schemas

Reference:
- Event envelope + payloads: `datagen/src/retail_datagen/streaming/schemas.py`
- Historical schemas: `datagen/AGENTS.md`

Handoff Notes:
- Keep exports of Fabric items in their respective `fabric/*` folders
- Update docs under `docs/` as assets land

