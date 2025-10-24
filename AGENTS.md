# AGENTS.md — Retail Fabric RTI Project

Authoritative spec for how agents should work across this repository. The project combines a synthetic retail data generator (`datagen/`) with Microsoft Fabric Real-Time Intelligence assets (`fabric/`).

Scope:
- Fabric assets live under `fabric/` with subfolders per asset type. Each contains `README.md`, `AGENTS.md`, and `CLAUDE.md` to guide implementation.
- Docs live under root `docs/` and are served with MkDocs.

Core Data Contracts:
- Event envelope and payloads: `datagen/src/retail_datagen/streaming/schemas.py`
- Historical fact and dimension schemas: `datagen/AGENTS.md` (Master/Fact tables)

Primary Use Cases (from whitepaper and schema):
- Real-time POS and promotions; live sales KPIs
- Inventory health, stockout detection, auto-replenishment
- In-store presence, zone dwell, conversion tracking
- Supply chain gate events, dwell, SLA monitoring
- Marketing attribution from impressions → visits → purchases

Safety & Constraints:
- All data is synthetic; no real PII
- Fabric assets should tolerate schema evolution and include validation
- Latency targets: KQL < 2s for hot tiles; Alerts < 30s for urgent

Completion Definition (initial phase):
- Scaffolding complete for Fabric assets with specs in each folder
- MkDocs site builds locally and documents current state
- Root docs reflect state-of-play and next steps

