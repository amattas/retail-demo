---
name: tech-stack
description: Describes the primary technologies, frameworks, libraries, and language conventions used in this codebase.
---

# Tech Stack Overview

## Primary Languages
- Backend: Python (datagen, notebooks)
- Data Processing: PySpark (Fabric notebooks)
- Query Language: KQL (Kusto Query Language for Eventhouse)
- Infrastructure: JSON/YAML (Fabric item definitions)

## Frameworks & Libraries
- Data Generation: DuckDB, Faker, Pydantic
- Lakehouse: Delta Lake, PySpark
- Real-Time Analytics: Microsoft Fabric Eventhouse (KQL)
- Streaming: Azure Event Hubs, Fabric Eventstream

## Language Conventions

### Python
- Follow PEP 8 for formatting
- Use type hints for function signatures
- Prefer dataclasses or Pydantic models for data structures

### KQL
- Use `.execute database script` for batch operations
- Prefix table names with domain (e.g., `receipt_created`, `inventory_updated`)
- Use materialized views for pre-aggregated KPIs

### Naming
- Tables: snake_case (e.g., `fact_receipts`, `dim_stores`)
- Functions: snake_case for Python, PascalCase for KQL functions
- Files: snake_case for Python, numbered prefix for KQL scripts

## Project-Specific Notes
- Event tables are streaming-only (from Eventstream)
- Historical dimension/fact tables are loaded from Lakehouse via shortcuts
- Gold layer aggregations are built in PySpark notebooks
