---
name: tech-stack
description: Describes the primary technologies, frameworks, libraries, and language conventions used in this codebase.
---

# Tech Stack Overview

## Language-Specific Conventions

- **Python**: See [PYTHON.md](./PYTHON.md) for detailed conventions and examples

## Primary Languages

| Language | Usage | Version |
|----------|-------|---------|
| Python | utility (retail-setup), notebooks | 3.11+ |
| PySpark | Fabric notebooks | Spark 3.x |
| KQL | Eventhouse queries | N/A |
| JSON/YAML | Fabric item definitions | N/A |

## Frameworks & Libraries

### Data Generation (utility / `retail-setup`)
- **PySpark**: Deterministic, Spark-native generation (Fabric provides Spark at runtime)
- **Pydantic v2**: Configuration models and validation
- **Typer**: `retail-setup` CLI (configure/render/deploy)
- **PyYAML**: Configuration files

### Lakehouse
- **Delta Lake**: ACID transactions, schema enforcement
- **PySpark**: Distributed data processing

### Real-Time Analytics
- **Microsoft Fabric Eventhouse**: KQL-based analytics
- **Spark Kusto connector**: Direct event writes from `stream-events.ipynb` to Eventhouse

## Project Architecture

```
Event Flow:
  stream-events.ipynb (PySpark)
    → Eventhouse KQL tables (Spark Kusto connector)
    → Lakehouse Silver (ag) → Gold (au)

Data Layers:
  Bronze (raw JSON)
    → Silver (typed Delta)
    → Gold (aggregated Delta)
    → Semantic Model (Power BI)
```

## Key Dependencies

See `utility/pyproject.toml` for Python dependencies.

Core packages:
- `pydantic` - Configuration and schema validation
- `typer` - `retail-setup` CLI
- `pyyaml` - Configuration files
- `pyspark` - Generation runtime (local dev/test; Fabric provides Spark)
