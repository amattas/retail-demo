# Setup Guide

Complete guide from data generation through user-facing dashboards.

This guide provides step-by-step instructions for deploying the Retail Demo solution using the Bronze/Silver/Gold medallion architecture.

## Quick Start Checklist

- [ ] **Phase 1**: [Data Generation](01-data-generation.md) - Generate and upload sample data
- [ ] **Phase 2**: [Bronze Layer](02-bronze-layer.md) - Create Fabric resources and shortcuts
- [ ] **Phase 3**: [Silver Layer](03-silver-layer.md) - Upload notebooks and run historical load
- [ ] **Phase 4**: [Gold Layer](04-gold-layer.md) - Verify aggregation tables
- [ ] **Phase 5**: [Pipelines](05-pipelines.md) - Create scheduled pipelines
- [ ] **Phase 6**: [Streaming](06-streaming.md) - Configure Eventstream for real-time data
- [ ] **Phase 7**: [Semantic Model](08-semantic-model-deployment.md) - Deploy Power BI semantic model
- [ ] **Phase 8**: [Dashboards](07-dashboards.md) - Create reports and dashboards

## Prerequisites

### Azure Resources

- **Azure Event Hubs Namespace** with hub named `retail-events`
- **Azure Data Lake Storage Gen2** account (e.g., `stdretail`)
  - Container: `supermarket`
- **Microsoft Fabric Workspace** with Real-Time Intelligence capacity

### Local Development

- **Python 3.9+** for data generator
- **Git** for cloning repository
- **Azure CLI** (optional, for automated deployment)

### Access & Permissions

- Fabric workspace Contributor or Admin
- ADLSv2 Storage Blob Data Contributor
- Event Hubs Data Sender

## Architecture Overview

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────────────────────────┐
│   Datagen   │────▶│ Azure Event  │────▶│         Microsoft Fabric            │
│   (Python)  │     │    Hubs      │     │                                     │
└─────────────┘     └──────────────┘     │  ┌─────────────┐  ┌─────────────┐  │
                                         │  │ Eventstream │  │  Lakehouse  │  │
                                         │  └──────┬──────┘  └──────┬──────┘  │
                                         │         │                │         │
                                         │         ▼                ▼         │
                                         │  ┌─────────────┐  ┌─────────────┐  │
                                         │  │ Eventhouse  │  │  Notebooks  │  │
                                         │  │   (KQL)     │  │  (PySpark)  │  │
                                         │  └──────┬──────┘  └──────┬──────┘  │
                                         │         │                │         │
                                         │         ▼                ▼         │
                                         │  ┌─────────────────────────────┐  │
                                         │  │      Semantic Model         │  │
                                         │  │    (Power BI DirectQuery)   │  │
                                         │  └─────────────────────────────┘  │
                                         └─────────────────────────────────────┘
```

## Schema Naming Convention

| Schema | Layer | Purpose |
|--------|-------|---------|
| `cusn` | Bronze | Eventhouse event table shortcuts (Tables/) |
| `ag` | Silver | Cleaned, deduplicated, typed Delta tables |
| `au` | Gold | Pre-aggregated KPIs for dashboards |

## Reference Documentation

- [Validation & Testing](validation.md) - End-to-end testing procedures
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
- [Configuration Reference](configuration.md) - Environment variables
- [Capacity Planning](capacity-planning.md) - Fabric SKU sizing
- [Disaster Recovery](disaster-recovery.md) - Rollback procedures

## Next Steps

Start with [Phase 1: Data Generation](01-data-generation.md) to generate the sample retail data.
