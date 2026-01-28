# Semantic Model

Power BI semantic model for unified analytics using DirectLake over Lakehouse Gold tables.

## Data Sources
- **Lakehouse Gold tables** (DirectLake): Pre-aggregated KPI tables from `au` schema
- **Dimension tables** (DirectLake): Master data from `ag` schema (dim_stores, dim_products, dim_date)

## Entities
- Sales, Inventory, Logistics, Marketing, Customers, Products, Stores

## Measures
- Sales (min/hour/day), Units, Margin, DOS, Dwell, On-time %, ROAS

## Artifacts
- **Power BI Project**: `retail_model.pbip` (includes semantic model + report)
- **TMDL model**: `retail_model.SemanticModel/` (model definition in TMDL format)
- **Report definition**: `retail_model.Report/` (report pages and visuals)

## Deployment

1. Open `fabric/semantic_model/retail_model.pbip` in Power BI Desktop
2. Configure Lakehouse connection credentials (OAuth 2.0)
3. Click **Publish** and select your Fabric workspace

See [Semantic Model Deployment Guide](../setup/08-semantic-model-deployment.md) for detailed instructions.
