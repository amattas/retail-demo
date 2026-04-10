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

## Optional Fabric Ontology

The repo also includes `fabric/lakehouse/30-create-ontology.ipynb`, an optional notebook that creates or replaces a Fabric ontology item from core Silver tables in `ag`.

- **Purpose**: Generate retail entity and relationship definitions directly in Fabric
- **Source tables**: `ag.dim_geographies`, `ag.dim_stores`, `ag.dim_distribution_centers`, `ag.dim_trucks`, `ag.dim_customers`, `ag.dim_products`, `ag.fact_receipts`, plus `ag.fact_receipt_lines` for receipt-to-product relationships
- **Parameters**: `SILVER_DB`, `LAKEHOUSE_NAME`, `ONTOLOGY_NAME`, `ONTOLOGY_DESCRIPTION`, `DELETE_EXISTING`
- **Naming note**: If `ONTOLOGY_NAME` contains spaces or other unsupported characters, the notebook normalizes it to a Fabric-safe item name using letters, numbers, and underscores
- **Run timing**: Manual, after `02-historical-data-load.ipynb` has created the Silver tables

This complements the semantic model: the semantic model remains the analytics layer, while the ontology captures business entities and relationships as a Fabric ontology item.

## Deployment

1. Open `fabric/semantic_model/retail_model.pbip` in Power BI Desktop
2. Configure Lakehouse connection credentials (OAuth 2.0)
3. Click **Publish** and select your Fabric workspace

See [Semantic Model Deployment Guide](../setup/08-semantic-model-deployment.md) for detailed instructions.
