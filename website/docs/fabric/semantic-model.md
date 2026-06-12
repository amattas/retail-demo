# Semantic Model

Power BI semantic model and report for unified analytics, shipped as a PBIP project in `fabric/powerbi/`. The model uses **Direct Lake** over the Lakehouse: Silver (`ag`) dimensions and facts plus Gold (`au`) aggregations and ML outputs.

## Artifacts

- **Power BI Project**: `retail_model.pbip` (semantic model + report)
- **TMDL model**: `retail_model.SemanticModel/` (model definition in TMDL format)
- **Report definition**: `retail_model.Report/` (PBIR pages and visuals)

## Model Contents (39 tables, 145 measures, 56 relationships)

### Dimensions (7)

`dim_date` (with calendar and fiscal columns; fiscal year starts in July), `dim_customers`, `dim_products`, `dim_stores`, `dim_geographies`, `dim_distribution_centers`, `dim_trucks`.

Hierarchies:

| Table | Hierarchies |
|-------|-------------|
| `dim_date` | Calendar (Year → Quarter → Month → Date), Fiscal, Week |
| `dim_products` | Product (Department → Category → Subcategory → Product), Brand (Company → Brand → Product) |
| `dim_stores` | Store Profile (Store Format → Volume Class → Store Number) |
| `dim_geographies` | Geography (Region → District → State → City) |
| `dim_distribution_centers` | Distribution Center (DC Number) |

### Facts (18)

Sales and transactions: `fact_receipts`, `fact_receipt_lines`, `fact_payments`, `fact_online_order_headers`, `fact_online_order_lines`, `fact_promotions`, `fact_promo_lines`.

Inventory, operations, and marketing: `fact_store_inventory_txn`, `fact_dc_inventory_txn`, `fact_stockouts`, `fact_reorders`, `fact_truck_moves`, `fact_truck_inventory`, `fact_store_ops`, `fact_foot_traffic`, `fact_ble_pings`, `fact_customer_zone_changes`, `fact_marketing`.

### Gold Aggregations (9)

`sales_minute_store`, `top_products_15m`, `inventory_position_current`, `dc_inventory_position_current`, `online_sales_daily`, `tender_mix_daily`, `marketing_cost_daily`, `truck_dwell_daily`, `zone_dwell_minute`.

### ML Tables (4)

| Table | Source Notebook | Description |
|-------|-----------------|-------------|
| `demand_forecast` | `06-ml-demand-forecast` | GBT demand predictions by store/product with bounds and MAPE |
| `customer_segments` | `08-ml-customer-segmentation` | RFM + K-means segments (Champions, Loyal, At Risk, …) |
| `churn_predictions` | `09-ml-churn-prediction` | Churn probability, prediction, and risk category per customer |
| `stockout_risk` | `12-ml-stockout-prediction` | Stockout probability, risk score/ranking, days of inventory |

Other ML output tables in the Lakehouse (`product_recommendations`, `price_elasticity`, `promotion_lift`, `journey_patterns`, `dwell_predictions`, `pricing_recommendations`, …) are not currently part of the semantic model — see [Lakehouse](./lakehouse.md) for the full list.

### System

`_watermarks` tracks streaming processing timestamps (not used in visuals).

## Measures (145)

Measures are organized in display folders on their host tables. Highlights by group:

- **Core sales** (`fact_receipts`, `fact_receipt_lines`): Total Sales, Net Sales, Receipt Count, Average Basket Size, Total Returns, Return Rate %, Total Units Sold, Gross Profit, Gross Margin %, Sales per Store, Sales per Customer
- **Time intelligence** (`fact_receipts`): Store Sales MTD/QTD/YTD, PY/PM/PQ/PD, YoY/MoM/QoQ/DoD/WoW %, rolling 7-day, plus Latest/Prior-Year helpers for year-agnostic visuals
- **Online and omnichannel**: Total Online Sales, Online Order Count, Average Online Order Value, Online Sales YTD/PY/YoY %, Omnichannel Revenue/Orders/AOV, Store vs Online % of Omnichannel
- **Payments** (`fact_payments`): Payment Count, Approved/Declined Payment Count, Payment Decline Rate %, Avg Payment Processing MS
- **Promotions**: Promotion Count, Promotion Discount Amount, Promo Line Quantity/Discount
- **Inventory** (`inventory_position_current`, `dc_inventory_position_current`, txn/stockout/reorder facts): Total Units on Hand, Total Inventory Value/Retail Value, Inventory Margin %, Inventory to Sales Ratio, SKU counts, Stockout Events, Reorder Events, High Priority Reorder Quantity, Reorder Fill Gap
- **Logistics** (`fact_truck_moves`, `truck_dwell_daily`): Truck Move Count, Avg Unload Duration, Avg Truck Dwell Minutes, Truck Dwell Breach Days
- **Store operations and traffic** (`fact_store_ops`, `fact_foot_traffic`, `zone_dwell_minute`): Store Operation Events, Foot Traffic Count, Avg Dwell Minutes, Traffic Conversion Proxy %, Zone Customers
- **Marketing** (`marketing_cost_daily`): Total Marketing Cost, Total Marketing Impressions, Marketing CPM, Marketing Spend % of Revenue
- **ML/predictive** (`demand_forecast`, `stockout_risk`, `customer_segments`, `churn_predictions`): Forecasted Units with bounds and MAPE, High Risk SKUs, Avg Stockout Probability, Avg Days of Inventory, Segmented Customer Count, RFM averages, Customers At Risk, Predicted Churn Rate %
- **Entity counts** (dimensions): # of Customers, Products, Stores, Locations, Distribution Centers, Trucks

## Relationships

56 relationships connect facts to dimensions and `dim_date` (many-to-one, single-direction). Role-playing relationships that would create ambiguity (e.g., payments → online orders/stores/customers, truck moves → stores, reorders → DCs, promotions → stores/customers) are kept **inactive** and activated in measures via `USERELATIONSHIP` where needed.

## Report Pages (8)

| Page | Focus |
|------|-------|
| **Executive Cockpit** | Top-level KPI cards, revenue trend, top stores |
| **Sales** | Core sales KPIs, department breakdown, YTD vs prior year, customer segments |
| **Supply Chain Control Tower** | Inventory KPIs, inventory by category, stockout risk table |
| **Omnichannel Fulfillment** | Omnichannel revenue/orders, store vs online trend, payment mix, demand forecast |
| **Operations & Marketing** | Operations KPIs, churn risk, marketing campaign performance |
| **Receipt Detail** | Drill-through: receipt lines, promotions applied, payments |
| **Store Detail** | Drill-through: store KPIs, traffic trend, top SKUs, stockout risk |
| **Product Detail** | Drill-through: product KPIs, inventory position, forecast, risk ranking |

## Optional Fabric Ontology

The repo also includes `fabric/lakehouse/30-create-ontology.ipynb`, an optional notebook that creates or replaces a Fabric ontology item from core Silver tables in `ag`.

- **Purpose**: Generate retail entity and relationship definitions directly in Fabric
- **Source tables**: Silver dimensions and facts (`ag`), plus ML outputs (`au.customer_segments`, `au.churn_predictions`)
- **Parameters**: `SILVER_DB`, `LAKEHOUSE_NAME`, `ONTOLOGY_NAME`, `ONTOLOGY_DESCRIPTION`, `DELETE_EXISTING`
- **Naming note**: If `ONTOLOGY_NAME` contains spaces or other unsupported characters, the notebook normalizes it to a Fabric-safe item name
- **Run timing**: Manual, after `02-historical-data-load.ipynb` has created the Silver tables

This complements the semantic model: the semantic model remains the analytics layer, while the ontology captures business entities and relationships as a Fabric ontology item.

## Deployment

1. Open `fabric/powerbi/retail_model.pbip` in Power BI Desktop
2. Configure Lakehouse connection credentials (OAuth 2.0)
3. Click **Publish** and select your Fabric workspace

`fabric/powerbi/CHANGES.md` documents the latest model changes (dim_date with fiscal calendar, ID type fixes, new date relationships) and rollback steps.

See [Semantic Model Deployment Guide](../setup/08-semantic-model-deployment.md) for detailed instructions.
