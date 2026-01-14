# Semantic Model

Power BI semantic model for unified analytics. Hybrid model over KQL (hot) and Lakehouse Gold (history).

## Sources
- **Gold Layer** (DirectLake): Pre-aggregated KPI tables from `au` schema
- **Dimension Tables** (DirectLake): Master data from `ag` schema (dim_stores, dim_products)
- **KQL views** (optional): Near-real-time tiles via DirectQuery

## Current Tables (11)

### Gold Aggregations (9 tables)
- `gold_sales_minute_store` - Sales velocity per minute/store
- `gold_top_products_15m` - Top products in 15-minute rolling windows
- `gold_inventory_position_current` - Current store inventory snapshot
- `gold_dc_inventory_position_current` - Current DC inventory snapshot
- `gold_truck_dwell_daily` - Logistics performance metrics
- `gold_tender_mix_daily` - Payment method distribution
- `gold_online_sales_daily` - Online order revenue
- `gold_zone_dwell_minute` - Customer dwell times by zone
- `gold_marketing_cost_daily` - Marketing spend by campaign

### Dimension Tables (2 tables)
- `dim_stores` - Store master data
- `dim_products` - Product master data

## Perspectives
- **Operations**: Sales, inventory, dwell, tender mix
- **Merchandising**: Products, inventory, online sales
- **Logistics**: Truck dwell, DC inventory
- **Marketing**: Marketing costs

## Deployment

1. **Import Model**:
   - In Fabric workspace: New → Semantic model
   - Import `model.tmdl`

2. **Configure Lakehouse Connection**:
   - Replace `<fabric-lakehouse-resource-id>` in model.tmdl with your Lakehouse resource ID
   - Or configure after import in Fabric

3. **Refresh Model**:
   - Bind to Lakehouse
   - Refresh to load data

## Future Enhancements

Additional Gold tables planned for future releases:
- Campaign revenue with conversion tracking
- Fulfillment metrics (pick/ship/deliver)
- BLE presence aggregations
- Stockout and reorder analytics
- Store operations metrics
- Promotion performance tracking
