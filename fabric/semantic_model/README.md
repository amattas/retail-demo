# Semantic Model

Power BI semantic model for unified analytics. Hybrid model over KQL (hot) and Lakehouse Gold (history).

## Sources
- **Gold Layer** (DirectLake): Pre-aggregated KPI tables from `au` schema
- **Dimension Tables** (DirectLake): Master data from `ag` schema (dim_stores, dim_products)
- **KQL views** (optional): Near-real-time tiles via DirectQuery

## Current Tables (12)

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

### Dimension Tables (3 tables)
- `dim_stores` - Store master data
- `dim_products` - Product master data
- `dim_date` - Date dimension with YYYYMMDD key, fiscal calendar attributes

## Perspectives
- **Operations**: Sales, inventory, dwell, tender mix
- **Merchandising**: Products, inventory, online sales
- **Logistics**: Truck dwell, DC inventory
- **Marketing**: Marketing costs

## Deployment

### Option 1: Automated Configuration (Recommended)

1. **Configure Lakehouse Connection**:
   ```bash
   # From repository root
   python scripts/configure_semantic_model.py \
       --workspace-id <your-workspace-guid> \
       --lakehouse-id <your-lakehouse-guid>
   ```

   Or use environment variables:
   ```bash
   export WORKSPACE_ID=<your-workspace-guid>
   export LAKEHOUSE_ID=<your-lakehouse-guid>
   python scripts/configure_semantic_model.py
   ```

2. **Import Model**:
   - In Fabric workspace: New → Semantic model
   - Import the configured model from `fabric/semantic_model/retail_model.SemanticModel`

3. **Refresh Model**:
   - The model is pre-configured to connect to your lakehouse
   - Refresh to load data

### Option 2: Manual Configuration

1. **Import Model**:
   - In Fabric workspace: New → Semantic model
   - Import `retail_model.SemanticModel`

2. **Configure Lakehouse Connection**:
   - Update the connection URL in `definition/expressions.tmdl`
   - Replace workspace and lakehouse GUIDs in the OneLake URL
   - Or configure after import in Fabric UI

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

## Report (PBIP)

This repo also includes a Power BI Project at `fabric/semantic_model/retail_model.pbip` with a starter report:

- **Sales Dashboard**: Sales KPIs plus store/category breakdowns
- **Supply Chain Control Tower**: Combined view of store/DC inventory, reorders, and truck dwell trends
- **Online, Payments & Marketing**: Online sales by day, tender mix, and marketing cost by campaign
- **Inventory & Replenishment**: On-hand units/value plus reorder quantity by priority
- **Logistics Control Center**: Truck dwell/throughput plus DC inventory on hand

Open it in **Power BI Desktop** via File → Open → `fabric/semantic_model/retail_model.pbip`.
