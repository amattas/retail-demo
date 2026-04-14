# Semantic Model

Power BI semantic model for unified analytics. Hybrid model over KQL (hot) and Lakehouse Gold (history).

## Sources
- **Gold Layer** (DirectLake): Pre-aggregated KPI tables from `au` schema
- **Dimension Tables** (DirectLake): Master data from `ag` schema (dim_stores, dim_products)
- **KQL views** (optional): Near-real-time tiles via DirectQuery

## Current Tables (35 core)

### Gold Aggregations (9 tables)
- `sales_minute_store` - Sales velocity per minute/store
- `top_products_15m` - Top products in 15-minute rolling windows
- `inventory_position_current` - Current store inventory snapshot
- `dc_inventory_position_current` - Current DC inventory snapshot
- `truck_dwell_daily` - Logistics performance metrics
- `tender_mix_daily` - Payment method distribution
- `online_sales_daily` - Online order revenue
- `zone_dwell_minute` - Customer dwell times by zone
- `marketing_cost_daily` - Marketing spend by campaign

### Dimension Tables (3 tables)
- `dim_stores` - Store master data
- `dim_products` - Product master data
- `dim_date` - Date dimension with YYYYMMDD key, fiscal calendar attributes

### ML & Predictive Analytics Tables (13 tables — not loaded by default)

> **Note:** These tables are defined in `definition/tables/` but not referenced in
> `model.tmdl` until the ML notebooks (06-14) have been run and their Gold tables
> exist in the lakehouse. To enable them, add the corresponding `ref table` lines
> to `model.tmdl` and relationship entries to `relationships.tmdl` (see commented
> examples at the bottom of each file), then refresh the model.

- `demand_forecast` - GBT demand predictions by store/product
- `product_recommendations` - Market basket "bought together" pairs
- `customer_segments` - RFM + K-means customer segments
- `churn_predictions` - Spark ML GBT churn risk scores
- `price_elasticity` - Log-log price elasticity coefficients
- `promotion_lift` - Promo episode lift analysis
- `zone_dwell_stats` - BLE zone dwell time statistics
- `zone_transitions` - Zone-to-zone transition probabilities
- `journey_patterns` - Customer journey path analysis
- `stockout_risk` - Spark ML GBT stockout risk predictions
- `dwell_predictions` - Delivery dwell time predictions with intervals
- `pricing_recommendations` - Elasticity-aware pricing recommendations
- `pricing_constraints` - Pricing business constraint reference

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

### ML Report Pages (5 pages — hidden until ML tables are enabled)

> These pages are not included in the report page order by default. After running
> ML notebooks 06-14, enable the ML tables in the semantic model (see above),
> then add these page IDs back to `pages.json`:
> `589d6c9c88ff9a9cff88`, `s1a0demand_stockout`, `s2a0pricing_promotions`,
> `c1a0customer_segments_churn`, `c2a0customer_journey`

- **Customer Segments & Churn** - Segment distribution, churn risk, RFM analysis
- **Customer Journey** - Zone dwell heatmap, transition matrix, path analysis
- **Demand & Stockout** - Forecast trends, stockout risk scatter, at-risk SKUs
- **Pricing & Promotions** - Elasticity by category, pricing recommendations, promotion lift
- **Logistics & Delivery Predictions** - Predicted vs actual dwell, outlier shipments

Open it in **Power BI Desktop** via File → Open → `fabric/semantic_model/retail_model.pbip`.
