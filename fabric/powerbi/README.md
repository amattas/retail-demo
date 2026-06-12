# Semantic Model

Power BI semantic model for unified analytics. Hybrid model over KQL (hot) and Lakehouse Gold (history).

## Sources
- **Gold Layer** (DirectLake): Pre-aggregated KPI tables from `au` schema
- **Dimension Tables** (DirectLake): Master data from `ag` schema (dim_stores, dim_products)
- **KQL views** (optional): Near-real-time tiles via DirectQuery

## Current Tables (35 model tables)

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

### Dimension Tables (7 tables)
- `dim_stores` - Store master data
- `dim_products` - Product master data
- `dim_date` - Date dimension with YYYYMMDD key, fiscal calendar attributes
- `dim_geographies` - Region, district, city, state, and ZIP slicing
- `dim_distribution_centers` - DC master data for network and replenishment views
- `dim_customers` - Customer master data for segmentation and churn analysis
- `dim_trucks` - Truck fleet master data for logistics analysis

### Operational Facts and Events (18 tables)

The default model only references tables created by the historical and streaming
Lakehouse notebooks so the PBIP opens cleanly in a freshly deployed demo.

- Sales and tender facts: `fact_receipts`, `fact_receipt_lines`, `fact_payments`
- Omnichannel facts: `fact_online_order_headers`, `fact_online_order_lines`
- Inventory and replenishment facts: `fact_store_inventory_txn`,
  `fact_dc_inventory_txn`, `fact_stockouts`, `fact_reorders`
- Logistics facts: `fact_truck_moves`, `fact_truck_inventory`
- Store/customer event facts: `Foot Traffic`, `BLE Pings`,
  `Customer Zone Changes`, `Store Operations`
- Marketing and promotion facts: `Marketing`, `Promotions`, `Promotion Lines`
- Technical tracking: `_watermarks`

### Optional ML & Predictive Analytics Tables

These advanced tables are intentionally excluded from the active
`definition/tables` folder because Power BI Desktop loads table definition files
even when they are not referenced by `definition/model.tmdl`. Add them back only
after the corresponding Gold/ML notebooks have generated the Lakehouse tables.

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

## Model Enhancements

- Explicit measures are organized into business folders for sales, inventory, replenishment, fulfillment, traffic, promotion, and logistics.
- `dim_date` includes calendar, fiscal, and week hierarchies and is related to store sales, online orders, reorders, and daily Gold aggregates for time intelligence.
- Geography, product, store profile, and distribution center hierarchies support region, district, department, category, subcategory, format, volume class, and DC drill paths.
- Implicit measures are discouraged so report visuals bind to curated DAX measures rather than accidental aggregations.

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
   - Import the configured model from `fabric/powerbi/retail_model.SemanticModel`

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

This repo also includes a Power BI Project at `fabric/powerbi/retail_model.pbip` with a rebuilt RTI retail demo report:

- **SLT Dashboard**: Executive view across omnichannel revenue, margin, inventory health, replenishment, traffic, and logistics
- **Supply Chain Control Tower**: Whitepaper-aligned inventory, stockout, replenishment, and DC command center
- **Distribution Center Operations**: DC inventory, category mix, high-priority reorders, and dwell pressure
- **Store Operations Command Center**: Real-time store sales velocity, basket, foot traffic, zone dwell, and store operations workload
- **Regional & Department Performance**: Region, district, store, department, category, margin, and time-intelligence performance
- **Omnichannel Fulfillment**: Store plus online commerce, order economics, fulfillment modes, tender mix, and channel share
- **Customer, Marketing & Personalization**: Customer, traffic, BLE presence, retail media spend, and intervention queues
- **Pricing & Promotion Optimization**: Margin, promotion discount, product mix, and pricing-action proxy views
- **Logistics & Dwell Intelligence**: Truck dwell, shipment movement, and logistics exception monitoring

The optional ML tables can be re-enabled later for forecast, churn, elasticity,
stockout-risk, and dwell-prediction visuals once those tables exist in the
Lakehouse; keep optional definitions outside the active `definition/tables`
folder until then.

Open it in **Power BI Desktop** via File -> Open -> `fabric/powerbi/retail_model.pbip`.
