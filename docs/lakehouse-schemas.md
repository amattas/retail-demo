# Lakehouse Schemas

This page documents the Silver and Gold Lakehouse tables aligned to datagen contracts.

Silver (normalized from Eventstream Bronze)
- silver_receipts: TraceId, EventTS, StoreID, CustomerID, ReceiptId, Subtotal, Tax, Total, TenderType
- silver_receipt_lines: TraceId, EventTS, ReceiptId, Line, ProductID, Qty, UnitPrice, ExtPrice, PromoCode
- silver_store_inventory_txn: TraceId, EventTS, StoreID, ProductID, QtyDelta, Reason, Source
- silver_dc_inventory_txn: TraceId, EventTS, DCID, ProductID, QtyDelta, Reason
- silver_foot_traffic: TraceId, EventTS, StoreID, SensorId, Zone, Dwell, Count
- silver_ble_pings: TraceId, EventTS, StoreID, BeaconId, CustomerBLEId, RSSI, Zone
- silver_marketing: TraceId, EventTS, Channel, CampaignId, CreativeId, CustomerAdId, ImpressionId, Cost, Device
- silver_online_orders: TraceId, EventTS, OrderId, CustomerID, FulfillmentMode, FulfillmentNodeType, FulfillmentNodeID, ItemCount, Subtotal, Tax, Total, TenderType
- silver_truck_moves: TraceId, EventTS, TruckId, DCID, StoreID, ShipmentId, Status, ArrivalTime, DepartureTime

Silver Views (for convenience)
- silver_receipt_header: joins receipts with latest payment and total discounts
- silver_receipt_detail: receipt lines with promo discounts per product

Gold (aggregated for dashboards)
- gold_sales_minute_store: StoreID, TS(minute), TotalSales, Receipts, AvgBasket
- gold_top_products_15m: ProductID, Revenue, Units, ComputedAt
- gold_inventory_position_current: StoreID, ProductID, OnHand, AsOf
- gold_truck_dwell_daily: Site, Day, AvgDwellMin, Trucks
- gold_campaign_revenue_daily: CampaignId, Day, Impressions, Conversions, Revenue

Assets
- DDL scripts: `fabric/lakehouse/silver/ddl.sql` and `fabric/lakehouse/gold/ddl.sql`
- Notebooks: `fabric/notebooks/bronze_to_silver.py` and `fabric/notebooks/silver_to_gold.py`
- Pipelines: `fabric/pipelines/pl_bronze_to_silver.template.json`, `fabric/pipelines/pl_silver_to_gold.template.json`

Notes
- Silver columns mirror datagen historical facts in `datagen/AGENTS.md`.
- Bronze path is `/Tables/bronze/events/`; notebooks read per `event_type` folder.
- Online orders are included (created events map to the silver_online_orders fact).
