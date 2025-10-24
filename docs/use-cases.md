# Use Cases

Based on the whitepaper and the generator's schema, the demo focuses on the following strategic use cases:

## 1. Real-Time POS and Promotions

**Business Need**: Monitor sales performance and promotional effectiveness in real-time to optimize revenue and margins.

**Capabilities**:
- Sales per minute by store, category, product
- Tender type distribution (cash, credit, mobile, loyalty points)
- Promotion lift analysis (sales with vs without promo codes)
- Real-time basket analytics (items per transaction, average ticket)

**KQL Queries**: Top SKUs, hourly sales trends, promo code redemption rates
**Event Types**: `receipt_created`, `receipt_line_added`, `payment_processed`, `promotion_applied`

---

## 2. Inventory Health and Optimization

**Business Need**: Prevent stockouts, reduce excess inventory, and minimize shrinkage through real-time visibility.

**Capabilities**:
- Low stock alerts (inventory below safety threshold)
- Stockout detection and lost sales estimation
- Automated reorder trigger recommendations
- Shrink signal detection (unexplained inventory deltas)
- DC-to-store replenishment tracking

**KQL Queries**: Current inventory by location, reorder backlog, velocity trends
**Event Types**: `inventory_updated`, `stockout_detected`, `reorder_triggered`

---

## 3. Customer Journey and Engagement

**Business Need**: Understand in-store customer behavior to optimize layouts, staffing, and conversion rates.

**Capabilities**:
- Store entry/exit tracking via sensors
- Zone dwell time analysis and heatmaps
- Customer flow patterns through store zones
- Conversion rate: foot traffic → purchases
- BLE beacon-based location tracking

**KQL Queries**: Zone occupancy, dwell distributions, conversion funnels
**Event Types**: `customer_entered`, `customer_zone_changed`, `ble_ping_detected`

---

## 4. Supply Chain and Logistics

**Business Need**: Ensure on-time deliveries, optimize truck utilization, and reduce supply chain disruptions.

**Capabilities**:
- Truck arrival/departure gate tracking
- Dwell time monitoring (time at DC or store dock)
- On-time delivery SLA compliance
- Lane performance analysis (route efficiency)
- Refrigerated transport monitoring

**KQL Queries**: Truck dwell KPIs, late arrival alerts, shipment status
**Event Types**: `truck_arrived`, `truck_departed`

---

## 5. Marketing Attribution and ROAS

**Business Need**: Measure marketing campaign effectiveness and optimize channel spend based on actual in-store conversions.

**Capabilities**:
- Digital ad impression tracking (display, social, search, email)
- Attribution chain: impressions → store visits → purchases
- Channel-specific conversion rates
- Return on Ad Spend (ROAS) calculation
- Campaign performance by geography and customer segment

**KQL Queries**: Conversion windows (1-48 hours), attribution lift, cost per acquisition
**Event Types**: `ad_impression`, customer visit events linked to impressions

---

## 6. CPG Supplier Collaboration Portal (Future)

**Business Need**: Monetize retail data by providing CPG manufacturers with real-time insights into product performance, enabling new revenue streams and stronger partnerships.

**Capabilities**:

### For Retailers (Revenue Generation):
- **Data Monetization**: Sell access to anonymized, aggregated sales and inventory insights
- **Premium Tier**: Real-time stockout alerts and promotion performance dashboards
- **Supplier Analytics as a Service**: Hosted analytics portal for CPG partners
- **Retail Media Network**: Targeted in-store and online advertising opportunities

### For CPG Suppliers (Operational Insights):
- **Product Performance Dashboard**:
  - Real-time sales velocity by SKU, store, and region
  - Inventory levels and days of supply
  - Out-of-stock alerts with lost sales estimates
  - Competitive set benchmarking (anonymized)

- **Promotion Effectiveness**:
  - Lift analysis for trade promotions and displays
  - Week-over-week performance comparison
  - ROI on promotional investments
  - Geographic hotspots and underperforming markets

- **Supply Chain Collaboration**:
  - Visibility into retailer DC inventory levels
  - Reorder forecasts and replenishment schedules
  - Lead time tracking and on-time delivery metrics
  - Collaborative demand planning

### Example Scenarios:

**Scenario 1: Stockout Alert**
- CPG supplier receives real-time alert: "Product XYZ out of stock in 5 Northeast stores"
- Dashboard shows lost sales estimate: $12K in past 24 hours
- Supplier expedites shipment to regional DC
- Retailer charges premium fee for real-time alert service

**Scenario 2: Promotion Analysis**
- CPG runs 2-week promotional display campaign
- Supplier logs into portal to view daily lift metrics
- Discovers 40% sales lift in urban stores vs 15% in suburban
- Reallocates display inventory to higher-performing markets mid-campaign

**Scenario 3: New Product Launch Monitoring**
- CPG launches new SKU across 100 stores
- Real-time dashboard tracks adoption: sell-through rate, repeat purchases, returns
- Supplier identifies underperforming stores and provides merchandising support
- Retailer monetizes this intelligence as premium service

### Technical Implementation (Planned):

**Data Governance**:
- Row-level security in Lakehouse (supplier only sees their products)
- Anonymized competitive benchmarks (no specific brand identification)
- Aggregated data only (no PII, no individual transactions)

**Portal Access**:
- Embedded Power BI reports via Fabric Semantic Model
- API access for suppliers to integrate into their own systems
- Scheduled daily/weekly summary emails

**Querysets for Suppliers**:
- `SupplierSalesPerformance`: Sales by SKU, store, day
- `SupplierInventoryStatus`: Current stock levels and days of supply
- `SupplierPromotionLift`: Comparison of promo vs base periods
- `SupplierStockoutLog`: Historical out-of-stock incidents

**Pricing Models**:
- Basic Tier (Free): Weekly aggregated sales summary
- Standard Tier ($): Daily dashboard access, monthly reports
- Premium Tier ($$): Real-time alerts, API access, promotion analytics
- Enterprise Tier ($$$): Demand forecasting collaboration, joint business planning

### Business Value:

**For Retailers**:
- New high-margin revenue stream (similar to Walmart Connect, Kroger 84.51°)
- Strengthened supplier relationships through transparency
- Reduced stockouts via faster supplier response

**For CPG Suppliers**:
- Faster reaction to market changes (hours vs weeks)
- Improved promotion ROI through data-driven optimization
- Better demand forecasting with retailer collaboration
- Reduced lost sales from stockouts

**Industry Precedent**:
- **Kroger 84.51°**: ~$100M+ annual revenue from CPG data services
- **Walmart Luminate**: Supplier portal with real-time sales and inventory insights
- **Target Roundel**: Retail media network leveraging customer data

**Status**: Future enhancement (Phase 5). Data contracts support this use case, but multi-tenant portal and pricing model not yet implemented.

---

Event types powering these use cases map directly to KQL tables and Lakehouse Silver/Gold aggregates.

