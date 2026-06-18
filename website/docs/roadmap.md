# Roadmap

This roadmap outlines the phased implementation of the Microsoft Fabric Real-Time Intelligence retail demo, with estimated timelines and acceptance criteria.

---

## Phase 1 — Scaffolding ✅ COMPLETE (Oct 2024)

**Status**: Complete

**Deliverables**:
- ✅ Create `fabric/*` asset folders with build specs (README, AGENTS, CLAUDE per directory)
- ✅ Initialize docs site (now Docusaurus) and update root guides
- ✅ Define data contracts in datagen (schemas, event types, fact tables)
- ✅ Implement data generator (master data, historical facts, real-time streaming)

**Acceptance Criteria**:
- All documentation structure in place
- Data generator produces realistic synthetic data
- Streaming notebook writes directly to Eventhouse/KQL tables

---

## Phase 2 — Ingestion ✅ COMPLETE

**Deliverables**:
- [x] Deploy Fabric workspace and Real-Time Intelligence capacity
- [x] Define KQL database tables, one per event type (`fabric/kql_database/01-create-tables.kql`)
- [x] Create ingestion mappings, JSON → KQL columns (`02-create-ingestion-mappings.kql`)
- [x] Lakehouse Bronze shortcuts to Eventhouse tables and ADLS parquet (`01-create-bronze-shortcuts.ipynb`)
- [x] Implement direct Eventhouse writes from `stream-events.ipynb` via the Fabric Spark connector for Kusto
- [ ] Validate end-to-end ingestion with `stream-events.ipynb` in a Fabric workspace

**Acceptance Criteria**:
- Events flowing from `stream-events` → Eventhouse/KQL in <5 seconds
- KQL tables populated with correct schema and data types
- Bronze layer receiving Eventhouse table shortcuts for each event type
- Zero data loss during 24-hour continuous streaming test

**Dependencies**:
- Fabric workspace with RTI capacity (F64 or higher recommended)
- Eventhouse default KQL database `retail_eventhouse`

---

## Phase 3 — Analytics ✅ COMPLETE (dashboard publishing is a deployment step)

**Deliverables**:
- [x] Materialized views for core KPIs (`04-create-materialized-views.kql`): `mv_store_sales_minute`, `mv_top_products_15m`, `mv_sales_product_minute`, `mv_tender_mix_15m`, `mv_zone_dwell_minute`
- [x] Querysets for each use case domain (`fabric/querysets/` — sales, tender mix, stockouts, online orders, fulfillment, marketing, BLE presence, zone dwell, truck dwell)
- [x] Real-Time Dashboard templates (`fabric/dashboards/retail-ops.template.json`, `pricing-approval.template.json`)
- [x] KQL functions for common calculations (`03-create-functions.kql`: `fn_attribution_window`, `fn_truck_sla`)

**Acceptance Criteria**:
- Dashboard tiles refresh in <2 seconds for 7-day queries
- Querysets validated against expected business questions
- 5+ operational dashboards covering all use cases
- Dashboard accessible to non-technical users (parameterized filters)

**Dependencies**:
- Phase 2 complete (KQL tables populated)
- Sample queries documented in querysets/

---

## Phase 4 — Medallion & History ✅ COMPLETE (semantic model publishing is a deployment step)

**Deliverables**:
- [x] Lakehouse with Bronze (shortcuts), Silver (`ag`), and Gold (`au`) schemas
- [x] Delta tables in Silver layer — 6 dimensions + `dim_date` + 18 fact tables
- [x] Bronze → Silver transforms for all 18 streaming event types (`03-streaming-to-silver.ipynb`, watermark-based)
- [x] Gold aggregates (`02-historical-data-load.ipynb`, `04-streaming-to-gold.ipynb`): sales, inventory positions, truck dwell, online sales, zone dwell, marketing cost, tender mix
- [x] Data Pipelines to orchestrate transforms (`fabric/pipelines/`: historical-data-load, streaming-data-load, daily-maintenance, machine-learning)
- [x] Semantic model authored as PBIP with curated DAX measures, hierarchies, and relationships (`fabric/powerbi/retail_model.pbip`); Power BI report rebuilt with SLT, supply chain, DC, store, regional, omnichannel, customer/marketing, pricing/promotion, and logistics pages
- [ ] Publish semantic model to workspace — *manual deployment step*
- [ ] Enable Fabric Copilot on KQL database and validate NL queries

**Acceptance Criteria**:
- Silver tables match historical fact schemas from datagen
- Gold aggregates update daily via pipelines
- Semantic Model supports both real-time and historical analysis
- Copilot correctly interprets 10+ common business questions
- Data retention: KQL 14 days, Silver 1 year, Gold 3+ years

**Dependencies**:
- Phase 3 complete (queries validated)
- Bronze layer accumulating data

---

## Phase 5 — Actions, AI & Advanced Features (In Progress)

**Deliverables**:

### Real-Time Rules & Alerts:
- [x] Alert rule query definitions (`fabric/rules/definitions.kql`: urgent reorders, stockouts, truck delays)
- [ ] Wire rules to email/Teams notifications via Activator (deployment step)
- [ ] Sales anomaly alerting (>20% deviation from forecast)
- [ ] Customer zone dwell alerts (potential service issues)

### AI & Machine Learning:
- [x] Train demand forecasting models — `06-ml-demand-forecast.ipynb` (GBT, 14-day horizon per SKU)
- [x] Market basket analysis — `07-ml-market-basket.ipynb` (FP-Growth, cross-sell recommendations)
- [x] Customer segmentation — `08-ml-customer-segmentation.ipynb` (RFM + K-means)
- [x] Customer churn prediction — `09-ml-churn-prediction.ipynb` (Spark ML GBTClassifier, AUC-ROC >0.75)
- [x] Promotion effectiveness analysis — `10-ml-promotion-effectiveness.ipynb` (price elasticity, promo lift)
- [x] In-store journey analysis — `11-ml-journey-analysis.ipynb` (BLE beacon path analysis)
- [x] Stockout prediction — `12-ml-stockout-prediction.ipynb` (Spark ML GBTClassifier, 3-day forecast)
- [x] Delivery time prediction — `13-ml-delivery-prediction.ipynb` (Spark ML GBTRegressor with empirical prediction intervals)
- [x] Dynamic pricing optimization — `14-ml-dynamic-pricing.ipynb` (elasticity-aware pricing recommendations)
- [x] Anomaly detection on sales and inventory streams — `06-ml-anomaly-detection.kql` (`fn_detect_transaction_velocity_anomalies`, `fn_detect_basket_size_anomalies`, `fn_detect_inventory_movement_anomalies`, `fn_detect_payment_anomalies`, `fn_detect_traffic_anomalies`, `fn_detect_all_anomalies`)
- [ ] Integrate Azure OpenAI for auto-generated executive summaries
- [ ] Build recommendation scoring pipeline (next-best-product)

### Advanced Use Cases:
- [ ] CPG supplier collaboration portal (row-level security, embedded dashboards)
- [ ] Retail media network tracking (ad revenue, impression-to-purchase)
- [x] Dynamic pricing approval workflow — `07-pricing-approval-tables.kql` (recommendation/approval event tables, `mv_pending_recommendations`, `mv_pricing_approval_metrics`) and `fabric/dashboards/pricing-approval.template.json`
- [ ] Closed-loop dynamic pricing deployment (automated price activation and feedback loops)
- [ ] External data integration (weather API, social sentiment feeds)

### Copilot Enhancements:
- [ ] Natural language query refinement and suggestions
- [ ] AI-powered root cause analysis for anomalies
- [ ] Voice-activated queries (optional, mobile app integration)

**Acceptance Criteria**:
- 5+ alert rules deployed and firing correctly
- ML models achieving >80% accuracy on validation set
- Copilot NL success rate >70% (useful answer)
- CPG supplier portal supports 3+ user roles with different permissions
- Dynamic pricing recommendations tested in simulation mode

**Dependencies**:
- Phase 4 complete (Semantic Model, Copilot enabled)
- 60+ days historical data for model training
- Azure OpenAI Service provisioned

---

## Phase 6 — Future Enhancements (Q3 2025+)

**Exploratory / Customer-Driven**

### Potential Additions:
- **Sustainability Tracking**: Carbon footprint calculation for logistics and refrigerated transport
- **Computer Vision**: Shelf compliance monitoring via IoT cameras
- **Geospatial Analytics**: Store performance heatmaps, trade area analysis
- **Reinforcement Learning**: Autonomous pricing and inventory optimization
- **Multi-Tenant SaaS**: Scale CPG supplier portal to 100+ external users
- **Mobile Apps**: Store manager iOS/Android apps with real-time alerts
- **Voice Analytics**: Call center sentiment analysis linked to in-store behavior

### Acceptance Criteria:
- Driven by customer feedback and industry trends
- Business case validated before investment

---

## Timeline Summary

| Phase | Status | Key Milestone |
|-------|--------|---------------|
| Phase 1 | ✅ Complete | Scaffolding & Data Generator |
| Phase 2 | ✅ Complete | Ingestion (stream-events → Eventhouse/KQL) |
| Phase 3 | ✅ Complete (dashboard publishing manual) | Analytics (Dashboards & Querysets) |
| Phase 4 | ✅ Complete (semantic model publishing manual) | Medallion & Semantic Model |
| Phase 5 | 🔨 In progress | AI, Alerts & Advanced Features |
| Phase 6 | Ongoing | Future Enhancements |

---

## Success Metrics

### Technical Metrics:
- Data ingestion latency: <5 seconds (generator → dashboard)
- Query performance: <2 seconds for real-time tiles
- Data quality: >99.9% schema compliance, zero duplicates
- System uptime: >99.5% availability

### Business Metrics:
- Demo effectiveness: >80% audience engagement in presentations
- Use case coverage: All 5 PDF whitepaper themes addressed
- AI adoption: >50% of demo users interact with Copilot
- Extensibility: New event type added in <1 day (end-to-end)

### Adoption & Feedback:
- Internal demo sessions: 10+ completed
- Customer feedback score: >4.0/5.0
- Sales pipeline influenced: Track opportunities citing this demo
- Community engagement: GitHub stars, forks, blog posts

---

## Risk Mitigation

| Risk | Mitigation Strategy |
|------|---------------------|
| Fabric capacity constraints | Start with F64, scale to F128 if needed |
| Streaming throughput | Tune `source_rows_per_second`, Spark resources, and Eventhouse capacity |
| ML model accuracy | Start with simple baselines, iterate with domain experts |
| Copilot limited availability | Have fallback demo flow without Copilot, prioritize Phase 5 |
| Timeline slippage | Phase 2-3 are MVP, Phase 4-5 can flex based on feedback |

---

## Resources Required

**Phase 2-3**:
- 1 Fabric engineer (full-time, 2 months)
- 1 Data analyst for queryset validation (part-time)
- Fabric capacity: F64 or F128

**Phase 4-5**:
- 1 Fabric engineer (full-time, 3 months)
- 1 Data scientist for ML models (full-time, 1.5 months)
- 1 UX designer for dashboards (part-time)
- Azure OpenAI Service quota (GPT-4)

---

**Next Action**: Complete semantic model publishing, validate `stream-events` direct Eventhouse ingestion in the workspace, then continue Phase 5 alerting and Copilot work.

