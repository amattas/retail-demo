# Roadmap

This roadmap outlines the phased implementation of the Microsoft Fabric Real-Time Intelligence retail demo, with estimated timelines and acceptance criteria.

---

## Phase 1 — Scaffolding ✅ COMPLETE (Oct 2024)

**Status**: Complete

**Deliverables**:
- ✅ Create `fabric/*` asset folders with build specs (README, AGENTS, CLAUDE per directory)
- ✅ Initialize MkDocs docs site and update root guides
- ✅ Define data contracts in datagen (schemas, event types, fact tables)
- ✅ Implement data generator (master data, historical facts, real-time streaming)

**Acceptance Criteria**:
- All documentation structure in place
- Data generator produces realistic synthetic data
- Streaming to Azure Event Hubs functional

---

## Phase 2 — Ingestion (In Progress - Target: Dec 2024)

**Timeline**: 4-6 weeks

**Deliverables**:
- [ ] Deploy Fabric workspace and Real-Time Intelligence capacity
- [ ] Create Eventstream resource and connect to Azure Event Hubs (`retail-events`)
- [ ] Wire Eventstream to dual sinks: KQL database + Lakehouse Bronze
- [ ] Define KQL database tables (one per event type)
- [ ] Create ingestion mappings (JSON → KQL columns)
- [ ] Validate end-to-end ingestion with data generator

**Acceptance Criteria**:
- Events flowing from generator → Event Hubs → Fabric in <5 seconds
- KQL tables populated with correct schema and data types
- Bronze layer receiving raw JSON partitioned by event_type and date
- Zero data loss during 24-hour continuous streaming test

**Dependencies**:
- Azure Event Hubs namespace provisioned
- Fabric workspace with RTI capacity (F64 or higher recommended)

---

## Phase 3 — Analytics (Target: Jan 2025)

**Timeline**: 3-4 weeks

**Deliverables**:
- [ ] Create materialized views for core KPIs:
  - Sales: receipts/minute, top products, tender mix
  - Inventory: current levels, stockout flags, reorder signals
  - Logistics: truck dwell, on-time arrivals
  - Customer: zone occupancy, conversion rates
  - Marketing: attribution lift, ROAS
- [ ] Build Querysets for each use case domain
- [ ] Design and publish initial Real-Time Dashboards
- [ ] Add KQL functions for common calculations (30-day moving average, percentiles)

**Acceptance Criteria**:
- Dashboard tiles refresh in <2 seconds for 7-day queries
- Querysets validated against expected business questions
- 5+ operational dashboards covering all use cases
- Dashboard accessible to non-technical users (parameterized filters)

**Dependencies**:
- Phase 2 complete (KQL tables populated)
- Sample queries documented in kql_database/querysets/

---

## Phase 4 — Medallion & History (Target: Feb 2025)

**Timeline**: 4-5 weeks

**Deliverables**:
- [ ] Create Lakehouse with Bronze/Silver/Gold folder structure
- [ ] Build Delta tables in Silver layer (typed fact and dimension tables)
- [ ] Implement transforms: Bronze → Silver (cleaning, enrichment)
- [ ] Build Gold aggregates (daily/weekly rollups, customer segments)
- [ ] Create Data Pipelines to orchestrate transforms (scheduled + event-driven)
- [ ] Build Semantic Model unifying KQL (DirectQuery) + Lakehouse (Import)
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

## Phase 5 — Actions, AI & Advanced Features (Target: Mar-Apr 2025)

**Timeline**: 6-8 weeks

**Deliverables**:

### Real-Time Rules & Alerts:
- [ ] Stockout detection rules with email/Teams notifications
- [ ] Reorder trigger rules with automated purchase order generation
- [ ] Truck delay alerts (>30 min past ETA)
- [ ] Sales anomaly detection (>20% deviation from forecast)
- [ ] Customer zone dwell alerts (potential service issues)

### AI & Machine Learning:
- [ ] Train demand forecasting models (7-14 day horizon per SKU)
- [ ] Deploy anomaly detection models on sales and inventory streams
- [ ] Integrate Azure OpenAI for auto-generated executive summaries
- [ ] Build recommendation scoring pipeline (next-best-product)
- [ ] Implement customer churn prediction model

### Advanced Use Cases:
- [ ] CPG supplier collaboration portal (row-level security, embedded dashboards)
- [ ] Retail media network tracking (ad revenue, impression-to-purchase)
- [ ] Dynamic pricing optimization (markdown recommendations) [NEW]
- [ ] External data integration (weather API, social sentiment feeds) [NEW]

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

| Phase | Target Completion | Duration | Key Milestone |
|-------|------------------|----------|---------------|
| Phase 1 | Oct 2024 ✅ | Complete | Scaffolding & Data Generator |
| Phase 2 | Dec 2024 | 4-6 weeks | Ingestion (Event Hubs → Fabric) |
| Phase 3 | Jan 2025 | 3-4 weeks | Analytics (Dashboards & Querysets) |
| Phase 4 | Feb 2025 | 4-5 weeks | Medallion & Semantic Model |
| Phase 5 | Mar-Apr 2025 | 6-8 weeks | AI, Alerts & Advanced Features |
| Phase 6 | Q3 2025+ | Ongoing | Future Enhancements |

**Total Estimated Timeline**: ~5 months (Phase 1-5)

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
| Event Hub throttling | Use partitioning, monitor metrics, request quota increase |
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

**Next Action**: Kick off Phase 2 with Fabric workspace provisioning and Eventstream deployment.

