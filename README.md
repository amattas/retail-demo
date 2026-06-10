# Microsoft Fabric Real-Time Intelligence - Retail Demo

> **⚠️ PROJECT STATUS**: This project is currently in early development. Both the data generator and Microsoft Fabric artifacts are works in progress. See [Development Status](#development-status) for details.

A comprehensive demonstration showcasing Microsoft Fabric's Real-Time Intelligence capabilities for the retail and consumer products goods industry. This project combines a sophisticated data generator with Fabric artifacts to deliver actionable, real-time insights across critical retail operations.

## Overview

This demo **will illustrate** how Microsoft Fabric's Real-Time Intelligence features enable retailers to make data-driven decisions at the speed of business. By combining streaming data ingestion, real-time analytics, and AI-powered insights, retailers can optimize operations, enhance customer experiences, and drive competitive advantage.

**Current Status**: The Python-based data generator is under active development. Fabric scaffolding is now in place under `fabric/` with specs for Eventstream, KQL DB, Querysets, Rules, Dashboards, Lakehouse, Pipelines, Notebooks, and a Semantic Model. See the Docs site under `docs/` (MkDocs) for architecture and plans.

## Key Scenarios (Planned)

The completed demo will showcase four critical retail scenarios powered by Microsoft Fabric Real-Time Intelligence:

### 1. Supply Chain Control Center
Monitor and optimize your supply chain in real-time:
- Live inventory tracking across warehouses and distribution centers
- Predictive stock-out alerts and automated replenishment triggers
- Real-time supplier performance monitoring
- Logistics optimization with live shipment tracking

### 2. Omni-Channel Fulfillment Visibility
Gain complete transparency across all fulfillment channels:
- Real-time order status across online, in-store, and mobile channels
- Dynamic inventory allocation and routing
- Live BOPIS (Buy Online, Pick-up In Store) performance metrics
- Same-day delivery tracking and optimization

### 3. Personalized Customer Engagement
Deliver hyper-personalized experiences powered by real-time data:
- Live customer behavior tracking and segmentation
- Real-time product recommendations
- Dynamic pricing and personalized promotions
- Instant customer sentiment analysis

### 4. AI-Driven Insights and Assistance
Leverage AI for intelligent decision-making:
- Anomaly detection in sales patterns and inventory levels
- Predictive analytics for demand forecasting
- Natural language queries against real-time data
- Automated alert generation and intelligent recommendations

## Architecture

### Data Generator
*[In Development]*

A Python-based data generator designed to produce realistic retail events including:
- **Transactional Data**: Point-of-sale transactions, online orders, returns
- **Inventory Events**: Stock movements, transfers, adjustments
- **Customer Interactions**: Browsing behavior, cart events, loyalty activities
- **Supply Chain Events**: Shipments, deliveries, supplier updates
- **Marketing Events**: Campaign interactions, promotional responses

Planned features:
- Realistic seasonal patterns and trends
- Multi-brand support with brand-specific behaviors
- Configurable data volumes and streaming rates
- RESTful API for on-demand generation
- Real-time streaming to Azure Event Hubs

### Microsoft Fabric Components
Scaffolded in `fabric/` with build specs:
- **Eventstream**: Ingest and route real-time retail events from Azure Event Hubs → KQL DB + Lakehouse Bronze
- **KQL Database**: Hot-path tables, ingestion mappings, materialized views
- **Querysets**: Curated KQL queries for dashboards and investigations
- **Real-Time Rules**: Alerts/actions for stockouts, reorders, dwell breaches
- **Dashboards**: Real-time operational views over KQL + historical overlays
- **Lakehouse**: Bronze/Silver/Gold medallion layers and transforms
- **Pipelines**: Orchestration for medallion and maintenance
- **Notebooks**: Transformations, ML, and exploratory analysis
- **Semantic Model**: Power BI model (hybrid KQL + Lakehouse)

## Getting Started

### Current Prerequisites
- Python 3.9+
- Azure subscription (for Event Hubs integration - optional)

### Future Prerequisites
- Microsoft Fabric workspace (required once Fabric artifacts are complete)

### Installation

```bash
# Clone the repository
git clone https://github.com/amattas/retail-demo.git
cd retail-demo

# Set up the data generator
cd datagen
pip install -e .

# Configure your environment (from datagen directory)
cp .env.example .env
# Edit .env with your Azure credentials (Event Hub connection string, etc.)

# Optional: local docs preview (requires mkdocs)
# pip install mkdocs mkdocs-material
# mkdocs serve
```

### Running the Data Generator

**Note**: The data generator is still in development. The examples below show the planned API usage.

```bash
# Start the FastAPI server (when available)
uvicorn retail_datagen.main:app --reload

# Generate sample data via API
curl -X POST http://localhost:8000/api/generate/transactions \
  -H "Content-Type: application/json" \
  -d '{"count": 1000, "brand": "TechStyle"}'

# Start real-time streaming
curl -X POST http://localhost:8000/api/streaming/start \
  -H "Content-Type: application/json" \
  -d '{"stream_type": "transactions", "rate": 100}'
```

## Project Structure

```
retail-demo/
├── datagen/                    # Data generation engine
│   ├── src/retail_datagen/
│   │   ├── api/               # FastAPI endpoints
│   │   ├── generators/        # Data generation logic
│   │   ├── streaming/         # Event Hub integration
│   │   ├── config/            # Configuration management
│   │   └── shared/            # Shared utilities
│   └── tests/                 # Comprehensive test suite
└── fabric/                    # Fabric artifacts and specs
    ├── eventstream/           # Eventstream definitions and mappings
    ├── kql_database/          # KQL DB tables, policies, views
    ├── querysets/             # Curated KQL queries
    ├── rules/                 # Real-time alerts/actions
    ├── dashboards/            # Real-time dashboards
    ├── lakehouse/             # Medallion structures and transforms
    ├── pipelines/             # Orchestration and maintenance
    ├── notebooks/             # Transform and analysis notebooks
    └── powerbi/               # Power BI hybrid model
```

## Development Status

### ✅ PHASE 1: COMPLETE (Oct 2024)

**Data Generator** (datagen/):
- ✅ Master data generation (stores, customers, products, DCs, trucks)
- ✅ Historical fact generation (9 fact tables with temporal patterns)
- ✅ Real-time streaming to Azure Event Hubs (15+ event types)
- ✅ Online orders integration (historical + streaming lifecycle)
- ✅ FastAPI web interface with progress tracking
- ✅ Marketing attribution and customer session orchestration
- ✅ Supply chain simulation (DC → Truck → Store flows)

**Project Scaffolding**:
- ✅ Fabric asset folders with build specs (`fabric/*`)
- ✅ MkDocs documentation site
- ✅ Data contracts and schemas defined

### 📋 PHASE 2-5: PLANNED (Dec 2024 - Apr 2025)

See [docs/roadmap.md](docs/roadmap.md) for detailed timeline.

**Next Up** (Phase 2 - Target: Dec 2024):
- Eventstream → KQL database + Lakehouse Bronze wiring
- KQL table definitions and ingestion mappings
- End-to-end data flow validation

**Future Phases**:
- Phase 3 (Jan 2025): Real-Time Dashboards, Querysets, Materialized Views
- Phase 4 (Feb 2025): Lakehouse Silver/Gold medallion, Semantic Model, Copilot enablement
- Phase 5 (Mar-Apr 2025): Real-time alerts/actions, AI models, CPG supplier portal

## Use Cases by Industry Segment (Planned)

Once complete, the demo will support industry-specific scenarios:

### Fashion & Apparel
- Real-time trend detection from social media and sales data
- Dynamic markdown optimization based on inventory velocity
- Size and style recommendation engines

### Grocery & Food Retail
- Fresh product lifecycle management
- Real-time demand sensing for perishables
- Dynamic delivery route optimization

### Electronics & Technology
- Product launch monitoring and inventory allocation
- Warranty and support trend analysis
- Competitive pricing intelligence

### Omni-Channel Retail
- Unified commerce analytics across all touchpoints
- Real-time cart abandonment intervention
- Cross-channel attribution and customer journey mapping

## Contributing

This is a demonstration project currently in active development. Contributions, suggestions, and feedback are welcome!

## License

[Specify your license here]

## Resources

- [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
- [Real-Time Intelligence Overview](https://learn.microsoft.com/fabric/real-time-intelligence/)
- [KQL Query Language Reference](https://learn.microsoft.com/azure/data-explorer/kusto/query/)
- [Eventstreams Documentation](https://learn.microsoft.com/fabric/real-time-intelligence/event-streams/)
 - [MkDocs](https://www.mkdocs.org/)

## Contact

For questions or feedback about this demo, please [open an issue](../../issues) or contact the project maintainer.

---

**Note**: This project is in early development. Both the data generator and Microsoft Fabric Real-Time Intelligence artifacts are actively being built. This demonstration project is designed to showcase Microsoft Fabric Real-Time Intelligence capabilities. All generated data is synthetic and for illustrative purposes only.
