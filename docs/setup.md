# Setup

Prerequisites:
- Azure Event Hubs (hub: `retail-events`) for streaming
- Microsoft Fabric workspace with Real-Time Intelligence

Steps (high-level):
- Deploy Eventstream and connect Event Hubs source
- Create KQL DB and wire Eventstream sinks (KQL + Lakehouse Bronze)
- Define KQL tables and ingestion mappings
- Create Lakehouse and folders for Bronze/Silver/Gold
- Build Querysets and Real-Time Dashboards
- Define alert Rules and delivery channels

Local Docs:
- Install: `pip install mkdocs mkdocs-material`
- Serve: `mkdocs serve`

