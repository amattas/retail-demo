# Semantic Model

Power BI semantic model for unified analytics. Hybrid model over KQL (hot) and Lakehouse Gold (history).

Sources:
- KQL views for near-real-time tiles (DirectQuery)
- Lakehouse Gold for historical context (Import)

Entities:
- Sales, Inventory, Logistics, Marketing, Customers, Products, Stores

Measures:
- Sales (min/hour/day), Units, Margin, DOS, Dwell, On-time %, ROAS

