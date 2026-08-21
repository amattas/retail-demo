# Microsoft Fabric Retail Demo

## About this demo

This demo shows how a retailer can bring sales, inventory, customer, marketing,
fulfillment, and supply-chain data together in Microsoft Fabric. All data is
synthetic, so you can use the solution for learning and demonstrations without
exposing real customer or company records.

The solution supports two complementary views of the business:

- **Historical analysis:** repeatable sales and operations history stored in a
  Lakehouse and presented through Power BI.
- **Optional live operations:** generated retail events written to Eventhouse
  and queried with KQL (Kusto Query Language).

It also includes machine-learning outputs, a business ontology, and Data
Agents. These extensions demonstrate how the same governed data can support
forecasts, risk signals, business relationships, and natural-language
questions.

## Choose the path that matches your role

| If you are... | Start with... | What you will learn |
| --- | --- | --- |
| A business user or executive | [Use cases](guides/use-cases.md) | Which retail questions the demo can answer and which capabilities are optional. |
| A business analyst | [Deployed walkthrough](guides/deployed-walkthrough.md) | How data moves from operations into Power BI, machine learning, and conversational experiences. |
| A presenter | [Presenter demo](guides/demo-script.md) | A supported talk track, evidence checks, and statements to avoid. |
| Setting up the demo for the first time | [Getting started](guides/getting-started.md) | Prerequisites, profiles, configuration, deployment, and first validation. |
| Operating an existing workspace | [Operations](guides/operations.md) | Readiness results, freshness, recovery, and safe reruns. |
| An entry-level developer | [Plain-language glossary](guides/glossary.md), then [Design overview](design/README.md) | Fabric terminology first, then components, contracts, and implementation boundaries. |

Not sure which guide you need? Use the [guide index](guides/README.md).

## What the demo contains

| Business capability | What you can demonstrate | Main Fabric area |
| --- | --- | --- |
| Sales and margin analysis | Compare stores, products, channels, and periods | Lakehouse and Power BI |
| Inventory and replenishment | Review movements, stockout signals, reorder activity, and current position | Lakehouse, Eventhouse, and Power BI |
| Omnichannel fulfillment | Follow online demand and fulfillment events alongside store sales | Lakehouse, Eventhouse, and Power BI |
| Marketing and customer analysis | Explore campaigns, attribution, customer segments, and churn risk | Lakehouse, machine learning, and Power BI |
| Optional live operations | Query recent generated events and operational summaries | Eventhouse and KQL |
| Business context and questions | Navigate Store, Product, Customer, and Receipt concepts or ask grounded questions | Ontology and Data Agents |

The base historical contract contains seven dimensions, nineteen fact tables,
and ten Gold aggregates. The live driver emits eighteen business event types;
KQL adds an `unknown_event` catch-all for unexpected event types. The active
Power BI semantic model contains 40 tables. Optional machine learning,
ontology, dashboard, rule, and agent experiences have separate readiness
checks so that an optional failure does not silently invalidate required
reporting.

## Learn more

- [Guide index](guides/README.md) explains every task-focused guide.
- [Plain-language glossary](guides/glossary.md) defines Fabric, data, reporting,
  deployment, streaming, and machine-learning terms.
- [Design documentation](design/README.md) contains architecture, requirements,
  exact specifications, and security controls for contributors.
