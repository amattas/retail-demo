# Plain-language glossary

Use this page when a guide, screenshot, or Fabric screen uses an unfamiliar
term. The definitions describe how each term is used in this demo rather than
every feature the Microsoft Fabric product supports.

## Business and reporting terms

| Term | Plain-language meaning |
| --- | --- |
| **Business measure** | A reusable calculation, such as net sales or gross margin, with one agreed definition. Power BI and Data Agents use measures so that answers are based on the same business rules. |
| **Dimension** | Descriptive data used to group or filter results. Examples include Store, Product, Customer, and Date. |
| **Fact** | A record of something that happened or changed, such as a receipt, payment, inventory movement, or online order. |
| **Grain** | What one row represents. For example, “one row per receipt” and “one row per product on a receipt” are different grains. |
| **KPI** | Key performance indicator. A KPI is a measure used to monitor an outcome, such as sales, margin, stockout risk, or fulfillment time. |
| **ROAS** | Return on ad spend. It compares attributed sales with advertising cost. Attribution is an estimate, so ROAS should be presented with its time window and attribution rules. |
| **Semantic model** | The reusable Power BI data layer that defines tables, relationships, measures, and business-friendly names. It is often called a dataset in everyday Power BI conversation. |
| **Synthetic data** | Generated demonstration data that resembles business records but does not describe real customers, employees, stores, or transactions. |

## Microsoft Fabric terms

| Term | Plain-language meaning |
| --- | --- |
| **Capacity** | The Fabric computing resources that run notebooks, queries, pipelines, and reports. Larger or busier demos need more capacity. |
| **Activator** | Fabric's event-driven alerting and action service. The repository contains rule ideas, but the default deployment does not publish a complete Activator workflow. |
| **Data Agent** | A Fabric conversational experience that answers natural-language questions using approved data sources. An answer still needs to be checked against its stated period, measures, and source data. |
| **Delta table** | A table stored in the Delta Lake format. Delta adds schema and transaction history to data files so that Spark and Fabric can update them safely. |
| **Direct Lake** | A Power BI connection mode that reads Fabric Lakehouse data directly from OneLake instead of copying it into a separate imported dataset. |
| **Eventhouse** | Fabric storage and query technology for high-volume, time-sensitive event data. This demo uses it for optional live retail events. |
| **Fabric workspace** | The shared Fabric area that contains the demo's Lakehouse, Eventhouse, notebooks, pipelines, reports, ontology, and other items. |
| **KQL** | Kusto Query Language, the query language used for Eventhouse data. KQL is designed for time-based events, logs, and operational analysis. |
| **KQL queryset** | A saved collection of KQL query tabs connected to an Eventhouse database. The demo deploys one queryset for repeatable operational questions. |
| **Lakehouse** | Fabric storage that combines data-lake files with table and SQL experiences. This demo stores durable historical and analytical tables in a Lakehouse. |
| **OneLake** | The organization-wide storage layer used by Microsoft Fabric. A Fabric Lakehouse stores its files and tables in OneLake. |
| **Ontology** | A business map that connects concepts such as Store, Product, Customer, and Receipt to the underlying data. It helps people and agents navigate data using business language. |
| **Power BI Project (PBIP)** | The folder-based, source-control-friendly format used for the checked-in Power BI report and semantic model. |
| **Real-Time Intelligence (RTI)** | The Fabric workload for event-driven analysis. Eventhouse, KQL querysets, dashboards, and Activator are part of this area. |
| **Spark** | A distributed data-processing engine used by Fabric notebooks. The setup and machine-learning notebooks use Spark to create or transform data. |
| **Task flow** | A visual workspace map that groups related Fabric items and shows how a user can move through the solution. It is a navigation aid, not proof that a process ran successfully. |
| **TMDL** | Tabular Model Definition Language, the text format used to define the Power BI semantic model in source control. |
| **Materialized view** | A continuously maintained query result stored by Eventhouse. It makes repeated summaries, such as sales by minute, faster to query. |

## Data-layer terms

| Term | Plain-language meaning |
| --- | --- |
| **Bronze** | The first data layer. It keeps source-shaped data with minimal changes. In this demo, optional Eventhouse shortcuts expose live event tables to Spark through the `cusn` schema. |
| **Silver** | The cleaned, typed, and consistently named data layer. This demo stores Silver tables in the `ag` schema. |
| **Gold** | The business-ready analytical layer. It contains summaries and model outputs designed for reporting or analysis. This demo stores Gold tables in the `au` schema. |
| **`ag` schema** | The short schema name used for Silver tables in this demo. For example, `ag.fact_receipts` contains durable receipt history. |
| **`au` schema** | The short schema name used for Gold tables and machine-learning outputs in this demo. For example, `au.sales_minute_store` contains summarized store sales. |
| **`cusn` schema** | The Lakehouse schema used for read-only shortcuts to Eventhouse tables. It lets Spark notebooks query live event data without copying it first. |
| **Medallion architecture** | A common way to organize data as Bronze, Silver, and Gold layers. Each layer adds structure and business usefulness. |
| **Shortcut** | A Fabric reference to data stored elsewhere. A shortcut makes data visible without creating another physical copy. |
| **Watermark** | A saved progress marker. `ag._watermarks` records how far a streaming transformation has processed so that the next run can continue safely. |

## Deployment and operations terms

| Term | Plain-language meaning |
| --- | --- |
| **Azure CLI** | Microsoft's command-line sign-in and management tool. The guided setup uses its signed-in identity to access the configured tenant. |
| **CI/CD** | Continuous integration and continuous delivery. In this repository, automated checks validate changes and deployment tooling publishes source-controlled Fabric items. |
| **Deployment profile** | A named package of demo capabilities. `core` is the smallest data-only profile, `standard` adds reporting and live-event assets, and `full-demo` adds preview and manually completed experiences. |
| **`fabric-cicd`** | Microsoft's open-source Python library for publishing source-controlled Fabric items into a workspace. |
| **Pipeline** | A repeatable sequence of activities. In this demo, pipelines run setup, transformations, and machine-learning notebooks in a controlled order. |
| **Preflight** | Checks performed before deployment changes Fabric. Preflight verifies configuration, sign-in context, capacity, tenant settings, and the intended target. |
| **REST API** | A web interface used by software to read or change Fabric resources. Deployment uses authenticated Fabric REST APIs for items, jobs, capacity checks, and readiness evidence. |
| **SKU or capacity tier** | The named size of a Fabric capacity, such as F64. The tier limits how much compute can run at one time. |
| **Terraform** | Infrastructure-as-code software used to create or resolve the workspace, Lakehouse, Eventhouse, capacity assignment, and related resources. |
| **Tenant** | The Microsoft Entra organization directory that owns identities, Fabric settings, capacities, and workspaces. |
| **Terminal success** | A run finished with a final `Completed` state. A successful request to start a pipeline is not terminal success because the work may still fail later. |
| **vCore** | Virtual processor core. Spark pool sizes and Fabric capacity limits are often expressed as vCores. |
| **`SUCCEEDED`** | Every selected required and optional readiness check passed. |
| **`DEGRADED`** | Required capabilities passed, but at least one optional capability has failed, stale, or missing evidence. The required demo path is usable; inspect the report before presenting the affected optional feature. |
| **`FAILED`** | A required capability failed or could not provide evidence. Do not present the deployment as ready. |
| **`UNKNOWN`** | The verifier could not obtain evidence for a selected check. A required `UNKNOWN` makes the overall result `FAILED`; an optional `UNKNOWN` makes it `DEGRADED`. |
| **`SKIPPED`** | The check does not apply to the selected deployment profile or verification mode. A skipped check does not count as a failure. |
| **`IMP-*` identifier** | A named improvement or implementation item in a technical backlog. The number provides a stable link to its acceptance criteria. |
| **`ENH-*` identifier** | An optional enhancement idea in a technical backlog. It is not required for the current supported demo unless another document says it has been implemented. |

## Streaming terms

| Term | Plain-language meaning |
| --- | --- |
| **Event** | A time-stamped message that describes something that happened, such as a receipt being created or inventory changing. |
| **Ingestion time** | When Eventhouse received an event. This can differ from the business event time because delivery is asynchronous. |
| **Micro-batch** | A small group of events processed together every few seconds. Grouping events makes streaming more efficient. |
| **Partition key** | A value used to keep related events together for processing. Consumers must still tolerate events arriving out of order. |
| **Spark Kusto connector** | The Fabric connector used by the Spark stream notebook to write event groups directly into Eventhouse KQL tables. |

## Machine-learning terms

| Term | Plain-language meaning |
| --- | --- |
| **Model output** | A table containing a prediction, segment, forecast, or recommendation produced by a machine-learning notebook. |
| **Required model** | One of the four outputs needed by the Power BI report: demand forecast, customer segments, churn predictions, or stockout risk. Reporting is not published until these pass validation. |
| **Optional model** | A useful extension that runs after Reporting in `full-demo`. Its failure does not remove the required report. |
| **Experimental model** | A preview output with stronger limitations. Treat it as an exploration, not an automated business decision. |

When a term is still unclear, start with the
[deployed walkthrough](deployed-walkthrough.md), which shows where each item
appears in the workspace, then follow the linked technical reference.
