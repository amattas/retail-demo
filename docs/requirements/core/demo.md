# Core demo requirements

## Outcomes

### REQ-CORE-001 - Supported path

The supported path shall use the Fabric-native `retail-setup` utility, setup
notebooks, deployment framework, and Fabric source assets.

**Acceptance:** A new contributor can identify one supported entry point, and
every public guide uses that Fabric-native path.

### REQ-CORE-002 - Synthetic data handling

The demo shall use synthetic data. Identity-like synthetic fields shall still
be treated as sensitive when exposed through reports, models, ontologies, or
agents.

**Acceptance:** Public guidance and access controls use the same classification.

### REQ-CORE-003 - Reproducibility

Historical generation shall be deterministic for a fixed configuration and
seed, including distributed Spark execution.

**Acceptance:** Contract tests prove repeatable table schemas and representative
values for the same seed.

### REQ-CORE-004 - End-to-end outcome

The supported deployment shall produce a usable Fabric workspace with the
required Lakehouse, Eventhouse/KQL database, notebooks, pipelines, semantic
model, report, and configured optional assets.

**Acceptance:** Post-deploy checks verify live workspace items and a minimal data
flow, not only local staging files.

### REQ-CORE-005 - Truthful capability labels

Documentation and demo scripts shall distinguish implemented, optional, manual,
preview, and proposed capabilities.

**Acceptance:** Every highlighted scenario has a current source artifact,
required setup step, and known limitation or backlog link.
