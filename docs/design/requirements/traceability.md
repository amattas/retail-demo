# Requirements traceability

This is the canonical evidence ledger. Use only the states defined in
[README.md](README.md). A row is `verified` only when both implementation and
verification evidence are exact.

## Requirements

| ID | State | Specification | Implementation evidence | Verification evidence |
| --- | --- | --- | --- | --- |
| `REQ-CORE-001` | `implemented` | [Core demo](core/demo.md) | `scripts/setup.py`; `utility/src/retail_setup/`; `deploy/`; `fabric/` | `tests/scripts/test_setup_bootstrap.py`; `utility/tests/test_cli_entrypoint.py` |
| `REQ-CORE-002` | `accepted` | [Core demo](core/demo.md); [access control](../specifications/modules/security/access-control.md) | `SECURITY.md`; `utility/src/retail_setup/generation/`; `fabric/powerbi/retail_model.SemanticModel/definition/tables/dim_customers.tmdl` | - |
| `REQ-CORE-003` | `verified` | [Data contract](../specifications/modules/generation/data-contract.md) | `utility/src/retail_setup/generation/runtime.py`; generation modules | `utility/tests/generation/test_runtime.py`; `utility/tests/generation/test_engine.py` |
| `REQ-CORE-004` | `accepted` | [Deployment framework](../specifications/modules/deployment/framework.md) | Terraform, deploy scripts, Fabric item sources | Local packaging tests only; live readiness is `IMP-013` |
| `REQ-CORE-005` | `accepted` | [Documentation site](../specifications/modules/documentation/site.md) | Canonical requirements and module backlogs | Zensical build plus docs-contract tests pending |
| `REQ-SETUP-001` | `implemented` | [Setup CLI](../specifications/modules/setup/cli.md) | `scripts/setup.ps1`; `scripts/setup.sh`; `scripts/setup.py` | `tests/scripts/test_setup_bootstrap.py` |
| `REQ-SETUP-002` | `verified` | [Setup CLI](../specifications/modules/setup/cli.md) | `utility/src/retail_setup/cli/main.py`; `utility/src/retail_setup/config/generation.py` | `utility/tests/test_cli_configure.py`; `utility/tests/test_generation_config.py` |
| `REQ-SETUP-003` | `verified` | [Setup CLI](../specifications/modules/setup/cli.md) | `utility/src/retail_setup/notebooks/`; `utility/scripts/build_notebooks.py`; setup notebooks 01-04 and `stream-events` | `utility/tests/test_cli_render.py`; `utility/tests/test_notebook_build.py` |
| `REQ-SETUP-004` | `implemented` | [Setup CLI](../specifications/modules/setup/cli.md) | `scripts/setup.py`; `utility/src/retail_setup/cli/main.py` | `utility/tests/test_cli_deploy.py` |
| `REQ-DEPLOY-001` | `accepted` | [Deployment framework](../specifications/modules/deployment/framework.md) | Environment YAML and Terraform variable files | Isolation and wrong-target tests pending (`IMP-004`) |
| `REQ-DEPLOY-002` | `implemented` | [Deployment framework](../specifications/modules/deployment/framework.md) | `utility/src/retail_setup/cli/main.py`; `deploy/scripts/` | `tests/deploy/test_build_artifacts.py`; `tests/deploy/test_apply_kql.py`; `tests/deploy/test_validate_deployment.py` |
| `REQ-DEPLOY-003` | `accepted` | [Deployment framework](../specifications/modules/deployment/framework.md) | Azure CLI and PowerShell configuration paths exist | Request-level auth-mode tests and live smoke pending (`IMP-001`) |
| `REQ-DEPLOY-004` | `accepted` | [Deployment framework](../specifications/modules/deployment/framework.md) | Deploy plan and retry helpers | Required-step injected-failure coverage pending (`IMP-002`) |
| `REQ-DEPLOY-005` | `accepted` | [Deployment framework](../specifications/modules/deployment/framework.md) | Notebook groups and feature flags exist | Profile inventory and tenant capability tests pending (`IMP-012`) |
| `REQ-GEN-001` | `verified` | [Data contract](../specifications/modules/generation/data-contract.md) | `utility/src/retail_setup/generation/schemas.py`; `gold.py` | `utility/tests/generation/test_schema_contract.py`; `test_engine.py`; `test_gold.py` |
| `REQ-GEN-002` | `verified` | [Data contract](../specifications/modules/generation/data-contract.md) | `utility/src/retail_setup/generation/schemas.py` | `utility/tests/generation/test_schema_contract.py` |
| `REQ-GEN-003` | `verified` | [Data contract](../specifications/modules/generation/data-contract.md) | `utility/src/retail_setup/generation/runtime.py` and seeded generation modules | `utility/tests/generation/test_runtime.py`; representative generation tests |
| `REQ-GEN-004` | `implemented` | [Data contract](../specifications/modules/generation/data-contract.md) | `utility/src/retail_setup/generation/invariants.py` | Generation module tests; shared live/batch invariant coverage pending (`IMP-010`) |
| `REQ-STREAM-001` | `implemented` | [Event contract](../specifications/modules/streaming/event-contract.md) | `utility/notebooks/templates/driver-05-stream.py`; `fabric/kql_database/01-create-tables.kql` | Cross-layer event fixture suite pending (`IMP-005`) |
| `REQ-STREAM-002` | `implemented` | [Event contract](../specifications/modules/streaming/event-contract.md) | `write_to_eventhouse()` and Delta sink in `driver-05-stream.py` | Fabric live smoke pending |
| `REQ-STREAM-003` | `accepted` | [Event contract](../specifications/modules/streaming/event-contract.md) | KQL mappings and `fabric/lakehouse/03-streaming-to-silver.ipynb` | Full payload-to-model contract suite pending (`IMP-005`) |
| `REQ-STREAM-004` | `accepted` | [Operations runbook](../specifications/modules/operations/runbook.md) | Spark checkpoints and `ag._watermarks` exist | Injected failure and replay tests pending (`IMP-002`) |
| `REQ-AN-001` | `implemented` | [Fabric analytics](../specifications/modules/analytics/fabric-analytics.md) | `fabric/kql_database/*.kql`; `deploy/scripts/apply_kql.py` | `tests/deploy/test_apply_kql.py`; KQL execution in a live database pending |
| `REQ-AN-002` | `implemented` | [Fabric analytics](../specifications/modules/analytics/fabric-analytics.md) | `fabric/lakehouse/01-create-bronze-shortcuts.ipynb`; streaming/historical transforms | Notebook contract and deployment packaging tests |
| `REQ-AN-003` | `accepted` | [Fabric analytics](../specifications/modules/analytics/fabric-analytics.md); [semantic model](../specifications/modules/power-bi/semantic-model.md) | Current KQL and DAX calculations | State/grain/time-slice tests pending (`IMP-009`) |
| `REQ-AN-004` | `accepted` | [Fabric analytics](../specifications/modules/analytics/fabric-analytics.md) | `fabric/querysets/`; `fabric/dashboards/`; `fabric/rules/` | First-class dashboard/Activator deployment pending (`ENH-001`) |
| `REQ-MLAI-001` | `accepted` | [ML and AI contracts](../specifications/modules/ml-ai/model-contracts.md) | ML notebook groups, ontology notebook, and agent sources | Profile/support-tier checks pending |
| `REQ-MLAI-002` | `accepted` | [ML and AI contracts](../specifications/modules/ml-ai/model-contracts.md) | `fabric/lakehouse/06-ml-*` through `14-ml-*` | Methodology, schema, lineage, and explainability gates pending (`IMP-008`, `ENH-007`) |
| `REQ-MLAI-003` | `implemented` | [ML and AI contracts](../specifications/modules/ml-ai/model-contracts.md) | `fabric/lakehouse/30-create-ontology.ipynb` | `tests/deploy/test_ontology_notebook.py`; live binding verification pending |
| `REQ-MLAI-004` | `accepted` | [Access control](../specifications/modules/security/access-control.md) | `fabric/data-agents/` | Agent instructions and role-based answer tests pending (`IMP-011`, `ENH-003`) |
| `REQ-BI-001` | `implemented` | [Semantic model](../specifications/modules/power-bi/semantic-model.md) | `fabric/powerbi/retail_model.SemanticModel/definition/expressions.tmdl`; `model.tmdl` | `tests/scripts/test_reference_integrity.py`; live binding pending |
| `REQ-BI-002` | `accepted` | [Semantic model](../specifications/modules/power-bi/semantic-model.md) | TMDL measures, relationships, hierarchies, and properties | Technical-field and KPI semantic tests pending (`IMP-009`) |
| `REQ-BI-003` | `accepted` | [Semantic model](../specifications/modules/power-bi/semantic-model.md) | Four ML tables are active in `definition/model.tmdl` | Default-profile gating and empty-state tests pending (`IMP-008`) |
| `REQ-BI-004` | `accepted` | [Semantic model](../specifications/modules/power-bi/semantic-model.md) | PBIP report pages and visual metadata | Accessibility, mobile, current-period, and empty-state review pending (`ENH-006`) |
| `REQ-OPS-001` | `accepted` | [Operations runbook](../specifications/modules/operations/runbook.md) | `deploy/scripts/validate_deployment.py`; task-flow and pipeline helpers | Live workspace readiness suite pending (`IMP-013`) |
| `REQ-OPS-002` | `accepted` | [Operations runbook](../specifications/modules/operations/runbook.md) | `setup_run_log`; `ag._watermarks`; Fabric run history | Unified freshness surface pending (`IMP-013`) |
| `REQ-OPS-003` | `accepted` | [Operations runbook](../specifications/modules/operations/runbook.md) | Dry-run, recreate, reset notebook, and manual fallbacks | Target validation, deletion polling, and recovery drills pending |
| `REQ-OPS-004` | `accepted` | [Operations runbook](../specifications/modules/operations/runbook.md) | Existing utility/deploy tests and GitHub workflows | Active-path parity, Windows coverage, and contract suites pending (`IMP-014`) |
| `REQ-SEC-001` | `implemented` | [Threat model](../security/threat-model.md); [controls](../security/controls.md) | Stable `THREAT-*` and `SEC-*` owners | Ledger structure and link validation |
| `REQ-SEC-002` | `accepted` | [Access control](../specifications/modules/security/access-control.md) | `SECURITY.md`; classification guidance | Model/agent access inventory and role tests pending |
| `REQ-SEC-003` | `accepted` | [Access control](../specifications/modules/security/access-control.md) | `.gitignore`; identity-based deploy design | Secret scanning and bearer-header tests pending (`IMP-001`, `IMP-003`) |
| `REQ-SEC-004` | `accepted` | [Access control](../specifications/modules/security/access-control.md) | Workspace roles and model/agent sources | RLS/aggregation and persona tests pending (`IMP-011`) |
| `REQ-DOCS-001` | `verified` | [Documentation site](../specifications/modules/documentation/site.md) | `docs/`; root and component README links | Clean Zensical link/build validation |
| `REQ-DOCS-002` | `verified` | [Documentation site](../specifications/modules/documentation/site.md) | Canonical requirements, specifications, architecture, security, guides, and backlogs; retired parallel owners removed | Unique ID/owner reconciliation and generated-site scope check |
| `REQ-DOCS-003` | `verified` | [Documentation site](../specifications/modules/documentation/site.md) | `zensical.toml`; `requirements-docs.txt` | `python -m zensical build --clean` reports no issues |
| `REQ-DOCS-004` | `implemented` | [Documentation site](../specifications/modules/documentation/site.md) | Reconciled canonical pages and README entry points | Generated inventory/docs-contract automation remains `IMP-015` |
| `REQ-PUBLISH-001` | `blocked` | [Documentation site](../specifications/modules/documentation/site.md) | `.github/workflows/docs.yml` pushes `site/` to `gh-pages` | Repository Pages source must be changed from GitHub Actions to `gh-pages` root, then workflow run verified |
| `REQ-PUBLISH-002` | `implemented` | [Documentation site](../specifications/modules/documentation/site.md) | `scripts/docs_versioning.py`; `scripts/publish_versioned_docs.py`; Zensical `mike` provider; full-history docs workflow | Local multi-version build and selector metadata test |

## Threats

| ID | State | Controls | Specification | Implementation evidence | Verification evidence |
| --- | --- | --- | --- | --- | --- |
| `THREAT-001` | `accepted` | `SEC-001`, `SEC-002`, `SEC-004`, `SEC-005` | [Access control](../specifications/modules/security/access-control.md) | Workspace roles and semantic-model sources | RLS/aggregation and role-query tests pending |
| `THREAT-002` | `accepted` | `SEC-001`, `SEC-003`, `SEC-009` | [Deployment framework](../specifications/modules/deployment/framework.md) | Identity clients and ignored local config | Bearer-header, endpoint, tenant, and secret-scanning tests pending |
| `THREAT-003` | `accepted` | `SEC-004`, `SEC-005`, `SEC-006` | [ML and AI contracts](../specifications/modules/ml-ai/model-contracts.md) | Ontology and data-agent source | Agent instructions and prohibited-detail tests pending |
| `THREAT-004` | `accepted` | `SEC-007` | [Documentation site](../specifications/modules/documentation/site.md) | Docs workflow uses immutable action SHAs and pinned Zensical | Repository-wide privileged workflow audit pending |
| `THREAT-005` | `accepted` | `SEC-008` | [Operations runbook](../specifications/modules/operations/runbook.md) | Fabric monitoring, run logs, watermarks | Unified audit/readiness checks pending |
| `THREAT-006` | `accepted` | `SEC-001`, `SEC-009` | [Deployment framework](../specifications/modules/deployment/framework.md) | Environment config, dry-run, recreate/reset flows | Isolated-state and wrong-target negative tests pending |
| `THREAT-007` | `accepted` | `SEC-008`, `SEC-011` | [Operations runbook](../specifications/modules/operations/runbook.md) | Checkpoints, watermarks, retry helpers | Injected required-step failure and replay tests pending |
| `THREAT-008` | `implemented` | `SEC-003`, `SEC-010` | [Documentation site](../specifications/modules/documentation/site.md) | Canonical `docs/` source, reviewed nav, generated `site/` ignored | Zensical build and `gh-pages` content inspection |
