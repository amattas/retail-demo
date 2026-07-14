# Security controls

Security controls use the states defined by
[requirements traceability](../requirements/traceability.md). A control is not
`verified` without exact implementation and verification evidence.

| ID | State | Control | Current evidence | Verification needed |
| --- | --- | --- | --- | --- |
| `SEC-001` | `accepted` | Use Entra ID and validate the configured tenant and target workspace before deployment. | `utility/src/retail_setup/cli/main.py`; `deploy/README.md` | Request-contract tests for both Azure CLI and Azure PowerShell plus a live target check. |
| `SEC-002` | `accepted` | Apply least-privilege Fabric workspace, Eventhouse, storage, model, and consumer roles. | Fabric role guidance and deployment configuration | Environment role review and role-based access tests. |
| `SEC-003` | `accepted` | Load secrets only from Azure sign-in, GitHub Actions secrets, environment variables, Key Vault, or ignored local files. | `.gitignore`; `deploy/README.md`; `SECURITY.md` | Secret scanning and a test that generated artifacts contain no credentials. |
| `SEC-004` | `accepted` | Classify customer-like data as synthetic-but-sensitive. | `SECURITY.md`; `fabric/powerbi/retail_model.SemanticModel/definition/tables/dim_customers.tmdl` | Model, report, ontology, and agent field inventory. |
| `SEC-005` | `proposed` | Make broad-use models and agents aggregated, field-restricted, or RLS-gated by default. | `IMP-011` | Role-based semantic-model and agent queries. |
| `SEC-006` | `proposed` | Give every data agent an owner, purpose, allowed-use instructions, and approved question set. | `fabric/data-agents/`; `IMP-011` | Export and inspect deployed agent configuration. |
| `SEC-007` | `accepted` | Pin privileged actions, plugins, providers, and dependency sets to reviewed immutable versions. | `.github/workflows/docs.yml`; `requirements-docs.txt`; `IMP-003` | Repository-wide workflow and dependency audit. |
| `SEC-008` | `accepted` | Retain deployment, pipeline, watermark, ingestion, model, and alert evidence with actionable failure signals. | Fabric monitoring surfaces; `setup_run_log`; `ag._watermarks` | Post-deploy readiness and freshness checks. |
| `SEC-009` | `accepted` | Isolate environments and require explicit confirmation and target validation for destructive operations. | Environment files; deploy dry-run and recreate flows | Separate state tests and wrong-target negative tests. |
| `SEC-010` | `verified` | Publish only reviewed current Markdown from `docs/` and documentation captured in immutable SemVer tags; exclude temporary plans and generated source artifacts. | `zensical.toml`; `.github/workflows/docs.yml`; `scripts/docs_versioning.py` | Successful Docs workflow run, `gh-pages` branch inspection, and live `/latest/` plus `versions.json` inspection. |
| `SEC-011` | `proposed` | Fail closed for required writes and preserve failed payloads or replay evidence before advancing progress. | `IMP-002` | Injected-failure tests for streaming, transforms, and deployment. |

## Minimum deployment baseline

Before a shared or customer-facing demo:

1. Confirm the signed-in tenant and target workspace.
2. Review workspace and item roles.
3. Confirm generated files contain no credentials or operator-specific secrets.
4. Restrict customer-like detail from broad-use models and agents.
5. Confirm monitoring and failure notifications are available.
6. Validate that reset and recreate actions target the intended environment.
