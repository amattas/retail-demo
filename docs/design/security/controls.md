# Security controls

Security controls use the states defined by
[requirements traceability](../requirements/traceability.md). A control is not
`verified` without exact implementation and verification evidence.

| ID | State | Control | Current evidence | Verification needed |
| --- | --- | --- | --- | --- |
| `SEC-001` | `implemented` | Use Entra ID and validate the configured tenant and target workspace before deployment. | `utility/src/retail_setup/cli/main.py`; `deploy/scripts/profile_preflight.py` | Existing auth/target contract tests; fresh alternate-auth and renamed-target live checks remain `IMP-001`. |
| `SEC-002` | `accepted` | Apply least-privilege Fabric workspace, Eventhouse, storage, model, and consumer roles. | Fabric role guidance and deployment configuration | Environment role review and role-based access tests. |
| `SEC-003` | `accepted` | Load secrets only from Azure sign-in, GitHub Actions secrets, environment variables, Key Vault, or ignored local files. | `.gitignore`; `deploy/README.md`; `SECURITY.md` | Secret scanning and a test that generated artifacts contain no credentials. |
| `SEC-004` | `retired` | Classify customer-like data as synthetic-but-sensitive. | Synthetic-only demo boundary | Retired: row-level classification controls are outside the release scope. |
| `SEC-005` | `retired` | Make broad-use models and agents aggregated, field-restricted, or RLS-gated by default. | Fabric workspace and item permissions | Retired: default RLS and field masking are outside the demo scope. |
| `SEC-006` | `retired` | Give every data agent an owner, purpose, allowed-use instructions, and approved question set. | Optional persona work under `ENH-003` | Retired as a release control. |
| `SEC-007` | `verified` | Pin privileged actions, plugins, providers, and dependency sets to reviewed immutable versions. | Full-SHA workflow actions with no runtime plugin marketplaces; SHA-256-only hash-locked Python requirements; exact `fabric-cicd` pin present in both deploy and CI locks; exact Fabric provider plus `.terraform.lock.hcl`; pinned Miniforge installers | `tests/scripts/test_workflow_references.py`; `tests/scripts/test_supply_chain.py`; native pip and Terraform lock validation. |
| `SEC-008` | `implemented` | Retain deployment, pipeline, watermark, ingestion, model, and alert evidence with actionable failure signals. | Atomic latest deploy journal and prior-run archives; bounded redacted readiness report; Fabric run history; `setup_run_log`; `ag._watermarks`; Eventhouse ingestion/tags | `utility/tests/test_deploy_resume.py`; actual live post-deploy readiness and freshness evidence. |
| `SEC-009` | `implemented` | Isolate environments and require explicit confirmation and target validation for destructive operations. | Environment-scoped config/render/staging; `main.py` saved-plan guard; `_deploy_resume.py` fingerprint/target checks | `utility/tests/test_deploy_resume.py`; CLI/deploy tests; fresh combined validation and disposable-workspace destructive drill outstanding. |
| `SEC-010` | `implemented` | Publish reviewed living documentation and immutable release docs; remove raw temporary evidence only after preserving durable content. | Reconciled `docs/`; `zensical.toml`; `.github/workflows/docs.yml`; `scripts/docs_versioning.py` | Docs contracts and clean build locally; fresh Docs workflow, `gh-pages`, `/latest/`, historical versions, and search-index inspection remain external. Navigation alone does not exclude files. |
| `SEC-011` | `implemented` | Fail closed for required writes and preserve failed payloads or replay evidence before advancing progress. | Fail-closed Eventhouse writes; replay/deep reconciliation; historical watermark guard; staged promotion/rollback; cleanup-success warning | Existing injected-failure tests plus new Silver/writer/maintenance regressions; fresh combined runtime and Fabric failure/recovery tests remain outstanding. |
| `SEC-012` | `accepted` | Keep the optional app draft-only; distinguish replay, local approval, best-effort telemetry, and authenticated durable production execution. | `app/backend/proposals.py`; `eventhouse.py`; `main.py`; [decision experience](../specifications/modules/ml-ai/decision-experience.md); offline Fabric Apps spike `app/fabric-app` with SSO-bound, append-only decision rows (`src/__tests__/permissions.test.ts`) | End-to-end mode/approval/failure tests; delegated identity and durable audit before shared hosting or external actions. Current telemetry can be lost. The spike is undeployed, and its separation of duties and decision timestamps are client-enforced. |
| `SEC-013` | `implemented` | Render configuration as literal/JSON data; fail closed on unresolved default Git refs; validate dictionary identity, inventory, and hashes before immutable cache reuse. | `utility/src/retail_setup/notebooks/inject.py`; `cli/main.py`; `utility/notebooks/templates/setup-01-seed-dictionaries.ipynb.py` manifest-last publication | Injection/render/cache tests; parent reports 8 cache passes, all five notebooks rebuilt and drift check passed. Fresh live validation remains outstanding. |

## Minimum deployment baseline

Before a shared or customer-facing demo:

1. Confirm the signed-in tenant and target workspace.
2. Review workspace and item roles.
3. Confirm generated files contain no credentials or operator-specific secrets.
4. Confirm the workspace contains generated synthetic demo data only.
5. Confirm monitoring and failure notifications are available.
6. Validate that reset and recreate actions target the intended environment.

## Supply-chain lock maintenance

Change the owning input (`requirements-*.in`, `utility/pyproject.toml`, or
`deploy/terraform/providers.tf`) before regenerating its reviewed lock:

```powershell
python -m pip install uv==0.11.29
python -m uv pip compile requirements-docs.in --universal --generate-hashes --output-file requirements-docs.txt
python -m uv pip compile requirements-test.in --universal --generate-hashes --output-file requirements-test.txt
python -m uv pip compile utility/pyproject.toml --extra deploy --universal --generate-hashes --output-file utility/requirements-deploy.txt
python -m uv pip compile utility/pyproject.toml --extra dev --extra deploy --universal --generate-hashes --output-file utility/requirements-ci.txt
```

After changing the Fabric provider version, regenerate all supported platform
checksums:

```powershell
terraform -chdir=deploy/terraform providers lock `
  -platform=linux_amd64 -platform=linux_arm64 `
  -platform=windows_amd64 -platform=windows_arm64 `
  -platform=darwin_amd64 -platform=darwin_arm64
```

Miniforge version changes require updating every platform URL and SHA-256 value
in `scripts/setup.sh` plus the exact winget version in `scripts/setup.ps1`.
