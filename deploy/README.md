# Fabric deployment framework

`deploy/` contains Terraform, generated `fabric-cicd` configuration, artifact
staging, KQL application, pipeline triggering, task-flow deployment, and offline
validation.

Use the high-level CLI for normal operation:

```powershell
retail-setup deploy --env dev --dry-run
retail-setup deploy --env dev --yes
```

The deploy command:

1. Generates environment-specific configuration.
2. Provisions or resolves Fabric resources through Terraform.
3. Stages selected Fabric item groups.
4. Publishes supported items through `fabric-cicd`.
5. Builds and applies the ordered KQL database script.
6. Deploys the task flow and validates generated artifacts.
7. Optionally triggers the asynchronous setup pipeline.

Generated state belongs under `deploy\.generated\<env>\` and
`deploy\workspace\`; both are ignored.

## Current deploy groups

The CLI currently stages `core`, `setup`, `ml`, `ontology`, `reset`, and
`stream`. The reset notebook is deployed but never orchestrated. Dashboard and
rule source files are not yet guaranteed publishable workspace items.

## Important runtime behavior

- The setup pipeline runs setup 01-04, ML notebooks 06-14, and ontology
  creation.
- Ontology creation occurs after initial task-flow deployment. Re-run
  `python -m deploy.scripts.taskflow deploy --workspace <workspace>` after the
  setup pipeline if ontology links are unresolved.
- KQL application uses the local operator identity.
- Secrets must come from identity, environment variables, a secret store, or
  ignored local files.

See the [deployment specification](../docs/specifications/modules/deployment/framework.md),
[operations guide](../docs/guides/operations.md), and
[deployment backlog](../docs/requirements/modules/deployment/backlog.md).
