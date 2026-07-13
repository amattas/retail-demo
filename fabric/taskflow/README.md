# Fabric workspace task flow

`taskflow.json` is a portable display-name-based representation of the workspace
task flow. `deploy.scripts.taskflow` resolves names to target item identifiers
and creates or updates the target flow.

```powershell
python -m deploy.scripts.taskflow export --workspace "Retail Demo" --path fabric\taskflow\taskflow.json
python -m deploy.scripts.taskflow deploy --workspace "retail-demo-dev" --path fabric\taskflow\taskflow.json
```

Task-flow access uses undocumented Power BI metadata-cluster endpoints and may
break independently of the public Fabric APIs. Unresolved items are omitted and
reported.

Ontology creation occurs at the end of the asynchronous setup pipeline, after
the deploy task-flow step. Re-run the deploy command above after the pipeline if
the ontology node is not linked.

See the [deployment specification](../../docs/design/specifications/modules/deployment/framework.md)
and [infrastructure architecture](../../docs/design/architecture/infrastructure.md).
