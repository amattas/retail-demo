# Fabric workspace task flow

`taskflow.json` is a portable display-name-based representation of the workspace
task flow. `deploy.scripts.taskflow` resolves names to target item identifiers
and creates or updates the target flow.

```powershell
python -m deploy.scripts.taskflow export --workspace "Retail Demo" --path fabric\taskflow\taskflow.json
python -m deploy.scripts.taskflow deploy --terraform-output deploy\.generated\<environment>\terraform-output.json --environment <environment> --profile full-demo --path fabric\taskflow\taskflow.json
```

Task-flow access uses undocumented Power BI metadata-cluster endpoints and may
break independently of the public Fabric APIs. Any selected task whose item
reference cannot be resolved fails deployment; selected references are never
silently omitted. After create or update, deployment reads the graph back and
requires all 11 tasks, 48 full-demo item references, and 11 edges to match.

Normal full-demo deployment runs `30-create-ontology` to create or revalidate
the stable ontology and derived graph, publishes both Data Agents, and writes
the complete graph.
Use the following only as an idempotent recovery command:

```powershell
retail-setup post-ontology --env <environment>
```

That command validates the ontology before publishing Data Agents and the task
flow. The direct deploy command above is a troubleshooting path and still
requires the configured environment and matching executable profile.

See the [deployment specification](../../docs/design/specifications/modules/deployment/framework.md)
and [infrastructure architecture](../../docs/design/architecture/infrastructure.md).
