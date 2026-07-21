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
silently omitted.

Ontology creation is a separate preview/operator action through
`30-create-ontology`; it is not part of `setup-pipeline`. Run that notebook,
wait for `RetailOntology_AutoGen` to exist, and complete the acknowledged
post-ontology publication step:

```powershell
retail-setup post-ontology --env <environment> --acknowledge ack.full-demo.ontology-created
```

That command validates the ontology before publishing Data Agents and the task
flow. The direct deploy command above is a troubleshooting path and still
requires the configured environment and matching executable profile.

See the [deployment specification](../../docs/design/specifications/modules/deployment/framework.md)
and [infrastructure architecture](../../docs/design/architecture/infrastructure.md).
