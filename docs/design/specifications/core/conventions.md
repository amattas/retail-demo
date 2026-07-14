# Core conventions

## Supported path

The supported repository path is:

1. `scripts/setup.ps1`, `scripts/setup.sh`, or `python scripts/setup.py`
2. `retail-setup configure`
3. `retail-setup render`
4. `retail-setup deploy` or manual import
5. setup notebooks 01 through 04
6. optional `stream-events`

## Configuration boundaries

| Location | Ownership |
| --- | --- |
| `deploy/config/deploy.yml` | Shared deployment defaults |
| `deploy/config/environments/<env>.yml` | Environment overlay |
| `utility/config.yaml` | Ignored local generation configuration |
| `utility/out/` | Rendered notebooks |
| `deploy/terraform/environments/<env>.tfvars` | Generated, tracked Terraform input |
| `deploy/fabric-cicd/config.yml` | Generated, tracked publication configuration |
| `deploy/fabric-cicd/parameter.yml` | Generated, tracked binding rewrites |
| `deploy/.generated/<env>/` | Ignored live Terraform outputs and combined KQL |
| `deploy/workspace/` | Generated Fabric item staging |

Environment selection must be explicit. Generated tracked files are reviewable
templates, not authoritative target state. Ignored generated and local files
are not durable sources of truth.

## Time and determinism

- Use UTC timestamps with timezone-aware APIs.
- Historical configuration is centered on `months`; the derived range ends
  yesterday.
- Seeded generation uses deterministic hash-based draws suitable for Spark.
- Event timestamps must preserve lifecycle order even when ingestion can be
  out of order.

## Naming

- New pipeline columns use `snake_case`.
- KQL event tables use `snake_case`.
- Presentation names may be user-friendly.
- The current physical contract still contains PascalCase and mixed-case
  columns required by existing TMDL bindings. Those exceptions are explicit in
  `schemas.py`; documentation must not claim the current model is pure
  `snake_case`.

## Support tiers

| Tier | Meaning |
| --- | --- |
| Core | Required for the supported historical demo |
| Optional | Supported only when explicitly selected and prepared |
| Preview | Requires tenant capability preflight and explicit consent |
| Manual | Source or template exists, but publication/configuration is not automated |
| Proposed | Backlog idea with no supported implementation claim |

## Documentation ownership

`docs/` is the only site source. Requirements, specifications, architecture,
security, and guides must link rather than duplicate normative content.
