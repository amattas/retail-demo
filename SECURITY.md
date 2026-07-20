# Security policy

## Reporting a vulnerability

Report suspected vulnerabilities through
[GitHub private vulnerability reporting](https://github.com/amattas/retail-demo/security/advisories/new).
Do not include credentials, tenant identifiers, customer information, or other
sensitive evidence in a public issue.

## Supported version

Security fixes target the current `main` branch. Historical snapshots and
generated deployment output are not supported.

## Scope

The supported surfaces are the Fabric-native setup utility, deployment
framework, Fabric source assets, Power BI project, and canonical documentation:

- `utility/`
- `deploy/`
- `fabric/`
- `scripts/`
- `docs/`

The generated documentation site and generated deployment folders are build
artifacts, not sources of truth.

## Data handling

The repository generates synthetic data for demonstration only. Do not load
production customer data into the demo. Standard Fabric workspace and item
permissions are the access boundary; field-level privacy controls for generated
records are outside the default release scope.

Never commit secrets. Use Azure sign-in, GitHub Actions secrets, environment
variables, Key Vault, or ignored local configuration.

See the canonical [threat model](docs/design/security/threat-model.md) and
[security controls](docs/design/security/controls.md).
