# retail-setup

Fabric-native setup utility: generates the demo's historical data directly into
Lakehouse Delta tables and configures the target environment.

See `docs/superpowers/specs/2026-06-12-setup-utility-design.md` for the design.

## Dev setup

    mamba create -n retail-setup python=3.12 -y
    mamba activate retail-setup
    pip install -e ".[dev]"
    pytest
