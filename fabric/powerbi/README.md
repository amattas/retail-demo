# Power BI

This directory contains the `retail_model` Power BI Project.

## Active semantic model

The model is Direct Lake over the Lakehouse and has 38 active tables:

- seven dimensions
- eighteen facts
- nine Gold aggregates
- four ML outputs: `churn_predictions`, `customer_segments`,
  `demand_forecast`, and `stockout_risk`

It is not a hybrid KQL/DirectQuery model. Additional ML definitions outside the
active model are future or experimental assets.

## Report

The report includes executive, supply-chain, distribution-center, store,
regional, omnichannel, customer/marketing, pricing/promotion, and logistics
pages. Demo claims must follow the limitations in the
[demo script](../../docs/guides/demo-script.md).

Use `scripts\configure_semantic_model.py` when a local PBIP copy needs its
OneLake target rewritten. Automated deployment performs target parameterization
through the deployment framework.

See the [semantic-model specification](../../docs/specifications/modules/power-bi/semantic-model.md)
and [Power BI backlog](../../docs/requirements/modules/power-bi/backlog.md).
