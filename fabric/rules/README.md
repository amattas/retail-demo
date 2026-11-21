# Real-time rule scenarios

This directory records candidate operational actions such as stockout,
replenishment, truck-dwell, queue, and customer-arrival alerts. The customer
rule is an arrival-surge/expected-dwell signal, not an occupancy rule, because
the current event contract does not include customer-exit events.

Thresholds (the 90-minute truck dwell SLA, rule windows, zone-surge limits,
and the high-value receipt total of 3 x the store type's average ticket) come
from `utility/src/retail_setup/generation/calibration.py`.
`utility/tests/generation/test_calibration.py` fails when a literal here
drifts from it. The high-value literal is the supercenter value; set it for the
deployed store type.

These are design inputs, not guaranteed deployed Fabric Activator or rule
artifacts. Do not present them as active automations until deployment and runtime
evidence exist.

See the [analytics backlog](../../docs/design/requirements/modules/analytics/backlog.md)
and [traceability ledger](../../docs/design/requirements/traceability.md).
