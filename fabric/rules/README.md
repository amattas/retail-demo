# Real-time rule scenarios

This directory records candidate operational actions such as stockout,
replenishment, truck-dwell, queue, and customer-arrival alerts. The customer
rule is an arrival-surge/expected-dwell signal, not an occupancy rule, because
the current event contract does not include customer-exit events.

Logistics rules read the incident model's events (`incidents.py`):

- **Late truck**: a truck that arrives 30 or more minutes after its planned
  arrival, with its `delay_reason` (DC outage, holiday capacity, lane delay,
  weather, traffic, truck availability).
- **Lane delay over threshold**: one alert per DC-to-store lane whose trucks
  ran on average 60 or more minutes late in the last hour because of a lane
  incident or severe weather (traffic alone raises only the late-truck alert).
- **DC outage started**: a `DC_OUTAGE` incident started; the alert lists every
  store the DC serves (`affected_store_ids`).
- **Incident stockouts**: one alert per open incident whose affected stores
  reported three or more distinct stockouts in the last hour since it started.

Thresholds (the 90-minute truck dwell SLA, the 30-minute late-truck and
60-minute lane-delay limits, the incident-stockout count, rule windows,
zone-surge limits, and the high-value receipt total of 3 x the store type's
average ticket) come from `utility/src/retail_setup/generation/calibration.py`.
`utility/tests/generation/test_calibration.py` fails when a literal here
drifts from it. The high-value literal is the supercenter value; set it for the
deployed store type.

These are design inputs, not guaranteed deployed Fabric Activator or rule
artifacts. Do not present them as active automations until deployment and runtime
evidence exist.

See the [analytics backlog](../../docs/design/requirements/modules/analytics/backlog.md)
and [traceability ledger](../../docs/design/requirements/traceability.md).
