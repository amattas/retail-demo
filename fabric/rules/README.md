# Real-time rule scenarios

This directory records candidate operational actions such as stockout,
replenishment, truck-dwell, queue, and customer-arrival alerts. The customer
rule is an arrival-surge signal, not an occupancy rule, because the current
event contract does not include customer-exit events.

Every rule deduplicates its rows on `trace_id` (a replayed micro-batch writes
the same events again) and raises one alert per entity and window:

- **Stockout detected**: one alert per store (or DC) listing the distinct
  products whose balance reached zero in the last 5 minutes.
- **Reorder URGENT**: one alert per store listing the products reordered at an
  empty shelf in the last 10 minutes.
- **Truck dwell breach**: a completed truck lifecycle over the 90-minute SLA,
  selected by when its departure reached the Eventhouse (`ingestion_time()`),
  so the alert follows the live stream and stops when it stops.
- **Zone arrival surge**: arrivals per store and zone in 5 minutes above the
  store type's surge count. The count sits four Poisson standard deviations
  above the busiest window a normal store can have in the live stream: the
  entrance (every visit passes it) of the largest store at the store type's
  busiest hour, weekday and month in the best weather. Peak holidays are left
  out on purpose, so a Black Friday crowd at a flagship store alerts. The
  expected-dwell branch was dropped: no zone's dwell range reaches it.
- **High-value receipt**: a receipt above 3 x the store type's average ticket.

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
60-minute lane-delay limits, the incident-stockout count, rule windows, the
zone-surge count, and the high-value receipt total of 3 x the store type's
average ticket) come from `utility/src/retail_setup/generation/calibration.py`.
`utility/tests/generation/test_calibration.py` fails when a literal here
drifts from it, and `utility/tests/generation/test_logistics_rules.py` runs
Python mirrors of the rules over simulated stream output. The two store-type
thresholds are read from the Eventhouse function `fn_rule_thresholds()`; the
deployment's KQL step writes the deployed store type's values into it.

These are design inputs, not guaranteed deployed Fabric Activator or rule
artifacts. Do not present them as active automations until deployment and runtime
evidence exist.

See the [analytics backlog](../../docs/design/requirements/modules/analytics/backlog.md)
and [traceability ledger](../../docs/design/requirements/traceability.md).
