# Generation requirements

### REQ-GEN-001 - Base table set

Historical generation shall create seven dimensions, twenty-one fact tables,
and ten Gold aggregates in the documented `ag` and `au` schemas.

### REQ-GEN-002 - Authoritative contract

`utility/src/retail_setup/generation/schemas.py` shall own the generated
Lakehouse table contract until a shared machine-readable manifest replaces it.

### REQ-GEN-003 - Deterministic business behavior

The same seed and configuration shall produce deterministic dimensions, facts,
lifecycle events, and Gold outputs. Dimensions shall depend only on the seed,
the scale settings, and an `anchor_date` pinned once per environment, never
on the run date, so rendering the same configuration on a later day produces
identical stores, geographies, customers, and products.

### REQ-GEN-004 - Business invariants

Generated output shall satisfy referential, calendar, pricing, tax, promotion,
inventory, return, and lifecycle invariants before publication.

### REQ-GEN-005 - Logistics disruptions

History and the live stream shall share one deterministic logistics incident
model (DC outages, lane delays, weather, holiday capacity, supplier
short-ships) with the stores each incident affects; record planned and actual
arrivals with a reason for every delay; keep DC stock fed by supplier orders
with lead times, so a DC can run out and short-ship its stores; and deliver
nothing on a store's closure day. Store reorders shall consolidate into
capacity-bounded shipments on trucks that are never in two places at once.
Every history shall end inside one deterministic DC outage, keyed only on the
seed and the history's last day, that is still open when history ends,
draws its stores' fast movers under three days of cover through the
simulation, and is resolved by the live stream at its planned end.

### REQ-GEN-006 - UTC time with store-local context

Generation shall follow each store's local wall clock (opening hours, hourly
curve, operations, delivery slots) and publish every timestamp as true UTC,
with `event_date` the UTC date. `dim_stores.TimeZone` shall name the store's
IANA zone and `fact_receipts.local_hour` shall carry the store-local hour for
hour-of-day reporting. History and the live stream shall use the same
convention.

### REQ-GEN-007 - Customer lifecycle

Customers shall have a home store and sign-up date, shop at a long-tailed
frequency, and churn through a feature-driven hazard calibrated to the
configured annual churn rate, most of them with a tapering pre-churn signal.
About the configured anonymous share of receipts shall have no customer.

### REQ-GEN-008 - Prices, baskets, and promotions

Shelf prices shall change over time (periodic reviews, markdowns, a
department trend) and product choice shall respond to price. Baskets shall
carry complementary products and merge repeated SKUs. A basket shall carry at
most one in-season promotion code whose minimum it meets, discounting only
lines in the code's scope, and fulfillment shall re-price the filled basket.

### REQ-GEN-009 - Returns and payments

Returns shall be partial, category-weighted, posted after the sale inside
store hours, linked to the original receipt, and refunded to the sale's
customer and tender. A declined first tender shall either retry on another
tender or void the sale, so every receipt and placed order has exactly one
approved payment and voided sales take no stock.

### REQ-GEN-010 - Calendar, weather, and tax

One US retail holiday calendar shall lift or close traffic by store class, a
persistent per-geography weather index shall modulate traffic and mark
severe-weather days, and line tax shall follow each state's taxability rules
for groceries and clothing. History and the live stream shall share all three.

### REQ-GEN-011 - Store-type profiles and visit paths

A store type's profile shall drive its traffic, operating hours, formats,
online share, and zones. Foot traffic shall count zone passages along one
shared visit path (entrance, departments, checkout for buyers, exit), and
BLE pings shall follow the same path.

### REQ-GEN-012 - Exposure-driven attribution

Impressions shall cost realistic CPMs and rotate a creative pool. Conversions
shall come from impressions that reached a customer who bought within the
seven-day window, calibrated to the target return on ad spend, so attributed
journeys look like every other impression.

### REQ-GEN-013 - Shared vocabulary and calibration

Batch generation, the live stream, KQL rules, anomaly functions, dashboards,
and the semantic model shall take identifiers, vocabularies, and thresholds
from one vocabulary module and one calibration module, and contract tests
shall fail when a consumer's literal drifts from them.

### REQ-GEN-014 - Feasibility guards

Configuration shall estimate generated volume (including beacon pings, the
store inventory series, and the largest semantic-model table), warn when a
configuration exceeds a smaller capacity's limits, and fail when it exceeds
every capacity's. A retried Silver publication of the same generation shall
succeed without publishing again.

See [the data-contract specification](../../../specifications/modules/generation/data-contract.md).
