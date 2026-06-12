# Setup Utility — Plan 2b: Remaining Facts, Returns, Invariants

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate the remaining 15 fact tables Spark-native (plus returns unioned into the receipts group), wire shared-table unions (`fact_payments`, `fact_receipts`/`_lines`), and add the invariant runner + `setup_run_log` so a single orchestrator call produces the full, validated `ag` fact layer.

**Architecture:** Extends Plan 2a's engine (`retail_setup/generation/`). Same rules: schemas.py is the contract (TMDL test is the arbiter), all randomness from seeded xxhash64 draws (factored into `runtime.seeded_draws`), integer cents, every generator a pure function returning DataFrames. Derivation order matters: receipts → returns (union) → promotions/payments/foot-traffic/sensors (derive from receipts) → inventory chain (SALE txns derive from receipt lines; reorders → truck lifecycle → DC/store/truck inventory → balances → stockouts). One orchestrator (`engine.py`) sequences it; `invariants.py` validates before anything is written.

**Tech Stack:** Same as Plan 2a (PySpark 3.5 local tests, conda env `retail-setup`).

**Spec:** `docs/superpowers/specs/2026-06-12-setup-utility-design.md`
**Ground truth:** extracted 2026-06-12 from `02-historical-data-load.ipynb`, `90-augment-and-dedupe-receipts.ipynb` (post-normalization snake_case names), TMDL tables/relationships, and datagen mixins (constants inlined per task with sources).
**Carry-notes from Plan 2a final review:** payments union before write; returns union into receipts; factor seed helpers; `F.pmod` over `F.abs`; notebooks must pin session TZ=UTC (2c).

**Scope notes (deliberate, document in code where relevant):**
- Distribution-faithful, not loop-faithful (per spec): counts via clamped-normal Poisson approximation, balances via windows.
- `__index_level_0__` legacy pandas-index columns: include in a table's schema ONLY if its TMDL binds it (the contract test decides); populate with `row_number() - 1` over a deterministic order.
- `fact_stockouts` keeps its PascalCase columns (`StoreID`, `DCID`, `ProductID`, `LastKnownQuantity`) — that is what the TMDL binds.
- Marketing campaign lifecycle is simplified to the 4 archetypes with their real channel/cost constants; campaign_id = `CAMP{yyyyMMdd}{archetype_idx:02d}`.
- Backorders, disruptions, and DC↔DC moves are out (demo-irrelevant complexity); online lines still get CANCELLED at the 2% order-cancellation rate.

---

## Environment

Same env as Plan 2a (`retail-setup` conda env, JDK 17, pyspark 3.5). Run tests from `utility/` with `/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest ... -q`.

---

### Task 1: Factor seeded draws into runtime; receipts.py consumes them

**Files:**
- Modify: `utility/src/retail_setup/generation/runtime.py`
- Modify: `utility/src/retail_setup/generation/receipts.py`
- Test: `utility/tests/generation/test_runtime.py` (extend)

- [ ] **Step 1: Failing tests** — append to `utility/tests/generation/test_runtime.py`:

```python
def test_seeded_draws_uniform_properties(spark):
    from pyspark.sql import functions as F
    from retail_setup.generation.runtime import seeded_draws

    d = seeded_draws(seed=42)
    df = spark.range(2000).withColumn("u", d.u(["id"], "test"))
    row = df.agg(F.min("u"), F.max("u"), F.avg("u")).first()
    assert 0.0 <= row[0] and row[1] < 1.0
    assert 0.4 < row[2] < 0.6
    # different salt -> different values; same salt -> identical
    df2 = df.withColumn("u2", d.u(["id"], "other")).withColumn("u3", d.u(["id"], "test"))
    assert df2.filter("u = u3").count() == 2000
    assert df2.filter("u = u2").count() < 100


def test_seeded_draws_seed_sensitivity(spark):
    from retail_setup.generation.runtime import seeded_draws

    a = seeded_draws(seed=1)
    b = seeded_draws(seed=2)
    df = spark.range(100)
    da = [r[0] for r in df.select(a.u(["id"], "s")).collect()]
    db = [r[0] for r in df.select(b.u(["id"], "s")).collect()]
    assert da != db


def test_pick_by_weights(spark):
    from retail_setup.generation.runtime import seeded_draws

    d = seeded_draws(seed=42)
    df = spark.range(5000).withColumn(
        "pick", d.pick_by_weights(["id"], "p", [("A", 0.7), ("B", 0.2), ("C", 0.1)])
    )
    counts = {r["pick"]: r["count"] for r in df.groupBy("pick").count().collect()}
    assert 0.6 < counts["A"] / 5000 < 0.8
    assert set(counts) == {"A", "B", "C"}
```

- [ ] **Step 2: Implement `seeded_draws` in runtime.py**

Move the closure logic out of `receipts.py` (keep semantics; use `F.pmod` so the
Long.MIN_VALUE edge is structurally impossible):

```python
class seeded_draws:
    """Deterministic draw expressions bound to a seed.

    u(cols, salt)        -> uniform [0,1) double
    gauss(cols, salt)    -> ~N(0,1) (Irwin-Hall of 3 uniforms, scaled)
    h64(cols, salt)      -> non-negative long hash
    pick_by_weights(cols, salt, [(value, weight), ...]) -> weighted categorical
    """

    _U_MOD = 10**12

    def __init__(self, seed: int):
        self.seed = seed

    def h64(self, cols: list, salt: str):
        from pyspark.sql import functions as F

        return F.pmod(F.xxhash64(*cols, F.lit(f"{salt}|{self.seed}")), F.lit(2**62))

    def u(self, cols: list, salt: str):
        from pyspark.sql import functions as F

        return (self.h64(cols, salt) % F.lit(self._U_MOD)) / F.lit(float(self._U_MOD))

    def gauss(self, cols: list, salt: str):
        from pyspark.sql import functions as F

        s = self.u(cols, f"{salt}|g1") + self.u(cols, f"{salt}|g2") + self.u(cols, f"{salt}|g3")
        return (s - F.lit(1.5)) * F.lit(2.0)

    def pick_by_weights(self, cols: list, salt: str, weighted: list[tuple[str, float]]):
        from pyspark.sql import functions as F

        total = sum(w for _, w in weighted)
        uu = self.u(cols, salt)
        expr, acc = None, 0.0
        for value, w in weighted[:-1]:
            acc += w / total
            expr = expr.when(uu < acc, value) if expr is not None else F.when(uu < acc, value)
        return expr.otherwise(weighted[-1][0]) if expr is not None else F.lit(weighted[0][0])
```

- [ ] **Step 3: Refactor receipts.py to use `seeded_draws(cfg.seed)`** — delete its
local closures and `_pick_by_weights`/`_pick_hour` duplicates where the new class
covers them (`_pick_hour` can stay but built on `d.u`). Behavior may shift draw
values (pmod + salt delimiters) — that's fine; tests assert properties, not bytes.

- [ ] **Step 4: Full suite green (70 + 3 new), commit**

```bash
git add utility/src/retail_setup/generation/runtime.py utility/src/retail_setup/generation/receipts.py utility/tests/generation/test_runtime.py
git commit -m "refactor(utility): factor seeded draw helpers into runtime"
```

---

### Task 2: Schema additions for the 15 tables

**Files:**
- Modify: `utility/src/retail_setup/generation/schemas.py`

- [ ] **Step 1: Append to `TABLES`** (extracted ground truth; the TMDL contract
test remains the arbiter — if it flags missing bound columns (e.g.
`__index_level_0__`, lifecycle `*_ts`), ADD them; if a listed column type
mismatches TMDL, fix to TMDL. Document every delta in the task report.)

```python
    "fact_store_ops": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("operation_type", "string"), ("event_date", "date"),
    ],
    "fact_foot_traffic": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("sensor_id", "string"), ("zone", "string"), ("dwell_seconds", "long"),
        ("count", "long"), ("event_date", "date"),
    ],
    "fact_ble_pings": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("beacon_id", "string"), ("customer_ble_id", "string"),
        ("customer_id", "double"), ("rssi", "long"), ("zone", "string"),
        ("event_date", "date"),
    ],
    "fact_customer_zone_changes": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("customer_ble_id", "string"), ("from_zone", "string"), ("to_zone", "string"),
        ("event_date", "date"),
    ],
    "fact_marketing": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("channel", "string"),
        ("campaign_id", "string"), ("creative_id", "string"),
        ("customer_ad_id", "string"), ("customer_id", "double"),
        ("impression_id_ext", "string"), ("cost", "string"), ("cost_cents", "long"),
        ("device", "string"), ("event_date", "date"),
    ],
    "fact_promotions": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("receipt_id_ext", "string"),
        ("promo_code", "string"), ("discount_amount", "string"),
        ("discount_cents", "long"), ("discount_type", "string"),
        ("product_count", "long"), ("product_ids", "string"), ("store_id", "long"),
        ("customer_id", "long"), ("event_date", "date"),
    ],
    "fact_promo_lines": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("receipt_id_ext", "string"),
        ("promo_code", "string"), ("line_number", "long"), ("product_id", "long"),
        ("quantity", "long"), ("discount_amount", "string"), ("discount_cents", "long"),
        ("event_date", "date"),
    ],
    "fact_online_order_headers": [
        ("order_id_ext", "string"), ("customer_id", "long"),
        ("subtotal_cents", "long"), ("tax_cents", "long"), ("total_cents", "long"),
        ("subtotal_amount", "string"), ("tax_amount", "string"),
        ("total_amount", "string"), ("payment_method", "string"),
        ("event_ts", "timestamp"), ("event_date", "date"),
    ],
    "fact_online_order_lines": [
        ("order_id", "string"), ("product_id", "long"), ("line_num", "long"),
        ("quantity", "long"), ("unit_price", "string"), ("unit_cents", "long"),
        ("ext_price", "string"), ("ext_cents", "long"), ("promo_code", "string"),
        ("fulfillment_mode", "string"), ("fulfillment_status", "string"),
        ("node_type", "string"), ("node_id", "long"),
        ("picked_ts", "timestamp"), ("shipped_ts", "timestamp"),
        ("delivered_ts", "timestamp"), ("event_ts", "timestamp"), ("event_date", "date"),
    ],
    "fact_reorders": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("dc_id", "long"), ("product_id", "long"), ("current_quantity", "long"),
        ("reorder_quantity", "long"), ("reorder_point", "long"), ("priority", "string"),
        ("event_date", "date"),
    ],
    "fact_truck_moves": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("truck_id", "long"),
        ("dc_id", "long"), ("store_id", "long"), ("shipment_id", "string"),
        ("status", "string"), ("eta", "timestamp"), ("etd", "timestamp"),
        ("departure_time", "timestamp"), ("actual_unload_duration", "double"),
        ("event_date", "date"),
    ],
    "fact_truck_inventory": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("truck_id", "long"),
        ("shipment_id", "string"), ("product_id", "long"), ("quantity", "long"),
        ("action", "string"), ("location_id", "long"), ("location_type", "string"),
        ("event_date", "date"),
    ],
    "fact_dc_inventory_txn": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("dc_id", "long"),
        ("product_id", "long"), ("quantity", "long"), ("balance", "long"),
        ("txn_type", "string"), ("source", "string"), ("event_date", "date"),
    ],
    "fact_store_inventory_txn": [
        ("event_ts", "timestamp"), ("trace_id", "string"), ("store_id", "long"),
        ("product_id", "long"), ("quantity", "long"), ("balance", "long"),
        ("txn_type", "string"), ("source", "string"), ("event_date", "date"),
    ],
    "fact_stockouts": [
        # PascalCase IS the contract: TMDL binds these names directly
        ("event_ts", "timestamp"), ("trace_id", "string"), ("StoreID", "double"),
        ("DCID", "double"), ("ProductID", "long"), ("LastKnownQuantity", "long"),
        ("event_date", "date"),
    ],
```

- [ ] **Step 2: Run the contract test; reconcile against TMDL until green**

```bash
/opt/homebrew/Caskroom/miniforge/base/envs/retail-setup/bin/python -m pytest tests/generation/test_schema_contract.py -q
```

- [ ] **Step 3: Commit**

```bash
git add utility/src/retail_setup/generation/schemas.py
git commit -m "feat(utility): schema contract entries for remaining 15 fact tables"
```

---

### Task 3: Returns (union into the receipts group)

**Files:**
- Create: `utility/src/retail_setup/generation/returns.py`
- Modify: `utility/src/retail_setup/config/generation.py` (add `return_rate: float = 0.01`)
- Test: `utility/tests/generation/test_returns.py`

Semantics (datagen utils_mixin): sample ~`return_rate` of SALE receipts per day
(Dec 26 → 6×, capped at 10% of the day's receipts); return header gets a new
`receipt_id_ext` prefixed `RET` (same 25-char layout, `RET`+ts+store4+seq6),
`receipt_type='RETURN'`, `event_ts` noon same day, `customer_id` NULL,
`tender_type`/`payment_method` `CREDIT_CARD`, discount "0.00", negated cents
(subtotal/tax/total all negative); lines mirror the original receipt's lines
with negative quantity and negated ext/cents, `promo_code='RETURN'`; one
payment per return (negative amount, APPROVED — refunds don't decline).

- [ ] **Step 1: Failing tests** — `utility/tests/generation/test_returns.py`:

```python
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.returns import generate_returns
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 12, 20),
                           end_date=date(2025, 12, 28), store_count=3, dc_count=1,
                           customer_count=300, seed=5, transactions_per_store_day=60,
                           return_rate=0.05)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    rets = generate_returns(spark, sales, dims, cfg)
    return cfg, sales, rets


def test_contract_columns(setup):
    _, _, rets = setup
    for t in ["fact_receipts", "fact_receipt_lines", "fact_payments"]:
        assert rets[t].columns == column_names(t)


def test_return_semantics(setup):
    _, sales, rets = setup
    r = rets["fact_receipts"]
    assert r.count() > 0
    assert r.filter(F.col("receipt_type") != "RETURN").count() == 0
    assert r.filter(~F.col("receipt_id_ext").startswith("RET")).count() == 0
    assert r.filter(F.col("total_cents") >= 0).count() == 0  # strictly negative
    assert r.filter(F.col("customer_id").isNotNull()).count() == 0
    assert r.filter(F.col("tender_type") != "CREDIT_CARD").count() == 0
    # return ids unique and disjoint from sales ids
    n = r.count()
    assert r.select("receipt_id_ext").distinct().count() == n
    overlap = r.join(sales["fact_receipts"], "receipt_id_ext", "inner")
    assert overlap.count() == 0


def test_return_lines_negative_and_linked(setup):
    _, _, rets = setup
    lines = rets["fact_receipt_lines"]
    assert lines.filter(F.col("quantity") >= 0).count() == 0
    assert lines.filter(F.col("promo_code") != "RETURN").count() == 0
    orphans = lines.join(rets["fact_receipts"], "receipt_id_ext", "left_anti")
    assert orphans.count() == 0


def test_return_rate_and_dec26_spike(setup):
    cfg, sales, rets = setup
    by_day_sales = {r["event_date"]: r["count"] for r in
                    sales["fact_receipts"].groupBy("event_date").count().collect()}
    by_day_rets = {r["event_date"]: r["count"] for r in
                   rets["fact_receipts"].groupBy("event_date").count().collect()}
    total_rate = sum(by_day_rets.values()) / sum(by_day_sales.values())
    assert 0.02 < total_rate < 0.10  # 5% nominal incl. one 6x day, 10% cap
    dec26 = date(2025, 12, 26)
    other = [by_day_rets.get(d, 0) / by_day_sales[d] for d in by_day_sales if d != dec26]
    assert by_day_rets.get(dec26, 0) / by_day_sales[dec26] > 2 * (sum(other) / len(other))


def test_return_payment_negative_approved(setup):
    _, _, rets = setup
    pay = rets["fact_payments"]
    assert pay.count() == rets["fact_receipts"].count()
    assert pay.filter(F.col("amount_cents") >= 0).count() == 0
    assert pay.filter(F.col("status") != "APPROVED").count() == 0
```

- [ ] **Step 2: Implement** `generate_returns(spark, sales_group, dims, cfg) -> dict`
with the same three keys as the receipts group. Sketch: take
`sales["fact_receipts"]` + `sales["fact_receipt_lines"]`; per receipt draw
`u(["receipt_id_ext"], "return") < day_rate` where
`day_rate = least(0.10, return_rate * when(month=12 & day=26, 6.0).otherwise(1.0))`;
build the return header from the sampled original (negate cents, noon
`event_ts` = `to_timestamp(concat(event_date,' 12:00:00'))`, new id
`concat('RET', date_format(event_ts,'yyyyMMddHHmm'), lpad(store_id,4,'0'), lpad(row_number() over (partition by store_id, event_date order by receipt_id_ext), 6, '0'))`,
`trace_id = concat('TRC', new_id)`); join lines on the original id, negate
quantity/ext_cents, re-derive formatted strings; payments mirror the receipts
pattern with status literal APPROVED and the original's payment_method replaced
by CREDIT_CARD. End every frame with `.select(*column_names(...))`.

- [ ] **Step 3: Tests green; full suite green; commit**

```bash
git add utility/src/retail_setup/generation/returns.py utility/src/retail_setup/config/generation.py utility/tests/generation/test_returns.py
git commit -m "feat(utility): returns generation unioned into the receipts contract"
```

---

### Task 4: Store ops + foot traffic (derive from receipts)

**Files:**
- Create: `utility/src/retail_setup/generation/store_activity.py`
- Test: `utility/tests/generation/test_store_activity.py`

Rules (datagen sensors/store_ops mixins):
- `fact_store_ops`: exactly 2 rows per store-day (`operation_type` in
  {"opened","closed"}), open/close hours parsed from `dim_stores.operating_hours`
  ("6-22" style and "24h" — map 24h → 0/24); skip Dec 25 entirely;
  `trace_id = concat('TRC-OPS-', store_id, '-', event_date, '-', operation_type)`.
- `fact_foot_traffic`: per store-hour with receipts, `total = greatest(receipts+1, round(receipts / conv))`
  where conv = 0.20 × 1.3 (hours 12,13,17,18,19) × 0.9 (weekend); split across
  the 5 sensor zones `ENTRANCE_MAIN, ENTRANCE_SIDE, AISLES_A, AISLES_B, CHECKOUT`
  with the store_format share table (hypermarket [.20,.10,.35,.25,.10],
  superstore [.25,.10,.30,.25,.10], standard [.30,.15,.25,.15,.15],
  neighborhood [.35,.15,.20,.10,.20]); `dwell_seconds` uniform per zone in
  ([45,90],[30,75],[180,420],[120,300],[90,240]); `sensor_id = format('SENSOR_%03d_%s', store_id, zone)`;
  one row per store-hour-zone; `event_ts` = the hour boundary.

- [ ] **Step 1: Failing tests** — `utility/tests/generation/test_store_activity.py`:

```python
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.store_activity import generate_foot_traffic, generate_store_ops
from retail_setup.generation.schemas import column_names

ZONES = ["ENTRANCE_MAIN", "ENTRANCE_SIDE", "AISLES_A", "AISLES_B", "CHECKOUT"]


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 12, 23),
                           end_date=date(2025, 12, 27), store_count=2, dc_count=1,
                           customer_count=200, seed=9, transactions_per_store_day=40)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    return cfg, dims, sales


def test_store_ops(spark, setup):
    cfg, dims, _ = setup
    ops = generate_store_ops(spark, dims, cfg)
    assert ops.columns == column_names("fact_store_ops")
    # 5 days minus Christmas = 4 op-days x 2 stores x 2 events
    assert ops.count() == 4 * 2 * 2
    assert ops.filter(F.col("event_date") == date(2025, 12, 25)).count() == 0
    per = ops.groupBy("store_id", "event_date").count().collect()
    assert all(r["count"] == 2 for r in per)
    assert {r.operation_type for r in ops.select("operation_type").distinct().collect()} == \
        {"opened", "closed"}


def test_foot_traffic(spark, setup):
    cfg, dims, sales = setup
    ft = generate_foot_traffic(spark, sales["fact_receipts"], dims, cfg)
    assert ft.columns == column_names("fact_foot_traffic")
    assert {r.zone for r in ft.select("zone").distinct().collect()} <= set(ZONES)
    assert ft.filter(F.col("count") < 0).count() == 0
    assert ft.filter((F.col("dwell_seconds") < 30) | (F.col("dwell_seconds") > 420)).count() == 0
    # traffic exceeds receipts for matching store-hours
    rc = sales["fact_receipts"].groupBy(
        "store_id", F.date_trunc("hour", "event_ts").alias("hr")).count()
    tt = ft.groupBy("store_id", F.col("event_ts").alias("hr")) \
           .agg(F.sum("count").alias("traffic"))
    j = rc.join(tt, ["store_id", "hr"])
    assert j.count() > 0
    assert j.filter(F.col("traffic") < F.col("count")).count() == 0
```

- [ ] **Step 2: Implement** `generate_store_ops(spark, dims, cfg)` (store×day grid
minus Dec 25, parse operating_hours with a small `F.when` chain over the four
known formats, two rows via explode of a 2-element array) and
`generate_foot_traffic(spark, receipts, dims, cfg)` (groupBy store/hour from
`fact_receipts`, conv-rate arithmetic, explode the 5-zone share array as
`(zone, share, dwell_lo, dwell_hi)` structs, `count = round(total * share)`,
dwell via `d.u`). End with `.select(*column_names(...))`.

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/generation/store_activity.py utility/tests/generation/test_store_activity.py
git commit -m "feat(utility): store ops + foot traffic generators"
```

---

### Task 5: Online orders (headers, lines, payments stream)

**Files:**
- Create: `utility/src/retail_setup/generation/online_orders.py`
- Modify: `utility/src/retail_setup/config/generation.py` (add `online_orders_per_day: int | None = None`, derived default `store_count * 8` in `_derive_scale_defaults`)
- Test: `utility/tests/generation/test_online_orders.py`

Rules (datagen online_order_generator.py): network-wide daily volume
(monthly-weight scaled, clamped-normal); `order_id_ext = concat('ONL', yyyyMMdd, lpad(seq,5,'0'), 3-digit draw)`;
basket sizes 60%→1-3, 30%→2-5, 10%→5-8 lines, qty 1-3 weighted [.7,.25,.05];
fulfillment per LINE 60% SHIP_FROM_DC / 30% SHIP_FROM_STORE / 10% BOPIS
(`node_type` DC|STORE, `node_id` a valid dc/store id); tender 60% CREDIT_CARD /
25% DEBIT_CARD / 10% PAYPAL / 5% OTHER; 2% of orders CANCELLED (all lines
CANCELLED, financials zeroed); promos on 10-30% of lines, codes
PROMO05/PROMO10/PROMO20 with matching 5/10/20% line discount; tax via the
customer-geography store-rate fallback — SIMPLIFICATION: use the mean store
tax_rate (document it); lifecycle per non-cancelled line: picked_ts = event_ts +
30-240min (BOPIS: 4-24h), shipped_ts = picked + 2-4h (BOPIS: NULL),
delivered_ts = shipped + 1-3 days (BOPIS: picked); statuses DELIVERED
(or CANCELLED). Payments: one per order, `order_id_ext` set,
`receipt_id_ext`/`store_id` NULL, amount = total_cents, decline multipliers
PAYPAL 1.1 / OTHER 0.5 added to the Plan-2a table, processing PAYPAL 2000-5000ms,
OTHER 1500-4000ms; CANCELLED orders still pay then refund? NO — cancelled
orders get NO payment row (document; keeps amount==total invariant).

- [ ] **Step 1: Failing tests** — `utility/tests/generation/test_online_orders.py`:

```python
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.online_orders import generate_online_orders
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 4, 7),
                           end_date=date(2025, 4, 13), store_count=3, dc_count=2,
                           customer_count=300, seed=13, online_orders_per_day=40)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    return cfg, dims, generate_online_orders(spark, dims, dicts.profile, cfg)


def test_contract_columns(setup):
    _, _, g = setup
    assert g["fact_online_order_headers"].columns == column_names("fact_online_order_headers")
    assert g["fact_online_order_lines"].columns == column_names("fact_online_order_lines")
    assert g["payments"].columns == column_names("fact_payments")


def test_volume_and_ids(setup):
    cfg, _, g = setup
    h = g["fact_online_order_headers"]
    n = h.count()
    assert 0.4 * 7 * cfg.online_orders_per_day < n < 2.0 * 7 * cfg.online_orders_per_day
    assert h.select("order_id_ext").distinct().count() == n
    assert h.filter(~F.col("order_id_ext").startswith("ONL")).count() == 0


def test_lines_link_and_money(setup):
    _, _, g = setup
    h, l = g["fact_online_order_headers"], g["fact_online_order_lines"]
    assert l.join(h, l.order_id == h.order_id_ext, "left_anti").count() == 0
    live = l.filter(F.col("fulfillment_status") != "CANCELLED")
    sums = live.groupBy("order_id").agg(F.sum("ext_cents").alias("s"))
    j = h.join(sums, h.order_id_ext == sums.order_id)
    assert j.filter(F.col("subtotal_cents") != F.col("s")).count() == 0
    assert h.filter(F.col("total_cents") != F.col("subtotal_cents") + F.col("tax_cents")).count() == 0


def test_fulfillment_modes_and_nodes(setup):
    _, dims, g = setup
    l = g["fact_online_order_lines"]
    modes = {r.fulfillment_mode for r in l.select("fulfillment_mode").distinct().collect()}
    assert modes <= {"SHIP_FROM_DC", "SHIP_FROM_STORE", "BOPIS"}
    dc_ids = {r.ID for r in dims["dim_distribution_centers"].select("ID").collect()}
    st_ids = {r.ID for r in dims["dim_stores"].select("ID").collect()}
    bad_dc = l.filter((F.col("node_type") == "DC") & ~F.col("node_id").isin(*dc_ids))
    bad_st = l.filter((F.col("node_type") == "STORE") & ~F.col("node_id").isin(*st_ids))
    assert bad_dc.count() == 0 and bad_st.count() == 0


def test_lifecycle_ordering(setup):
    _, _, g = setup
    l = g["fact_online_order_lines"].filter(F.col("fulfillment_status") == "DELIVERED")
    assert l.count() > 0
    assert l.filter(F.col("picked_ts") < F.col("event_ts")).count() == 0
    shipped = l.filter(F.col("shipped_ts").isNotNull())
    assert shipped.filter(F.col("shipped_ts") < F.col("picked_ts")).count() == 0
    assert shipped.filter(F.col("delivered_ts") < F.col("shipped_ts")).count() == 0


def test_cancellations_zeroed_no_payment(setup):
    _, _, g = setup
    h, pay = g["fact_online_order_headers"], g["payments"]
    cancelled = h.join(
        g["fact_online_order_lines"].filter("fulfillment_status = 'CANCELLED'")
        .select(F.col("order_id").alias("order_id_ext")).distinct(),
        "order_id_ext")
    assert cancelled.filter(F.col("total_cents") != 0).count() == 0
    assert pay.join(cancelled.select("order_id_ext"), "order_id_ext").count() == 0
    # payments basics
    assert pay.filter(F.col("receipt_id_ext").isNotNull()).count() == 0
    assert pay.filter(F.col("store_id").isNotNull()).count() == 0
```

- [ ] **Step 2: Implement** `generate_online_orders(spark, dims, profile, cfg)`
returning `{"fact_online_order_headers", "fact_online_order_lines", "payments"}`.
Build a day grid (single-partition keys: day + seq via explode of
clamped-normal counts), draw customer/tender/cancel per order; explode basket
sizes to lines; product pick = uniform over the full catalog (online ignores
department weights — document); per-line mode/node/promo/lifecycle from draws;
aggregate live lines → header money with the receipts tax formula (mean store
rate); zero out cancelled orders' financials. Payments mirror Task 5 rules.

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/generation/online_orders.py utility/src/retail_setup/config/generation.py utility/tests/generation/test_online_orders.py
git commit -m "feat(utility): online orders group with line lifecycle + payments stream"
```

---

### Task 6: Promotions + promo lines (pure derivation from receipt lines)

**Files:**
- Create: `utility/src/retail_setup/generation/promotions.py`
- Test: `utility/tests/generation/test_promotions.py`

Rules (datagen promotions_mixin): `fact_promo_lines` = one row per SALE receipt
line with `promo_code` NOT NULL and a positive line discount
(`discount_cents = unit_cents*quantity - ext_cents`); `fact_promotions` = one
row per (receipt_id_ext, promo_code) aggregating those lines: discount sums,
`product_count` distinct products, `product_ids` comma-joined sorted ids,
`discount_type` = 'PERCENTAGE' (our generated codes are percent-style —
document), store/customer/event fields from the receipt header,
`trace_id = concat('TRC-PRM-', receipt_id_ext, '-', promo_code)`.

- [ ] **Step 1: Failing tests** — `utility/tests/generation/test_promotions.py`:

```python
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.promotions import generate_promotions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 5, 5),
                           end_date=date(2025, 5, 11), store_count=2, dc_count=1,
                           customer_count=200, seed=21, transactions_per_store_day=50)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    promos, promo_lines = generate_promotions(spark, sales)
    return sales, promos, promo_lines


def test_contract_columns(setup):
    _, promos, promo_lines = setup
    assert promos.columns == column_names("fact_promotions")
    assert promo_lines.columns == column_names("fact_promo_lines")


def test_promo_lines_match_discounted_receipt_lines(setup):
    sales, _, promo_lines = setup
    discounted = sales["fact_receipt_lines"].filter(
        F.col("promo_code").isNotNull()
        & (F.col("unit_cents") * F.col("quantity") - F.col("ext_cents") > 0))
    assert promo_lines.count() == discounted.count()
    assert promo_lines.filter(F.col("discount_cents") <= 0).count() == 0


def test_promotions_aggregate(setup):
    _, promos, promo_lines = setup
    agg = promo_lines.groupBy("receipt_id_ext", "promo_code").agg(
        F.sum("discount_cents").alias("d"),
        F.countDistinct("product_id").alias("pc"))
    j = promos.join(agg, ["receipt_id_ext", "promo_code"])
    assert j.count() == promos.count()
    assert j.filter(F.col("discount_cents") != F.col("d")).count() == 0
    assert j.filter(F.col("product_count") != F.col("pc")).count() == 0


def test_promotions_link_to_receipts(setup):
    sales, promos, _ = setup
    assert promos.join(sales["fact_receipts"], "receipt_id_ext", "left_anti").count() == 0
    assert promos.filter(F.col("discount_type") != "PERCENTAGE").count() == 0
```

- [ ] **Step 2: Implement** `generate_promotions(spark, sales_group) -> (promos_df, promo_lines_df)` — pure joins/groupBys, no draws.

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/generation/promotions.py utility/tests/generation/test_promotions.py
git commit -m "feat(utility): promotions + promo lines derived from receipt lines"
```

---### Task 7: Marketing

**Files:**
- Create: `utility/src/retail_setup/generation/marketing.py`
- Test: `utility/tests/generation/test_marketing.py`

Rules (datagen marketing_campaign.py constants): 4 archetypes —
`seasonal_sale` (channels FACEBOOK/GOOGLE/EMAIL, 1000 imp/day),
`product_launch` (INSTAGRAM/YOUTUBE/DISPLAY, 2000),
`loyalty_program` (EMAIL/SOCIAL, 500), `flash_sale` (SOCIAL/SEARCH, 5000 but
only ~1 day in 7). Scale all volumes by `cfg.store_count / 86` (legacy fleet
size — document). Channel cost ranges (uniform draw, dollars):
EMAIL .005-.05, DISPLAY .10-.50, SOCIAL .08-.25, SEARCH .15-.75,
FACEBOOK .10-.40, GOOGLE .25-1.50, INSTAGRAM .08-.35, YOUTUBE .30-1.50.
Device pick MOBILE .6 / DESKTOP .3 / TABLET .1 with cost multiplier
1.2/0.8/0.9. `campaign_id = concat('CAMP', yyyyMMdd, lpad(archetype_idx,2,'0'))`;
`impression_id_ext = concat('IMP', campaign_id, lpad(seq,7,'0'))`;
`creative_id = concat('CREAT', substring(impression_id_ext, 4, 30))`;
customer assignment: uniform customer per impression, `customer_ad_id` from
`dim_customers.AdId`; `customer_id` (double) populated for 5% of impressions,
else NULL. `cost_cents = round(cost_dollars*100)`, `cost = format '%.2f'`.
event_ts uniform within the day.

- [ ] **Step 1: Failing tests** — `utility/tests/generation/test_marketing.py`:

```python
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.marketing import CHANNEL_COSTS, generate_marketing
from retail_setup.generation.schemas import column_names


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 6, 2),
                           end_date=date(2025, 6, 8), store_count=4, dc_count=1,
                           customer_count=200, seed=31)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    return cfg, dims, generate_marketing(spark, dims, cfg)


def test_contract_columns(setup):
    *_, mk = setup
    assert mk.columns == column_names("fact_marketing")


def test_ids_and_costs(setup):
    *_, mk = setup
    n = mk.count()
    assert n > 0
    assert mk.select("impression_id_ext").distinct().count() == n
    assert mk.filter(~F.col("campaign_id").startswith("CAMP")).count() == 0
    # cost_cents within channel band x device multiplier envelope
    for r in mk.select("channel", "cost_cents").collect():
        lo, hi = CHANNEL_COSTS[r.channel]
        assert lo * 100 * 0.8 - 1 <= r.cost_cents <= hi * 100 * 1.2 + 1, r


def test_crm_match_share(setup):
    *_, mk = setup
    share = mk.filter(F.col("customer_id").isNotNull()).count() / mk.count()
    assert 0.01 < share < 0.12  # 5% nominal


def test_device_mix(setup):
    *_, mk = setup
    mob = mk.filter("device = 'MOBILE'").count() / mk.count()
    assert 0.45 < mob < 0.75
```

- [ ] **Step 2: Implement** `generate_marketing(spark, dims, cfg)` with module
constants `ARCHETYPES` and `CHANNEL_COSTS` (dict channel → (lo, hi) dollars).
Day grid × archetypes (flash_sale gated by `u < 1/7`), per-day impression
counts clamped-normal, explode to impressions, channel pick uniform over the
archetype's channels, device/cost/CRM draws as above.

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/generation/marketing.py utility/tests/generation/test_marketing.py
git commit -m "feat(utility): marketing impressions generator"
```

---

### Task 8: BLE pings + customer zone changes

**Files:**
- Create: `utility/src/retail_setup/generation/sensors.py`
- Test: `utility/tests/generation/test_sensors.py`

Rules (datagen sensors_mixin / zone_changes_mixin), Spark-native (the
correlation is mild enough for window functions — document the deviation from
the spec's "pandas-UDF island" expectation):
- Visits = SALE receipts (one visit per receipt): 30% "known" → `customer_ble_id`
  from `dim_customers.BLEId` of the receipt's customer, `customer_id` set
  (double); 70% anonymous → `concat('ANON-', store_id, '-', h64 % 900000 + 100000)`,
  `customer_id` NULL.
- Zones `ENTRANCE, ELECTRONICS, GROCERY, CLOTHING, CHECKOUT`; zones-per-visit
  2-5, pings-per-zone 2-5; rssi uniform [-80,-29]; ping times = receipt
  event_ts ± 15 min spread (clamped ordering: assign each ping
  `offset_min = (zone_rank - 1) * 7 + ping_seq + u` so zone sequence is
  time-ordered within the visit — entry near ENTRANCE first is NOT required;
  keep it simple and document).
- `beacon_id = format('BEACON_%03d_%s', store_id, zone)`;
  `trace_id = concat('TRC-BLE-', receipt_id_ext, '-', seq)`.
- `fact_customer_zone_changes` derived from pings: window per
  (store_id, customer_ble_id, visit) ordered by event_ts, `lag(zone)`,
  keep rows where zone changed; `from_zone`/`to_zone`;
  `trace_id = concat('TRC-ZC-', ...)`.

- [ ] **Step 1: Failing tests** — `utility/tests/generation/test_sensors.py`:

```python
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.sensors import generate_ble
from retail_setup.generation.schemas import column_names

BLE_ZONES = {"ENTRANCE", "ELECTRONICS", "GROCERY", "CLOTHING", "CHECKOUT"}


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 7, 7),
                           end_date=date(2025, 7, 9), store_count=2, dc_count=1,
                           customer_count=150, seed=17, transactions_per_store_day=30)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    pings, zc = generate_ble(spark, sales["fact_receipts"], dims, cfg)
    return sales, pings, zc


def test_contract_columns(setup):
    _, pings, zc = setup
    assert pings.columns == column_names("fact_ble_pings")
    assert zc.columns == column_names("fact_customer_zone_changes")


def test_ping_properties(setup):
    sales, pings, _ = setup
    n_receipts = sales["fact_receipts"].count()
    # 2-5 zones x 2-5 pings per visit => 4-25 pings per receipt
    assert 4 * n_receipts <= pings.count() <= 25 * n_receipts
    assert pings.filter((F.col("rssi") < -80) | (F.col("rssi") > -29)).count() == 0
    assert {r.zone for r in pings.select("zone").distinct().collect()} <= BLE_ZONES
    known = pings.filter(F.col("customer_id").isNotNull()).count() / pings.count()
    assert 0.15 < known < 0.45  # 30% of visits nominal
    anon = pings.filter(F.col("customer_ble_id").startswith("ANON-"))
    assert anon.filter(F.col("customer_id").isNotNull()).count() == 0


def test_zone_changes_derive_from_pings(setup):
    _, pings, zc = setup
    assert zc.count() > 0
    assert zc.filter(F.col("from_zone") == F.col("to_zone")).count() == 0
    # every (store, ble) in zone-changes exists in pings
    zk = zc.select("store_id", "customer_ble_id").distinct()
    pk = pings.select("store_id", "customer_ble_id").distinct()
    assert zk.join(pk, ["store_id", "customer_ble_id"], "left_anti").count() == 0
```

- [ ] **Step 2: Implement** `generate_ble(spark, receipts, dims, cfg) -> (pings_df, zone_changes_df)` per the rules above. Zone selection without
replacement: rank the 5 zones by `d.u(["receipt_id_ext"], f"z{z}")` per visit
and take the top `n_zones` — Spark-native via posexplode of the zone array +
per-zone draw + window rank.

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/generation/sensors.py utility/tests/generation/test_sensors.py
git commit -m "feat(utility): BLE pings + zone changes generators"
```

---

### Task 9: Inventory & logistics chain

**Files:**
- Create: `utility/src/retail_setup/generation/inventory.py`
- Test: `utility/tests/generation/test_inventory.py`

The flow (one function, internally staged; all draws via `seeded_draws`):

1. **Store demand** = SALE txns derived 1:1 from `fact_receipt_lines`
   (quantity → negative, `txn_type='SALE'`, `source='CUSTOMER_PURCHASE'`,
   event fields from the line) plus RETURN add-backs from return lines
   (positive qty, `txn_type='RETURN'`, `source=receipt_id_ext`).
2. **Reorders**: per store-day, aggregate that day's demand per product; for
   the top-demand products draw `reorder_point` 5-20 and emit a reorder when
   cumulative demand since last shipment exceeds it — SIMPLIFICATION: emit one
   `fact_reorders` row per (store, day) for each of the day's top-N demanded
   products where `u < 0.4` (N=5), `current_quantity = greatest(0, reorder_point - demand_today)`,
   `reorder_quantity` 50-200, priority from deficit pct (URGENT ≥50%,
   HIGH ≥25%, else NORMAL), `event_ts` = day 23:00, dc_id = store's assigned
   DC (store_id % dc_count + 1 — document the deterministic store→DC map).
3. **Shipments**: one per (store, day) having reorders — `shipment_id =
   concat('SHIP', yyyyMMdd, lpad(dc,2,'0'), lpad(store,3,'0'))`; truck =
   assigned truck of that DC (round-robin by day); departure next day 06:00,
   travel hours 2-12 (draw), unload 0.5-2.0h; emit the 6 `fact_truck_moves`
   status rows (SCHEDULED day 23:30, LOADING dep-2h... wait: LOADING = dep
   - 2h? datagen: departure+2h was relative to creation; SIMPLIFY and
   document: SCHEDULED 23:30 day0, LOADING 04:00, IN_TRANSIT 06:00 (=
   departure), ARRIVED 06:00+travel (=eta), UNLOADING eta+0.25h, COMPLETED
   eta+unload (=etd, departure_time=etd, actual_unload_duration=unload
   minutes, min 30).
4. **Truck inventory**: LOAD rows (one per shipment product, location DC,
   ts=LOADING) and UNLOAD rows (location store, ts=UNLOADING). Shipment
   products = that store-day's reorder rows (qty = reorder_quantity).
5. **DC txns**: `INBOUND_SHIPMENT` supplier deliveries — per (dc, day) 1-3
   shipments × 5 products × qty 50-500, `source = concat('SUPPLIER-', ...)`;
   `OUTBOUND_SHIPMENT` rows mirroring truck LOADs (negative qty,
   source=shipment_id).
6. **Store inbound**: `INBOUND_SHIPMENT` rows mirroring truck UNLOADs
   (positive qty, source=shipment_id) at the UNLOADING ts.
7. **Balances**: per (node, product) window `sum(quantity) over (order by
   event_ts, txn ordinal rows unbounded preceding)` + an initial-stock seed
   (store: 40-120 per product seen; DC: 500-2000) folded in as a day-0
   synthetic txn (`txn_type='INITIAL'`, `source='SEED'`) so balances rarely go
   negative; clamp NOT applied — negatives become stockout signals.
8. **Stockouts**: rows where balance ≤ 0 and the previous balance > 0
   (window lag), one per (node, product, day) max — `StoreID`/`DCID` mutually
   exclusive doubles, `ProductID`, `LastKnownQuantity = abs(quantity)` of the
   crossing txn, event fields from it.

Returns `dict` with keys: `fact_store_inventory_txn`, `fact_dc_inventory_txn`,
`fact_truck_moves`, `fact_truck_inventory`, `fact_reorders`, `fact_stockouts`.

- [ ] **Step 1: Failing tests** — `utility/tests/generation/test_inventory.py`:

```python
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.dims import generate_dimensions
from retail_setup.generation.inventory import generate_inventory_chain
from retail_setup.generation.receipts import generate_receipts_group
from retail_setup.generation.returns import generate_returns
from retail_setup.generation.schemas import column_names

TABLES = ["fact_store_inventory_txn", "fact_dc_inventory_txn", "fact_truck_moves",
          "fact_truck_inventory", "fact_reorders", "fact_stockouts"]


@pytest.fixture(scope="module")
def setup(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 8, 4),
                           end_date=date(2025, 8, 10), store_count=2, dc_count=1,
                           customer_count=200, seed=23, transactions_per_store_day=40)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    dims = generate_dimensions(spark, dicts, cfg)
    sales = generate_receipts_group(spark, dims, dicts.profile, cfg)
    rets = generate_returns(spark, sales, dims, cfg)
    inv = generate_inventory_chain(spark, sales, rets, dims, cfg)
    return cfg, dims, sales, inv


def test_contract_columns(setup):
    *_, inv = setup
    for t in TABLES:
        assert inv[t].columns == column_names(t), t


def test_sales_txns_mirror_receipt_lines(setup):
    _, _, sales, inv = setup
    txn = inv["fact_store_inventory_txn"].filter("txn_type = 'SALE'")
    assert txn.count() == sales["fact_receipt_lines"].count()
    assert txn.filter(F.col("quantity") >= 0).count() == 0


def test_balances_are_running(setup):
    *_, inv = setup
    txn = inv["fact_store_inventory_txn"]
    # spot-check one (store, product): balance deltas equal quantities
    key = txn.groupBy("store_id", "product_id").count().orderBy(F.desc("count")).first()
    seq = (txn.filter((F.col("store_id") == key.store_id)
                      & (F.col("product_id") == key.product_id))
           .orderBy("event_ts").collect())
    running = 0
    for r in seq:
        running += r.quantity
        assert r.balance == running, (r.event_ts, r.balance, running)


def test_truck_lifecycle(setup):
    *_, inv = setup
    moves = inv["fact_truck_moves"]
    if moves.count() == 0:
        pytest.skip("no shipments triggered in this window")
    per = moves.groupBy("shipment_id").agg(
        F.collect_set("status").alias("statuses"), F.count("*").alias("n"))
    for r in per.collect():
        assert set(r.statuses) == {"SCHEDULED", "LOADING", "IN_TRANSIT", "ARRIVED",
                                   "UNLOADING", "COMPLETED"}
        assert r.n == 6
    done = moves.filter("status = 'COMPLETED'")
    assert done.filter(F.col("actual_unload_duration") < 30).count() == 0
    # load/unload pairs exist per shipment product
    ti = inv["fact_truck_inventory"]
    loads = ti.filter("action = 'LOAD'").groupBy("shipment_id", "product_id").count()
    unloads = ti.filter("action = 'UNLOAD'").groupBy("shipment_id", "product_id").count()
    assert loads.join(unloads, ["shipment_id", "product_id"], "left_anti").count() == 0


def test_reorder_priorities(setup):
    *_, inv = setup
    ro = inv["fact_reorders"]
    if ro.count() == 0:
        pytest.skip("no reorders in window")
    assert {r.priority for r in ro.select("priority").distinct().collect()} <= \
        {"NORMAL", "HIGH", "URGENT"}
    assert ro.filter((F.col("reorder_quantity") < 50) | (F.col("reorder_quantity") > 200)).count() == 0
    assert ro.filter((F.col("reorder_point") < 5) | (F.col("reorder_point") > 20)).count() == 0


def test_stockouts_mutually_exclusive(setup):
    *_, inv = setup
    so = inv["fact_stockouts"]
    both = so.filter(F.col("StoreID").isNotNull() & F.col("DCID").isNotNull())
    neither = so.filter(F.col("StoreID").isNull() & F.col("DCID").isNull())
    assert both.count() == 0 and neither.count() == 0
```

- [ ] **Step 2: Implement** per the staged flow. This is the largest module —
keep stages as private functions (`_sale_txns`, `_reorders`, `_shipments`,
`_truck_frames`, `_dc_txns`, `_with_balances`, `_stockouts`) inside
`inventory.py`; if it exceeds ~400 lines, split balance/stockout helpers into
`inventory_balances.py` and report the split.

- [ ] **Step 3: Green + full suite + commit**

```bash
git add utility/src/retail_setup/generation/inventory.py utility/tests/generation/test_inventory.py
git commit -m "feat(utility): inventory + logistics chain (txns, reorders, trucks, stockouts)"
```

---

### Task 10: Orchestrator, payments/receipts unions, invariants, run log

**Files:**
- Create: `utility/src/retail_setup/generation/engine.py`
- Create: `utility/src/retail_setup/generation/invariants.py`
- Test: `utility/tests/generation/test_engine.py`

- [ ] **Step 1: Failing tests** — `utility/tests/generation/test_engine.py`:

```python
from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation.engine import generate_all
from retail_setup.generation.invariants import run_invariants
from retail_setup.generation.schemas import TABLES, column_names


@pytest.fixture(scope="module")
def result(spark):
    cfg = GenerationConfig(store_type="grocery", start_date=date(2025, 9, 1),
                           end_date=date(2025, 9, 7), store_count=2, dc_count=1,
                           customer_count=200, seed=99, transactions_per_store_day=30,
                           online_orders_per_day=20)
    dicts = load_dictionaries(default_dictionary_root(), "grocery")
    return cfg, generate_all(spark, dicts, cfg)


def test_all_contract_tables_present(result):
    _, out = result
    fact_tables = [t for t in TABLES if t.startswith("fact_")]
    for t in fact_tables:
        assert t in out.tables, t
        assert out.tables[t].columns == column_names(t), t
    for t in ["dim_stores", "dim_products", "dim_date"]:
        assert t in out.tables


def test_unions_applied(result):
    _, out = result
    receipts = out.tables["fact_receipts"]
    assert receipts.filter("receipt_type = 'RETURN'").count() > 0
    assert receipts.filter("receipt_type = 'SALE'").count() > 0
    pay = out.tables["fact_payments"]
    assert pay.filter(F.col("order_id_ext").isNotNull()).count() > 0   # online
    assert pay.filter(F.col("receipt_id_ext").isNotNull()).count() > 0  # in-store + returns
    both = pay.filter(F.col("order_id_ext").isNotNull() & F.col("receipt_id_ext").isNotNull())
    assert both.count() == 0


def test_invariants_pass_and_report(result, spark):
    _, out = result
    report = run_invariants(spark, out.tables)
    assert report.passed, report.failures
    assert report.row_counts["fact_receipts"] > 0
    assert len(report.checks) >= 10


def test_invariants_catch_violations(result, spark):
    _, out = result
    broken = dict(out.tables)
    broken["fact_receipt_lines"] = out.tables["fact_receipt_lines"].withColumn(
        "receipt_id_ext", F.lit("RCPBOGUS"))
    report = run_invariants(spark, broken)
    assert not report.passed
    assert any("fact_receipt_lines" in f for f in report.failures)
```

- [ ] **Step 2: Implement engine.py**

```python
"""Orchestrates full generation. Returns DataFrames; writing happens in 2c."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import DictionarySet
from retail_setup.generation import (
    dims as dims_mod, inventory, marketing, online_orders, promotions,
    receipts as receipts_mod, returns as returns_mod, sensors, store_activity,
)


@dataclass
class GenerationResult:
    tables: dict[str, DataFrame]


def generate_all(spark: SparkSession, dicts: DictionarySet, cfg: GenerationConfig) -> GenerationResult:
    t: dict[str, DataFrame] = {}
    t.update(dims_mod.generate_dimensions(spark, dicts, cfg))
    t["dim_date"] = dims_mod.generate_dim_date(
        spark, cfg.start_date.replace(year=cfg.start_date.year - 5),
        cfg.end_date.replace(year=cfg.end_date.year + 5))

    sales = receipts_mod.generate_receipts_group(spark, t_dims := t, dicts.profile, cfg)
    rets = returns_mod.generate_returns(spark, sales, t, cfg)
    t["fact_receipts"] = sales["fact_receipts"].unionByName(rets["fact_receipts"])
    t["fact_receipt_lines"] = sales["fact_receipt_lines"].unionByName(rets["fact_receipt_lines"])

    online = online_orders.generate_online_orders(spark, t, dicts.profile, cfg)
    t["fact_online_order_headers"] = online["fact_online_order_headers"]
    t["fact_online_order_lines"] = online["fact_online_order_lines"]
    # single-writer union for the shared payments table (2a carry-note)
    t["fact_payments"] = (sales["fact_payments"]
                          .unionByName(rets["fact_payments"])
                          .unionByName(online["payments"]))

    promos, promo_lines = promotions.generate_promotions(spark, sales)
    t["fact_promotions"], t["fact_promo_lines"] = promos, promo_lines
    t["fact_store_ops"] = store_activity.generate_store_ops(spark, t, cfg)
    t["fact_foot_traffic"] = store_activity.generate_foot_traffic(
        spark, sales["fact_receipts"], t, cfg)
    pings, zc = sensors.generate_ble(spark, sales["fact_receipts"], t, cfg)
    t["fact_ble_pings"], t["fact_customer_zone_changes"] = pings, zc
    t.update(inventory.generate_inventory_chain(spark, sales, rets, t, cfg))
    return GenerationResult(tables=t)
```

(Resolve the `t_dims := t` drafting artifact — just pass `t`. Match the actual
signatures from Tasks 3–9; if a signature differs, adapt the orchestrator, not
the generator.)

- [ ] **Step 3: Implement invariants.py**

```python
"""Cross-table invariant checks. Pure reads; raises nothing — returns a report."""

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class InvariantReport:
    checks: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return not self.failures


def _check(report: InvariantReport, name: str, bad_count: int) -> None:
    report.checks.append(name)
    if bad_count:
        report.failures.append(f"{name}: {bad_count} violations")


def run_invariants(spark: SparkSession, t: dict[str, DataFrame]) -> InvariantReport:
    r = InvariantReport()
    for name, df in t.items():
        r.row_counts[name] = df.count()

    receipts, lines, pay = t["fact_receipts"], t["fact_receipt_lines"], t["fact_payments"]
    _check(r, "fact_receipts.receipt_id_ext unique",
           r.row_counts["fact_receipts"] - receipts.select("receipt_id_ext").distinct().count())
    _check(r, "fact_receipt_lines -> fact_receipts FK",
           lines.join(receipts, "receipt_id_ext", "left_anti").count())
    _check(r, "fact_payments xor keys",
           pay.filter(F.col("receipt_id_ext").isNotNull() == F.col("order_id_ext").isNotNull()).count())
    _check(r, "fact_payments -> receipts FK",
           pay.filter(F.col("receipt_id_ext").isNotNull())
              .join(receipts, "receipt_id_ext", "left_anti").count())

    products = t["dim_products"].select(F.col("ID").alias("product_id"))
    for tbl in ["fact_receipt_lines", "fact_online_order_lines", "fact_promo_lines",
                "fact_store_inventory_txn", "fact_dc_inventory_txn", "fact_reorders"]:
        _check(r, f"{tbl} -> dim_products FK",
               t[tbl].join(products, "product_id", "left_anti").count())

    stores = t["dim_stores"].select(F.col("ID").alias("store_id"))
    for tbl in ["fact_receipts", "fact_store_inventory_txn", "fact_reorders",
                "fact_store_ops", "fact_foot_traffic", "fact_ble_pings"]:
        _check(r, f"{tbl} -> dim_stores FK",
               t[tbl].join(stores, "store_id", "left_anti").count())

    for tbl, df in t.items():
        if tbl.startswith("fact_") and "event_date" in df.columns:
            _check(r, f"{tbl}.event_date not null",
                   df.filter(F.col("event_date").isNull()).count())

    oh, ol = t["fact_online_order_headers"], t["fact_online_order_lines"]
    _check(r, "online order ids unique",
           r.row_counts["fact_online_order_headers"]
           - oh.select("order_id_ext").distinct().count())
    _check(r, "online lines -> headers FK",
           ol.join(oh, ol.order_id == oh.order_id_ext, "left_anti").count())

    so = t["fact_stockouts"]
    _check(r, "stockouts StoreID xor DCID",
           so.filter(F.col("StoreID").isNotNull() == F.col("DCID").isNotNull()).count())
    return r
```

- [ ] **Step 4: Green (target ~full suite + ~4 new); FULL suite; commit**

```bash
git add utility/src/retail_setup/generation/engine.py utility/src/retail_setup/generation/invariants.py utility/tests/generation/test_engine.py
git commit -m "feat(utility): generation orchestrator, shared-table unions, invariant runner"
```

---

## Self-review checklist (after all tasks)

- [ ] Full suite green from clean checkout; total runtime < ~5 min locally
- [ ] Contract test green for ALL `TABLES` entries; every TMDL-driven schema delta documented in task reports
- [ ] `grep -rn "F.rand\|applyInPandas" utility/src` → empty
- [ ] Determinism: `generate_all` twice with same cfg → same `fact_receipts` count and id set; different seed → different
- [ ] Every fact frame ends with `.select(*column_names(...))`
- [ ] No file named with "credentials"/"secret"

## Carry-notes from the Plan 2b final review (MUST address in the 2c plan)

- **Single-partition `__index_level_0__` windows** (marketing, sensors,
  online_orders, promotions, `inventory._with_index`): fine at test scale,
  a full-volume write bottleneck — switch to a partitioned or hash-derived
  index in 2c. Treat as must-address.
- Column-wise customer build before real volumes (confirmed still pending).
- Notebooks MUST pin `spark.sql.session.timeZone=UTC` — string-built
  timestamps throughout the engine depend on it (locally the test fixture
  sets it).
- The engine's ±5y dim_date padding is load-bearing for the semantic model's
  date relationships — the 2c writer must preserve it.
- Minor cleanups for 2c: drop the now-unused `partition_seed` column +
  stale docstring in `runtime.store_day_grid` (sensors went Spark-native);
  remove the unused tuple element in `marketing.py`'s day grid; add a guard
  comment on `online_orders`' 5-digit seq lpad (collides above 99,999
  orders/day; config max ~16k).

## Deferred to Plan 2c

- Gold aggregates (9 `au.*` tables), the four notebooks + build script,
  GitHub dictionary fetch (pinned ref + local-first), `setup_run_log` Delta
  write + writer wiring of `generate_all` output, local E2E harness,
  injectable dictionary root for the Fabric runtime, notebooks pin
  `spark.sql.session.timeZone=UTC`, column-wise customer build for 500k-scale.
