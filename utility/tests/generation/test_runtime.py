from datetime import date

from retail_setup.generation.runtime import derive_seed, store_day_grid


def test_derive_seed_deterministic_and_distinct():
    a = derive_seed(42, "receipts", 7, date(2025, 3, 1))
    b = derive_seed(42, "receipts", 7, date(2025, 3, 1))
    c = derive_seed(42, "receipts", 8, date(2025, 3, 1))
    d = derive_seed(43, "receipts", 7, date(2025, 3, 1))
    assert a == b
    assert len({a, c, d}) == 3
    assert 0 <= a < 2**31  # numpy-seedable


def test_store_day_grid(spark):
    grid = store_day_grid(
        spark,
        store_ids=[1, 2],
        start=date(2025, 1, 1),
        end=date(2025, 1, 3),
        global_seed=42,
        section="receipts",
    )
    rows = grid.collect()
    assert len(rows) == 6  # 2 stores x 3 days
    cols = set(grid.columns)
    assert {"store_id", "day", "partition_seed"} <= cols
    seeds = {(r.store_id, str(r.day)): r.partition_seed for r in rows}
    assert len(set(seeds.values())) == 6  # all distinct
