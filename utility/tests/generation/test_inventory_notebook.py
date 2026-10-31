"""Verify the notebook's flat worker closure without launching Spark."""

import json
import subprocess
import sys

from build_notebooks import ENGINE_MODULES, SRC, rewrite_engine_aliases, strip_package_imports


def test_flat_inventory_worker_serializes_without_retail_package_imports():
    modules = list(ENGINE_MODULES)
    for module in ("generation/inventory_policy.py", "generation/fulfillment.py"):
        if module not in modules:
            modules.insert(modules.index("generation/inventory.py"), module)
    sources = []
    for module in modules:
        source = strip_package_imports((SRC / module).read_text(encoding="utf-8"))
        if module == "generation/engine.py":
            source = rewrite_engine_aliases(source)
        sources.append((module, source))

    # __main__ matches a notebook cell and forces cloudpickle's by-value path.
    producer = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import base64 as _probe_base64
import json as _probe_json
import sys as _probe_sys
from pyspark import cloudpickle as _probe_pickle
for _probe_name, _probe_source in _probe_json.load(_probe_sys.stdin):
    exec(compile(_probe_source, _probe_name, "exec"), globals())
_probe_worker = partial(
    _simulate_arrow_partition, start_date=date(2024, 1, 1), end_date=date(2024, 1, 3),
    seed=17, capacity=25, daily_prior=1.0, time_zone="UTC",
)
print(_probe_base64.b64encode(_probe_pickle.dumps(_probe_worker)).decode("ascii"))
""",
        ],
        input=json.dumps(sources),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert producer.returncode == 0, producer.stderr

    consumer = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import base64
import importlib.abc
import sys
from datetime import datetime, timezone
import pyarrow as pa
from pyspark import cloudpickle

class BlockRetailImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "retail_setup" or fullname.startswith("retail_setup."):
            raise ModuleNotFoundError(f"Worker attempted a package import: {fullname}")

assert not any(name.startswith("retail_setup") for name in sys.modules)
sys.meta_path.insert(0, BlockRetailImports())
worker = cloudpickle.loads(base64.b64decode(sys.stdin.read()))
utc = pa.timestamp("us", tz="UTC")
batch = pa.RecordBatch.from_pydict({
    "store_id": pa.array([1, 1], pa.int64()),
    "product_id": pa.array([1, 1], pa.int64()),
    "event_ts": pa.array([datetime(2024, 1, 1, 9), datetime(2024, 1, 1, 10)], utc),
    "receipt_id_ext": ["R1", "R2"],
    "line_num": pa.array([1, 1], pa.int64()),
    "quantity": pa.array([3, 1000], pa.int64()),
    "return_ts": pa.array([datetime(2024, 1, 2, 12), datetime(2024, 1, 2, 13)], utc),
    "return_id": ["RET1", "RET2"],
})
events = pa.Table.from_batches(list(worker(iter([batch])))).to_pylist()
assert {event["kind"] for event in events} >= {
    "INITIAL", "SALE", "LOST_SALE", "RETURN", "REORDER", "SHIPMENT",
}
sales = [event for event in events if event["kind"] == "SALE"]
assert len(sales) == 1 and sales[0]["quantity"] == -3
lost = [event for event in events if event["kind"] == "LOST_SALE"]
assert len(lost) == 1 and lost[0]["unfulfilled_quantity"] == 1000
returns = [event for event in events if event["kind"] == "RETURN"]
assert len(returns) == 1 and returns[0]["source"] == "RET1"
assert returns[0]["event_ts"] == datetime(2024, 1, 2, 12, tzinfo=timezone.utc)
assert all(event["quantity"] <= 25 for event in events if event["kind"] == "SHIPMENT")
assert not any(name.startswith("retail_setup") for name in sys.modules)
""",
        ],
        input=producer.stdout,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert consumer.returncode == 0, consumer.stderr
