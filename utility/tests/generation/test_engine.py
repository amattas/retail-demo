from datetime import date

import pytest
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import default_dictionary_root, load_dictionaries
from retail_setup.generation import engine
from retail_setup.generation.engine import generate_all
from retail_setup.generation.invariants import run_invariants
from retail_setup.generation.schemas import TABLES, column_names


CFG = GenerationConfig(
    store_type="grocery",
    start_date=date(2025, 9, 1),
    end_date=date(2025, 9, 7),
    store_count=2,
    dc_count=1,
    customer_count=200,
    seed=99,
    transactions_per_store_day=30,
    online_orders_per_day=20,
    return_rate=0.05,
)


@pytest.fixture(scope="module")
def dicts():
    return load_dictionaries(default_dictionary_root(), "grocery")


@pytest.fixture(scope="module")
def result(spark, dicts):
    return CFG, generate_all(spark, dicts, CFG)


class MemoryHandoffs:
    """HandoffStore stand-in: each save is a local checkpoint kept by name."""

    def __init__(self):
        self.tables = {}

    def name(self, stage, output):
        return f"memory.{stage}.{output}"

    def save(self, stage, output, frame):
        self.tables[self.name(stage, output)] = frame.localCheckpoint(eager=True)
        return self.tables[self.name(stage, output)]

    def load(self, stage, output):
        return self.tables[self.name(stage, output)]


def _rows(frame):
    return sorted((tuple(row) for row in frame.collect()), key=repr)


def test_notebook_pipeline_reproduces_single_session_generation(result, spark, dicts):
    _, expected = result
    store = MemoryHandoffs()
    for notebook in engine.STAGE_NOTEBOOKS:  # pipeline order is a topological order
        engine.run_generation_notebook(spark, dicts, CFG, notebook, store)
    tables, prestaged = engine.load_publication(spark, dicts, CFG, store)

    assert list(tables) == list(expected.tables)
    for name, frame in tables.items():
        assert frame.schema == expected.tables[name].schema, name
        assert _rows(frame) == _rows(expected.tables[name]), name
    # Every single-handoff fact is promoted straight from its handoff.
    assert prestaged == {
        name: store.name(*sources[0])
        for name, sources in engine.SILVER_SOURCES.items()
        if len(sources) == 1
    }
    assert "fact_receipt_lines" not in prestaged


def _stage(name, deps, build):
    return engine.Stage(name, "test", deps, (name,), build)


def test_stages_wait_for_dependencies(spark):
    order = []

    def build(name, value):
        def run(_ctx, *deps):
            order.append(name)
            return {name: spark.range(value + sum(d[n].count() for d in deps for n in d))}

        return run

    ctx = engine.GenerationContext(spark, None, CFG, {})
    out = engine.run_stages(
        ctx,
        [
            _stage("c", ("a", "b"), build("c", 3)),
            _stage("b", ("a",), build("b", 2)),
            _stage("a", (), build("a", 1)),
        ],
        {},
        lambda _stage, frames: frames,
    )
    assert order.index("a") < order.index("b") < order.index("c")
    assert out["c"]["c"].count() == 3 + 1 + 3


def test_stage_failure_is_raised_and_dependants_never_start(spark):
    started = []

    def broken(_ctx):
        raise RuntimeError("builder exploded")

    def after(_ctx, _bad):
        started.append("after")
        return {"after": spark.range(1)}

    ctx = engine.GenerationContext(spark, None, CFG, {})
    with pytest.raises(RuntimeError, match="builder exploded"):
        engine.run_stages(
            ctx,
            [
                _stage("bad", (), broken),
                _stage("after", ("bad",), after),
            ],
            {},
            lambda _stage, frames: frames,
        )
    assert started == []


def test_reliable_checkpoints_survive_and_release_deletes_their_files(spark, tmp_path):
    spark.sparkContext.setCheckpointDir(tmp_path.as_posix())
    frames = engine._checkpoint_frames({"a": spark.range(5), "b": spark.range(3)}, True)
    files = {
        name: frame._jdf.queryExecution().logical().rdd().getCheckpointFile().get()
        for name, frame in frames.items()
    }

    def exists(path):
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
        return hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration()).exists(hadoop_path)

    assert all(exists(path) for path in files.values())
    assert frames["a"].count() == 5
    engine._release([frames["a"]])
    assert not exists(files["a"]) and exists(files["b"])
    assert frames["b"].count() == 3


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
    assert pay.filter(F.col("order_id_ext").isNotNull()).count() > 0  # online
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
        "receipt_id_ext", F.lit("RCPBOGUS")
    )
    report = run_invariants(spark, broken)
    assert not report.passed
    assert any("fact_receipt_lines" in f for f in report.failures)
