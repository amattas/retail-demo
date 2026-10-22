"""Generation stage graph, shared by local runs and the setup pipeline notebooks.

Generation is a dependency graph of stages (``STAGES``). Each stage builds one
or more frames from the dimensions and the outputs of the stages it depends
on. ``generate_all`` runs the whole graph in one Spark session (local tests);
the setup pipeline instead runs each notebook's slice of the graph in its own
session (``run_generation_notebook``), hands outputs between notebooks as
Delta tables, and ``load_publication`` assembles the Silver tables for the
publish notebook. In both modes, stages whose inputs are ready run
concurrently. Generation is deterministic, so neither the split nor the
completion order changes the output.
"""

import warnings
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date

from pyspark import inheritable_thread_target
from pyspark.sql import DataFrame, SparkSession

from retail_setup.config.generation import GenerationConfig
from retail_setup.dictionaries.loader import DictionarySet
from retail_setup.generation import (
    attribution,
    dims as dims_mod,
    inventory,
    marketing,
    online_orders,
    promotions,
    receipts as receipts_mod,
    returns as returns_mod,
    sensors,
    store_activity,
)
from retail_setup.generation.fulfillment import FulfillmentResult, resolve_fulfillment
from retail_setup.generation.handoff import HandoffStore, delete_lakehouse_path


@dataclass
class GenerationResult:
    tables: dict[str, DataFrame]


def _shift_year(d: date, years: int) -> date:
    """Shift a date by whole years; Feb 29 falls back to Feb 28."""
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return date(d.year + years, d.month, 28)


DICTIONARIES_NOTEBOOK = "setup-01-seed-dictionaries"
DIMENSIONS_NOTEBOOK = "setup-02-generate-dimensions"
PUBLISH_NOTEBOOK = "setup-10-publish-silver"
# Ready stages run concurrently; Spark schedules their jobs side by side so
# narrow stages, the Python inventory policy, and stragglers do not leave the
# pool idle.
MAX_PARALLEL_STAGES = 6

StageOutputs = dict[str, DataFrame]


@dataclass(frozen=True)
class GenerationContext:
    spark: SparkSession
    dicts: DictionarySet
    cfg: GenerationConfig
    dims: dict[str, DataFrame]


@dataclass(frozen=True)
class Stage:
    """One generation step: ``build(ctx, *outputs_of_deps) -> outputs``.

    ``notebook`` is the setup pipeline notebook that runs the stage. Stages
    with ``handoff=False`` are only consumed inside their own notebook and
    are never written as handoff tables.
    """

    name: str
    notebook: str
    deps: tuple[str, ...]
    outputs: tuple[str, ...]
    build: Callable[..., StageOutputs]
    handoff: bool = True


_RECEIPT_GROUP = ("fact_receipts", "fact_receipt_lines", "fact_payments")


def _accepted(plan: StageOutputs) -> StageOutputs:
    return {name: plan[name] for name in _RECEIPT_GROUP}


def _build_candidates(ctx: GenerationContext) -> StageOutputs:
    return receipts_mod.generate_receipts_group(ctx.spark, ctx.dims, ctx.dicts.profile, ctx.cfg)


def _build_plan(ctx: GenerationContext, candidates: StageOutputs) -> StageOutputs:
    # Inventory-constrained fulfillment precedes every receipt consumer.
    plan = resolve_fulfillment(candidates, ctx.dims, ctx.cfg)
    return {"return_headers": plan.return_headers, "events": plan.events, **plan.sales}


def _build_online(ctx: GenerationContext) -> StageOutputs:
    return online_orders.generate_online_orders(ctx.spark, ctx.dims, ctx.dicts.profile, ctx.cfg)


def _build_ads(ctx: GenerationContext) -> StageOutputs:
    return {"fact_marketing": marketing.generate_marketing(ctx.spark, ctx.dims, ctx.cfg)}


def _build_refunds(ctx: GenerationContext, plan: StageOutputs) -> StageOutputs:
    return returns_mod.generate_returns(
        ctx.spark, _accepted(plan), ctx.dims, ctx.cfg, return_headers=plan["return_headers"])


def _build_promos(ctx: GenerationContext, plan: StageOutputs) -> StageOutputs:
    promos, promo_lines = promotions.generate_promotions(ctx.spark, _accepted(plan))
    return {"fact_promotions": promos, "fact_promo_lines": promo_lines}


def _build_store_ops(ctx: GenerationContext) -> StageOutputs:
    return {"fact_store_ops": store_activity.generate_store_ops(ctx.spark, ctx.dims, ctx.cfg)}


def _build_foot_traffic(ctx: GenerationContext, plan: StageOutputs) -> StageOutputs:
    return {"fact_foot_traffic": store_activity.generate_foot_traffic(
        ctx.spark, plan["fact_receipts"], ctx.dims, ctx.cfg)}


def _build_ble(ctx: GenerationContext, plan: StageOutputs) -> StageOutputs:
    pings, zone_changes = sensors.generate_ble(ctx.spark, plan["fact_receipts"], ctx.dims, ctx.cfg)
    return {"fact_ble_pings": pings, "fact_customer_zone_changes": zone_changes}


def _build_ledgers(ctx: GenerationContext, plan: StageOutputs) -> StageOutputs:
    result = FulfillmentResult(_accepted(plan), plan["return_headers"], plan["events"])
    # Restocking returns are already events in the fulfillment plan, so the
    # ledgers need no refund frames.
    return inventory.generate_inventory_chain(
        ctx.spark, _accepted(plan), {}, ctx.dims, ctx.cfg, fulfillment=result)


def _union(frames: list[DataFrame]) -> DataFrame:
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def _build_attributed(
    ctx: GenerationContext, plan: StageOutputs, refunds: StageOutputs,
    online: StageOutputs, promos: StageOutputs, ads: StageOutputs,
) -> StageOutputs:
    # IMP-007: deterministic last-touch attribution over every purchase stream.
    inputs = {
        **ctx.dims, **promos, **ads,
        "fact_receipts": _union([plan["fact_receipts"], refunds["fact_receipts"]]),
        "fact_receipt_lines": _union([plan["fact_receipt_lines"], refunds["fact_receipt_lines"]]),
        "fact_online_order_headers": online["fact_online_order_headers"],
        "fact_payments": _union(
            [plan["fact_payments"], refunds["fact_payments"], online["payments"]]),
    }
    out = attribution.generate_attribution(ctx.spark, inputs, ctx.cfg)
    return {name: out[name] for name in _ATTRIBUTED}


_ATTRIBUTED = (
    "fact_receipts", "fact_online_order_headers", "fact_payments",
    "fact_promotions", "fact_marketing", "fact_marketing_attribution",
)
_LEDGERS = (
    "fact_store_inventory_txn", "fact_dc_inventory_txn", "fact_truck_moves",
    "fact_truck_inventory", "fact_reorders", "fact_stockouts",
)

STAGES: tuple[Stage, ...] = (
    Stage("candidates", "setup-03-generate-sales", (), _RECEIPT_GROUP,
          _build_candidates, handoff=False),
    Stage("plan", "setup-03-generate-sales", ("candidates",),
          ("return_headers", "events", *_RECEIPT_GROUP), _build_plan),
    Stage("online", "setup-04-generate-online-orders", (),
          ("fact_online_order_headers", "fact_online_order_lines", "payments"), _build_online),
    Stage("ads", "setup-05-generate-marketing", (), ("fact_marketing",), _build_ads),
    Stage("refunds", "setup-06-generate-returns-and-promotions", ("plan",),
          _RECEIPT_GROUP, _build_refunds),
    Stage("promos", "setup-06-generate-returns-and-promotions", ("plan",),
          ("fact_promotions", "fact_promo_lines"), _build_promos),
    Stage("store_ops", "setup-07-generate-store-activity", (), ("fact_store_ops",),
          _build_store_ops),
    Stage("foot_traffic", "setup-07-generate-store-activity", ("plan",),
          ("fact_foot_traffic",), _build_foot_traffic),
    Stage("ble", "setup-07-generate-store-activity", ("plan",),
          ("fact_ble_pings", "fact_customer_zone_changes"), _build_ble),
    Stage("ledgers", "setup-08-generate-inventory", ("plan",), _LEDGERS, _build_ledgers),
    Stage("attributed", "setup-09-attribute-marketing",
          ("plan", "refunds", "online", "promos", "ads"), _ATTRIBUTED, _build_attributed),
)
_STAGE_BY_NAME = {stage.name: stage for stage in STAGES}
STAGE_NOTEBOOKS = tuple(dict.fromkeys(stage.notebook for stage in STAGES))

# Published Silver fact table -> the stage outputs it is made of (unioned in order).
SILVER_SOURCES: dict[str, tuple[tuple[str, str], ...]] = {
    "fact_receipts": (("attributed", "fact_receipts"),),
    "fact_receipt_lines": (("plan", "fact_receipt_lines"), ("refunds", "fact_receipt_lines")),
    "fact_online_order_headers": (("attributed", "fact_online_order_headers"),),
    "fact_online_order_lines": (("online", "fact_online_order_lines"),),
    "fact_payments": (("attributed", "fact_payments"),),
    "fact_promotions": (("attributed", "fact_promotions"),),
    "fact_promo_lines": (("promos", "fact_promo_lines"),),
    "fact_marketing": (("attributed", "fact_marketing"),),
    "fact_marketing_attribution": (("attributed", "fact_marketing_attribution"),),
    "fact_store_ops": (("store_ops", "fact_store_ops"),),
    "fact_foot_traffic": (("foot_traffic", "fact_foot_traffic"),),
    "fact_ble_pings": (("ble", "fact_ble_pings"),),
    "fact_customer_zone_changes": (("ble", "fact_customer_zone_changes"),),
    **{name: (("ledgers", name),) for name in _LEDGERS},
}
_PUBLISHED_STAGES = {stage for sources in SILVER_SOURCES.values() for stage, _ in sources}


def setup_notebook_dependencies() -> dict[str, tuple[str, ...]]:
    """Upstream notebooks of each generation and publish notebook.

    The setup pipeline's activity graph must equal this mapping (tested), so
    the pipeline is the stage graph drawn at notebook granularity. Generation
    regenerates dimensions in memory, so root generation notebooks need only
    the dictionaries; setup-02's dimension validation runs alongside them and
    gates publication instead.
    """
    graph: dict[str, tuple[str, ...]] = {}
    for notebook in STAGE_NOTEBOOKS:
        upstream = {
            _STAGE_BY_NAME[dep].notebook
            for stage in STAGES if stage.notebook == notebook
            for dep in stage.deps
        } - {notebook}
        graph[notebook] = tuple(sorted(upstream)) or (DICTIONARIES_NOTEBOOK,)
    graph[PUBLISH_NOTEBOOK] = tuple(sorted(
        {DIMENSIONS_NOTEBOOK}
        | {_STAGE_BY_NAME[stage].notebook for stage in _PUBLISHED_STAGES}))
    return graph


def generate_dimension_frames(
    spark: SparkSession, dicts: DictionarySet, cfg: GenerationConfig,
) -> dict[str, DataFrame]:
    dims = dims_mod.generate_dimensions(spark, dicts, cfg)
    # dim_date is padded ±5 years — the semantic model's date relationships
    # need the headroom around the generated fact window.
    dims["dim_date"] = dims_mod.generate_dim_date(
        spark, _shift_year(cfg.start_date, -5), _shift_year(cfg.end_date, 5))
    return dims


def generate_all(
    spark: SparkSession, dicts: DictionarySet, cfg: GenerationConfig,
    *, checkpoint_dir: str | None = None,
) -> GenerationResult:
    """Run the whole stage graph in one session and return every Silver table.

    Each stage's outputs are checkpointed before dependants consume them (see
    ``_checkpoint_frames``). With ``checkpoint_dir`` (a shared filesystem
    path) checkpoints are written there and survive executor loss; without
    it they stay on the executors, which suits local runs only. The caller
    owns deleting the directory once the tables are no longer needed.
    """
    reliable = checkpoint_dir is not None
    if reliable:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
    # Every builder reads the dimensions; materialize them once instead of
    # re-serializing their driver-built rows from Python for each consumer.
    dims = _checkpoint_frames(generate_dimension_frames(spark, dicts, cfg), reliable)
    ctx = GenerationContext(spark, dicts, cfg, dims)
    out = run_stages(ctx, STAGES, {}, lambda _stage, frames: _checkpoint_frames(frames, reliable))
    tables = {**dims, **{
        name: _union([out[stage][output] for stage, output in sources])
        for name, sources in SILVER_SOURCES.items()
    }}
    # Release stage data the published tables no longer read.
    referenced = {id(out[stage][output]) for sources in SILVER_SOURCES.values()
                  for stage, output in sources}
    _release(frame for frames in out.values() for frame in frames.values()
             if id(frame) not in referenced)
    return GenerationResult(tables=tables)


def run_generation_notebook(
    spark: SparkSession, dicts: DictionarySet, cfg: GenerationConfig, notebook: str,
    store: HandoffStore, *, checkpoint_dir: str | None = None,
) -> dict[str, StageOutputs]:
    """Run one setup notebook's stages, reading and writing handoff tables.

    Upstream stage outputs are loaded from handoffs (fingerprint-checked);
    this notebook's handoff stages are written to handoffs, and internal
    stages are checkpointed (reliably under ``checkpoint_dir`` when given,
    as the setup notebooks always do).
    """
    stages = [stage for stage in STAGES if stage.notebook == notebook]
    if not stages:
        raise ValueError(f"no generation stages run in notebook {notebook!r}")
    reliable = checkpoint_dir is not None
    if reliable:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
    dims = _checkpoint_frames(generate_dimension_frames(spark, dicts, cfg), reliable)
    ctx = GenerationContext(spark, dicts, cfg, dims)
    local = {stage.name for stage in stages}
    upstream = {dep for stage in stages for dep in stage.deps} - local
    inputs = {
        dep: {output: store.load(dep, output) for output in _STAGE_BY_NAME[dep].outputs}
        for dep in sorted(upstream)
    }

    def materialize(stage: Stage, frames: StageOutputs) -> StageOutputs:
        if not stage.handoff:
            return _checkpoint_frames(frames, reliable)
        return _persist_frames(
            frames, lambda output, frame: store.save(stage.name, output, frame))

    out = run_stages(ctx, stages, inputs, materialize)
    _release(dims.values())
    return {name: out[name] for name in local if _STAGE_BY_NAME[name].handoff}


def load_publication(
    spark: SparkSession, dicts: DictionarySet, cfg: GenerationConfig,
    store: HandoffStore, *, checkpoint_dir: str | None = None,
) -> tuple[dict[str, DataFrame], dict[str, str]]:
    """Assemble every Silver table from handoffs for the publish notebook.

    Returns ``(tables, prestaged)``: ``prestaged`` maps each table that is a
    single handoff to that handoff's name, so publication promotes it directly
    instead of copying it to a second staging table.
    """
    reliable = checkpoint_dir is not None
    if reliable:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
    tables = _checkpoint_frames(generate_dimension_frames(spark, dicts, cfg), reliable)
    prestaged: dict[str, str] = {}
    for name, sources in SILVER_SOURCES.items():
        tables[name] = _union([store.load(stage, output) for stage, output in sources])
        if len(sources) == 1:
            prestaged[name] = store.name(*sources[0])
    return tables, prestaged


def run_stages(
    ctx: GenerationContext, stages: Iterable[Stage], inputs: dict[str, StageOutputs],
    materialize: Callable[[Stage, StageOutputs], StageOutputs],
) -> dict[str, StageOutputs]:
    """Run each stage once its dependencies finish, up to MAX_PARALLEL_STAGES at once.

    ``inputs`` holds already-available outputs of stages outside ``stages``.
    Each stage builds and materializes its frames in its own thread. A stage
    that publishes nothing is released once every consumer in this run has
    finished. After a failure no further stage starts; stages already
    running finish (cancelling their jobs mid-cache can deadlock Spark's cache
    manager), then the first error is raised.
    """
    pending = {stage.name: stage for stage in stages}
    results: dict[str, StageOutputs] = dict(inputs)
    consumers = {
        name: {other.name for other in pending.values() if name in other.deps}
        for name in pending
    }
    running: dict[Future, str] = {}
    failure: BaseException | None = None

    def run(stage: Stage) -> StageOutputs:
        frames = stage.build(ctx, *[results[dep] for dep in stage.deps])
        if set(frames) != set(stage.outputs):
            raise ValueError(
                f"stage {stage.name!r} built {sorted(frames)}, declared {sorted(stage.outputs)}")
        return materialize(stage, frames)

    def release_finished() -> None:
        for name, users in consumers.items():
            if (name in results and name not in _PUBLISHED_STAGES and name not in inputs
                    and users and all(user in results for user in users)):
                _release(results[name].values())
                users.clear()

    with ThreadPoolExecutor(MAX_PARALLEL_STAGES) as pool:
        while pending or running:
            for name, stage in list(pending.items()):
                if failure is not None:
                    break
                if len(running) < MAX_PARALLEL_STAGES and all(d in results for d in stage.deps):
                    del pending[name]
                    # Copies the caller's Spark local properties (job group,
                    # Fabric run tagging) into the worker thread.
                    running[pool.submit(_inheriting(run), stage)] = name
            if not running:
                if failure is not None:
                    break
                raise ValueError(f"generation stages have unmet dependencies: {sorted(pending)}")
            done, _ = wait(running, return_when=FIRST_COMPLETED)
            for future in done:
                name = running.pop(future)
                try:
                    results[name] = future.result()
                except BaseException as exc:  # noqa: BLE001 - re-raised below
                    failure = failure or exc
            if failure is None:
                release_finished()
    if failure is not None:
        raise failure
    return {name: frames for name, frames in results.items() if name not in inputs}


def _inheriting(fn: Callable) -> Callable:
    """Wrap ``fn`` so its thread inherits the caller's Spark local properties.

    Uses the function-only form, which every supported PySpark accepts (the
    session form is not available on all Fabric runtimes). Newer PySpark warns
    that job tags are not inherited; generation does not use job tags.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Spark session is not provided")
        return inheritable_thread_target(fn)


def _persist_frames(
    frames: StageOutputs, persist: Callable[[str, DataFrame], DataFrame],
) -> StageOutputs:
    """Persist sibling frames one at a time, computing shared work once.

    Siblings are cached first so a later sibling reuses work an earlier one
    computed. They are persisted sequentially: concurrent jobs over related
    cached plans can deadlock Spark's cache manager when one of them fails.
    Stages, and the notebooks around them, still run concurrently.
    """
    cached = {name: frame.cache() for name, frame in frames.items()}
    try:
        return {name: persist(name, frame) for name, frame in cached.items()}
    finally:
        for frame in cached.values():
            frame.unpersist()


def _checkpoint_frames(frames: StageOutputs, reliable: bool) -> StageOutputs:
    """Materialize each frame once and cut its lineage.

    Generation reuses frames heavily, so without truncation every downstream
    plan embeds the full upstream lineage: at full-demo scale the driver spent
    most of its time re-analyzing and cache-matching those plans for every
    action. Reliable checkpoints are written to the SparkContext checkpoint
    directory and survive executor loss; local checkpoints keep rows on the
    executors that computed them and are only safe for local runs.
    """
    def checkpoint(_name: str, frame: DataFrame) -> DataFrame:
        return frame.checkpoint(eager=True) if reliable else frame.localCheckpoint(eager=True)

    return _persist_frames(frames, checkpoint)


def _release(frames: Iterable[DataFrame]) -> None:
    """Free checkpointed rows: executor blocks and, if reliable, their files.

    Frames that are not checkpoints (for example handoff table scans) are
    left alone.
    """
    for frame in frames:
        plan = frame._jdf.queryExecution().logical()
        if plan.getClass().getSimpleName() != "LogicalRDD":
            continue
        rdd = plan.rdd()
        rdd.unpersist(False)
        checkpoint_file = rdd.getCheckpointFile()
        if checkpoint_file.isDefined():
            delete_lakehouse_path(frame.sparkSession, checkpoint_file.get())


