"""Write layer. Notebooks call write_to_lakehouse/write_all; tests use
write_table with a format/location override (no delta-spark dependency
locally).

``write_all`` publishes every candidate table through the stage -> validate
-> promote (-> rollback) state machine in ``publication.py``: nothing final
is touched until every candidate has staged and validated successfully, and
any promotion failure rolls back everything already promoted in this run.
Catalog mode (``lakehouse=``) uses real Delta staging tables and
``RESTORE TABLE ... TO VERSION AS OF``/``DROP TABLE`` for rollback; local
path mode (``base_path=``) uses a filesystem staging/backup strategy so
tests don't need delta-spark.
"""

import re
import shutil
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.publication import (
    PublicationBackend,
    PublicationCoordinator,
    TableTarget,
    TargetState,
)

_UNSAFE_IDENTIFIER = re.compile(r"[^0-9A-Za-z_]")


def sanitize_identifier(value: str) -> str:
    """Make ``value`` safe to embed in a Spark/Delta table identifier.

    Used to derive run-scoped staging table names from ``run_id`` (which may
    contain ``-`` and other characters legal in a run id but not desirable in
    an unquoted identifier segment).
    """
    return _UNSAFE_IDENTIFIER.sub("_", value)


def write_table(df: DataFrame, location: str, fmt: str = "delta") -> None:
    df.write.format(fmt).mode("overwrite").save(location)


def write_to_lakehouse(df: DataFrame, lakehouse: str, schema: str, table: str) -> None:
    """Overwrite-by-design, matching 02-historical-data-load semantics."""
    df.write.format("delta").mode("overwrite").saveAsTable(f"{lakehouse}.{schema}.{table}")


def _schema_signature(df: DataFrame) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


class _LakehouseBackend:
    """PublicationBackend for Fabric Lakehouse Delta tables.

    Every candidate is staged to ``{lakehouse}.{db}_stage.{run_token}__{name}``
    (a schema distinct from and never colliding with the final ``{db}``
    schema, or with another run's staging tables — ``run_token`` is derived
    from the already-unique ``run_id``). Promotion uses
    ``CREATE OR REPLACE TABLE ... USING DELTA AS SELECT * FROM <staging>``.
    Rollback restores pre-existing targets with
    ``RESTORE TABLE ... TO VERSION AS OF`` (version captured via
    ``DESCRIBE HISTORY ... LIMIT 1`` before promotion) and drops targets that
    were newly created by this run.
    """

    def __init__(
        self, spark, lakehouse: str, run_id: str, sources: dict[tuple[str, str], DataFrame]
    ) -> None:
        self.spark = spark
        self.lakehouse = lakehouse
        self.run_token = sanitize_identifier(run_id)
        self.sources = sources
        self._staging_dbs_created: set[str] = set()

    def _final(self, target: TableTarget) -> str:
        return f"{self.lakehouse}.{target.db}.{target.name}"

    def _ensure_staging_db(self, db: str) -> None:
        stage_db = f"{db}_stage"
        if stage_db not in self._staging_dbs_created:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.lakehouse}.{stage_db}")
            self._staging_dbs_created.add(stage_db)

    def stage(self, target: TableTarget) -> int:
        df = self.sources[(target.db, target.name)]
        self._ensure_staging_db(target.db)
        df.write.format("delta").mode("overwrite").saveAsTable(target.staging_name)
        return self.spark.table(target.staging_name).count()

    def validate(self, target: TableTarget, staged_row_count: int) -> None:
        df = self.sources[(target.db, target.name)]
        staged = self.spark.table(target.staging_name)
        staged_schema = _schema_signature(staged)
        source_schema = _schema_signature(df)
        if staged_schema != source_schema:
            raise ValueError(
                f"schema mismatch staging {target.staging_name!r}: "
                f"{staged_schema} != {source_schema}"
            )
        source_count = df.count()
        if staged_row_count != source_count:
            raise ValueError(
                f"row count mismatch staging {target.staging_name!r}: "
                f"staged={staged_row_count} source={source_count}"
            )

    def target_state(self, target: TableTarget) -> TargetState:
        final = self._final(target)
        if not self.spark.catalog.tableExists(final):
            return TargetState(existed=False)
        version = self.spark.sql(f"DESCRIBE HISTORY {final} LIMIT 1").collect()[0]["version"]
        return TargetState(existed=True, restore_token=version)

    def promote(self, target: TableTarget) -> int:
        final = self._final(target)
        self.spark.sql(
            f"CREATE OR REPLACE TABLE {final} USING DELTA AS SELECT * FROM {target.staging_name}"
        )
        return self.spark.table(final).count()

    def restore(self, target: TableTarget, state: TargetState) -> None:
        final = self._final(target)
        self.spark.sql(f"RESTORE TABLE {final} TO VERSION AS OF {state.restore_token}")

    def drop(self, target: TableTarget) -> None:
        self.spark.sql(f"DROP TABLE IF EXISTS {self._final(target)}")

    def cleanup(self, target: TableTarget) -> None:
        self.spark.sql(f"DROP TABLE IF EXISTS {target.staging_name}")


class _FilesystemBackend:
    """PublicationBackend for local path/parquet test mode.

    No Delta dependency: staging writes to a run-scoped directory under
    ``<base_path>/.setup_staging/<run_token>/``, promotion replaces the final
    directory outright, and rollback restores from a pre-promotion backup
    copy (pre-existing targets, backed up under ``.setup_backup/<run_token>/``
    before the first promotion touches them) or simply removes the directory
    (targets created by this run).
    """

    def __init__(
        self, spark, base_path: Path, fmt: str, run_id: str, sources: dict[tuple[str, str], DataFrame]
    ) -> None:
        self.spark = spark
        self.base_path = base_path
        self.fmt = fmt
        self.run_token = sanitize_identifier(run_id)
        self.sources = sources
        self._staging_root = base_path / ".setup_staging" / self.run_token
        self._backup_root = base_path / ".setup_backup" / self.run_token

    def _final_path(self, target: TableTarget) -> Path:
        return self.base_path / target.db / target.name

    def _staging_path(self, target: TableTarget) -> Path:
        return self._staging_root / target.db / target.name

    def _backup_path(self, target: TableTarget) -> Path:
        return self._backup_root / target.db / target.name

    def stage(self, target: TableTarget) -> int:
        df = self.sources[(target.db, target.name)]
        path = self._staging_path(target)
        df.write.format(self.fmt).mode("overwrite").save(str(path))
        return self.spark.read.format(self.fmt).load(str(path)).count()

    def validate(self, target: TableTarget, staged_row_count: int) -> None:
        df = self.sources[(target.db, target.name)]
        staged = self.spark.read.format(self.fmt).load(str(self._staging_path(target)))
        staged_schema = _schema_signature(staged)
        source_schema = _schema_signature(df)
        if staged_schema != source_schema:
            raise ValueError(
                f"schema mismatch staging {target.name!r}: {staged_schema} != {source_schema}"
            )
        source_count = df.count()
        if staged_row_count != source_count:
            raise ValueError(
                f"row count mismatch staging {target.name!r}: "
                f"staged={staged_row_count} source={source_count}"
            )

    def target_state(self, target: TableTarget) -> TargetState:
        final = self._final_path(target)
        if not final.exists():
            return TargetState(existed=False)
        backup = self._backup_path(target)
        backup.parent.mkdir(parents=True, exist_ok=True)
        if backup.exists():
            shutil.rmtree(backup)
        shutil.copytree(final, backup)
        return TargetState(existed=True, restore_token=str(backup))

    def promote(self, target: TableTarget) -> int:
        final = self._final_path(target)
        if final.exists():
            shutil.rmtree(final)
        final.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(self._staging_path(target), final)
        return self.spark.read.format(self.fmt).load(str(final)).count()

    def restore(self, target: TableTarget, state: TargetState) -> None:
        final = self._final_path(target)
        if final.exists():
            shutil.rmtree(final)
        shutil.copytree(Path(str(state.restore_token)), final)

    def drop(self, target: TableTarget) -> None:
        final = self._final_path(target)
        if final.exists():
            shutil.rmtree(final)

    def cleanup(self, target: TableTarget) -> None:
        staged = self._staging_path(target)
        if staged.exists():
            shutil.rmtree(staged)
        backup = self._backup_path(target)
        if backup.exists():
            shutil.rmtree(backup)


def write_all(
    tables: dict[str, DataFrame],
    gold: dict[str, DataFrame],
    cfg: GenerationConfig,
    run_id: str,
    *,
    lakehouse: str | None = None,
    base_path: str | None = None,
    fmt: str = "delta",
) -> list[str]:
    """Publish dims+facts to silver, gold to gold, then setup_run_log.

    Two modes: catalog (lakehouse=...) used by notebooks, or path
    (base_path=...) used by local tests/E2E. Either ``tables`` or ``gold``
    (or both) may be non-empty; at least one of the two must contain a table.
    dim_date is written as generated by the engine (±5y padding preserved —
    the semantic model's date relationships need the headroom).

    Every candidate table is staged to a run-scoped location, validated
    against its source, and only then promoted to its fixed final name (see
    ``retail_setup.generation.publication``). If any promotion fails, every
    target already promoted in this run is rolled back and a
    ``RuntimeError`` is raised — this function never returns after a partial
    or rolled-back publish; a return only happens once every candidate has
    been promoted.

    Returns the list of written table names (silver + gold); the
    setup_run_log table itself is not included in the returned list.

    The Spark session is derived from the first DataFrame in ``tables`` or
    ``gold`` (``df.sparkSession``) — no explicit session parameter is needed.
    """
    if (lakehouse is None) == (base_path is None):
        raise ValueError("Provide exactly one of lakehouse= or base_path=")

    first_df = next(iter(tables.values()), None) or next(iter(gold.values()), None)
    if first_df is None:
        raise ValueError("write_all requires at least one table in tables or gold")
    spark = first_df.sparkSession

    log_name = "setup_run_log"
    log_table = f"{lakehouse}.{cfg.silver_db}.{log_name}" if lakehouse is not None else None
    log_path = f"{base_path}/{cfg.silver_db}/{log_name}" if base_path is not None else None

    def _log_exists() -> bool:
        if log_table is not None:
            return spark.catalog.tableExists(log_table)
        return Path(str(log_path)).exists()

    def _read_log() -> DataFrame:
        if log_table is not None:
            return spark.table(log_table)
        return spark.read.format(fmt).load(str(log_path))

    def _append_log(
        table_name: str,
        row_count: int | None,
        status: str,
        error: str | None = None,
    ) -> None:
        row = [
            (
                run_id,
                cfg.store_type,
                cfg.seed,
                cfg.start_date,
                cfg.end_date,
                table_name,
                row_count,
                status,
                error,
            )
        ]
        log_df = spark.createDataFrame(
            row,
            "run_id string, store_type string, seed long, start_date date, "
            "end_date date, table_name string, row_count long, status string, "
            "error string",
        ).withColumn("generated_at", F.current_timestamp())
        writer = log_df.write.format("delta" if log_table is not None else fmt)
        writer = writer.mode("append").option("mergeSchema", "true")
        if log_table is not None:
            writer.saveAsTable(log_table)
        else:
            writer.save(str(log_path))

    if _log_exists() and _read_log().filter(F.col("run_id") == run_id).limit(1).count():
        raise ValueError(f"setup run_id already exists: {run_id!r}")

    run_token = sanitize_identifier(run_id)
    sources: dict[tuple[str, str], DataFrame] = {}
    targets: list[TableTarget] = []
    for db, frames in ((cfg.silver_db, tables), (cfg.gold_db, gold)):
        for name, df in frames.items():
            sources[(db, name)] = df
            if lakehouse is not None:
                staging_name = f"{lakehouse}.{db}_stage.{run_token}__{name}"
            else:
                staging_name = f"{db}/{name}"
            targets.append(TableTarget(name=name, db=db, staging_name=staging_name))

    backend: PublicationBackend
    if lakehouse is not None:
        for db in (cfg.silver_db, cfg.gold_db):
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {lakehouse}.{db}")
        backend = _LakehouseBackend(spark, lakehouse, run_id, sources)
    else:
        assert base_path is not None  # enforced by the exactly-one-of check above
        backend = _FilesystemBackend(spark, Path(base_path), fmt, run_id, sources)

    def _log(table_name: str, status: str, row_count: int | None, error: str | None) -> None:
        _append_log(table_name, row_count, status, error)

    coordinator = PublicationCoordinator(backend, _log)
    outcome = coordinator.publish(targets)

    if not outcome.ok:
        raise RuntimeError(
            f"setup publication {outcome.state} for run_id={run_id!r}: {outcome.error}"
        )

    return outcome.promoted
