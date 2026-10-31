"""Hand generated stage outputs between setup notebooks.

Each setup notebook runs a slice of the generation stage graph in its own Spark
session. Stage outputs that a later notebook consumes are written as handoff
tables named ``gen__<stage>__<output>`` in the Silver staging schema
(``<silver_db>_stage``). The publish notebook promotes them into Silver and then
drops every handoff.

Historical setup is single-writer, so handoff names are fixed and an activity
retry simply overwrites them. Every handoff records the generation
fingerprint (the configuration it was generated for) atomically with its data
(Delta commit ``userMetadata``); loading a handoff produced for a different
configuration fails instead of silently mixing runs.

Path mode (``base_path=``) stores parquet plus a fingerprint marker file so
local tests need no Delta dependency.
"""

import hashlib
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from retail_setup.config.generation import GenerationConfig

HANDOFF_PREFIX = "gen__"
_FINGERPRINT_MARKER = "_retail_generation"


def delete_lakehouse_path(spark: SparkSession, path: str) -> None:
    """Recursively delete a path on the session's default filesystem (if present)."""
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(hadoop_path, True)


def generation_fingerprint(cfg: GenerationConfig) -> str:
    """Stable identity of everything that shapes generated data."""
    payload = cfg.model_dump_json(exclude={"dictionary_root"})
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


class HandoffMismatchError(RuntimeError):
    """A handoff is missing or belongs to a different generation."""


class HandoffStore:
    def __init__(
        self,
        spark: SparkSession,
        cfg: GenerationConfig,
        *,
        lakehouse: str | None = None,
        base_path: str | None = None,
    ) -> None:
        if (lakehouse is None) == (base_path is None):
            raise ValueError("Provide exactly one of lakehouse= or base_path=")
        self.spark = spark
        self.fingerprint = generation_fingerprint(cfg)
        self.lakehouse = lakehouse
        self.schema = f"{cfg.silver_db}_stage"
        self.base_path = Path(base_path) if base_path is not None else None

    def name(self, stage: str, output: str) -> str:
        table = f"{HANDOFF_PREFIX}{stage}__{output}"
        if self.base_path is not None:
            return str(self.base_path / self.schema / table)
        return f"{self.lakehouse}.{self.schema}.{table}"

    def ensure_schema(self) -> None:
        """Create the handoff schema (setup-02 does this before any parallel writer)."""
        if self.base_path is None:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.lakehouse}.{self.schema}")

    def save(self, stage: str, output: str, frame: DataFrame) -> DataFrame:
        """Write one stage output and return a frame that reads it back."""
        name = self.name(stage, output)
        if self.base_path is None:
            (
                frame.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .option("userMetadata", self.fingerprint)
                .saveAsTable(name)
            )
            return self.spark.table(name)
        frame.write.mode("overwrite").parquet(name)
        (Path(name) / _FINGERPRINT_MARKER).write_text(self.fingerprint, encoding="utf-8")
        return self.spark.read.parquet(name)

    def load(self, stage: str, output: str) -> DataFrame:
        name = self.name(stage, output)
        found = self._fingerprint_of(name)
        if found != self.fingerprint:
            reason = (
                "is missing" if found is None else "was generated for a different configuration"
            )
            raise HandoffMismatchError(
                f"Handoff {name} {reason}; rerun the setup notebook that produces "
                f"stage {stage!r} (or the whole setup pipeline)."
            )
        if self.base_path is None:
            return self.spark.table(name)
        return self.spark.read.parquet(name)

    def drop_all(self) -> None:
        """Remove every handoff, including leftovers from failed runs."""
        if self.base_path is not None:
            root = self.base_path / self.schema
            for path in root.glob(f"{HANDOFF_PREFIX}*") if root.exists() else []:
                shutil.rmtree(path)
            return
        self.ensure_schema()
        for row in self.spark.sql(f"SHOW TABLES IN {self.lakehouse}.{self.schema}").collect():
            if row.tableName.startswith(HANDOFF_PREFIX):
                self.spark.sql(
                    f"DROP TABLE IF EXISTS {self.lakehouse}.{self.schema}.{row.tableName}"
                )

    def _fingerprint_of(self, name: str) -> str | None:
        if self.base_path is not None:
            marker = Path(name) / _FINGERPRINT_MARKER
            return marker.read_text(encoding="utf-8") if marker.exists() else None
        if not self.spark.catalog.tableExists(name):
            return None
        latest = self.spark.sql(f"DESCRIBE HISTORY {name} LIMIT 1").collect()
        return latest[0]["userMetadata"] if latest else None
