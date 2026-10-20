from datetime import date

import pytest

from retail_setup.config.generation import GenerationConfig
from retail_setup.generation.handoff import (
    HandoffMismatchError,
    HandoffStore,
    generation_fingerprint,
)

CFG = GenerationConfig(store_type="grocery", start_date=date(2025, 9, 1),
                       end_date=date(2025, 9, 7), store_count=2, seed=99)


def test_fingerprint_tracks_generation_settings_not_dictionary_location():
    moved = CFG.model_copy(update={"dictionary_root": "/elsewhere"})
    reseeded = CFG.model_copy(update={"seed": 100})
    assert generation_fingerprint(moved) == generation_fingerprint(CFG)
    assert generation_fingerprint(reseeded) != generation_fingerprint(CFG)


def test_handoffs_round_trip_and_reject_other_generations(spark, tmp_path):
    store = HandoffStore(spark, CFG, base_path=str(tmp_path))
    saved = store.save("plan", "events", spark.range(4))
    assert saved.count() == 4
    assert store.load("plan", "events").count() == 4

    other = HandoffStore(spark, CFG.model_copy(update={"seed": 100}), base_path=str(tmp_path))
    with pytest.raises(HandoffMismatchError, match="different configuration"):
        other.load("plan", "events")
    with pytest.raises(HandoffMismatchError, match="is missing"):
        store.load("ble", "fact_ble_pings")


def test_drop_all_removes_only_handoffs(spark, tmp_path):
    store = HandoffStore(spark, CFG, base_path=str(tmp_path))
    store.save("plan", "events", spark.range(1))
    unrelated = tmp_path / "ag_stage" / "run__fact_receipts"
    unrelated.mkdir(parents=True)
    store.drop_all()
    assert not any(path.name.startswith("gen__") for path in (tmp_path / "ag_stage").iterdir())
    assert unrelated.exists()
