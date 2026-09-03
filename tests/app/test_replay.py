from __future__ import annotations

import json

from app.backend import proposals, replay


def test_replay_routes_semantic_and_ontology_questions() -> None:
    semantic = replay.chat(
        "Which product families are growing inside declining categories?"
    )
    ontology = replay.chat(
        "Which stores carry Momentum Runner and how are they supplied?"
    )

    assert semantic["routedTo"] == "data-agent"
    assert ontology["routedTo"] == "ontology"
    assert "FulfillmentNode" in ontology["trace"]["entities"]


def test_replay_action_creates_deduplicated_drafts(tmp_path, monkeypatch) -> None:
    store = tmp_path / "proposals.json"
    store.write_text("[]", encoding="utf-8")
    monkeypatch.setattr(proposals, "_STORE", store)

    first = replay.chat(
        "What should we do to protect Momentum Runner growth without stockouts?"
    )
    second = replay.chat(
        "What should we do to protect Momentum Runner growth without stockouts?"
    )

    assert len(first["recommendation"]["actions"]) == 3
    assert len(second["recommendation"]["actions"]) == 3
    assert len(json.loads(store.read_text(encoding="utf-8"))) == 3
    assert replay.actions_dashboard()["kpis"]["total"] == 3


def test_replay_override_recalculates_transfer_source() -> None:
    replay._override = None

    decision = replay.apply_override("Local event requires safety stock.")

    assert decision["override"]["excludedCandidate"] == "STORE-031"
    assert decision["effectiveTransferSource"] == "STORE-027"
