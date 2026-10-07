import base64

import pytest

from deploy.scripts._native_items import json_part
from deploy.scripts._twin_report_checks import (
    report_references,
    validate_report_model_fields,
)


def report():
    return {"parts": [json_part("definition/pages/test/visuals/test/visual.json", {
        "filter": {
            "From": [{"Name": "s", "Entity": "dim_stores"}],
            "Where": {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "ID"}},
        },
        "query": {"Measure": {"Expression": {"SourceRef": {"Entity": "dim_stores"}}, "Property": "Twin State"}},
    })]}


def model(text):
    return {"parts": [{
        "path": "definition/tables/dim_stores.tmdl",
        "payload": base64.b64encode(text.encode()).decode(),
        "payloadType": "InlineBase64",
    }]}


def test_both_native_and_aliased_field_references_are_resolved():
    assert report_references(report()) == {
        ("dim_stores", "column", "ID"), ("dim_stores", "measure", "Twin State"),
    }
    validate_report_model_fields(
        report(), model("table dim_stores\n\tcolumn ID\n\t\tdataType: int64\n\tmeasure 'Twin State' = 1\n"),
    )


def test_source_valid_field_missing_live_is_not_published():
    with pytest.raises(ValueError, match="missing live model fields"):
        validate_report_model_fields(report(), model("table dim_stores\n\tcolumn ID\n"))
