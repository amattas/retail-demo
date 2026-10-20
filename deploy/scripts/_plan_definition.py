"""Render the offline, human-governed retail workbook as native PlanV1 parts."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from uuid import UUID

_SOURCE = (
    Path(__file__).resolve().parents[2] / "fabric/planning/retail-replenishment.Plan"
)


def _identifier(value: str, field: str) -> str:
    """Require a real, non-nil runtime UUID; never accept template tokens."""
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a non-nil UUID string")
    try:
        identifier = UUID(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be a non-nil UUID string") from exc
    if identifier.int == 0:
        raise ValueError(f"{field} must be a non-nil UUID string")
    return str(identifier)


def _name(value: str, field: str) -> str:
    """Validate names without interpreting their contents as JSON or templates."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")
    return value


def _validate_parts(documents: dict[str, dict]) -> None:
    """Reject an empty, incomplete, or unexpectedly extended workbook."""
    required = {".platform", "definition.json", "planProperties.json"}
    sheets = documents["definition.json"]["sheets"]
    if not sheets:
        raise ValueError("Plan source must contain planning sheets")
    for sheet in sheets:
        prefix = f"sheets/{sheet['recordGuid']}"
        required.update({f"{prefix}/sheet.json", f"{prefix}/commentSettings.json"})
        for visual in sheet["workloadItemEntityVisuals"]:
            visual_path = f"{prefix}/visuals/{visual['visualId']}"
            required.update(
                {
                    f"{visual_path}/dataInput.json",
                    f"{visual_path}/properties.json",
                }
            )
            scenarios = f"{visual_path}/scenarios.json"
            if scenarios in documents:
                required.add(scenarios)
    if required != documents.keys():
        missing = sorted(required - documents.keys())
        unexpected = sorted(documents.keys() - required)
        raise ValueError(
            f"Invalid Plan parts: missing={missing}, unexpected={unexpected}"
        )


def render_plan_definition(
    *,
    workspace_id: str,
    workspace_name: str,
    semantic_model_id: str,
    semantic_model_name: str,
    connection_id: str,
    display_name: str,
) -> dict:
    """Bind explicit targets and return a Fabric multipart item definition.

    This pure filesystem renderer does not authenticate, create connections,
    publish, open a Plan, run optimization, or write to an external system.
    The caller must verify connection access and Direct Lake fixed credentials.
    Runtime values are assigned to parsed JSON fields, not interpolated into
    source text. Source files and their stable content identifiers stay intact.

    Raises:
        ValueError: An input is invalid or the source multipart graph is incomplete.
        OSError: A checked-in source definition cannot be read.
        json.JSONDecodeError: A source part is not valid JSON.
    """
    reference = {
        "connectionId": _identifier(connection_id, "connection_id"),
        "semanticModelId": _identifier(semantic_model_id, "semantic_model_id"),
        "semanticModelName": _name(semantic_model_name, "semantic_model_name"),
        "semanticModelWorkspaceId": _identifier(workspace_id, "workspace_id"),
        "semanticModelWorkspaceName": _name(workspace_name, "workspace_name"),
        "directLakeMode": True,
        "directQueryMode": False,
        "sourceType": "WORKLOAD",
    }
    display_name = _name(display_name, "display_name")
    files = [_SOURCE / ".platform", *sorted(_SOURCE.rglob("*.json"))]
    documents = {
        path.relative_to(_SOURCE).as_posix(): json.loads(
            path.read_text(encoding="utf-8")
        )
        for path in files
    }
    _validate_parts(documents)
    documents["definition.json"]["workloadItemName"] = display_name
    documents["definition.json"]["semanticModelReference"] = reference
    documents[".platform"]["metadata"]["displayName"] = display_name
    parts = [
        {
            "path": path,
            "payload": base64.b64encode(
                json.dumps(document, ensure_ascii=False, allow_nan=False).encode(
                    "utf-8"
                )
            ).decode("ascii"),
            "payloadType": "InlineBase64",
        }
        for path, document in sorted(documents.items())
    ]
    return {"format": "PlanV1", "parts": parts}
