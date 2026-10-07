"""Typed bindings for the stopped native retail Operations Agent and its action."""

from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from deploy.scripts._native_items import json_part

ROOT = Path(__file__).resolve().parents[2]
AGENT_NAME = "retail-availability-operations"
ACTION_NAME = "prepare-replenishment-case"


def render_operations_definition(
    *,
    workspace_id: str,
    kql_database_id: str,
    notebook_id: str,
    recipient: str | None = None,
    include_plan: bool = True,
) -> dict[str, Any]:
    workspace_id, kql_database_id, notebook_id = (
        str(UUID(value)) for value in (workspace_id, kql_database_id, notebook_id)
    )
    configuration: dict[str, Any] = {
        "instructions": (
            ROOT / "fabric" / "operations" / "availability.instructions.md"
        ).read_text(encoding="utf-8").replace(
            "{{PLANNING_HANDOFF}}",
            (
                "After preparation, direct the planner to the retail-replenishment Plan in this workspace."
                if include_plan else
                "After preparation, ask the planner to review ops.replenishment_cases in the Lakehouse "
                "and use the existing retail report through SSO. Native Planning is not deployed. "
                "Do not request fixed credentials or direct users to a native Plan."
            ),
        ),
        "dataSources": {
            "availability": {
                "id": kql_database_id,
                "workspaceId": workspace_id,
                "type": "KustoDatabase",
            },
        },
        "actions": {
            "prepareCase": {
                "id": str(uuid5(NAMESPACE_URL, "retail-demo:operations:prepare-case")),
                "displayName": "Prepare replenishment case",
                "description": "Create a draft planning case after human approval; never place an order.",
                "kind": "FabricJobAction",
                "connection": {
                    "jobArtifactId": notebook_id,
                    "jobWorkspaceId": workspace_id,
                    "itemType": "Notebook",
                    "jobType": "RunNotebook",
                },
                "parameters": [
                    {
                        "name": "CASE_ID",
                        "description": "case_id of the selected availability signal",
                    }
                ],
            },
        },
    }
    if recipient:
        if "@" not in recipient or any(character.isspace() for character in recipient):
            raise ValueError(
                "Operations recipient must be a Teams user principal name."
            )
        configuration["messageDestination"] = {
            "kind": "Recipient",
            "recipient": recipient,
        }
    return {
        "parts": [
            json_part(
                "Configurations.json",
                {
                    "configuration": configuration,
                    "playbook": {},
                    "shouldRun": False,
                },
            )
        ]
    }


def render_case_notebook(
    *,
    workspace_id: str,
    lakehouse_id: str,
    lakehouse_name: str,
    kusto_uri: str,
    database_name: str,
    include_plan: bool = True,
) -> dict[str, Any]:
    workspace_id, lakehouse_id = str(UUID(workspace_id)), str(UUID(lakehouse_id))
    parameters = {
        "CASE_ID": "",
        "LAKEHOUSE_NAME": lakehouse_name,
        "KUSTO_URI": kusto_uri,
        "KQL_DATABASE": database_name,
        "PLANNING_REVIEW_TARGET": (
            "the retail-replenishment Plan" if include_plan
            else "the Lakehouse ops.replenishment_cases table and existing retail report through SSO"
        ),
    }
    cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                "# Prepare a synthetic replenishment planning case\n",
                "Human-approved draft only; no ERP or purchasing integration.\n",
            ],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "outputs": [],
            "metadata": {"tags": ["parameters"]},
            "source": [f"{key} = {value!r}\n" for key, value in parameters.items()],
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "outputs": [],
            "metadata": {},
            "source": (ROOT / "fabric" / "operations" / "prepare-replenishment-case.py")
            .read_text(encoding="utf-8")
            .splitlines(keepends=True),
        },
    ]
    document = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "cells": cells,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "dependencies": {
                "lakehouse": {
                    "default_lakehouse": lakehouse_id,
                    "default_lakehouse_name": lakehouse_name,
                    "default_lakehouse_workspace_id": workspace_id,
                }
            },
        },
    }
    return {"format": "ipynb", "parts": [json_part("notebook-content.ipynb", document)]}
