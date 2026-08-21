"""Validate live tenant and capacity prerequisites before Fabric mutation."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deploy.scripts import _output as console
from deploy.scripts.export_items import build_session
from deploy.scripts.fabric_runtime import paginated_get

if TYPE_CHECKING:
    import requests

    from deploy.scripts.deploy_config import DeployConfig

FABRIC_API = "https://api.fabric.microsoft.com/v1"
TENANT_SETTINGS_URL = f"{FABRIC_API}/admin/tenantsettings"
CAPACITIES_URL = f"{FABRIC_API}/capacities"
TENANT_SETTINGS_PORTAL = (
    "https://app.fabric.microsoft.com/admin-portal/tenantSettings"
)

_TENANT_SETTING_TITLES = {
    "OntologyPreview": "Users can create Ontology (preview) items",
    "EnableAOAI": "Users can use Copilot and other features powered by Azure OpenAI",
    "CopilotCapacitySetupPermissionSwitch": (
        "Capacities can be designated as Fabric Copilot capacities"
    ),
    "AllowSendAOAIDataToOtherRegions": (
        "Data sent to Azure OpenAI can be processed outside your capacity's "
        "geographic region, compliance boundary, or national cloud instance"
    ),
    "AllowStoreAOAIDataInOtherRegions": (
        "Data sent to Azure OpenAI can be stored outside your capacity's "
        "geographic region, compliance boundary, or national cloud instance"
    ),
}
_NODE_VCORES = {
    "Small": 4,
    "Medium": 8,
    "Large": 16,
    "XLarge": 32,
    "XXLarge": 64,
}
_EU_DATA_BOUNDARY_REGIONS = {
    "austria east",
    "belgium central",
    "denmark east",
    "finland central",
    "france central",
    "france south",
    "germany north",
    "germany west central",
    "italy north",
    "north europe",
    "norway east",
    "norway west",
    "poland central",
    "spain central",
    "sweden central",
    "sweden south",
    "switzerland north",
    "switzerland west",
    "west europe",
}


class LiveProfilePreflightError(RuntimeError):
    """Raised when live prerequisites are missing or cannot be proven."""

    def __init__(self, message: str, *, tenant_settings_fixable: bool = False):
        super().__init__(message)
        self.tenant_settings_fixable = tenant_settings_fixable


@dataclass(frozen=True)
class LiveProfilePreflightReport:
    """Evidence returned by a successful live profile preflight."""

    capacity_name: str
    capacity_sku: str
    capacity_region: str
    checked_tenant_settings: tuple[str, ...]


def _capacity_units(sku: str) -> int | None:
    """Return Fabric capacity units for F/FTL SKUs."""

    match = re.fullmatch(r"(?:F|FTL)(\d+)", sku, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _inside_us_or_eu_data_boundary(region: str) -> bool:
    normalized = " ".join(region.casefold().split())
    return bool(re.search(r"\bus\b", normalized)) or (
        normalized in _EU_DATA_BOUNDARY_REGIONS
    )


def _resolve_capacity(
    capacities: list[dict[str, Any]],
    config: DeployConfig,
) -> dict[str, Any]:
    capacity_id = config.workspace.capacity_id
    capacity_name = config.workspace.capacity_name
    if capacity_id:
        matches = [
            capacity
            for capacity in capacities
            if str(capacity.get("id", "")).casefold() == capacity_id.casefold()
        ]
        target = f"id {capacity_id!r}"
    elif capacity_name:
        matches = [
            capacity
            for capacity in capacities
            if str(capacity.get("displayName", "")).casefold()
            == capacity_name.casefold()
        ]
        target = f"name {capacity_name!r}"
    else:
        raise LiveProfilePreflightError(
            "No capacity_id or capacity_name is configured. Re-run "
            "`retail-setup configure` and select an active Fabric capacity."
        )

    if len(matches) != 1:
        available = ", ".join(
            sorted(
                str(capacity.get("displayName", ""))
                for capacity in capacities
                if capacity.get("displayName")
            )
        )
        raise LiveProfilePreflightError(
            f"Expected exactly one accessible capacity with {target}; found "
            f"{len(matches)}. Accessible capacities: {available or '(none)'}. "
            "Re-run `retail-setup configure --capacity-name <name>`."
        )
    return matches[0]


def _validate_capacity(
    capacity: dict[str, Any],
    config: DeployConfig,
) -> list[str]:
    errors: list[str] = []
    name = str(capacity.get("displayName", ""))
    state = str(capacity.get("state", ""))
    sku = str(capacity.get("sku", ""))
    if state != "Active":
        errors.append(
            f"Capacity {name!r} is {state or 'unknown'}, not Active. Resume it "
            "or select another active capacity."
        )

    if not config.profile.uses_custom_pool:
        return errors

    units = _capacity_units(sku)
    if units is None or units < 64:
        alternatives = "F64 or larger (or FTL64 or larger)"
        errors.append(
            f"Full-demo custom Spark defaults require {alternatives}; capacity "
            f"{name!r} uses SKU {sku or 'unknown'}. Select a suitable capacity "
            "or use `--profile standard` for the starter Spark pool."
        )
        return errors

    node_vcores = _NODE_VCORES.get(config.spark.node_size)
    if node_vcores is None:
        errors.append(
            f"Cannot size unsupported Spark node {config.spark.node_size!r}."
        )
        return errors
    available_vcores = units * 2
    requested_vcores = config.spark.max_node_count * node_vcores
    if requested_vcores > available_vcores:
        max_nodes = available_vcores // node_vcores
        errors.append(
            f"Spark pool requests {requested_vcores} base vCores "
            f"({config.spark.max_node_count} {config.spark.node_size} nodes), "
            f"but {sku} provides {available_vcores}. Set spark.max_node_count "
            f"to {max_nodes} or less, or select a larger capacity."
        )
    return errors


def _required_tenant_setting_names(
    config: DeployConfig,
    capacity_region: str,
) -> tuple[str, ...]:
    names: list[str] = []
    if config.profile.selects("asset.ontology"):
        names.append("OntologyPreview")
    if config.profile.selects("asset.data-agents"):
        names.extend(
            (
                "EnableAOAI",
                "CopilotCapacitySetupPermissionSwitch",
            )
        )
        if not _inside_us_or_eu_data_boundary(capacity_region):
            names.extend(
                (
                    "AllowSendAOAIDataToOtherRegions",
                    "AllowStoreAOAIDataInOtherRegions",
                )
            )
    return tuple(names)


def _validate_tenant_settings(
    settings: list[dict[str, Any]],
    required_names: tuple[str, ...],
) -> list[str]:
    by_name = {
        str(setting.get("settingName", "")): setting
        for setting in settings
    }
    errors: list[str] = []
    for name in required_names:
        title = _TENANT_SETTING_TITLES[name]
        setting = by_name.get(name)
        if setting is None:
            errors.append(
                f"Tenant setting {title!r} ({name}) was not returned by Fabric."
            )
            continue
        if setting.get("enabled") is not True:
            errors.append(f"Enable tenant setting: {title}.")
            continue
        enabled_groups = setting.get("enabledSecurityGroups") or []
        excluded_groups = setting.get("excludedSecurityGroups") or []
        if enabled_groups or excluded_groups:
            enabled_names = ", ".join(
                str(group.get("name") or group.get("graphId"))
                for group in enabled_groups
                if isinstance(group, dict)
            )
            excluded_names = ", ".join(
                str(group.get("name") or group.get("graphId"))
                for group in excluded_groups
                if isinstance(group, dict)
            )
            errors.append(
                f"Tenant setting {title!r} is security-group scoped. Fabric's "
                "tenant-settings API does not expose current-user membership; "
                f"have an admin include the deploy operator. Enabled groups: "
                f"{enabled_names or '(none)'}; excluded groups: "
                f"{excluded_names or '(none)'}."
            )
    return errors


def validate_live_profile_preflight(
    session: requests.Session,
    config: DeployConfig,
) -> LiveProfilePreflightReport:
    """Validate live capacity and tenant prerequisites without mutation."""

    try:
        capacities = paginated_get(session, CAPACITIES_URL)
    except Exception as exc:
        raise LiveProfilePreflightError(
            "Could not list accessible Fabric capacities. Ensure the deploy "
            "operator has Capacity.Read.All access."
        ) from exc
    capacity = _resolve_capacity(capacities, config)
    errors = _validate_capacity(capacity, config)
    region = str(capacity.get("region", ""))
    required_settings = _required_tenant_setting_names(config, region)

    tenant_errors: list[str] = []
    if required_settings:
        try:
            settings = paginated_get(session, TENANT_SETTINGS_URL)
        except Exception as exc:
            raise LiveProfilePreflightError(
                "Could not inspect Fabric tenant settings. Sign in as a Fabric "
                "administrator, then rerun setup; the documented admin API "
                "requires Tenant.Read.All or Tenant.ReadWrite.All."
            ) from exc
        tenant_errors = _validate_tenant_settings(settings, required_settings)
        errors.extend(tenant_errors)

    if errors:
        details = "\n".join(f"- {error}" for error in errors)
        if tenant_errors:
            details += (
                "\n- Fabric currently documents tenant-setting inspection but "
                "not a tenant-setting update API. Change these settings in "
                f"{TENANT_SETTINGS_PORTAL}"
            )
        raise LiveProfilePreflightError(
            f"live profile preflight failed for "
            f"{config.profile.deployment_name!r}:\n{details}",
            tenant_settings_fixable=bool(tenant_errors)
            and len(tenant_errors) == len(errors),
        )

    return LiveProfilePreflightReport(
        capacity_name=str(capacity.get("displayName", "")),
        capacity_sku=str(capacity.get("sku", "")),
        capacity_region=region,
        checked_tenant_settings=required_settings,
    )


def main() -> int:
    """Run live profile preflight and optionally recheck admin changes."""

    from deploy.scripts.deploy_config import load_environment

    parser = argparse.ArgumentParser(
        description="Validate live Fabric profile prerequisites"
    )
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--environment", required=True)
    parser.add_argument(
        "--auth-mode",
        choices=("azure_cli", "azure_powershell"),
        default="azure_cli",
    )
    parser.add_argument("--tenant-id")
    parser.add_argument("--interactive", action="store_true")
    args = parser.parse_args()

    config = load_environment(
        args.environment,
        config_path=args.repo_root / "deploy" / "config" / "deploy.yml",
        environments_root=args.repo_root / "deploy" / "config" / "environments",
    )
    session = build_session(
        auth_mode=args.auth_mode,
        tenant_id=args.tenant_id,
    )
    while True:
        try:
            report = validate_live_profile_preflight(session, config)
        except LiveProfilePreflightError as exc:
            console.error(str(exc))
            if (
                not args.interactive
                or not sys.stdin.isatty()
                or not exc.tenant_settings_fixable
            ):
                return 1
            response = input(
                "After a Fabric admin changes the listed settings, enter R to "
                "recheck (or press Enter to stop): "
            )
            if response.strip().casefold() != "r":
                return 1
            continue

        settings = (
            ", ".join(report.checked_tenant_settings)
            if report.checked_tenant_settings
            else "none required"
        )
        console.info(
            "Live profile preflight passed: "
            f"{report.capacity_name} ({report.capacity_sku}, "
            f"{report.capacity_region}); tenant settings: {settings}"
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
