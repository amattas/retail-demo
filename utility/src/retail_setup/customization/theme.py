"""Deterministic brand themes derived from a customization palette.

:func:`derive_theme` turns the five palette colors into every token the themed
surfaces need, nudging lightness until text/background pairs meet WCAG AA so a
pale or neon brand color never produces unreadable reports. Appliers rewrite
*staged* copies only:

- Power BI reports (:func:`apply_report_theme`): every hardcoded hex color is
  remapped through the committed role map
  ``fabric/powerbi/theme-color-roles.json`` and the custom theme's data colors
  are replaced. Unmapped colors fail closed so new visuals cannot silently keep
  the default palette.
- Fabric Apps (:func:`app_css`): CSS custom-property overrides.
- Plans (:func:`apply_plan_theme`): sheet canvas wallpaper tint.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from retail_setup.customization.spec import Palette

HEX = re.compile(r"#[0-9A-Fa-f]{6}(?![0-9A-Fa-f])")
WHITE = "#FFFFFF"
APP_DARK_SURFACE = "#151C2A"
ROLE_MAP_PATH = Path("fabric") / "powerbi" / "theme-color-roles.json"
# Report text placeholders that carry the demo's fictional company name.
BRAND_TEXT_TOKENS = ("SmartRetail",)
THEME_RESOURCE = "BrandTheme.json"

# Roles that keep their original color: white canvases, black text, and status
# colors (good/warn/bad must stay recognizable regardless of brand).
FIXED_ROLE = "fixed"


def _rgb(color: str) -> tuple[int, int, int]:
    return int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)


def _hex(rgb: tuple[float, float, float]) -> str:
    return "#" + "".join(f"{max(0, min(255, round(c))):02X}" for c in rgb)


def mix(a: str, b: str, weight: float) -> str:
    """Return ``a`` blended toward ``b`` by ``weight`` (0 keeps ``a``)."""

    ra, rb = _rgb(a), _rgb(b)
    r, g, bl = (x + (y - x) * weight for x, y in zip(ra, rb))
    return _hex((r, g, bl))


def luminance(color: str) -> float:
    def channel(value: int) -> float:
        c = value / 255
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    r, g, b = (channel(v) for v in _rgb(color))
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast(a: str, b: str) -> float:
    high, low = sorted((luminance(a), luminance(b)), reverse=True)
    return (high + 0.05) / (low + 0.05)


def ensure_contrast(color: str, background: str, minimum: float) -> str:
    """Darken (on light backgrounds) or lighten ``color`` until it reaches ``minimum``."""

    target = "#000000" if luminance(background) > 0.18 else WHITE
    for step in range(21):
        candidate = mix(color, target, step / 20)
        if contrast(candidate, background) >= minimum:
            return candidate
    return target


@dataclass(frozen=True)
class Theme:
    """Every derived color token, plus brand text for titles."""

    display_name: str
    accent: str
    accent_strong: str
    accent_soft: str
    secondary: str
    heading: str
    muted: str
    muted_light: str
    border: str
    surface: str
    data_colors: tuple[str, ...]
    dark_accent: str
    dark_accent_strong: str
    dark_accent_soft: str
    text_replacements: Mapping[str, str] = field(default_factory=dict)

    def role(self, name: str) -> str:
        if name.startswith("data_"):
            return self.data_colors[int(name.split("_", 1)[1]) - 1]
        value = getattr(self, name, None)
        if not isinstance(value, str) or not HEX.fullmatch(value):
            raise ValueError(f"unknown theme color role {name!r}")
        return value


def derive_theme(palette: Palette, display_name: str) -> Theme:
    """Derive all theme tokens from ``palette``. Same input, same output."""

    accent = ensure_contrast(palette.primary, WHITE, 4.5)
    secondary = ensure_contrast(palette.secondary, WHITE, 3.0)
    heading = ensure_contrast(palette.dark, WHITE, 7.0)
    muted = ensure_contrast(mix(heading, WHITE, 0.35), WHITE, 4.5)
    dark_accent = ensure_contrast(palette.primary, APP_DARK_SURFACE, 4.5)
    data_colors = (
        accent,
        secondary,
        ensure_contrast(palette.accent, WHITE, 2.0),
        heading,
        mix(accent, WHITE, 0.45),
        mix(secondary, WHITE, 0.45),
        mix(palette.accent, "#000000", 0.3),
        mix(accent, secondary, 0.5),
    )
    return Theme(
        display_name=display_name,
        accent=accent,
        accent_strong=mix(accent, "#000000", 0.2),
        accent_soft=mix(accent, WHITE, 0.9),
        secondary=secondary,
        heading=heading,
        muted=muted,
        muted_light=mix(muted, WHITE, 0.45),
        border=mix(mix(palette.light, accent, 0.15), WHITE, 0.55),
        surface=mix(palette.light, WHITE, 0.7),
        data_colors=data_colors,
        dark_accent=dark_accent,
        dark_accent_strong=mix(dark_accent, WHITE, 0.2),
        dark_accent_soft=mix(dark_accent, APP_DARK_SURFACE, 0.8),
        text_replacements={token: display_name for token in BRAND_TEXT_TOKENS},
    )


# ----- Power BI -----------------------------------------------------------------


def load_role_map(repo_root: Path) -> dict[str, str]:
    """Load the committed ``{HEX: role}`` map for report colors."""

    raw = json.loads((repo_root / ROLE_MAP_PATH).read_text(encoding="utf-8"))
    roles = raw["colors"]
    return {color.upper(): role for color, role in roles.items()}


def report_colors(documents: Mapping[str, Any]) -> set[str]:
    """Every hex color literal in themable report parts (base themes excluded)."""

    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, str):
            found.update(match.upper() for match in HEX.findall(value))
        elif isinstance(value, Mapping):
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    for path, document in documents.items():
        if _themable(path):
            walk(document)
    return found


def _themable(path: str) -> bool:
    return "BaseThemes/" not in path and path.endswith(".json")


def _rewrite(value: Any, colors: Mapping[str, str], texts: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        value = HEX.sub(lambda m: colors[m.group(0).upper()], value)
        for old, new in texts.items():
            value = value.replace(old, new)
        return value
    if isinstance(value, dict):
        return {key: _rewrite(item, colors, texts) for key, item in value.items()}
    if isinstance(value, list):
        return [_rewrite(item, colors, texts) for item in value]
    return value


def _color_map(colors: set[str], roles: Mapping[str, str], theme: Theme) -> dict[str, str]:
    unmapped = sorted(colors - set(roles))
    if unmapped:
        raise ValueError(
            f"report colors have no theme role in {ROLE_MAP_PATH.as_posix()}: {unmapped}"
        )
    return {
        color: color if roles[color] == FIXED_ROLE else theme.role(roles[color])
        for color in colors
    }


def _theme_document(theme: Theme) -> dict[str, Any]:
    return {
        "name": f"{theme.display_name} theme",
        "dataColors": list(theme.data_colors),
        "foreground": theme.heading,
        "background": WHITE,
        "tableAccent": theme.accent,
    }


def apply_report_theme(
    documents: dict[str, Any], theme: Theme, roles: Mapping[str, str]
) -> dict[str, Any]:
    """Return PBIR ``documents`` (``{relative path: parsed JSON}``) themed for ``theme``.

    The report's registered custom theme gets the brand data colors; a report
    without one gains a ``BrandTheme.json`` registered resource.
    """

    colors = _color_map(report_colors(documents), roles, theme)
    themed = {
        path: _rewrite(doc, colors, theme.text_replacements) if _themable(path) else doc
        for path, doc in documents.items()
    }
    report = themed["definition/report.json"]
    collection = report.setdefault("themeCollection", {})
    custom = collection.get("customTheme")
    if custom and custom.get("type") == "RegisteredResources":
        path = f"StaticResources/RegisteredResources/{custom['name']}"
        if path not in themed:
            raise ValueError(f"registered custom theme {path} is missing")
        themed[path] = {**themed[path], **_theme_document(theme)}
        return themed

    version = collection.get("baseTheme", {}).get("reportVersionAtImport")
    collection["customTheme"] = {
        "name": THEME_RESOURCE,
        **({"reportVersionAtImport": version} if version else {}),
        "type": "RegisteredResources",
    }
    packages = report.setdefault("resourcePackages", [])
    registered = next((p for p in packages if p.get("type") == "RegisteredResources"), None)
    if registered is None:
        registered = {"name": "RegisteredResources", "type": "RegisteredResources", "items": []}
        packages.append(registered)
    registered["items"] = [
        item for item in registered.get("items", []) if item.get("name") != THEME_RESOURCE
    ] + [{"name": THEME_RESOURCE, "path": THEME_RESOURCE, "type": "CustomTheme"}]
    themed[f"StaticResources/RegisteredResources/{THEME_RESOURCE}"] = _theme_document(theme)
    return themed


def load_report_documents(report_dir: Path) -> dict[str, Any]:
    """Read every JSON part of an on-disk PBIR report keyed by relative path."""

    return {
        path.relative_to(report_dir).as_posix(): json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(report_dir.rglob("*.json"))
    }


def theme_report_dir(report_dir: Path, theme: Theme, roles: Mapping[str, str]) -> list[str]:
    """Theme a staged on-disk PBIR report in place; return the rewritten paths."""

    documents = load_report_documents(report_dir)
    themed = apply_report_theme(documents, theme, roles)
    changed = []
    for path, document in themed.items():
        if documents.get(path) == document:
            continue
        target = report_dir / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        changed.append(path)
    return changed


# ----- Fabric Apps and Plans ----------------------------------------------------


def app_css(theme: Theme) -> str:
    """CSS overriding the app's ``--cp-*`` brand tokens (status colors unchanged)."""

    light = {
        "--cp-bg": mix(theme.surface, "#F5F7FB", 0.5),
        "--cp-surface-2": mix(theme.surface, WHITE, 0.4),
        "--cp-border": theme.border,
        "--cp-text": theme.heading,
        "--cp-muted": theme.muted,
        "--cp-faint": theme.muted_light,
        "--cp-accent": theme.accent,
        "--cp-accent-strong": theme.accent_strong,
        "--cp-accent-soft": theme.accent_soft,
        "--cp-series-2": theme.data_colors[5],
    }
    dark = {
        "--cp-accent": theme.dark_accent,
        "--cp-accent-strong": theme.dark_accent_strong,
        "--cp-accent-soft": theme.dark_accent_soft,
    }

    def block(selector: str, tokens: Mapping[str, str]) -> str:
        body = "".join(f"  {name}: {value.lower()};\n" for name, value in tokens.items())
        return f"{selector} {{\n{body}}}\n"

    return (
        "\n/* Generated brand theme (retail-setup customize); do not edit. */\n"
        + block(":root", light)
        + block(":root.dark", dark)
    )


def apply_plan_theme(documents: dict[str, Any], theme: Theme) -> dict[str, Any]:
    """Tint every Plan sheet's canvas wallpaper with the brand surface color."""

    themed = dict(documents)
    for path, document in documents.items():
        if not (path.startswith("sheets/") and path.endswith("/sheet.json")):
            continue
        sheet = json.loads(json.dumps(document))
        canvas = sheet["properties"]["canvasStyle"]
        canvas["wallpaper"]["value"]["color"] = theme.surface
        canvas["wallpaper"]["isEnabled"] = True
        themed[path] = sheet
    return themed
