"""Deterministic brand themes derived from a customization palette.

:func:`derive_theme` turns the five palette colors into every token the themed
surfaces need, nudging lightness until text/background pairs meet WCAG AA so a
pale or neon brand color never produces unreadable reports. Appliers rewrite
*staged* copies only:

- Power BI reports (:func:`apply_report_theme`): every hardcoded hex color,
  including colors inside embedded SVG wallpapers, is remapped through the
  committed role map ``fabric/powerbi/theme-color-roles.json`` and the custom
  theme's data colors are replaced. Unmapped colors fail closed so new visuals
  cannot silently keep the default palette. Page headers gain the business
  logo (or the business name in the brand color when there is no logo).
- Fabric Apps (:func:`app_css`): CSS custom-property overrides.
- Plans (:func:`apply_plan_theme`): sheet canvas wallpaper tint.
"""

from __future__ import annotations

import base64
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from retail_setup.customization.logo import Logo
from retail_setup.customization.spec import Palette

HEX = re.compile(r"#[0-9A-Fa-f]{6}(?![0-9A-Fa-f])")
WHITE = "#FFFFFF"
APP_DARK_SURFACE = "#151C2A"
ROLE_MAP_PATH = Path("fabric") / "powerbi" / "theme-color-roles.json"
# Report text placeholders that carry the demo's fictional company name.
BRAND_TEXT_TOKENS = ("SmartRetail",)
THEME_RESOURCE = "BrandTheme.json"
LOGO_RESOURCE = "BrandLogo.svg"
LOGO_VISUAL = "brand_logo"
DATA_SVG = "data:image/svg+xml;base64,"
# A trailing " — SmartRetail Live" or " — retail-twins" in a page title; branded
# headers show the logo or business name first instead.
_TITLE_BRAND_SUFFIX = re.compile(
    r"\s*[—–-]\s*(?:" + "|".join(map(re.escape, (*BRAND_TEXT_TOKENS, "retail-twins"))) + r")\b.*$"
)

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
        """Color for a role name; ``role~0.8`` is that role tinted 80% toward white."""

        base, _, tint = name.partition("~")
        if tint:
            weight = float(tint)
            if not 0 <= weight < 1:
                raise ValueError(f"tint must be in [0, 1) in theme color role {name!r}")
            return mix(self.role(base), WHITE, weight)
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
            text = _svg_text(value) if value.startswith(DATA_SVG) else value
            found.update(match.upper() for match in HEX.findall(text))
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


def _svg_text(url: str) -> str:
    return base64.b64decode(url[len(DATA_SVG) :]).decode("utf-8")


def _rewrite(value: Any, colors: Mapping[str, str], texts: Mapping[str, str]) -> Any:
    if isinstance(value, str) and value.startswith(DATA_SVG):
        # Embedded wallpapers: recolor the SVG itself (its text is not report text).
        svg = HEX.sub(lambda m: colors[m.group(0).upper()], _svg_text(value))
        return DATA_SVG + base64.b64encode(svg.encode("utf-8")).decode("ascii")
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
        color: color if roles[color] == FIXED_ROLE else theme.role(roles[color]) for color in colors
    }


def _theme_document(theme: Theme) -> dict[str, Any]:
    return {
        "name": f"{theme.display_name} theme",
        "dataColors": list(theme.data_colors),
        "foreground": theme.heading,
        "background": WHITE,
        "tableAccent": theme.accent,
    }


def _literal(value: str | int | float | bool) -> dict[str, Any]:
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, str):
        text = "'" + value.replace("'", "''") + "'"
    else:
        text = f"{value}D"
    return {"expr": {"Literal": {"Value": text}}}


def _registered_items(report: dict[str, Any]) -> list[dict[str, Any]]:
    packages = report.setdefault("resourcePackages", [])
    registered = next((p for p in packages if p.get("type") == "RegisteredResources"), None)
    if registered is None:
        registered = {"name": "RegisteredResources", "type": "RegisteredResources", "items": []}
        packages.append(registered)
    items: list[dict[str, Any]] = registered.setdefault("items", [])
    return items


def _register(report: dict[str, Any], name: str, kind: str) -> None:
    items = _registered_items(report)
    items[:] = [item for item in items if item.get("name") != name]
    items.append({"name": name, "path": name, "type": kind})


def _headers(documents: Mapping[str, Any]) -> list[str]:
    """Paths of page-header textboxes (``*_header`` visuals)."""

    return [
        path
        for path, document in documents.items()
        if path.startswith("definition/pages/")
        and path.endswith("/visual.json")
        and isinstance(document, dict)
        and str(document.get("name", "")).endswith("_header")
        and document.get("visual", {}).get("visualType") == "textbox"
    ]


def _title_runs(header: dict[str, Any]) -> list[dict[str, Any]]:
    general = header["visual"].get("objects", {}).get("general", [])
    paragraphs = general[0].get("properties", {}).get("paragraphs", []) if general else []
    runs: list[dict[str, Any]] = paragraphs[0].get("textRuns", []) if paragraphs else []
    return runs


def _strip_title_brand(documents: dict[str, Any]) -> dict[str, Any]:
    """Drop " — SmartRetail Live"-style suffixes from header titles (copies only)."""

    result = dict(documents)
    for path in _headers(documents):
        header = json.loads(json.dumps(documents[path]))
        for run in _title_runs(header):
            if isinstance(run.get("value"), str):
                run["value"] = _TITLE_BRAND_SUFFIX.sub("", run["value"])
        result[path] = header
    return result


# Bold Segoe UI averages under 0.54 em per character; textboxes pad ~12 px a side.
_TITLE_EM_PER_CHAR = 0.54
_TEXTBOX_PADDING = 24
_MIN_TITLE_PT = 14


def fit_title(runs: list[dict[str, Any]], box_width: float) -> None:
    """Shrink the title runs' font so the title stays on one line in ``box_width``.

    Branding adds a logo or name to every header, which can push a long title
    onto a second line and clip the subtitle. The width estimate is conservative
    and deterministic; titles never shrink below 14pt.
    """

    text = "".join(str(run.get("value", "")) for run in runs)
    match = re.fullmatch(
        r"(\d+(?:\.\d+)?)pt", str(runs[0].get("textStyle", {}).get("fontSize", ""))
    )
    if not text or match is None:
        return
    size = float(match.group(1))
    needed = len(text) * size * 96 / 72 * _TITLE_EM_PER_CHAR
    available = box_width - _TEXTBOX_PADDING
    if needed <= available:
        return
    fitted = max(_MIN_TITLE_PT, int(size * available / needed))
    for run in runs:
        run.setdefault("textStyle", {})["fontSize"] = f"{fitted}pt"


def logo_width(logo: Logo, header_width: float) -> int:
    """Header logo tile width: about 56 px tall when it fits, 56-180 px wide.

    The tile spans the header's height and the image scales to fit inside it,
    so wide wordmarks shrink rather than crowd the title (at most a quarter of
    the header).
    """

    limit = header_width * 0.25
    return round(max(min(56 * logo.info.aspect, 180.0, limit), min(56.0, limit)))


def _logo_visual(header: dict[str, Any], alt_text: str, name: str, width: float) -> dict[str, Any]:
    position = header["position"]
    hidden = [{"properties": {"show": _literal(False)}}]
    return {
        "$schema": header.get("$schema"),
        "name": name,
        "position": {
            "x": position["x"],
            "y": position["y"],
            "z": position.get("z", 0),
            "height": position["height"],
            "width": width,
        },
        "visual": {
            "visualType": "image",
            "objects": {
                "general": [
                    {
                        "properties": {
                            "imageUrl": {
                                "expr": {
                                    "ResourcePackageItem": {
                                        "PackageName": "RegisteredResources",
                                        "PackageType": 1,
                                        "ItemName": LOGO_RESOURCE,
                                    }
                                }
                            }
                        }
                    }
                ],
                "imageScaling": [{"properties": {"imageScalingType": _literal("Fit")}}],
            },
            "visualContainerObjects": {
                "background": hidden,
                "border": hidden,
                "dropShadow": hidden,
                "title": hidden,
                "general": [{"properties": {"altText": _literal(alt_text)}}],
            },
            "drillFilterOtherVisuals": True,
        },
    }


def _brand_headers(documents: dict[str, Any], theme: Theme, logo: Logo | None) -> dict[str, Any]:
    """Put the logo (or the brand-colored business name) at the start of every page header."""

    result = dict(documents)
    for path in _headers(documents):
        header = json.loads(json.dumps(documents[path]))
        position = header["position"]
        runs = _title_runs(header)
        if logo is not None:
            width = logo_width(logo, position["width"])
            visual_dir = path.rsplit("/", 2)[0]
            name = f"{header['name']}_{LOGO_VISUAL}"
            result[f"{visual_dir}/{name}/visual.json"] = _logo_visual(
                header, f"{theme.display_name} logo", name, width
            )
            shift = width + 16
            position["x"] = position["x"] + shift
            position["width"] = position["width"] - shift
        elif runs:
            style = dict(runs[0].get("textStyle", {}))
            runs[:0] = [
                {"value": theme.display_name, "textStyle": {**style, "color": theme.accent}},
                {
                    "value": "  ·  ",
                    "textStyle": {**style, "color": theme.muted_light, "fontWeight": "normal"},
                },
            ]
        if runs:
            fit_title(runs, position["width"])
        result[path] = header
    if logo is not None:
        _register(result["definition/report.json"], LOGO_RESOURCE, "Image")
        result[f"StaticResources/RegisteredResources/{LOGO_RESOURCE}"] = logo.svg
    return result


def apply_report_theme(
    documents: dict[str, Any],
    theme: Theme,
    roles: Mapping[str, str],
    logo: Logo | None = None,
) -> dict[str, Any]:
    """Return PBIR ``documents`` (``{relative path: parsed JSON}``) themed for ``theme``.

    The report's registered custom theme gets the brand data colors; a report
    without one gains a ``BrandTheme.json`` registered resource. Page headers
    show ``logo`` (added as the ``BrandLogo.svg`` bytes part), or the business
    name in the brand color when there is no logo.
    """

    colors = _color_map(report_colors(documents), roles, theme)
    themed = {
        path: _rewrite(doc, colors, theme.text_replacements) if _themable(path) else doc
        for path, doc in _strip_title_brand(documents).items()
    }
    themed = _brand_headers(themed, theme, logo)
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
    _register(report, THEME_RESOURCE, "CustomTheme")
    themed[f"StaticResources/RegisteredResources/{THEME_RESOURCE}"] = _theme_document(theme)
    return themed


def load_report_documents(report_dir: Path) -> dict[str, Any]:
    """Read every JSON part of an on-disk PBIR report keyed by relative path."""

    return {
        path.relative_to(report_dir).as_posix(): json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(report_dir.rglob("*.json"))
    }


def theme_report_dir(
    report_dir: Path, theme: Theme, roles: Mapping[str, str], logo: Logo | None = None
) -> list[str]:
    """Theme a staged on-disk PBIR report in place; return the rewritten paths."""

    documents = load_report_documents(report_dir)
    themed = apply_report_theme(documents, theme, roles, logo)
    changed = []
    for path, document in themed.items():
        if documents.get(path) == document:
            continue
        target = report_dir / path
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(document, bytes):
            target.write_bytes(document)
        else:
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
