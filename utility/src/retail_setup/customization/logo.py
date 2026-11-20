"""Business logos from Wikimedia Commons for custom layouts.

A logo is chosen once, while a layout is customized, and saved beside the spec
with its SHA-256; deployment reads only the saved file. GitHub Copilot picks the
file from real search results (see :mod:`retail_setup.customization.generator`);
this module supplies the candidates, verifies the pick, and sanitizes the SVG.
Without Copilot, :func:`fetch_logo` picks deterministically from the business's
Wikidata ``P154`` (logo image) claims: deprecated, ended (``P582``), and retired
(``(1961-2019)``-style) logos are skipped, and preferred-rank, newest-start
claims win. Only SVG files from Wikimedia hosts are accepted.

Logos are trademarks of their owners. They are used only to brand a demo
deployment for that business and are never committed.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
COMMONS_FILE = "https://commons.wikimedia.org/wiki/Special:FilePath/"
COMMONS_PAGE = "https://commons.wikimedia.org/wiki/File:"
USER_AGENT = "retail-demo-setup/1.0 (https://github.com/amattas/retail-demo)"
LOGO_FILE = "logo.svg"
MAX_SVG_BYTES = 1_000_000
_QID = re.compile(r"^Q[1-9][0-9]{0,11}$")
_RETIRED = re.compile(r"\(\s*\d{4}\s*[-–]\s*\d{4}\s*\)")
SVG_NS = "http://www.w3.org/2000/svg"
XLINK_NS = "http://www.w3.org/1999/xlink"
_DROP_ELEMENTS = {
    "script",
    "foreignObject",
    "iframe",
    "object",
    "embed",
    "set",
    "animate",
    "animateMotion",
    "animateTransform",
    "animateColor",
    "handler",
    "listener",
}
_UNSAFE_CSS = re.compile(r"@import|expression\s*\(|javascript:|url\(\s*['\"]?(?!#)", re.I)

Fetch = Callable[[str], bytes]


class LogoError(RuntimeError):
    """No usable logo could be found or accepted."""


class LogoInfo(BaseModel):
    """Provenance of the saved logo (stored in the customization spec)."""

    model_config = ConfigDict(extra="forbid")

    source: str = Field(description="'wikimedia' or 'file'")
    title: str
    page_url: str | None = None
    wikidata_id: str | None = None
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    width: float = Field(gt=0)
    height: float = Field(gt=0)

    @property
    def aspect(self) -> float:
        return self.width / self.height


@dataclass(frozen=True)
class Logo:
    info: LogoInfo
    svg: bytes

    def framed(self, margin: float = 0.14) -> bytes:
        """The logo inside a transparent margin, for tiles that scale images to fit."""

        width, height = self.info.width, self.info.height
        pad = margin * max(width, height)
        box = f"{-pad:g} {-pad:g} {width + 2 * pad:g} {height + 2 * pad:g}"
        inner = _strip_declaration(self.svg)
        return (
            (
                f'<svg xmlns="{SVG_NS}" viewBox="{box}" width="{width + 2 * pad:g}" '
                f'height="{height + 2 * pad:g}">'
            ).encode()
            + _nest(inner, width, height)
            + b"</svg>"
        )


def _strip_declaration(svg: bytes) -> bytes:
    return re.sub(rb"^\s*<\?xml[^>]*\?>\s*", b"", svg)


def _nest(svg: bytes, width: float, height: float) -> bytes:
    # A nested <svg> keeps the logo's own viewBox and coordinate system.
    return re.sub(
        rb"^<svg\b",
        f'<svg x="0" y="0" width="{width:g}" height="{height:g}"'.encode(),
        svg,
        count=1,
    )


def valid_wikidata_id(value: str | None) -> bool:
    return value is None or bool(_QID.fullmatch(value))


# ----- network ------------------------------------------------------------------


def _allowed(url: str) -> bool:
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or ""
    return parts.scheme == "https" and (
        host in {"wikidata.org", "wikimedia.org"}
        or host.endswith((".wikidata.org", ".wikimedia.org"))
    )


def http_get(url: str) -> bytes:
    """GET with Wikimedia's required User-Agent; only Wikimedia hosts are allowed."""

    if not _allowed(url):
        raise LogoError(f"refusing to fetch {url}")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - host checked
        if not _allowed(response.geturl()):
            raise LogoError(f"redirected to an unexpected host: {response.geturl()}")
        data = response.read(MAX_SVG_BYTES + 1)
    if len(data) > MAX_SVG_BYTES:
        raise LogoError("logo is larger than 1 MB")
    return data


def _api(fetch: Fetch, base: str = WIKIDATA_API, **params: str) -> dict:
    query = urllib.parse.urlencode({**params, "format": "json"})
    return json.loads(fetch(f"{base}?{query}"))


def _normalize(value: str) -> str:
    value = re.sub(r"['’]", "", value.casefold())
    return re.sub(r"[^a-z0-9]+", " ", re.sub(r"^the\s+", "", value)).strip()


def _year(claim: dict, prop: str) -> int:
    for qualifier in claim.get("qualifiers", {}).get(prop, []):
        value = qualifier.get("datavalue", {}).get("value", {})
        match = re.match(r"[+-]?(\d{4})", str(value.get("time", "")))
        if match:
            return int(match.group(1))
    return 0


def select_logo_file(claims: list[dict]) -> str | None:
    """Pick the current SVG logo from Wikidata ``P154`` claims, or None."""

    candidates = []
    for claim in claims:
        name = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
        if (
            not isinstance(name, str)
            or claim.get("rank") == "deprecated"
            or "P582" in claim.get("qualifiers", {})
            or _RETIRED.search(name)
            or not name.lower().endswith(".svg")
        ):
            continue
        rank = 1 if claim.get("rank") == "preferred" else 0
        candidates.append((rank, _year(claim, "P580"), name))
    if not candidates:
        return None
    return max(candidates)[2]


def _entities(fetch: Fetch, ids: list[str]) -> dict:
    if not ids:
        return {}
    data = _api(
        fetch, action="wbgetentities", ids="|".join(ids), props="claims|labels", languages="en"
    )
    return data.get("entities", {})


def _match_rank(entity: dict, business_name: str) -> int:
    """2 for an exact label match, 1 for a partial one, 0 for none."""

    label = entity.get("labels", {}).get("en", {}).get("value", "")
    want, got = _normalize(business_name), _normalize(label)
    if not want or not got:
        return 0
    if want == got:
        return 2
    return 1 if f" {want} " in f" {got} " else 0


def _matching_entities(
    business_name: str, wikidata_id: str | None, fetch: Fetch
) -> list[tuple[str, dict]]:
    """Wikidata items whose English label matches the business, best match first.

    A supplied ``wikidata_id`` wins when its label matches; otherwise exact label
    matches beat partial ones ("Walgreens" before "Walgreens Boots Alliance").
    """

    search = _api(
        fetch,
        action="wbsearchentities",
        search=business_name,
        language="en",
        type="item",
        limit="5",
    )
    ids = [r["id"] for r in search.get("search", []) if isinstance(r.get("id"), str)]
    if wikidata_id and valid_wikidata_id(wikidata_id) and wikidata_id not in ids:
        ids.insert(0, wikidata_id)
    entities = _entities(fetch, ids)
    ranked = []
    for position, qid in enumerate(ids):
        rank = _match_rank(entities.get(qid, {}), business_name)
        if rank:
            preferred = 1 if qid == wikidata_id else 0
            ranked.append((-preferred, -rank, position, qid))
    return [(qid, entities[qid]) for *_, qid in sorted(ranked)]


def _logo_claims(entity: dict) -> list[dict]:
    claims = entity.get("claims", {}).get("P154", [])
    return claims if isinstance(claims, list) else []


def find_logo_file(
    business_name: str, wikidata_id: str | None, fetch: Fetch = http_get
) -> tuple[str, str] | None:
    """Return ``(wikidata_id, commons_file)`` for the business's current logo, or None."""

    for qid, entity in _matching_entities(business_name, wikidata_id, fetch):
        name = select_logo_file(_logo_claims(entity))
        if name:
            return qid, name
    return None


# ----- Commons candidates (what Copilot chooses from) -----------------------------


class LogoCandidate(BaseModel):
    """One SVG file on Wikimedia Commons, as shown to Copilot."""

    model_config = ConfigDict(extra="forbid")

    file: str
    width: int
    height: int
    uploaded: str
    description: str
    retired: bool = Field(
        default=False, description="Wikidata or the file name marks it as a former logo"
    )
    wikidata_id: str | None = None


_IMAGEINFO = {
    "prop": "imageinfo",
    "iiprop": "size|mime|timestamp|extmetadata",
    "iiextmetadatafilter": "ImageDescription",
}


def _plain(value: object, limit: int = 200) -> str:
    text = html.unescape(re.sub(r"<[^>]+>", " ", str(value or "")))
    return " ".join(text.split())[:limit]


def _candidates(pages: dict) -> list[LogoCandidate]:
    found = []
    for page in sorted(pages.values(), key=lambda p: (p.get("index", 0), p.get("title", ""))):
        title = page.get("title", "")
        info = (page.get("imageinfo") or [{}])[0]
        if not title.startswith("File:") or info.get("mime") != "image/svg+xml":
            continue
        meta = info.get("extmetadata", {}).get("ImageDescription", {})
        found.append(
            LogoCandidate(
                file=title.removeprefix("File:"),
                width=int(info.get("width") or 0),
                height=int(info.get("height") or 0),
                uploaded=str(info.get("timestamp", ""))[:10],
                description=_plain(meta.get("value")),
                retired=bool(_RETIRED.search(title)),
            )
        )
    return found


def search_commons(query: str, fetch: Fetch = http_get, limit: int = 10) -> list[LogoCandidate]:
    """Full-text search of Commons file pages, limited to SVG files."""

    query = " ".join(query.split())[:100]
    if not query:
        return []
    data = _api(
        fetch,
        COMMONS_API,
        action="query",
        generator="search",
        gsrnamespace="6",
        gsrsearch=f"{query} filemime:image/svg+xml",
        gsrlimit=str(limit),
        **_IMAGEINFO,
    )
    return _candidates(data.get("query", {}).get("pages", {}))


def wikidata_candidates(business_name: str, fetch: Fetch = http_get) -> list[LogoCandidate]:
    """Every ``P154`` logo of the best-matching Wikidata items, former logos flagged."""

    files: dict[str, tuple[str | None, bool]] = {}
    for qid, entity in _matching_entities(business_name, None, fetch)[:2]:
        claims = _logo_claims(entity)
        current = select_logo_file(claims)
        for claim in claims:
            name = claim.get("mainsnak", {}).get("datavalue", {}).get("value")
            if isinstance(name, str) and name not in files:
                files[name] = (qid, name != current)
    if not files:
        return []
    data = _api(
        fetch,
        COMMONS_API,
        action="query",
        titles="|".join(f"File:{name}" for name in list(files)[:20]),
        **_IMAGEINFO,
    )
    found = _candidates(data.get("query", {}).get("pages", {}))
    for candidate in found:
        source_qid, former = files.get(candidate.file, (None, True))
        candidate.wikidata_id = source_qid
        candidate.retired = candidate.retired or former
    return found


# ----- SVG sanitizing ------------------------------------------------------------


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _number(value: str | None) -> float | None:
    match = re.match(r"^\s*([0-9]*\.?[0-9]+)\s*(px)?\s*$", value or "")
    return float(match.group(1)) if match else None


def sanitize_svg(data: bytes) -> tuple[bytes, float, float]:
    """Return a script-free SVG and its intrinsic width/height. Raises LogoError."""

    if len(data) > MAX_SVG_BYTES:
        raise LogoError("logo is larger than 1 MB")
    if re.search(rb"<!ENTITY", data, re.I) or re.search(rb"<!DOCTYPE[^>]*\[", data, re.I):
        raise LogoError("logo SVG declares entities or an internal DTD")
    # Editors such as Illustrator emit a plain external SVG DOCTYPE; it carries
    # nothing the renderer needs, so drop it before parsing.
    data = re.sub(rb"<!DOCTYPE[^>]*>", b"", data, flags=re.I)
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise LogoError(f"logo is not valid SVG: {exc}") from None
    if root.tag != f"{{{SVG_NS}}}svg":
        raise LogoError("logo is not an SVG document")

    def clean(element: ET.Element) -> None:
        for child in list(element):
            if not isinstance(child.tag, str) or _local(child.tag) in _DROP_ELEMENTS:
                element.remove(child)
                continue
            clean(child)
        for name in list(element.attrib):
            local = _local(name).lower()
            value = element.attrib[name]
            if (
                local.startswith("on")
                or (local == "href" and not value.startswith("#"))
                or (local == "style" and _UNSAFE_CSS.search(value))
                or "javascript:" in value.lower()
            ):
                del element.attrib[name]
        if _local(element.tag) == "style" and element.text and _UNSAFE_CSS.search(element.text):
            element.text = _UNSAFE_CSS.sub("", element.text)

    clean(root)
    view_box = [float(v) for v in re.split(r"[\s,]+", root.get("viewBox", "").strip()) if v]
    width, height = _number(root.get("width")), _number(root.get("height"))
    if len(view_box) == 4 and view_box[2] > 0 and view_box[3] > 0:
        width, height = width or view_box[2], height or view_box[3]
        if root.get("width") is None or root.get("height") is None:
            width, height = view_box[2], view_box[3]
    if not width or not height:
        raise LogoError("logo SVG has no usable size (width/height or viewBox)")
    if len(view_box) != 4:
        root.set("viewBox", f"0 0 {width:g} {height:g}")
    root.set("width", f"{width:g}")
    root.set("height", f"{height:g}")
    ET.register_namespace("", SVG_NS)
    ET.register_namespace("xlink", XLINK_NS)
    return ET.tostring(root, encoding="utf-8", xml_declaration=False), width, height


# ----- public API ----------------------------------------------------------------


def _logo(
    svg: bytes,
    *,
    source: str,
    title: str,
    page_url: str | None = None,
    wikidata_id: str | None = None,
) -> Logo:
    clean, width, height = sanitize_svg(svg)
    info = LogoInfo(
        source=source,
        title=title,
        page_url=page_url,
        wikidata_id=wikidata_id,
        sha256=hashlib.sha256(clean).hexdigest(),
        width=width,
        height=height,
    )
    return Logo(info=info, svg=clean)


def download_logo(file: str, fetch: Fetch = http_get, wikidata_id: str | None = None) -> Logo:
    """Download and sanitize one Commons SVG by file name. Raises LogoError."""

    file = file.removeprefix("File:").strip()
    if not file.lower().endswith(".svg"):
        raise LogoError(f"{file!r} is not an SVG file")
    quoted = urllib.parse.quote(file.replace(" ", "_"))
    return _logo(
        fetch(COMMONS_FILE + quoted),
        source="wikimedia",
        title=file,
        page_url=COMMONS_PAGE + quoted,
        wikidata_id=wikidata_id,
    )


def fetch_logo(business_name: str, wikidata_id: str | None, fetch: Fetch = http_get) -> Logo:
    """Deterministic fallback: the business's current Wikidata logo. Raises LogoError."""

    found = find_logo_file(business_name, wikidata_id, fetch)
    if found is None:
        raise LogoError(f"no current SVG logo for {business_name!r} was found on Wikimedia Commons")
    qid, title = found
    return download_logo(title, fetch, qid)


def logo_from_file(path: Path) -> Logo:
    """Use an operator-supplied SVG (sanitized like a downloaded one)."""

    if path.suffix.lower() != ".svg":
        raise LogoError("--logo-file must be an .svg file")
    return _logo(path.read_bytes(), source="file", title=path.name)


def save_logo(directory: Path, logo: Logo | None) -> None:
    path = directory / LOGO_FILE
    if logo is None:
        path.unlink(missing_ok=True)
        return
    directory.mkdir(parents=True, exist_ok=True)
    path.write_bytes(logo.svg)


def load_logo(directory: Path, info: LogoInfo | None) -> Logo | None:
    """Load the saved logo for ``info``; raises LogoError when it is missing or altered."""

    if info is None:
        return None
    path = directory / LOGO_FILE
    if not path.is_file():
        raise LogoError(f"the saved layout expects {path}; run `retail-setup customize`")
    svg = path.read_bytes()
    if hashlib.sha256(svg).hexdigest() != info.sha256:
        raise LogoError(f"{path} does not match the SHA-256 recorded in the layout")
    return Logo(info=info, svg=svg)


def restore_logo(directory: Path, info: LogoInfo | None, fetch: Fetch = http_get) -> Logo | None:
    """Load the saved logo, re-downloading a missing Wikimedia logo by its recorded name.

    A shared spec (``customize --from-spec``) carries only the logo's provenance;
    the re-downloaded file must still match the recorded SHA-256.
    """

    try:
        return load_logo(directory, info)
    except LogoError:
        if info is None or info.source != "wikimedia":
            raise
    logo = download_logo(info.title, fetch, info.wikidata_id)
    if logo.info.sha256 != info.sha256:
        raise LogoError(
            f"Wikimedia's {info.title!r} changed since the layout was saved; run "
            "`retail-setup customize --regenerate` or pass --logo-file"
        )
    save_logo(directory, logo)
    return Logo(info=info, svg=logo.svg)
