# %% [markdown]
# # Setup 01 — Seed dictionaries
# Part of the retail-demo setup utility. Re-runnable (overwrite-by-design).
#
# Fetches the dictionary JSON files into `Files/setup/dictionaries/` in the
# default lakehouse. Immutable revisions use a content-verified local cache;
# mutable revisions are refreshed on every run. No engine code lives in this
# notebook.

# %%
# PARAMETERS — rendered by `retail-setup render`; defaults work unrendered
def _param(value: str, default: str) -> str:
    return default if len(value) > 1 and value[0] == value[1] == "{" else value

LAKEHOUSE_NAME = _param("{{LAKEHOUSE_NAME}}", "retail_lakehouse")
SILVER_DB = _param("{{SILVER_DB}}", "ag")
GOLD_DB = _param("{{GOLD_DB}}", "au")
STORE_TYPE = _param("{{STORE_TYPE}}", "supercenter")
START_DATE = _param("{{START_DATE}}", "2025-01-01")
END_DATE = _param("{{END_DATE}}", "2025-03-31")
STORE_COUNT = int(_param("{{STORE_COUNT}}", "50"))
SEED = int(_param("{{SEED}}", "42"))
DICTIONARY_REF = _param("{{DICTIONARY_REF}}", "main")

spark.conf.set("spark.sql.session.timeZone", "UTC")  # engine timestamps depend on it

# %% [markdown]
# ## Fetch dictionaries (revision-safe cache)

# %%
import hashlib
import json
import os
import re
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Callable

# Local filesystem mount of the default lakehouse Files area.
LOCAL_ROOT = "/lakehouse/default/Files/setup/dictionaries"
# Same location as seen by mssparkutils (relative Files/ path).
FS_ROOT = "Files/setup/dictionaries"
MANIFEST_NAME = "_dictionary_manifest.json"
MANIFEST_VERSION = 1


def _dictionary_paths(store_type: str) -> tuple[list[str], list[str]]:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", store_type):
        raise ValueError("store_type must be a single safe dictionary directory name")
    required = [
        "_shared/first_names.json",
        "_shared/last_names.json",
        "_shared/geographies.json",
        "_shared/tax_rates.json",
        f"{store_type}/profile.json",
        f"{store_type}/products.json",
        f"{store_type}/brands.json",
    ]
    return required, [f"{store_type}/tags.json"]


def _validate_repository_component(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", value):
        raise ValueError(f"invalid GitHub repository {label}: {value!r}")
    return value


def _base_url(repository_owner: str, repository_name: str, ref: str) -> str:
    owner = _validate_repository_component(repository_owner, "owner")
    name = _validate_repository_component(repository_name, "name")
    encoded_ref = urllib.parse.quote(ref, safe="")
    if not encoded_ref:
        raise ValueError("dictionary ref must not be empty")
    return (
        f"https://raw.githubusercontent.com/{owner}/{name}/{encoded_ref}/"
        "utility/data/dictionaries"
    )


def _source(
    repository_owner: str,
    repository_name: str,
    ref: str,
    store_type: str,
) -> dict[str, str]:
    return {
        "repository_owner": repository_owner,
        "repository_name": repository_name,
        "ref": ref,
        "store_type": store_type,
        "base_url": _base_url(repository_owner, repository_name, ref),
    }


def _is_immutable_ref(ref: str) -> bool:
    return re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", ref) is not None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_manifest(destination_root: Path) -> dict[str, object] | None:
    try:
        value = json.loads(
            (destination_root / MANIFEST_NAME).read_text(encoding="utf-8")
        )
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return value if isinstance(value, dict) else None


def cache_matches_manifest(
    destination_root: str | Path,
    *,
    repository_owner: str,
    repository_name: str,
    ref: str,
    store_type: str,
) -> bool:
    root = Path(destination_root)
    manifest = _load_manifest(root)
    if manifest is None or manifest.get("version") != MANIFEST_VERSION:
        return False

    expected_source = _source(repository_owner, repository_name, ref, store_type)
    if manifest.get("source") != expected_source:
        return False

    required, optional = _dictionary_paths(store_type)
    file_entries = manifest.get("files")
    missing_optional = manifest.get("missing_optional")
    if not isinstance(file_entries, dict) or not isinstance(missing_optional, list):
        return False
    if not set(required) <= set(file_entries) <= set(required + optional):
        return False
    if sorted(missing_optional) != sorted(set(optional) - set(file_entries)):
        return False

    actual_files = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*.json")
        if path.name != MANIFEST_NAME
    }
    if actual_files != set(file_entries):
        return False

    for relative_path, metadata in file_entries.items():
        path = root / relative_path
        if not isinstance(metadata, dict) or not path.is_file():
            return False
        try:
            if path.stat().st_size != metadata["size"]:
                return False
            if _sha256(path) != metadata["sha256"]:
                return False
        except (KeyError, OSError, TypeError):
            return False
    return True


def _download_to_staging(
    staging_root: Path,
    *,
    base_url: str,
    required: list[str],
    optional: list[str],
    opener: Callable[[str], object],
) -> tuple[dict[str, dict[str, object]], list[str]]:
    files: dict[str, dict[str, object]] = {}
    missing_optional: list[str] = []
    for relative_path in required + optional:
        url = f"{base_url}/{relative_path}"
        try:
            with opener(url) as response:
                data = response.read()
        except urllib.error.HTTPError as error:
            if error.code == 404 and relative_path in optional:
                missing_optional.append(relative_path)
                print(f"optional {relative_path}: not present upstream (404)")
                continue
            raise

        try:
            json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"{relative_path}: upstream response is not valid JSON") from error

        destination = staging_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        files[relative_path] = {
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
        }
        print(f"staged {relative_path} ({len(data):,} bytes)")
    return files, missing_optional


def _publish_staging(
    staging_root: Path,
    destination_root: Path,
    manifest: dict[str, object],
) -> None:
    destination_root.mkdir(parents=True, exist_ok=True)
    manifest_path = destination_root / MANIFEST_NAME
    manifest_path.unlink(missing_ok=True)

    published_files = set(manifest["files"])
    for existing in destination_root.rglob("*.json"):
        if existing.name != MANIFEST_NAME:
            existing.unlink()

    for relative_path in sorted(published_files):
        source = staging_root / relative_path
        destination = destination_root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(source, destination)

    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination_root,
        prefix=f".{MANIFEST_NAME}.",
        suffix=".tmp",
    )
    temporary_manifest = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
        os.replace(temporary_manifest, manifest_path)
    finally:
        temporary_manifest.unlink(missing_ok=True)


def synchronize_dictionaries(
    *,
    destination_root: str | Path,
    repository_owner: str,
    repository_name: str,
    ref: str,
    store_type: str,
    opener: Callable[[str], object] = urllib.request.urlopen,
) -> dict[str, object]:
    root = Path(destination_root)
    if _is_immutable_ref(ref) and cache_matches_manifest(
        root,
        repository_owner=repository_owner,
        repository_name=repository_name,
        ref=ref,
        store_type=store_type,
    ):
        manifest = _load_manifest(root)
        if manifest is None:
            raise RuntimeError("validated dictionary cache has no manifest")
        print(f"Verified immutable dictionary cache for {ref} under {FS_ROOT}.")
        return manifest

    required, optional = _dictionary_paths(store_type)
    source = _source(repository_owner, repository_name, ref, store_type)
    root.parent.mkdir(parents=True, exist_ok=True)
    staging_root = Path(
        tempfile.mkdtemp(prefix=".dictionaries-staging-", dir=root.parent)
    )
    try:
        print(f"Fetching dictionaries from {source['base_url']}")
        files, missing_optional = _download_to_staging(
            staging_root,
            base_url=source["base_url"],
            required=required,
            optional=optional,
            opener=opener,
        )
        manifest = {
            "version": MANIFEST_VERSION,
            "source": source,
            "files": files,
            "missing_optional": sorted(missing_optional),
        }
        _publish_staging(staging_root, root, manifest)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)

    if not cache_matches_manifest(
        root,
        repository_owner=repository_owner,
        repository_name=repository_name,
        ref=ref,
        store_type=store_type,
    ):
        raise RuntimeError("published dictionary cache failed manifest validation")
    return manifest


# Render injects only the ref. Fork deployments must set these environment
# overrides so a fork-only commit is fetched from the repository that owns it.
REPOSITORY_OWNER = os.environ.get("RETAIL_SETUP_REPOSITORY_OWNER", "amattas")
REPOSITORY_NAME = os.environ.get("RETAIL_SETUP_REPOSITORY_NAME", "retail-demo")
manifest = synchronize_dictionaries(
    destination_root=LOCAL_ROOT,
    repository_owner=REPOSITORY_OWNER,
    repository_name=REPOSITORY_NAME,
    ref=DICTIONARY_REF,
    store_type=STORE_TYPE,
)

# %% [markdown]
# ## Manifest

# %%
print(
    "Dictionary source: "
    f"{manifest['source']['repository_owner']}/{manifest['source']['repository_name']} "
    f"at {manifest['source']['ref']}"
)
for relative_path, metadata in sorted(manifest["files"].items()):
    print(f"{relative_path:50s} {metadata['size']:>12,} bytes {metadata['sha256']}")
print(f"{len(manifest['files'])} verified files under {LOCAL_ROOT}")
