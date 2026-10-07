"""Tests for the dictionary notebook's revision-safe cache helpers."""

from __future__ import annotations

import ast
import io
import json
import types
import urllib.error
from pathlib import Path

import pytest

TEMPLATE = (
    Path(__file__).resolve().parents[1]
    / "notebooks"
    / "templates"
    / "setup-01-seed-dictionaries.ipynb.py"
)
SHA_A = "a" * 40
SHA_B = "b" * 40


def _load_helpers() -> types.SimpleNamespace:
    tree = ast.parse(TEMPLATE.read_text(encoding="utf-8"))
    helper_nodes = []
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef)):
            helper_nodes.append(node)
        elif isinstance(node, ast.Assign):
            try:
                ast.literal_eval(node.value)
            except (TypeError, ValueError):
                continue
            helper_nodes.append(node)
    namespace: dict[str, object] = {}
    exec(compile(ast.Module(helper_nodes, type_ignores=[]), str(TEMPLATE), "exec"), namespace)
    return types.SimpleNamespace(**namespace)


@pytest.fixture()
def seed_helpers() -> types.SimpleNamespace:
    return _load_helpers()


def _payloads(revision: str, include_optional: bool = True) -> dict[str, bytes]:
    paths = [
        "_shared/first_names.json",
        "_shared/last_names.json",
        "_shared/geographies.json",
        "_shared/tax_rates.json",
        "grocery/profile.json",
        "grocery/products.json",
        "grocery/brands.json",
    ]
    if include_optional:
        paths.append("grocery/tags.json")
    return {path: json.dumps({"path": path, "revision": revision}).encode() for path in paths}


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


class _Opener:
    def __init__(self, payloads: dict[str, bytes], fail_after: int | None = None):
        self.payloads = payloads
        self.fail_after = fail_after
        self.calls: list[str] = []

    def __call__(self, url: str):
        self.calls.append(url)
        if self.fail_after is not None and len(self.calls) > self.fail_after:
            raise OSError("interrupted download")
        relative_path = next(
            (path for path in self.payloads if url.endswith(f"/{path}")),
            None,
        )
        if relative_path is None:
            raise urllib.error.HTTPError(url, 404, "not found", {}, None)
        return _Response(self.payloads[relative_path])


def _sync(seed_helpers, root: Path, ref: str, opener: _Opener):
    return seed_helpers.synchronize_dictionaries(
        destination_root=root,
        repository_owner="example-owner",
        repository_name="retail-demo",
        ref=ref,
        store_type="grocery",
        opener=opener,
    )


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_immutable_revision_change_replaces_the_complete_cache(seed_helpers, tmp_path):
    _sync(seed_helpers, tmp_path, SHA_A, _Opener(_payloads("revision-a")))

    manifest = _sync(
        seed_helpers,
        tmp_path,
        SHA_B,
        _Opener(_payloads("revision-b")),
    )

    assert manifest["source"]["ref"] == SHA_B
    product = json.loads((tmp_path / "grocery/products.json").read_text())
    assert product["revision"] == "revision-b"
    assert seed_helpers.cache_matches_manifest(
        tmp_path,
        repository_owner="example-owner",
        repository_name="retail-demo",
        ref=SHA_B,
        store_type="grocery",
    )


def test_corrupted_immutable_cache_is_downloaded_again(seed_helpers, tmp_path):
    payloads = _payloads("stable")
    _sync(seed_helpers, tmp_path, SHA_A, _Opener(payloads))
    (tmp_path / "_shared/first_names.json").write_text("corrupt", encoding="utf-8")
    retry = _Opener(payloads)

    _sync(seed_helpers, tmp_path, SHA_A, retry)

    assert retry.calls
    assert (tmp_path / "_shared/first_names.json").read_bytes() == payloads[
        "_shared/first_names.json"
    ]


def test_interrupted_download_does_not_publish_partial_revision(seed_helpers, tmp_path):
    _sync(seed_helpers, tmp_path, SHA_A, _Opener(_payloads("revision-a")))
    before = _snapshot(tmp_path)

    with pytest.raises(OSError, match="interrupted download"):
        _sync(
            seed_helpers,
            tmp_path,
            SHA_B,
            _Opener(_payloads("revision-b"), fail_after=2),
        )

    assert _snapshot(tmp_path) == before
    assert seed_helpers.cache_matches_manifest(
        tmp_path,
        repository_owner="example-owner",
        repository_name="retail-demo",
        ref=SHA_A,
        store_type="grocery",
    )


def test_valid_immutable_cache_hit_performs_no_http_requests(seed_helpers, tmp_path):
    first = _Opener(_payloads("stable"))
    original = _sync(seed_helpers, tmp_path, SHA_A, first)
    no_network = _Opener({})

    cached = _sync(seed_helpers, tmp_path, SHA_A, no_network)

    assert cached == original
    assert no_network.calls == []


def test_mutable_ref_is_refreshed_on_every_run(seed_helpers, tmp_path):
    _sync(seed_helpers, tmp_path, "main", _Opener(_payloads("first")))
    refresh = _Opener(_payloads("second"))

    _sync(seed_helpers, tmp_path, "main", refresh)

    assert refresh.calls
    product = json.loads((tmp_path / "grocery/products.json").read_text())
    assert product["revision"] == "second"


def test_optional_404_removes_stale_optional_file(seed_helpers, tmp_path):
    _sync(seed_helpers, tmp_path, SHA_A, _Opener(_payloads("with-tags")))
    assert (tmp_path / "grocery/tags.json").is_file()

    manifest = _sync(
        seed_helpers,
        tmp_path,
        SHA_B,
        _Opener(_payloads("without-tags", include_optional=False)),
    )

    assert not (tmp_path / "grocery/tags.json").exists()
    assert manifest["missing_optional"] == ["grocery/tags.json"]
    assert "grocery/tags.json" not in manifest["files"]


def test_manifest_is_the_last_atomically_replaced_file(seed_helpers, tmp_path, monkeypatch):
    replacements: list[str] = []
    original_replace = seed_helpers.os.replace

    def recording_replace(source, destination):
        replacements.append(Path(destination).name)
        original_replace(source, destination)

    monkeypatch.setattr(seed_helpers.os, "replace", recording_replace)

    _sync(seed_helpers, tmp_path, SHA_A, _Opener(_payloads("stable")))

    assert replacements[-1] == seed_helpers.MANIFEST_NAME


def test_repository_source_is_validated_and_ref_is_url_encoded(seed_helpers):
    assert "%2F" in seed_helpers._base_url("fork-owner", "retail-demo", "feature/test")
    with pytest.raises(ValueError, match="repository owner"):
        seed_helpers._base_url("../owner", "retail-demo", SHA_A)


@pytest.mark.parametrize("store_type", ["../outside", "grocery/../../other", r"..\outside", ""])
def test_dictionary_store_type_cannot_escape_cache_root(seed_helpers, store_type):
    with pytest.raises(ValueError, match="safe dictionary directory"):
        seed_helpers._dictionary_paths(store_type)


def test_interrupted_publication_has_no_success_manifest(seed_helpers, tmp_path, monkeypatch):
    _sync(seed_helpers, tmp_path, SHA_A, _Opener(_payloads("old")))
    replace = seed_helpers.os.replace
    published = []

    def interrupt(source, destination):
        if published:
            raise OSError("publication interrupted")
        published.append(str(destination))
        replace(source, destination)

    monkeypatch.setattr(seed_helpers.os, "replace", interrupt)
    with pytest.raises(OSError, match="publication interrupted"):
        _sync(seed_helpers, tmp_path, SHA_B, _Opener(_payloads("new")))
    assert not (tmp_path / seed_helpers.MANIFEST_NAME).exists()
    assert not seed_helpers.cache_matches_manifest(
        tmp_path,
        repository_owner="example-owner",
        repository_name="retail-demo",
        ref=SHA_A,
        store_type="grocery",
    )
    monkeypatch.setattr(seed_helpers.os, "replace", replace)
    _sync(seed_helpers, tmp_path, SHA_B, _Opener(_payloads("new")))
    assert seed_helpers.cache_matches_manifest(
        tmp_path,
        repository_owner="example-owner",
        repository_name="retail-demo",
        ref=SHA_B,
        store_type="grocery",
    )
