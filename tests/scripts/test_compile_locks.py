from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load():
    path = Path(__file__).resolve().parents[2] / "scripts" / "compile_locks.py"
    spec = importlib.util.spec_from_file_location("retail_demo_compile_locks", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


compile_locks = _load()


def test_pip_index_follows_pip_precedence():
    config = "\n".join(
        [
            "global.index-url='https://proxy.example/pypi/simple/'",
            "global.extra-index-url='https://ignored.example/simple'",
            "install.extra-index-url='https://a.example/simple https://b.example/simple'",
            "global.timeout='60'",
        ]
    )
    assert compile_locks.pip_indexes(config) == (
        "https://proxy.example/pypi/simple/",
        ["https://a.example/simple", "https://b.example/simple"],
    )
    environment = config + "\n:env:.index-url='https://env.example/simple'"
    assert compile_locks.pip_indexes(environment)[0] == "https://env.example/simple"
    assert compile_locks.pip_indexes("") == (None, [])


def test_configured_index_is_passed_to_uv():
    command = compile_locks.compile_command(
        "utility/requirements-deploy.txt",
        "https://proxy.example/simple",
        ["https://extra.example/simple"],
    )
    assert command[-4:] == [
        "--index-url",
        "https://proxy.example/simple",
        "--extra-index-url",
        "https://extra.example/simple",
    ]
    assert "--index-url" not in compile_locks.compile_command(
        "requirements-docs.txt", None, []
    )


def test_proxy_md5_hashes_are_removed_without_breaking_continuations():
    lock = (
        "pkg==1.0 \\\n"
        "    --hash=md5:aaa \\\n"
        "    --hash=sha256:bbb \\\n"
        "    --hash=md5:ccc\n"
        "    # via other\n"
    )
    assert compile_locks.sha256_only(lock) == (
        "pkg==1.0 \\\n    --hash=sha256:bbb\n    # via other\n"
    )
