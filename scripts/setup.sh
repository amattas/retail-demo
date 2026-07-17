#!/usr/bin/env bash
# Unix/macOS bootstrap for the retail-demo guided setup.
#
# Prepares a Python environment for scripts/setup.py, then delegates all
# arguments to that Python setup engine.
#
# Environment selection order:
#   1. If conda is installed, use a conda environment named "retail-demo"
#      (created with Python 3.13 when it does not exist) and activate it.
#   2. Otherwise, if a local virtual environment (.venv in the repo root)
#      exists, activate it; if it does not exist, create it from a suitable
#      system Python (3.11+) and activate it.
#   3. If conda is missing and no system Python is available to build a venv,
#      install Miniforge into "$HOME/miniforge3" and fall back to the conda path.
#
# Only Miniforge is installed here. The remaining CLI prerequisites
# (git, terraform, az) are installed by scripts/setup.py using the OS package
# manager unless --skip-prereqs is passed.
#
# All arguments are forwarded to scripts/setup.py, for example:
#   ./scripts/setup.sh --workspace-name retail-demo-alice
#   ./scripts/setup.sh --workspace-name retail-demo-alice --deploy
#   ./scripts/setup.sh --workspace-name retail-demo-alice --dry-run

set -Eeuo pipefail

ScriptDir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RepoRoot="$(cd -- "$ScriptDir/.." && pwd)"
SetupPy="$ScriptDir/setup.py"
CondaEnvName="retail-demo"
CondaPythonVersion="3.13"
VenvPath="$RepoRoot/.venv"

is_sourced() {
    [[ "${BASH_SOURCE[0]}" != "$0" ]]
}

fail() {
    printf 'error: %s\n' "$*" >&2
    if is_sourced; then
        return 1
    fi
    exit 1
}

test_python_version() {
    local python_exe="$1"
    "$python_exe" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
}

resolve_conda() {
    if command -v conda >/dev/null 2>&1; then
        command -v conda
        return 0
    fi

    local candidates=(
        "$HOME/miniforge3/bin/conda"
        "$HOME/miniconda3/bin/conda"
        "$HOME/mambaforge/bin/conda"
        "/opt/miniforge3/bin/conda"
        "/opt/miniconda3/bin/conda"
    )
    local candidate
    for candidate in "${candidates[@]}"; do
        if [[ -x "$candidate" ]]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    return 1
}

resolve_system_python() {
    local name
    for name in python3 python; do
        if command -v "$name" >/dev/null 2>&1; then
            local python_exe
            python_exe="$(command -v "$name")"
            if test_python_version "$python_exe"; then
                printf '%s\n' "$python_exe"
                return 0
            fi
        fi
    done
    return 1
}

miniforge_installer_url() {
    local os_name arch
    os_name="$(uname -s)"
    arch="$(uname -m)"

    case "$os_name:$arch" in
        Linux:x86_64|Linux:amd64)
            printf '%s\n' "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh"
            ;;
        Linux:aarch64|Linux:arm64)
            printf '%s\n' "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh"
            ;;
        Darwin:x86_64)
            printf '%s\n' "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-x86_64.sh"
            ;;
        Darwin:arm64)
            printf '%s\n' "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh"
            ;;
        *)
            fail "unsupported platform for automatic Miniforge install: $os_name $arch"
            ;;
    esac
}

download_file() {
    local url="$1"
    local output="$2"

    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url" -o "$output"
        return
    fi
    if command -v wget >/dev/null 2>&1; then
        wget -q "$url" -O "$output"
        return
    fi
    fail "curl or wget is required to install Miniforge automatically"
}

install_miniforge() {
    local prefix="$HOME/miniforge3"
    local conda="$prefix/bin/conda"

    if [[ -x "$conda" ]]; then
        printf '%s\n' "$conda"
        return 0
    fi

    if [[ -e "$prefix" ]]; then
        fail "$prefix exists but $conda was not found; fix or remove it, then re-run"
    fi

    local installer
    installer="$(mktemp "${TMPDIR:-/tmp}/miniforge.XXXXXX.sh")"
    trap 'rm -f "$installer"' RETURN

    printf 'Installing Miniforge into %s...\n' "$prefix" >&2
    download_file "$(miniforge_installer_url)" "$installer"
    bash "$installer" -b -p "$prefix"

    if [[ ! -x "$conda" ]]; then
        fail "Miniforge install completed but conda was not found at $conda"
    fi
    printf '%s\n' "$conda"
}

conda_env_exists() {
    local conda="$1"
    local env_name="$2"
    "$conda" env list | awk '{print $1}' | grep -Fxq "$env_name"
}

enable_conda_env() {
    local conda="$1"
    local env_name="$2"

    if conda_env_exists "$conda" "$env_name"; then
        printf "Using existing conda environment '%s'.\n" "$env_name"
    else
        printf "Creating conda environment '%s' (Python %s)...\n" "$env_name" "$CondaPythonVersion"
        "$conda" create -n "$env_name" "python=$CondaPythonVersion" -y
    fi

    # Load conda's bash integration so activation affects this process.
    # shellcheck disable=SC1090
    eval "$("$conda" shell.bash hook)"
    conda activate "$env_name"
    if [[ -z "${CONDA_PREFIX:-}" ]]; then
        fail "failed to activate the conda environment '$env_name'"
    fi
}

enable_venv() {
    local path="$1"
    local activate="$path/bin/activate"
    if [[ ! -f "$activate" ]]; then
        fail "virtual environment activation script not found at $activate"
    fi
    # shellcheck disable=SC1090
    source "$activate"
}

main() {
    local python_exe=""
    local conda=""

    if conda="$(resolve_conda)"; then
        enable_conda_env "$conda" "$CondaEnvName"
        python_exe="$CONDA_PREFIX/bin/python"
    elif [[ -f "$VenvPath/bin/activate" ]]; then
        printf 'Using existing virtual environment at %s.\n' "$VenvPath"
        enable_venv "$VenvPath"
        python_exe="$VenvPath/bin/python"
    else
        local system_python=""
        if system_python="$(resolve_system_python)"; then
            printf 'Creating virtual environment at %s (using %s)...\n' "$VenvPath" "$system_python"
            "$system_python" -m venv "$VenvPath"
            enable_venv "$VenvPath"
            python_exe="$VenvPath/bin/python"
        else
            printf 'No conda and no suitable Python found; bootstrapping with Miniforge.\n'
            conda="$(install_miniforge)"
            enable_conda_env "$conda" "$CondaEnvName"
            python_exe="$CONDA_PREFIX/bin/python"
        fi
    fi

    if [[ -z "$python_exe" || ! -x "$python_exe" ]]; then
        fail "could not locate a Python interpreter after preparing the environment"
    fi

    printf 'Using Python: %s\n' "$python_exe"
    "$python_exe" "$SetupPy" "$@"
}

main "$@"
