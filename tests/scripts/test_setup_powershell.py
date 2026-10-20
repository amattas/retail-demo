"""Execute the Windows launcher in isolated shells, without installs or deployment."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = ROOT / "scripts" / "setup.ps1"
POWERSHELL = shutil.which("pwsh") or shutil.which("powershell")
pytestmark = pytest.mark.skipif(
    os.name != "nt" or not POWERSHELL, reason="Windows PowerShell launcher integration"
)


def _quote(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _run(script: str, directory: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [POWERSHELL, "-NoProfile", "-NonInteractive", "-Command", script],
        cwd=directory,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )


def _functions() -> str:
    return f"""
    $ErrorActionPreference = 'Stop'
    $tokens = $null; $errors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile(
        {_quote(LAUNCHER)}, [ref]$tokens, [ref]$errors)
    if ($errors) {{ throw 'Launcher parse errors' }}
    foreach ($definition in $ast.FindAll({{
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst]
    }}, $true)) {{
        Invoke-Expression $definition.Extent.Text
    }}
    """


def test_stale_conda_alias_does_not_replace_the_native_executable(tmp_path):
    executable = tmp_path / "conda.bat"
    executable.write_text('@echo off\necho {"envs":[]}\n', encoding="ascii")
    script = _functions() + f"""
    $env:PATH = {_quote(tmp_path)} + ';' + $env:PATH
    Remove-Item Env:CONDA_EXE, Env:_CONDA_EXE -ErrorAction SilentlyContinue
    $module = New-Module -Name Conda -ScriptBlock {{
        function Invoke-Conda {{ & $env:CONDA_EXE @args }}
        Set-Alias conda Invoke-Conda
        Export-ModuleMember -Function Invoke-Conda -Alias conda
    }}
    Import-Module $module
    $resolved = Resolve-Conda
    if ($resolved -ne {_quote(executable)}) {{
        throw "Expected native executable; resolved '$resolved'"
    }}
    if (Test-CondaEnvExists -Conda $resolved -EnvName 'retail-demo') {{
        throw 'Unexpected environment'
    }}
    """
    result = _run(script, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


def test_conda_environment_executable_wins_over_shell_aliases(tmp_path):
    executable = tmp_path / "selected conda.bat"
    executable.write_text("@echo off\n", encoding="ascii")
    script = _functions() + f"""
    $env:CONDA_EXE = {_quote(executable)}
    function conda {{ throw 'Must not invoke this wrapper' }}
    $resolved = Resolve-Conda
    if ($resolved -ne {_quote(executable)}) {{ throw 'Active installation was ignored' }}
    """
    result = _run(script, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


def test_system_python_resolution_ignores_shell_functions(tmp_path):
    script = _functions() + f"""
    $env:PATH = {_quote(Path(sys.executable).parent)} + ';' + $env:PATH
    function python {{ throw 'Must not use this shell function' }}
    function python3 {{ throw 'Must not use this shell function' }}
    $resolved = Resolve-SystemPython
    if ($resolved -ne {_quote(sys.executable)}) {{ throw 'Native Python was not selected' }}
    """
    result = _run(script, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


def test_failed_setup_can_run_again_without_leaking_conda_shell_state(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copyfile(LAUNCHER, scripts / "setup.ps1")
    marker = tmp_path / "first-run"
    (scripts / "setup.py").write_text(
        "import sys\nfrom pathlib import Path\n"
        f"marker = Path({str(marker)!r})\n"
        "if not marker.exists():\n"
        "    marker.write_text('attempted')\n"
        "    sys.exit(1)\n"
        "print('SECOND_RUN_REACHED_PYTHON')\n",
        encoding="utf-8",
    )
    executable = tmp_path / "conda.bat"
    fake = tmp_path / "fake_conda.py"
    prefix = Path(sys.executable).parent
    activation = "\n".join([
        f"$env:CONDA_PREFIX = {_quote(prefix)}",
        f"$env:CONDA_EXE = {_quote(executable)}",
        "$env:CONDA_DEFAULT_ENV = 'retail-demo'",
        "$env:CONDA_SHLVL = '1'",
    ])
    fake.write_text(
        "import json, sys\n"
        "args = sys.argv[1:]\n"
        "if args == ['env', 'list', '--json']:\n"
        f"    print({json.dumps({'envs': [str(tmp_path / 'retail-demo')]})!r})\n"
        "elif args == ['shell.powershell', 'activate', 'retail-demo']:\n"
        f"    print({activation!r})\n"
        "else:\n"
        "    print('Unexpected conda command: ' + repr(args), file=sys.stderr)\n"
        "    sys.exit(7)\n",
        encoding="utf-8",
    )
    executable.write_text(
        f'@echo off\n"{sys.executable}" "{fake}" %*\n', encoding="utf-8"
    )
    script = f"""
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $false
    $env:PATH = {_quote(tmp_path)} + ';' + $env:PATH
    Remove-Item Env:CONDA_EXE, Env:_CONDA_EXE, Env:CONDA_PREFIX -ErrorAction SilentlyContinue
    function global:prompt {{ 'ORIGINAL_PROMPT' }}
    $originalPath = $env:PATH
    $originalConda = Get-Command conda -ErrorAction SilentlyContinue
    & {_quote(scripts / 'setup.ps1')}
    if ($LASTEXITCODE -ne 1) {{ throw "First failure exit code was not propagated" }}
    if ($env:PATH -ne $originalPath -or $env:CONDA_PREFIX -or $env:CONDA_EXE) {{
        throw 'Environment leaked after failure'
    }}
    if ((prompt) -ne 'ORIGINAL_PROMPT') {{ throw 'Prompt leaked after failure' }}
    if ((Get-Command conda).CommandType -ne $originalConda.CommandType) {{
        throw 'Conda alias leaked after failure'
    }}
    & {_quote(scripts / 'setup.ps1')}
    if ($LASTEXITCODE -ne 0) {{ throw 'Second launch failed' }}
    if ($env:PATH -ne $originalPath -or $env:CONDA_PREFIX -or $env:CONDA_EXE) {{
        throw 'Environment leaked after success'
    }}
    """
    result = _run(script, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SECOND_RUN_REACHED_PYTHON" in result.stdout


@pytest.mark.parametrize(
    ("phase", "message"),
    [
        ("list", "environment listing failed"),
        ("create", "environment creation failed"),
        ("activate", "activation failed"),
    ],
)
def test_conda_preparation_failures_stop_before_activation_succeeds(tmp_path, phase, message):
    executable = tmp_path / "conda.bat"
    fake = tmp_path / "fake_conda.py"
    fake.write_text(
        "import sys\n"
        f"phase = {phase!r}\n"
        "if sys.argv[1:4] == ['env', 'list', '--json'] and phase != 'list':\n"
        f"    print({json.dumps({'envs': [str(tmp_path / 'retail-demo')] if phase == 'activate' else []})!r})\n"
        "else:\n"
        "    sys.exit(7)\n",
        encoding="utf-8",
    )
    executable.write_text(
        f'@echo off\n"{sys.executable}" "{fake}" %*\n', encoding="utf-8"
    )
    script = _functions() + f"""
    $PSNativeCommandUseErrorActionPreference = $false
    $CondaPythonVersion = '3.13'
    try {{
        Enable-CondaEnv -Conda {_quote(executable)} -EnvName 'retail-demo'
        throw 'EXPECTED_FAILURE_WAS_IGNORED'
    }} catch {{
        if ($_.Exception.Message -notlike '*{message}*') {{ throw }}
    }}
    Write-Output 'EXPECTED_FAILURE_HANDLED'
    """
    result = _run(script, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "EXPECTED_FAILURE_HANDLED" in result.stdout


def test_miniforge_install_output_cannot_become_part_of_executable_path(tmp_path):
    script = _functions() + r"""
    $MiniforgeVersion = '26.3.2-3'
    function winget {
        Write-Output 'Installing...'
        Write-Output 'Installed successfully'
        $global:LASTEXITCODE = 0
    }
    function Resolve-Conda { return 'C:\test\conda.exe' }
    $resolved = Install-Miniforge
    if ($resolved -isnot [string] -or $resolved -ne 'C:\test\conda.exe') {
        throw 'Installer output polluted the executable path'
    }
    """
    result = _run(script, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


def test_invalid_conda_inventory_is_not_treated_as_a_missing_environment(tmp_path):
    executable = tmp_path / "conda.bat"
    executable.write_text("@echo off\necho {}\n", encoding="ascii")
    script = _functions() + f"""
    try {{
        Test-CondaEnvExists -Conda {_quote(executable)} -EnvName 'retail-demo'
        throw 'INVALID_INVENTORY_WAS_ACCEPTED'
    }} catch {{
        if ($_.Exception.Message -notlike '*invalid environment inventory*') {{ throw }}
    }}
    Write-Output 'INVALID_INVENTORY_REJECTED'
    """
    result = _run(script, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "INVALID_INVENTORY_REJECTED" in result.stdout
