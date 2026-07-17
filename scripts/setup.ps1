<#
.SYNOPSIS
    Windows bootstrap for the retail-demo guided setup.

.DESCRIPTION
    Prepares a Python environment for the guided setup, delegates to
    scripts/setup.py, and then switches the shell back to the environment that
    was active before the script ran.

    Environment selection order:
      1. If conda is installed, use a conda environment named 'retail-demo'
         (created with Python 3.13 when it does not exist) and activate it.
      2. Otherwise, if a local virtual environment (.venv in the repo root)
         exists, activate it; if it does not exist, create it from a suitable
         system Python (3.11+) and activate it.
      3. If conda is missing and no system Python is available to build a venv,
         install Miniforge with winget and fall back to the conda path.

    Only Miniforge is installed here. The remaining CLI prerequisites
    (git, terraform, az) are installed by scripts/setup.py using the OS package
    manager (winget on Windows) unless --skip-prereqs is passed.

    All arguments are forwarded to scripts/setup.py, for example:
        ./scripts/setup.ps1 --workspace-name retail-demo-alice
        ./scripts/setup.ps1 --workspace-name retail-demo-alice --deploy
        ./scripts/setup.ps1 --workspace-name retail-demo-alice --dry-run

.NOTES
    The environment that is active when the script starts is captured up front
    and restored when setup.py returns (even on failure), so running this script
    leaves your shell on the environment you started from.
#>

[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $ForwardArgs
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$SetupPy = Join-Path $PSScriptRoot 'setup.py'
$CondaEnvName = 'retail-demo'
$CondaPythonVersion = '3.13'
$VenvPath = Join-Path $RepoRoot '.venv'

function Get-EnvSnapshot {
    # Capture the current process environment so it can be restored on exit.
    $snapshot = New-Object System.Collections.Hashtable ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($entry in [System.Environment]::GetEnvironmentVariables('Process').GetEnumerator()) {
        $snapshot[[string]$entry.Key] = [string]$entry.Value
    }
    return $snapshot
}

function Restore-EnvSnapshot {
    # Switch the shell back to the captured environment: drop variables that were
    # added (e.g. CONDA_PREFIX, VIRTUAL_ENV) and restore originals (e.g. PATH).
    param([hashtable] $Snapshot)
    $currentKeys = @([System.Environment]::GetEnvironmentVariables('Process').Keys)
    foreach ($key in $currentKeys) {
        if (-not $Snapshot.ContainsKey([string]$key)) {
            # Remove-Item truly deletes the variable; SetEnvironmentVariable($null)
            # only blanks it on PowerShell, leaving an empty CONDA_PREFIX/VIRTUAL_ENV.
            Remove-Item -LiteralPath ('Env:\' + [string]$key) -ErrorAction SilentlyContinue
        }
    }
    foreach ($entry in $Snapshot.GetEnumerator()) {
        [System.Environment]::SetEnvironmentVariable([string]$entry.Key, [string]$entry.Value, 'Process')
    }
}

function Test-PythonVersion {
    param([string] $PythonExe)
    try {
        $version = & $PythonExe -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>$null
        if (-not $version) { return $false }
        $parts = $version.Trim().Split('.')
        return ([int]$parts[0] -gt 3) -or ([int]$parts[0] -eq 3 -and [int]$parts[1] -ge 11)
    }
    catch {
        return $false
    }
}

function Resolve-Conda {
    $conda = Get-Command conda -ErrorAction SilentlyContinue
    if ($conda) { return $conda.Source }
    $candidates = @(
        (Join-Path $env:USERPROFILE 'miniforge3\Scripts\conda.exe'),
        (Join-Path $env:USERPROFILE 'miniconda3\Scripts\conda.exe'),
        (Join-Path $env:LOCALAPPDATA 'miniforge3\Scripts\conda.exe'),
        (Join-Path $env:LOCALAPPDATA 'miniconda3\Scripts\conda.exe'),
        'C:\ProgramData\miniforge3\Scripts\conda.exe',
        'C:\ProgramData\miniconda3\Scripts\conda.exe'
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) { return $candidate }
    }
    return $null
}

function Resolve-SystemPython {
    # Find a system Python (3.11+) suitable for creating a virtual environment.
    foreach ($name in @('python', 'python3')) {
        $cmd = Get-Command $name -ErrorAction SilentlyContinue
        if ($cmd -and (Test-PythonVersion $cmd.Source)) {
            return $cmd.Source
        }
    }
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        try {
            $exe = & $py.Source '-3' '-c' 'import sys; print(sys.executable)' 2>$null
            if ($exe -and (Test-PythonVersion $exe.Trim())) {
                return $exe.Trim()
            }
        }
        catch { }
    }
    return $null
}

function Install-Miniforge {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        throw 'winget is not available. Install Python 3.11+ or Miniforge manually, then re-run.'
    }
    Write-Host 'Installing Miniforge with winget...'
    winget install --id CondaForge.Miniforge3 -e `
        --accept-package-agreements --accept-source-agreements
    $conda = Resolve-Conda
    if (-not $conda) {
        throw 'Miniforge install completed but conda was not found. Open a new shell and re-run.'
    }
    return $conda
}

function Test-CondaEnvExists {
    param([string] $Conda, [string] $EnvName)
    $list = & $Conda env list --json | ConvertFrom-Json
    foreach ($path in $list.envs) {
        if ((Split-Path $path -Leaf) -eq $EnvName) { return $true }
    }
    return $false
}

function Enable-CondaEnv {
    # Create the named environment if missing, then activate it in this process.
    param([string] $Conda, [string] $EnvName)
    if (Test-CondaEnvExists -Conda $Conda -EnvName $EnvName) {
        Write-Host "Using existing conda environment '$EnvName'."
    }
    else {
        Write-Host "Creating conda environment '$EnvName' (Python $CondaPythonVersion)..."
        & $Conda create -n $EnvName "python=$CondaPythonVersion" -y
    }
    # Load conda's PowerShell integration so 'conda activate' works in-process.
    $hook = & $Conda 'shell.powershell' 'hook' | Out-String
    Invoke-Expression $hook
    conda activate $EnvName
    if (-not $env:CONDA_PREFIX) {
        throw "Failed to activate the conda environment '$EnvName'."
    }
}

function Enable-Venv {
    # Activate a local virtual environment in this process.
    param([string] $Path)
    $activate = Join-Path $Path 'Scripts\Activate.ps1'
    if (-not (Test-Path $activate)) {
        throw "Virtual environment activation script not found at $activate."
    }
    & $activate
}

# Remember the environment that is active right now so we can switch back to it
# once the guided setup finishes (or fails).
$originalEnv = Get-EnvSnapshot
$exitCode = 1
try {
    $python = $null
    $conda = Resolve-Conda

    if ($conda) {
        # 1) Conda is installed: use (or create) the 'retail-demo' environment.
        Enable-CondaEnv -Conda $conda -EnvName $CondaEnvName
        $python = Join-Path $env:CONDA_PREFIX 'python.exe'
    }
    elseif (Test-Path (Join-Path $VenvPath 'Scripts\Activate.ps1')) {
        # 2a) No conda, but a local virtual environment already exists: use it.
        Write-Host "Using existing virtual environment at $VenvPath."
        Enable-Venv -Path $VenvPath
        $python = Join-Path $VenvPath 'Scripts\python.exe'
    }
    else {
        # 2b) No conda and no venv yet: build one from a suitable system Python.
        $systemPython = Resolve-SystemPython
        if ($systemPython) {
            Write-Host "Creating virtual environment at $VenvPath (using $systemPython)..."
            & $systemPython -m venv $VenvPath
            Enable-Venv -Path $VenvPath
            $python = Join-Path $VenvPath 'Scripts\python.exe'
        }
        else {
            # 3) No conda and no Python to build a venv: install Miniforge.
            Write-Host 'No conda and no suitable Python found; bootstrapping with Miniforge.'
            $conda = Install-Miniforge
            Enable-CondaEnv -Conda $conda -EnvName $CondaEnvName
            $python = Join-Path $env:CONDA_PREFIX 'python.exe'
        }
    }

    if (-not $python -or -not (Test-Path $python)) {
        throw "Could not locate a Python interpreter after preparing the environment."
    }
    Write-Host "Using Python: $python"

    # Delegate to the Python guided setup engine. Forward setup.py's exit code
    # without letting PowerShell 7.4+ turn a non-zero exit into a thrown error.
    $PSNativeCommandUseErrorActionPreference = $false
    & $python $SetupPy @ForwardArgs
    $exitCode = $LASTEXITCODE
}
finally {
    # Switch the shell back to the environment that was active on entry.
    Restore-EnvSnapshot -Snapshot $originalEnv
}

exit $exitCode
