<#
.SYNOPSIS
    Windows bootstrap for the retail-demo guided setup.

.DESCRIPTION
    Ensures a Python 3.11+ environment exists, then delegates to
    scripts/setup.py. When neither a suitable Python nor conda is found, this
    script installs Miniforge with winget and creates a conda environment so
    users with nothing installed can still run the guided setup.

    All arguments are forwarded to scripts/setup.py, for example:
        ./scripts/setup.ps1 --env dev
        ./scripts/setup.ps1 --env dev --deploy
        ./scripts/setup.ps1 --env dev --dry-run

.NOTES
    On macOS and Linux, run scripts/setup.py directly with an activated
    Python 3.11+ environment.
#>

[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $ForwardArgs
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$SetupPy = Join-Path $PSScriptRoot 'setup.py'
$CondaEnvName = 'retail'

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
        'C:\ProgramData\miniforge3\Scripts\conda.exe',
        'C:\ProgramData\miniconda3\Scripts\conda.exe'
    )
    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) { return $candidate }
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

function Get-CondaPython {
    param([string] $Conda, [string] $EnvName)
    $envs = & $Conda env list
    $exists = $envs | Where-Object { ($_ -split '\s+')[0] -eq $EnvName }
    if (-not $exists) {
        Write-Host "Creating conda environment '$EnvName' (Python 3.11)..."
        & $Conda create -n $EnvName 'python=3.11' -y
    }
    $python = & $Conda run -n $EnvName python -c 'import sys; print(sys.executable)'
    return $python.Trim()
}

# 1) Use the active interpreter when it is already Python 3.11+.
$python = $null
$current = Get-Command python -ErrorAction SilentlyContinue
if ($current -and (Test-PythonVersion $current.Source)) {
    $python = $current.Source
    Write-Host "Using Python on PATH: $python"
}

# 2) Otherwise use (or create) a conda environment, installing Miniforge if needed.
if (-not $python) {
    $conda = Resolve-Conda
    if (-not $conda) {
        $conda = Install-Miniforge
    }
    $python = Get-CondaPython -Conda $conda -EnvName $CondaEnvName
    Write-Host "Using conda environment '$CondaEnvName': $python"
}

# 3) Delegate to the Python guided setup engine.
& $python $SetupPy @ForwardArgs
exit $LASTEXITCODE
