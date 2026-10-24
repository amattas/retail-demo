<#
.SYNOPSIS
    Windows bootstrap for the retail-demo guided setup.

.DESCRIPTION
    Prepares Python, runs scripts/setup.py, and restores the original shell
    environment when setup returns, including on failure.

    Environment selection order:
      1. If Conda is installed, activate its 'retail-demo' environment.
         Create it with Python 3.13 if it does not exist.
      2. Otherwise, activate .venv in the repo root, or create it with an
         available Python 3.11+ interpreter.
      3. If neither Conda nor Python 3.11+ is available, install Miniforge
         with winget and use its Conda environment.

    This wrapper installs only Miniforge. scripts/setup.py installs Git,
    Terraform, and Azure CLI through the system package manager unless
    --skip-prereqs is passed.

    All arguments are forwarded to scripts/setup.py, for example:
        ./scripts/setup.ps1 --workspace-name retail-demo-alice
        ./scripts/setup.ps1 --workspace-name retail-demo-alice --deploy
        ./scripts/setup.ps1 --workspace-name retail-demo-alice --dry-run

#>

[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]] $ForwardArgs
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$RepoRoot = Split-Path -Parent $PSScriptRoot
$SetupPy = Join-Path $PSScriptRoot 'setup.py'
$CondaEnvName = 'retail-demo'
$CondaPythonVersion = '3.13'
$MiniforgeVersion = '26.3.2-3'
$VenvPath = Join-Path $RepoRoot '.venv'

function Get-EnvSnapshot {
    $snapshot = New-Object System.Collections.Hashtable ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($entry in [System.Environment]::GetEnvironmentVariables('Process').GetEnumerator()) {
        $snapshot[[string]$entry.Key] = [string]$entry.Value
    }
    return $snapshot
}

function Restore-EnvSnapshot {
    # Remove variables added during setup; restore values captured on entry.
    param([hashtable] $Snapshot)
    $currentKeys = @([System.Environment]::GetEnvironmentVariables('Process').Keys)
    foreach ($key in $currentKeys) {
        if (-not $Snapshot.ContainsKey([string]$key)) {
            # SetEnvironmentVariable($null) leaves an empty variable in PowerShell.
            # Remove-Item deletes it, which matters for CONDA_PREFIX/VIRTUAL_ENV.
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
    # Activation may leave a conda alias whose Source is a module name, not a
    # runnable path. Prefer a real executable from the active installation.
    foreach ($candidate in @($env:CONDA_EXE, $env:_CONDA_EXE)) {
        if ($candidate -and (Test-Path -LiteralPath $candidate -PathType Leaf)) {
            return (Get-Item -LiteralPath $candidate).FullName
        }
    }
    $conda = Get-Command conda -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($conda) { return $conda.Path }
    $candidates = @(
        (Join-Path $env:USERPROFILE 'miniforge3\Scripts\conda.exe'),
        (Join-Path $env:USERPROFILE 'miniconda3\Scripts\conda.exe'),
        (Join-Path $env:USERPROFILE 'anaconda3\Scripts\conda.exe'),
        (Join-Path $env:LOCALAPPDATA 'miniforge3\Scripts\conda.exe'),
        (Join-Path $env:LOCALAPPDATA 'miniconda3\Scripts\conda.exe'),
        (Join-Path $env:LOCALAPPDATA 'anaconda3\Scripts\conda.exe'),
        'C:\ProgramData\miniforge3\Scripts\conda.exe',
        'C:\ProgramData\miniconda3\Scripts\conda.exe'
    )
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate -PathType Leaf) { return $candidate }
    }
    return $null
}

function Resolve-SystemPython {
    foreach ($name in @('python', 'python3')) {
        $cmd = Get-Command $name -CommandType Application -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($cmd -and (Test-PythonVersion $cmd.Path)) {
            return $cmd.Path
        }
    }
    $py = Get-Command py -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($py) {
        try {
            $exe = & $py.Path '-3' '-c' 'import sys; print(sys.executable)' 2>$null
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
        throw 'winget is not available. Install Python 3.11+ or Miniforge, then run setup again.'
    }
    Write-Host 'Installing Miniforge with winget...'
    winget install --id CondaForge.Miniforge3 -e `
        --version $MiniforgeVersion --source winget `
        --disable-interactivity --silent `
        --accept-package-agreements --accept-source-agreements | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Miniforge installation failed (exit code $LASTEXITCODE)."
    }
    $conda = Resolve-Conda
    if (-not $conda) {
        throw 'Miniforge installed, but Conda was not found. Open a new shell and run setup again.'
    }
    return $conda
}

function Test-CondaEnvExists {
    param([string] $Conda, [string] $EnvName)
    $json = & $Conda env list --json
    if ($LASTEXITCODE -ne 0) {
        throw "Conda environment listing failed (exit code $LASTEXITCODE)."
    }
    $list = $json | ConvertFrom-Json
    if ($null -eq $list -or $list.envs -isnot [array]) {
        throw 'Conda returned an invalid environment inventory.'
    }
    foreach ($path in $list.envs) {
        if ((Split-Path $path -Leaf) -eq $EnvName) { return $true }
    }
    return $false
}

function Enable-CondaEnv {
    param([string] $Conda, [string] $EnvName)
    if (Test-CondaEnvExists -Conda $Conda -EnvName $EnvName) {
        Write-Host "Using Conda environment '$EnvName'."
    }
    else {
        Write-Host "Creating Conda environment '$EnvName' (Python $CondaPythonVersion)..."
        & $Conda create -n $EnvName "python=$CondaPythonVersion" -y | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Conda environment creation failed (exit code $LASTEXITCODE)."
        }
    }
    # Apply activation directly, without importing persistent Conda aliases or
    # replacing the caller's prompt via the full shell hook.
    $activation = & $Conda 'shell.powershell' 'activate' $EnvName | Out-String
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($activation)) {
        throw "Conda activation failed for '$EnvName' (exit code $LASTEXITCODE)."
    }
    Invoke-Expression $activation
    if (-not $env:CONDA_PREFIX -or $env:CONDA_DEFAULT_ENV -ne $EnvName) {
        throw "Failed to activate the conda environment '$EnvName'."
    }
}

function Enable-Venv {
    param([string] $Path)
    $activate = Join-Path $Path 'Scripts\Activate.ps1'
    if (-not (Test-Path $activate)) {
        throw "Virtual environment activation script not found at $activate."
    }
    & $activate
}

# Restore this environment even if setup fails.
$originalEnv = Get-EnvSnapshot
$exitCode = 1
try {
    $python = $null
    $conda = Resolve-Conda

    if ($conda) {
        Enable-CondaEnv -Conda $conda -EnvName $CondaEnvName
        $python = Join-Path $env:CONDA_PREFIX 'python.exe'
    }
    elseif (Test-Path (Join-Path $VenvPath 'Scripts\Activate.ps1')) {
        Write-Host "Using virtual environment at $VenvPath."
        Enable-Venv -Path $VenvPath
        $python = Join-Path $VenvPath 'Scripts\python.exe'
    }
    else {
        $systemPython = Resolve-SystemPython
        if ($systemPython) {
            Write-Host "Creating virtual environment at $VenvPath (using $systemPython)..."
            & $systemPython -m venv $VenvPath
            Enable-Venv -Path $VenvPath
            $python = Join-Path $VenvPath 'Scripts\python.exe'
        }
        else {
            Write-Host 'Python 3.11+ and Conda were not found. Installing Miniforge.'
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
    & $python $SetupPy '--profile' 'full-demo' @ForwardArgs
    $exitCode = $LASTEXITCODE
}
finally {
    # Switch the shell back to the environment that was active on entry.
    Restore-EnvSnapshot -Snapshot $originalEnv
}

exit $exitCode
