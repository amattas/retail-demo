<#
.SYNOPSIS
  Launch the Retail Ontology Explorer (FastAPI backend + single-page frontend).

.DESCRIPTION
  Starts uvicorn on http://localhost:8080. Tokens are minted from your current
  `az login` session, so run `az login` for the target tenant first if needed.

.EXAMPLE
  ./run.ps1
  ./run.ps1 -Port 9000 -Python "C:\path\to\python.exe"
#>
param(
  [int]$Port = 8080,
  [string]$Python = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

if (-not $Python) {
  # Prefer the retail-demo conda env (has azure-identity + fabric tooling),
  # fall back to whatever `python` is on PATH.
  $candidate = Join-Path $env:LOCALAPPDATA "anaconda3\envs\retail-demo\python.exe"
  if (Test-Path $candidate) { $Python = $candidate } else { $Python = "python" }
}

Write-Host "Using Python: $Python"
Write-Host "Installing backend dependencies (idempotent)…"
& $Python -m pip install -q -r (Join-Path $PSScriptRoot "requirements.txt")

Write-Host "Starting Retail Ontology Explorer at http://localhost:$Port"
Push-Location $repoRoot
try {
  $env:PYTHONIOENCODING = "utf-8"
  & $Python -m uvicorn app.backend.main:app --host 127.0.0.1 --port $Port
}
finally {
  Pop-Location
}
