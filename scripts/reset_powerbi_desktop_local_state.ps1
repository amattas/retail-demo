[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'Medium')]
param(
    [string]$RepoRoot
)

if (-not $RepoRoot) {
    $scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
    $RepoRoot = (Resolve-Path (Join-Path $scriptDirectory '..')).Path
}

$desktopProcesses = Get-Process -Name 'PBIDesktop' -ErrorAction SilentlyContinue
if ($desktopProcesses) {
    throw 'Close Power BI Desktop before running this script.'
}

$repoTargets = @(
    (Join-Path $RepoRoot 'fabric\powerbi\retail_model.Report\.pbi\localSettings.json'),
    (Join-Path $RepoRoot 'fabric\powerbi\retail_model.SemanticModel\.pbi\localSettings.json')
)

$appRoot = Join-Path $env:USERPROFILE 'Microsoft\Power BI Desktop Store App'
$cacheRoots = @(
    (Join-Path $appRoot 'AnalysisServicesWorkspaces'),
    (Join-Path $appRoot 'TempSaves\Backups'),
    (Join-Path $appRoot 'TempSaves\CloudUploads'),
    (Join-Path $appRoot 'FoldedArtifactsCache'),
    (Join-Path $appRoot 'LuciaCache'),
    (Join-Path $appRoot 'Cache\Temp')
)

$removedPaths = New-Object System.Collections.Generic.List[string]

function Remove-IfPresent {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LiteralPath
    )

    if (-not (Test-Path -LiteralPath $LiteralPath)) {
        return
    }

    if ($PSCmdlet.ShouldProcess($LiteralPath, 'Remove path')) {
        Remove-Item -LiteralPath $LiteralPath -Recurse -Force -ErrorAction Stop
        $removedPaths.Add($LiteralPath) | Out-Null
    }
}

function Remove-ChildrenIfPresent {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LiteralPath
    )

    if (-not (Test-Path -LiteralPath $LiteralPath)) {
        return
    }

    Get-ChildItem -LiteralPath $LiteralPath -Force -ErrorAction SilentlyContinue |
        ForEach-Object {
            $childPath = $_.FullName
            if ($PSCmdlet.ShouldProcess($childPath, 'Remove cached item')) {
                Remove-Item -LiteralPath $childPath -Recurse -Force -ErrorAction Stop
                $removedPaths.Add($childPath) | Out-Null
            }
        }
}

foreach ($target in $repoTargets) {
    Remove-IfPresent -LiteralPath $target
}

foreach ($root in $cacheRoots) {
    Remove-ChildrenIfPresent -LiteralPath $root
}

if ($removedPaths.Count -eq 0) {
    Write-Host 'No Power BI Desktop local state needed to be removed.'
    return
}

Write-Host "Removed $($removedPaths.Count) Power BI Desktop local-state path(s):"
$removedPaths | ForEach-Object { Write-Host " - $_" }
