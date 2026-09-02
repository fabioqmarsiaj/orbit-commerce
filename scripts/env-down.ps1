<#
.SYNOPSIS
    Stops the orbit-commerce local infrastructure. Data volumes are preserved
    by default; use -Volumes to also remove them (full reset).
#>
[CmdletBinding()]
param(
    [switch]$Volumes
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

Push-Location $repoRoot
try {
    if ($Volumes) {
        docker compose down --volumes
        Write-Host "orbit-commerce infrastructure stopped and volumes removed." -ForegroundColor Yellow
    } else {
        docker compose down
        Write-Host "orbit-commerce infrastructure stopped. Data volumes preserved." -ForegroundColor Green
    }
}
finally {
    Pop-Location
}
