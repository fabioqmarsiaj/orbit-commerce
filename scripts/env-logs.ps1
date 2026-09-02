<#
.SYNOPSIS
    Tails logs from the orbit-commerce infrastructure containers.
.PARAMETER Service
    Optional service name to filter logs (e.g. "broker", "postgres").
#>
[CmdletBinding()]
param(
    [string]$Service
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

Push-Location $repoRoot
try {
    if ($Service) {
        docker compose logs -f $Service
    } else {
        docker compose logs -f
    }
}
finally {
    Pop-Location
}
