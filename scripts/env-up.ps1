<#
.SYNOPSIS
    Brings up the orbit-commerce local infrastructure (Kafka, Schema Registry,
    Postgres, Kafka UI) via Docker Compose.
#>
[CmdletBinding()]
param(
    [switch]$Build
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

Push-Location $repoRoot
try {
    if ($Build) {
        docker compose up -d --build
    } else {
        docker compose up -d
    }
    if (-not $?) { throw "docker compose up failed" }

    Write-Host ""
    Write-Host "orbit-commerce infrastructure is starting up." -ForegroundColor Green
    Write-Host "  Kafka broker:      localhost:9092"
    Write-Host "  Schema Registry:   http://localhost:8081"
    Write-Host "  Kafka UI:          http://localhost:8080"
    Write-Host "  Postgres:          localhost:5432 (user: orbit / password: orbit)"
    Write-Host ""
    Write-Host "Run '.\scripts\env-logs.ps1' to follow logs, or 'docker compose ps' to check status."
}
finally {
    Pop-Location
}
