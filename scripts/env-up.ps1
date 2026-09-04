<#
.SYNOPSIS
    Brings up the orbit-commerce local infrastructure (Kafka, Schema Registry,
    Postgres, Kafka UI) via Docker Compose. Add -Apps to also bring up the 5
    dockerized Java services (Phase 9's `apps` Compose profile).
#>
[CmdletBinding()]
param(
    [switch]$Build,
    [switch]$Apps
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot

Push-Location $repoRoot
try {
    $composeArgs = @("up", "-d")
    if ($Apps) { $composeArgs = @("--profile", "apps") + $composeArgs }
    if ($Build) { $composeArgs += "--build" }

    docker compose @composeArgs
    if (-not $?) { throw "docker compose up failed" }

    Write-Host ""
    Write-Host "orbit-commerce infrastructure is starting up." -ForegroundColor Green
    Write-Host "  Kafka broker:      localhost:9092"
    Write-Host "  Schema Registry:   http://localhost:8081"
    Write-Host "  Kafka UI:          http://localhost:8080"
    Write-Host "  Postgres:          localhost:5432 (user: orbit / password: orbit)"

    if ($Apps) {
        Write-Host ""
        Write-Host "  order-service:     http://localhost:8082"
        Write-Host "  inventory-service: http://localhost:8083"
        Write-Host "  payment-service:   http://localhost:8084"
        Write-Host "  shipping-service:  http://localhost:8085"
        Write-Host "  query-service:     http://localhost:8086"
        Write-Host ""
        Write-Host "  Prometheus:        http://localhost:9090"
        Write-Host "  Grafana:           http://localhost:3000 (admin/admin default login)"
    }

    Write-Host ""
    Write-Host "Run '.\scripts\env-logs.ps1' to follow logs, or 'docker compose ps' to check status."
}
finally {
    Pop-Location
}
