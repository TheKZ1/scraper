param(
  [int]$Port = 5000
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot

try {
  $existing = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
  if ($existing) {
    Write-Host "Scraper server already listening on port $Port"
    exit 0
  }
}
catch {
}

Set-Location $projectRoot
node server.js
