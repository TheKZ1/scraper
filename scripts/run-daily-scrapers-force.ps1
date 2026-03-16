$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$env:FORCE_DAILY_RUN = '1'
try {
  & "$PSScriptRoot\run-daily-scrapers.ps1"
}
finally {
  Remove-Item Env:FORCE_DAILY_RUN -ErrorAction SilentlyContinue
}
