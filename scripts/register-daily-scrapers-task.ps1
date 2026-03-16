param(
  [string]$TaskName = 'CyclingScrapersDaily',
  [int]$DelayMinutes = 2
)

$ErrorActionPreference = 'Stop'

function Test-IsAdministrator {
  $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
  $principal = New-Object Security.Principal.WindowsPrincipal($identity)
  return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-IsAdministrator)) {
  throw "Administrator rights are required to register an AtLogOn scheduled task. Re-run this command from an elevated PowerShell (Run as Administrator)."
}

$projectRoot = Split-Path -Parent $PSScriptRoot
$runnerScript = Join-Path $projectRoot 'scripts\run-daily-scrapers.ps1'

if (-not (Test-Path $runnerScript)) {
  throw "Runner script not found: $runnerScript"
}

$action = New-ScheduledTaskAction `
  -Execute 'powershell.exe' `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$runnerScript`" -AutomaticRun"

$logonTrigger = New-ScheduledTaskTrigger -AtLogOn
if ($DelayMinutes -gt 0) {
  $logonTrigger.Delay = "PT${DelayMinutes}M"
}
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -RunOnlyIfNetworkAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

try {
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $logonTrigger `
    -Principal $principal `
    -Settings $settings `
    -Description 'Runs cycling scrapers at Windows logon (max once per day)' `
    -Force | Out-Null
} catch {
  $message = $_.Exception.Message
  if ($message -match '0x80070005|Access is denied') {
    throw "Access denied while creating scheduled task '$TaskName'. Open PowerShell as Administrator and run: npm run scrape:daily:register"
  }
  throw
}

Write-Host "Scheduled task '$TaskName' registered with logon trigger (once-per-day enforced by runner state)"
