param(
  [string]$TaskName = 'CyclingScraperServer',
  [int]$Port = 5000
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$runnerScript = Join-Path $projectRoot 'scripts\run-local-scraper-server.ps1'

if (-not (Test-Path $runnerScript)) {
  throw "Runner script not found: $runnerScript"
}

$powerShellExe = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$actionArguments = @(
  '-NoProfile',
  '-WindowStyle', 'Hidden',
  '-ExecutionPolicy', 'Bypass',
  '-File', ('"{0}"' -f $runnerScript),
  '-Port', $Port
) -join ' '

$action = New-ScheduledTaskAction `
  -Execute $powerShellExe `
  -Argument $actionArguments `
  -WorkingDirectory $projectRoot

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew

Register-ScheduledTask `
  -TaskName $TaskName `
  -Action $action `
  -Trigger $trigger `
  -Principal $principal `
  -Settings $settings `
  -Description 'Starts local cycling scraper API server at Windows logon' `
  -Force | Out-Null

Write-Host "Scheduled task '$TaskName' registered to start local scraper server on logon"
