$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$desktopPath = [Environment]::GetFolderPath('Desktop')
$shortcutPath = Join-Path $desktopPath 'Run Daily Scraper.lnk'
$runnerScript = Join-Path $projectRoot 'scripts\run-daily-scrapers-force.ps1'

if (-not (Test-Path $runnerScript)) {
	throw "Runner script not found: $runnerScript"
}

$targetPath = 'powershell.exe'
$arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$runnerScript`""

$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $targetPath
$shortcut.Arguments = $arguments
$shortcut.WorkingDirectory = $projectRoot
$shortcut.WindowStyle = 1
$shortcut.Description = 'Run the local daily cycling scraper pipeline'
$shortcut.Save()

Write-Host "Desktop shortcut created: $shortcutPath"
