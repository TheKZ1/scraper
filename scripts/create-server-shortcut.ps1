$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$desktopPath = [Environment]::GetFolderPath('Desktop')
$shortcutPath = Join-Path $desktopPath 'Start Scraper Server.lnk'

$targetPath = 'powershell.exe'
$arguments = "-NoProfile -ExecutionPolicy Bypass -Command `"Set-Location -LiteralPath '$projectRoot'; npm run scrape:server`""

$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $targetPath
$shortcut.Arguments = $arguments
$shortcut.WorkingDirectory = $projectRoot
$shortcut.WindowStyle = 1
$shortcut.Description = 'Start the local cycling scraper API server'
$shortcut.Save()

Write-Host "Desktop shortcut created: $shortcutPath"
