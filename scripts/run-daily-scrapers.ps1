param(
  [switch]$AutomaticRun
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$logDir = Join-Path $projectRoot 'logs'
if (-not (Test-Path $logDir)) {
  New-Item -Path $logDir -ItemType Directory | Out-Null
}

$timestamp = Get-Date -Format 'yyyy-MM-dd_HH-mm-ss'
$logPath = Join-Path $logDir "scraper-run-$timestamp.log"
$statePath = Join-Path $logDir 'daily-run-state.json'
$todayKey = Get-Date -Format 'yyyy-MM-dd'

function Write-Log {
  param([string]$Message)
  $line = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
  Write-Host $line
  Add-Content -Path $logPath -Value $line
}

function Test-SupabaseTcpReachable {
  param(
    [string]$TargetHost = 'zbvibhtopcsqrnecxgim.supabase.co',
    [int]$Port = 443,
    [int]$TimeoutMs = 3000
  )

  $client = $null
  try {
    $client = New-Object System.Net.Sockets.TcpClient
    $iar = $client.BeginConnect($TargetHost, $Port, $null, $null)
    if (-not $iar.AsyncWaitHandle.WaitOne($TimeoutMs, $false)) {
      return $false
    }
    $client.EndConnect($iar)
    return $true
  }
  catch {
    return $false
  }
  finally {
    if ($client) {
      $client.Close()
      $client.Dispose()
    }
  }
}

function Wait-ForSupabaseInternet {
  param(
    [int]$TimeoutSeconds = 60,
    [int]$PollIntervalSeconds = 5
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    if (Test-SupabaseTcpReachable) {
      Write-Log 'Internet/Supabase reachable.'
      return $true
    }
    Start-Sleep -Seconds $PollIntervalSeconds
  }

  return $false
}

function Invoke-SupabaseRest {
  param(
    [Parameter(Mandatory = $true)][string]$Method,
    [Parameter(Mandatory = $true)][string]$Uri,
    [Parameter(Mandatory = $true)][hashtable]$Headers,
    [string]$Body,
    [int]$MaxAttempts = 8,
    [int]$InitialDelaySeconds = 5
  )

  $attempt = 1
  $delay = [Math]::Max(1, $InitialDelaySeconds)
  while ($attempt -le $MaxAttempts) {
    try {
      if ($Body) {
        return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $Headers -Body $Body
      }
      return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $Headers
    }
    catch {
      if ($attempt -ge $MaxAttempts) {
        throw
      }
      Write-Log "Supabase call failed ($Method $Uri) attempt ${attempt}/${MaxAttempts}: $($_.Exception.Message)"
      Start-Sleep -Seconds $delay
      $delay = [Math]::Min(60, $delay * 2)
      $attempt++
    }
  }
}

function Get-RunState {
  if (-not (Test-Path $statePath)) {
    return $null
  }

  try {
    return Get-Content -Path $statePath -Raw | ConvertFrom-Json
  }
  catch {
    return $null
  }
}

function Save-RunState {
  param(
    [string]$Date,
    [string]$Status,
    [string]$LastRunAt
  )

  $payload = @{
    date = $Date
    status = $Status
    lastRunAt = $LastRunAt
  } | ConvertTo-Json

  Set-Content -Path $statePath -Value $payload -Encoding UTF8
}

function Finish-SupabaseRun {
  param(
    [Nullable[long]]$RunId,
    [string]$Status,
    [string]$Message = ""
  )

  $supabaseUrl = 'https://zbvibhtopcsqrnecxgim.supabase.co'
  $serviceKey  = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InpidmliaHRvcGNzcXJuZWN4Z2ltIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MTkyMjI5OCwiZXhwIjoyMDg3NDk4Mjk4fQ.jBvKDo_j0TvZ3FuQ2DpNAQmHXGABuyFcVhfjSc-G42w'
  if (-not $supabaseUrl -or -not $serviceKey) { return }

  $headers = @{
    apikey        = $serviceKey
    Authorization = "Bearer $serviceKey"
    "Content-Type"= "application/json"
  }

  $body = @{
    finished_at = (Get-Date).ToUniversalTime().ToString("o")
    status      = $Status
    message     = $Message
  } | ConvertTo-Json

  if ($RunId) {
    try {
      Invoke-SupabaseRest -Method 'Patch' -Uri "$supabaseUrl/rest/v1/scraper_runs?id=eq.$RunId" -Headers $headers -Body $body | Out-Null
      return
    }
    catch {
      Write-Log "Warning: Could not update Supabase run entry id=${RunId}: $($_.Exception.Message)"
    }
  }

  # Fallback: if run entry was never created (or patch failed), create a completed row so UI can still show latest run.
  $fallbackBody = @{
    status = $Status
    message = $Message
    finished_at = (Get-Date).ToUniversalTime().ToString("o")
  } | ConvertTo-Json

  try {
    Invoke-SupabaseRest -Method 'Post' -Uri "$supabaseUrl/rest/v1/scraper_runs" -Headers $headers -Body $fallbackBody | Out-Null
    Write-Log 'Fallback Supabase run entry created.'
  }
  catch {
    Write-Log "Warning: Could not create fallback Supabase run entry: $($_.Exception.Message)"
  }
}

function Send-ScraperReportEmail {
  param(
    [string]$Status,
    [string]$Message = ''
  )

  try {
    $env:SCRAPER_REPORT_STATUS = $Status
    $env:SCRAPER_REPORT_MESSAGE = $Message

    # Use the same command path as the local server/test button for consistency.
    npm run scrape:report:email *>&1 | Tee-Object -FilePath $logPath -Append

    if ($LASTEXITCODE -ne 0) {
      Write-Log "Warning: npm report email command exited with code $LASTEXITCODE; trying node fallback."
      node scripts/send-scraper-report-email.js *>&1 | Tee-Object -FilePath $logPath -Append

      if ($LASTEXITCODE -ne 0) {
        Write-Log "Warning: report email fallback script exited with code $LASTEXITCODE"
      }
    }
  }
  catch {
    Write-Log "Warning: failed to run report email script: $($_.Exception.Message)"
  }
  finally {
    Remove-Item Env:SCRAPER_REPORT_STATUS -ErrorAction SilentlyContinue
    Remove-Item Env:SCRAPER_REPORT_MESSAGE -ErrorAction SilentlyContinue
  }
}

$runId = $null
$runState = Get-RunState
$forceDailyRun = [System.Environment]::GetEnvironmentVariable('FORCE_DAILY_RUN')
$isForced = $forceDailyRun -eq '1' -or $forceDailyRun -eq 'true' -or $forceDailyRun -eq 'TRUE'
$isAutomaticRun = $AutomaticRun.IsPresent

if (-not $isForced -and $runState -and $runState.date -eq $todayKey) {
  if ($runState.status -eq 'success') {
    Write-Log "Daily scraper already ran successfully today; skipping."
    exit 0
  }
  Write-Log "Previous run today ended with status '$($runState.status)'; retrying."
}

try {
  if ($isAutomaticRun) {
    Write-Log 'Automatic run detected: waiting up to 180 seconds for internet/Supabase...'
    if (-not (Wait-ForSupabaseInternet -TimeoutSeconds 180 -PollIntervalSeconds 5)) {
      Write-Log 'Warning: Internet/Supabase still not reachable after startup wait; continuing scraper run anyway.'
    }
  }

  Write-Log 'Daily scraper run started'

  Write-Log 'Running scrapers...'
  npm run scrape:daily:all *>&1 | Tee-Object -FilePath $logPath -Append
  
  if ($LASTEXITCODE -ne 0) {
    throw "Scrapers failed with exit code $LASTEXITCODE"
  }

  Write-Log 'Daily scraper run completed successfully'
  Save-RunState -Date $todayKey -Status 'success' -LastRunAt (Get-Date).ToUniversalTime().ToString('o')
  Finish-SupabaseRun -RunId $runId -Status "success" -Message "Completed successfully"
  Send-ScraperReportEmail -Status 'success' -Message 'Completed successfully'
}
catch {
  Write-Log "ERROR: $($_.Exception.Message)"
  Save-RunState -Date $todayKey -Status 'failed' -LastRunAt (Get-Date).ToUniversalTime().ToString('o')
  Finish-SupabaseRun -RunId $runId -Status "failed" -Message $_.Exception.Message
  Send-ScraperReportEmail -Status 'failed' -Message $_.Exception.Message
  exit 1
}
