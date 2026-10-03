$repoDir = "$env:REPO_DIR"
$rdpPassFile = "$env:TEMP\_rdppass"
$tailscaleOk = "$env:TEMP\_ts_ok"
$tailscaleIpFile = "$env:TEMP\_ts_ip"

# Capture env vars for $using: in jobs (Start-Job can't see $env: directly)
$mTsAuthKey = $env:TAILSCALE_AUTH_KEY
$mRunId = $env:GITHUB_RUN_ID

# ========== JOB: RDP Config + User ==========
$rdpJob = Start-Job -Name rdp -ScriptBlock {
  param($pf)
  Set-ItemProperty 'HKLM:\System\CurrentControlSet\Control\Terminal Server' fDenyTSConnections 0 -Force
  Set-ItemProperty 'HKLM:\System\CurrentControlSet\Control\Terminal Server\WinStations\RDP-Tcp' UserAuthentication 0 -Force
  Set-ItemProperty 'HKLM:\System\CurrentControlSet\Control\Terminal Server' fSingleSessionPerUser 0 -Force
  Set-NetFirewallProfile -Profile Domain,Public,Private -Enabled False -ErrorAction SilentlyContinue
  Stop-Service mpssvc -Force -ErrorAction SilentlyContinue; Set-Service mpssvc -StartupType Disabled -ErrorAction SilentlyContinue
  if ((Get-Service TermService).Status -ne 'Running') { Restart-Service TermService -Force }
  $chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*'
  $p = -join ((1..20) | ForEach-Object { $chars[(Get-Random -Maximum $chars.Length)] })
  $sp = ConvertTo-SecureString $p -AsPlainText -Force
  if (Get-LocalUser "RDP" -ErrorAction SilentlyContinue) { Remove-LocalUser "RDP" }
  New-LocalUser "RDP" -Password $sp -AccountNeverExpires -PasswordNeverExpires
  Add-LocalGroupMember -Group "Administrators" -Member "RDP"
  Add-LocalGroupMember -Group "Remote Desktop Users" -Member "RDP"
  $p | Set-Content $pf -Force
} -ArgumentList $rdpPassFile

# ========== JOB: Tailscale Install (cached MSI, skip if already installed) ==========
$tsInstallJob = Start-Job -Name tsi -ScriptBlock {
  $svc = Get-Service Tailscale -ErrorAction SilentlyContinue
  $tsExe = "C:\Program Files\Tailscale\tailscale.exe"
  if ($svc -and (Test-Path $tsExe)) {
    Write-Host "[TAILSCALE] Already installed (service + files found)"
    return
  }
  $msiDir = "$env:USERPROFILE\.cache"
  New-Item -ItemType Directory -Force -Path $msiDir | Out-Null
  $i = "$msiDir\tailscale.msi"
  if (-not (Test-Path $i) -or ((Get-Item $i).Length -lt 5MB)) {
    Write-Host "[TAILSCALE] Downloading MSI..."
    $url = "https://pkgs.tailscale.com/stable/tailscale-setup-latest-amd64.msi"
    Invoke-WebRequest -Uri $url -OutFile $i -UseBasicParsing -TimeoutSec 60
  } else {
    Write-Host "[TAILSCALE] Using cached MSI"
  }
  # FIX: the old code hand-registered the service with New-Service when the
  # cached Program Files files existed, which NEVER installs the WinTun driver.
  # Without the driver tailscaled cannot create the tunnel adapter and
  # `tailscale up` hangs at "Tailscale is starting" forever (exactly the
  # symptom seen on every run). Always run the MSI when the service is missing
  # so service + WinTun driver + firewall rules are all provisioned.
  Write-Host "[TAILSCALE] Installing via MSI (service + WinTun driver)..."
  $t = Measure-Command { Start-Process msiexec.exe -ArgumentList "/i `"$i`" /qn /norestart" -Wait }
  Write-Host "[TAILSCALE] MSI done in $([math]::Round($t.TotalSeconds,1))s"
  $svc = Get-Service Tailscale -ErrorAction SilentlyContinue
  if (-not $svc -and (Test-Path $tsExe)) {
    # MSI finished but no service (rare) — hand-register as a last resort
    Write-Host "[TAILSCALE] MSI done but no service — hand-registering daemon"
    $tsd = "C:\Program Files\Tailscale\tailscaled.exe"
    $null = & $tsd install-system-daemon 2>&1
    $svc = Get-Service Tailscale -ErrorAction SilentlyContinue
    if (-not $svc) { New-Service -Name Tailscale -BinaryPathName "`"$tsd`"" -StartupType Automatic -DisplayName "Tailscale" -ErrorAction SilentlyContinue }
  }
  if (Get-Service Tailscale -ErrorAction SilentlyContinue) {
    Write-Host "[TAILSCALE] Service registered (binary: $((Get-CimInstance Win32_Service -Filter "Name='Tailscale'").PathName))"
  } else {
    Write-Warning "[TAILSCALE] WARN: no Tailscale service after install"
  }
}

# ========== JOB: FFmpeg Install (cached by choco) ==========
$ffJob = Start-Job -Name ffmpeg -ScriptBlock {
  Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue
  $env:NO_PROXY = "community.chocolatey.org,chocolatey.org"
  if ((Test-Path "C:\ProgramData\chocolatey\bin\ffmpeg.exe") -and (Test-Path "C:\ProgramData\chocolatey\bin\ffprobe.exe")) {
    Write-Host "[FFMPEG] Using cached binaries"
  } else {
    Write-Host "[FFMPEG] Installing..."
    $t = Measure-Command { choco install ffmpeg -y --no-progress 2>&1 | Out-Null }
    Write-Host "[FFMPEG] Done in $([math]::Round($t.TotalSeconds,1))s"
  }
}

# ========== JOB: Clone + Build (skip if cached) ==========
$buildJob = Start-Job -Name build -ScriptBlock {
  param($d)
  New-Item -ItemType Directory -Force -Path "D:\repos" | Out-Null
  New-Item -ItemType Directory -Force -Path $d | Out-Null
  # Always refresh repo files (requirements.txt, scripts/, .env templates) into
  # REPO_DIR even when the DVR binary is restored from the cache - the binary
  # cache restore only populates chaturbate-dvr.exe, and later steps (cookie
  # deps install, cookie grab) need the full repo present.
  Copy-Item -Path "$env:GITHUB_WORKSPACE\*" -Destination $d -Recurse -Force -ErrorAction SilentlyContinue
  # ALWAYS build fresh from current source. Caching the DVR binary (workflow
  # actions/cache) let the fleet run stale builds, so assignment/recording fixes
  # never reached the nodes. We never trust a prebuilt or restored exe — rebuild
  # on every run. (The repo copy above refreshes scripts/.env/requirements too.)
  $needBuild = $true
  if ($needBuild) {
    Set-Location $d
    Write-Host "(BUILD) Building Go binaries..."
    $sa = $env:ALL_PROXY; Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue
    # A first-run module download occasionally times out during a TLS handshake
    # with proxy.golang.org.  GOPROXY only falls through for 404/410 responses,
    # not transport errors, so explicitly download the module graph with a
    # patient retry loop before compiling.  Disabling HTTP/2 here avoids the
    # flaky shared-runner connection that caused the observed handshake stalls.
    $env:GOPROXY = "https://proxy.golang.org,direct"
    $env:GODEBUG = "http2client=0"
    # Resolve go.exe explicitly so a PATH hiccup inside the job surfaces as a clear
    # error instead of a CommandNotFound crash with no captured output.
    $goCmd = Get-Command go -ErrorAction SilentlyContinue
    $goPath = if ($goCmd) { $goCmd.Source } else { "$env:GOROOT\bin\go.exe" }
    if (-not (Test-Path $goPath)) { throw "(BUILD) go.exe not found (checked PATH and GOROOT=$env:GOROOT)" }

    $modulesReady = $false
    for ($attempt = 1; $attempt -le 6; $attempt++) {
      Write-Host "(BUILD) Downloading Go modules (attempt $attempt/6)..."
      $downloadOut = & $goPath mod download 2>&1
      if ($LASTEXITCODE -eq 0) {
        $modulesReady = $true
        break
      }
      Write-Host "(BUILD) Module download attempt $attempt failed (exit $LASTEXITCODE):"
      $downloadOut | ForEach-Object { Write-Host "  $_" }
      if ($attempt -lt 6) { Start-Sleep -Seconds ([Math]::Min(45, 5 * $attempt)) }
    }
    if (-not $modulesReady) { throw "(BUILD) Go module download failed after 6 attempts" }

    foreach ($bin in @("chaturbate-dvr.exe .")) {
      $parts = $bin -split ' ', 2
      $name = $parts[0]; $pkg = $parts[1]
      $ok = $false
      for ($attempt = 0; $attempt -lt 4; $attempt++) {
        $t = [Diagnostics.Stopwatch]::StartNew()
        $out = & $goPath build -ldflags="-s -w" -o $name $pkg 2>&1
        $t.Stop(); $exit = $LASTEXITCODE
        if ($exit -eq 0) { Write-Host "(BUILD) $($name): $([math]::Round($t.Elapsed.TotalSeconds,1))s"; $ok = $true; break }
        Write-Host "(BUILD) $name attempt $($attempt+1) failed (exit $exit):"
        $out | ForEach-Object { Write-Host "  $_" }
        Start-Sleep -Seconds ([Math]::Min(30, 5 * ($attempt + 1)))
      }
      if (-not $ok) { throw "(BUILD) $name failed after 4 attempts" }
    }
    if ($sa) { $env:ALL_PROXY = $sa; $env:all_proxy = $sa }
  } else { Write-Host "(BUILD) Using cached binaries" }
} -ArgumentList $repoDir

Write-Host "[MAIN] Waiting for RDP, Tailscale..."; $null = [System.Console]::Out.Flush()
$null = $rdpJob, $tsInstallJob | Wait-Job -Timeout 60
$rdpPassword = Receive-Job $rdpJob -ErrorAction SilentlyContinue
if ($rdpPassword) { echo "RDP_PASSWORD=$rdpPassword" >> $env:GITHUB_ENV; Write-Host "(OK) RDP user created" } else { Write-Warning "(RDP) No password received" }
Receive-Job $tsInstallJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "(TS) $_" }
Remove-Job $rdpJob, $tsInstallJob -Force -ErrorAction SilentlyContinue

# ========== WAIT for FFmpeg (dedicated budget, no proxy needed) ==========
function Resolve-FFmpegBin {
  param([string]$name)
  $cands = @("C:\ProgramData\chocolatey\bin\$name.exe", "C:\Program Files\ffmpeg\bin\$name.exe", "C:\Program Files\FFmpeg\bin\$name.exe", "$env:LOCALAPPDATA\Microsoft\WinGet\Links\$name.exe")
  foreach ($c in $cands) { if (Test-Path $c) { return $c } }
  $gc = Get-Command $name -ErrorAction SilentlyContinue
  if ($gc) { return $gc.Source }
  $hit = Get-ChildItem "C:\ProgramData\chocolatey\lib" -Recurse -Filter "$name.exe" -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($hit) { return $hit.FullName }
  return $null
}
Write-Host "[MAIN] Waiting for FFmpeg..."; $null = [System.Console]::Out.Flush()
$ffShim = "C:\ProgramData\chocolatey\bin\ffmpeg.exe"
$fpShim = "C:\ProgramData\chocolatey\bin\ffprobe.exe"
for ($i = 0; $i -lt 60; $i++) {
  if ($ffJob.State -eq 'Completed' -or ((Test-Path $ffShim) -and (Test-Path $fpShim))) { break }
  if ($ffJob.State -eq 'Failed' -or $ffJob.State -eq 'Stopped') { break }
  Start-Sleep -Seconds 3
}
Receive-Job $ffJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" }
if ($ffJob.State -ne 'Running') { Remove-Job $ffJob -Force -ErrorAction SilentlyContinue }

$ffResolved = Resolve-FFmpegBin "ffmpeg"
$fpResolved = Resolve-FFmpegBin "ffprobe"
if ($ffResolved) { Write-Host "[FFMPEG] Found ffmpeg: $ffResolved" }
if ($fpResolved) { Write-Host "[FFMPEG] Found ffprobe: $fpResolved" }

if (-not $ffResolved -or -not $fpResolved) {
  Write-Host "[FFMPEG] Binary missing, retrying install..."
  Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue
  $env:NO_PROXY = "community.chocolatey.org,chocolatey.org"
  $retry = Measure-Command { choco install ffmpeg -y --no-progress 2>&1 | Out-Null }
  Write-Host "[FFMPEG] Retry done in $([math]::Round($retry.TotalSeconds,1))s"
  $ffResolved = Resolve-FFmpegBin "ffmpeg"; $fpResolved = Resolve-FFmpegBin "ffprobe"
}

if (-not $ffResolved -or -not $fpResolved) {
  Write-Host "[FFMPEG] Static build fallback (BtbN ffmpeg-master-latest-win64-gpl)..."
  Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue
  $zip = "D:\ffmpeg-static.zip"
  try {
    Invoke-WebRequest -Uri "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip" -OutFile $zip -UseBasicParsing -TimeoutSec 240
    New-Item -ItemType Directory -Force -Path "D:\ffmpeg" | Out-Null
    Expand-Archive -Path $zip -DestinationPath "D:\ffmpeg" -Force
    $dir = Get-ChildItem "D:\ffmpeg" -Directory | Select-Object -First 1
    $bin = "$($dir.FullName)\bin"
    Copy-Item "$bin\ffmpeg.exe" "C:\ProgramData\chocolatey\bin\" -Force -ErrorAction SilentlyContinue
    Copy-Item "$bin\ffprobe.exe" "C:\ProgramData\chocolatey\bin\" -Force -ErrorAction SilentlyContinue
    $ffResolved = Resolve-FFmpegBin "ffmpeg"; $fpResolved = Resolve-FFmpegBin "ffprobe"
  } catch { Write-Warning "(WARN) ffmpeg static download failed: $_" }
}

if ($ffResolved) { echo "FFMPEG_PATH=$ffResolved" >> $env:GITHUB_ENV }
if ($fpResolved) { echo "FFPROBE_PATH=$fpResolved" >> $env:GITHUB_ENV }
if ($ffResolved -and $fpResolved) {
  Write-Host "(OK) ffmpeg: $ffResolved / $fpResolved"
  if (-not (Test-Path $ffShim)) { Copy-Item $ffResolved $ffShim -Force -ErrorAction SilentlyContinue }
  if (-not (Test-Path $fpShim)) { Copy-Item $fpResolved $fpShim -Force -ErrorAction SilentlyContinue }
}
else { Write-Warning "(WARN) ffmpeg shims missing - muxing/thumbnails may fail" }


# ========== JOB: Tailscale Connect (depends on install) ==========
$tsDiagFile = "$env:TEMP\_ts_diag"
$tsErrFile = "$env:TEMP\_tsd_err"
$tsOutFile = "$env:TEMP\_tsd_out"
$tsConnectJob = Start-Job -Name tsc -ArgumentList $tailscaleIpFile, $tsDiagFile, $tsErrFile, $tsOutFile, $mTsAuthKey, $mRunId -ScriptBlock {
  param($ipFile, $diagFile, $tsErrFile, $tsOutFile, $authKey, $runId)

  # FIX: GitHub-hosted runners inject proxy env vars that can hijack
  # tailscaled's control-plane TLS handshake (hangs at "Tailscale is
  # starting", exactly the symptom seen on every run). Strip them like the
  # build job does so the daemon talks to the control plane directly.
  Remove-Item Env:ALL_PROXY,Env:all_proxy,Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:http_proxy,Env:https_proxy -ErrorAction SilentlyContinue

  function diag { param($m) $d = (Get-Content $diagFile -Raw -ErrorAction SilentlyContinue); "$d`n$m" | Set-Content $diagFile -Force }
  function Dump-TsdLogs {
    diag "--- tailscaled stderr tail ---"
    (Get-Content $tsErrFile -Tail 25 -ErrorAction SilentlyContinue) | ForEach-Object { diag "  $_" }
    diag "--- tailscaled stdout tail ---"
    (Get-Content $tsOutFile -Tail 25 -ErrorAction SilentlyContinue) | ForEach-Object { diag "  $_" }
  }
  function Get-TsState {
    try { $j = (& $ts status --json 2>&1 | Out-String) | ConvertFrom-Json; return $j.BackendState } catch { return "unknown" }
  }
  function Get-TsIP { (& $ts ip -4 2>&1 | Out-String).Trim() }
  function Test-TsNet {
    diag "--- netcheck ---"
    (& $ts netcheck 2>&1) | ForEach-Object { diag "  $_" }
    foreach ($h in @('controlplane.tailscale.com','login.tailscale.com')) {
      try { $r = Test-NetConnection -ComputerName $h -Port 443 -InformationLevel Quiet -WarningAction SilentlyContinue; diag "tcp 443 $h = $r" } catch { diag "tcp 443 $h threw: $_" }
    }
  }
  function Wait-TsIP {
    param([int]$Seconds)
    $deadline = (Get-Date).AddSeconds($Seconds)
    $ip = ""; $lastState = ""
    while ((Get-Date) -lt $deadline) {
      $ip = Get-TsIP
      if ($ip -match '^\d+\.\d+\.\d+\.\d+$') {
        diag "IP obtained: $ip"
        $ip | Set-Content $ipFile -Force
        return $ip
      }
      $st = Get-TsState
      if ($st -ne $lastState) { diag "backend state: $st"; $lastState = $st }
      if ($st -in @('NeedsLogin','LoggedOut')) { diag "backend stalled at $st — aborting wait"; return $null }
      Start-Sleep -Seconds 5
    }
    diag "no IPv4 after ${Seconds}s (state=$lastState)"
    return $null
  }

  diag "authKey set: $(-not [string]::IsNullOrWhiteSpace($authKey))  authKey length: $($authKey.Length)"
  $ts = "$env:ProgramFiles\Tailscale\tailscale.exe"
  $tsd = "$env:ProgramFiles\Tailscale\tailscaled.exe"
  $svc = Get-Service Tailscale -ErrorAction SilentlyContinue
  if (-not $svc) { diag "FAIL: Tailscale service not found"; return }
  $bin = (Get-CimInstance Win32_Service -Filter "Name='Tailscale'").PathName
  diag "service binary: $bin"
  diag "service status: $($svc.Status)"
  $daemonUp = $false
  if ($svc.Status -eq 'Running') { $daemonUp = $true }
  else {
    try { Start-Service Tailscale -ErrorAction Stop; diag "service started"; $daemonUp = $true } catch { diag "WARN: Start-Service: $_" }
  }
  if (-not $daemonUp) {
    diag "fallback: running tailscaled directly (bypassing SCM)"
    $stateDir = "$env:ProgramData\Tailscale"
    New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
    $stateFile = Join-Path $stateDir "server-state.conf"
    $p = Start-Process -FilePath $tsd -ArgumentList "--state=`"$stateFile`"" -WindowStyle Hidden -PassThru -RedirectStandardError $tsErrFile -RedirectStandardOutput $tsOutFile
    diag "tailscaled launched pid=$($p.Id)"
    for ($i = 0; $i -lt 15; $i++) {
      Start-Sleep -Seconds 2
      $st = Get-Process -Id $p.Id -ErrorAction SilentlyContinue
      if (-not $st) { diag "WARN: tailscaled pid $($p.Id) exited early"; break }
      try { $out = (& $ts status 2>&1) -join ' '; if ($LASTEXITCODE -eq 0 -or ($out -match "Logged out|NeedsLogin|Running|stopped|failed")) { diag "direct daemon ready (exit $LASTEXITCODE): $($out.Substring(0, [Math]::Min(120, $out.Length)))"; $daemonUp = $true; break } } catch { diag "direct status $i threw: $_" }
    }
    if (-not $daemonUp) { diag "FAIL: direct tailscaled not responsive"; Dump-TsdLogs; return }
  }
  for ($i = 0; $i -lt 15; $i++) {
    try { $out = (& $ts status 2>&1) -join ' '; $ec = $LASTEXITCODE; if ($ec -eq 0 -or ($out -match "Logged out|NeedsLogin|Running|stopped")) { diag "status ready (exit $ec): $($out.Substring(0, [Math]::Min(120, $out.Length)))"; break } } catch { diag "status call $i threw: $_" }
    Start-Sleep -Seconds 2
  }
  diag "running: ts up --authkey=*** --hostname=github-rdp-$runId --accept-routes --accept-dns=false --timeout=120s --reset"
  $upArgs = @("up","--authkey=$authKey","--hostname=github-rdp-$runId","--accept-routes","--accept-dns=false","--timeout=120s","--reset")
  $upOut = (& $ts @upArgs 2>&1) -join ' '
  $upEc = $LASTEXITCODE
  diag "ts up exit: $upEc  output: $upOut"
  if ($upEc -ne 0) {
    diag "ts up failed — dumping daemon logs + netcheck"
    Dump-TsdLogs
    Test-TsNet
    return
  }

  # FIX: give the login a real window (150s) and track the backend state
  # instead of nuking a still-progressing login after 45s.
  $ip = Wait-TsIP -Seconds 150
  if ($ip) { return }

  diag "WARNING: no IPv4 after 150s — restarting tailscaled daemon with --tun=userspace-networking..."
  Dump-TsdLogs
  Test-TsNet
  # Kill ALL existing tailscale daemon instances, then start fresh userspace one.
  Stop-Service Tailscale -Force -ErrorAction SilentlyContinue
  Get-Process tailscaled -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
  Start-Sleep -Seconds 3  # let SCM fully release the named pipe

  # Use a SEPARATE state file so the userspace daemon doesn't conflict with
  # the Windows service state. Only pass flags tailscaled actually accepts.
  $stateDir = "$env:TEMP\TailscaleUS"
  New-Item -ItemType Directory -Force -Path $stateDir | Out-Null
  $stateFile = Join-Path $stateDir "server-state.conf"
  $p = Start-Process -FilePath $tsd `
    -ArgumentList "--state=`"$stateFile`" --tun=userspace-networking" `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardError $tsErrFile `
    -RedirectStandardOutput $tsOutFile
  diag "userspace tailscaled launched pid=$($p.Id)"

  # Poll until the new daemon responds to 'tailscale status' (up to 30s)
  $daemonReady = $false
  for ($j = 0; $j -lt 15; $j++) {
    Start-Sleep -Seconds 2
    if (-not (Get-Process -Id $p.Id -ErrorAction SilentlyContinue)) {
      diag "WARN: userspace tailscaled (pid $($p.Id)) already exited"
      Dump-TsdLogs
      break
    }
    $st2 = (& $ts status 2>&1) -join ' '
    if ($LASTEXITCODE -ne 255 -or ($st2 -match "Logged out|NeedsLogin|Running|NoState|starting")) {
      diag "userspace daemon responsive (exit $LASTEXITCODE, attempt $j): $($st2.Substring(0,[Math]::Min(80,$st2.Length)))"
      $daemonReady = $true
      break
    }
  }
  if (-not $daemonReady) {
    diag "WARN: userspace daemon did not respond in 30s — proceeding anyway"
  }

  # Now call tailscale up against the userspace daemon
  $upOutUS = (& $ts @upArgs 2>&1) -join ' '
  $upEcUS = $LASTEXITCODE
  diag "userspace ts up exit: $upEcUS  output: $upOutUS"
  if ($upEcUS -ne 0) {
    diag "userspace ts up failed — dumping daemon logs + netcheck"
    Dump-TsdLogs
    Test-TsNet
    return
  }

  # Wait up to 180s for IP (userspace tunnel takes longer to negotiate)
  $ip = Wait-TsIP -Seconds 180
  if ($ip) { return }

  diag "FAIL: no IPv4 via userspace-networking"
  Dump-TsdLogs
  Test-TsNet
  diag "--- full status ---"
  ((& $ts status 2>&1) | Out-String) -split "`r?`n" | ForEach-Object { diag "  $_" }
}

$stJob = Start-Job -Name tasks -ArgumentList $repoDir -ScriptBlock {
  param($d)
  $cfCache = "C:\cloudflared\cloudflared.exe"
  New-Item -ItemType Directory -Force -Path "C:\cloudflared" | Out-Null
  if (-not (Test-Path $cfCache) -or ((Get-Item $cfCache).Length -lt 1MB)) {
    Write-Host "[CLOUDFLARED] Downloading..."
    for ($r = 1; $r -le 3; $r++) { try { Invoke-WebRequest -Uri "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe" -OutFile $cfCache -UseBasicParsing -TimeoutSec 30; break } catch { Start-Sleep -Seconds 2 } }
  } else { Write-Host "[CLOUDFLARED] Using cached binary" }
  Copy-Item $cfCache "$d\cloudflared.exe" -Force
  New-Item -ItemType Directory -Force -Path "D:\videos" | Out-Null
}

# ========== WAIT for Build ==========
# A cold build (source-hash change => module download + full compile) regularly
# exceeds 90s on a shared windows-latest runner. The old fixed Wait-Job -Timeout 90
# killed an in-flight `go build`, producing a bare "(FATAL) DVR binary not built" on
# every source change. Poll up to a generous budget and surface the job output.
Write-Host "[MAIN] Waiting for build (budget 900s)..."; $null = [System.Console]::Out.Flush()
$buildDeadline = (Get-Date).AddSeconds(900)
while ($buildJob.State -eq 'Running' -and (Get-Date) -lt $buildDeadline) {
  Start-Sleep -Seconds 15
}
$null = $buildJob | Wait-Job -Timeout 10 -ErrorAction SilentlyContinue
$buildOut = @(Receive-Job $buildJob -ErrorAction SilentlyContinue)
$buildOut | ForEach-Object { Write-Host "  $_" }
$buildState = $buildJob.State
Remove-Job $buildJob -Force -ErrorAction SilentlyContinue
if (-not (Test-Path "$repoDir\chaturbate-dvr.exe")) {
  if ($buildState -eq 'Failed' -or $buildState -eq 'Stopped') {
    Write-Host "(BUILD) job finished in state '$buildState' - captured output above"
    Write-Error "(FATAL) DVR build job failed"; exit 1
  }
  Write-Host "(BUILD) job state: $buildState"
  Write-Error "(FATAL) DVR binary not built within 900s"; exit 1
}
Write-Host "(OK) chaturbate-dvr.exe: $([math]::Round((Get-Item "$repoDir\chaturbate-dvr.exe").Length / 1MB, 1)) MB"


# ========== .env (runs parallel with stJob) ==========
Write-Host "[MAIN] Writing .env..."
Set-Location $repoDir
if (-not [string]::IsNullOrWhiteSpace($env:MINI_ENV)) {
  [System.IO.File]::WriteAllText("$repoDir\.env", $env:MINI_ENV, (New-Object System.Text.UTF8Encoding($false)))
  $miniLen = if ($env:MINI_ENV) { $env:MINI_ENV.Length } else { 0 }
  Write-Host "[.env] Written from secret ($miniLen chars)"
  $ev = Get-Content "$repoDir\.env" -Raw -ErrorAction SilentlyContinue
  if ($ev) {
    if (-not [string]::IsNullOrWhiteSpace($env:VOESX_API_KEY)) {
      if ($ev -match '(?m)^VOESX_API_KEY=') { $ev = $ev -replace '(?m)^VOESX_API_KEY=.*$', "VOESX_API_KEY=$env:VOESX_API_KEY" } else { $ev = $ev.TrimEnd("`r","`n") + "`nVOESX_API_KEY=$env:VOESX_API_KEY`n" } }
    if (-not [string]::IsNullOrWhiteSpace($env:VIDARA_API_KEY)) {
      if ($ev -match '(?m)^VIDARA_KEY=') { $ev = $ev -replace '(?m)^VIDARA_KEY=.*$', "VIDARA_KEY=$env:VIDARA_API_KEY" } else { $ev = $ev.TrimEnd("`r","`n") + "`nVIDARA_KEY=$env:VIDARA_API_KEY`n" } }
    if (-not [string]::IsNullOrWhiteSpace($env:VIDMOLY_API_KEY)) {
      if ($ev -match '(?m)^VIDMOLY_KEY=') { $ev = $ev -replace '(?m)^VIDMOLY_KEY=.*$', "VIDMOLY_KEY=$env:VIDMOLY_API_KEY" } else { $ev = $ev.TrimEnd("`r","`n") + "`nVIDMOLY_KEY=$env:VIDMOLY_API_KEY`n" } }
            if (-not [string]::IsNullOrWhiteSpace($env:AFFILIATE_WM)) {
      if ($ev -match '(?m)^AFFILIATE_WM=') { $ev = $ev -replace '(?m)^AFFILIATE_WM=.*$', "AFFILIATE_WM=$env:AFFILIATE_WM" } else { $ev = $ev.TrimEnd("`r","`n") + "`nAFFILIATE_WM=$env:AFFILIATE_WM`n" } }
    # Route Catbox image uploads through the Cloudflare Worker proxy so nodes
    # never hit Catbox directly from datacenter IPs (HTTP 412 "Invalid
    # uploader" blocks). Also carry the userhash so previews are permanent.
    if (-not [string]::IsNullOrWhiteSpace($env:CATBOX_PROXY_URL)) {
      if ($ev -match '(?m)^CATBOX_PROXY_URL=') { $ev = $ev -replace '(?m)^CATBOX_PROXY_URL=.*$', "CATBOX_PROXY_URL=$env:CATBOX_PROXY_URL" } else { $ev = $ev.TrimEnd("`r","`n") + "`nCATBOX_PROXY_URL=$env:CATBOX_PROXY_URL`n" } }
    if (-not [string]::IsNullOrWhiteSpace($env:CATBOX_USERHASH)) {
      if ($ev -match '(?m)^CATBOX_USERHASH=') { $ev = $ev -replace '(?m)^CATBOX_USERHASH=.*$', "CATBOX_USERHASH=$env:CATBOX_USERHASH" } else { $ev = $ev.TrimEnd("`r","`n") + "`nCATBOX_USERHASH=$env:CATBOX_USERHASH`n" } }
    if (-not [string]::IsNullOrWhiteSpace($env:IMGBB_API_KEY)) {
      if ($ev -match '(?m)^IMGBB_API_KEY=') { $ev = $ev -replace '(?m)^IMGBB_API_KEY=.*$', "IMGBB_API_KEY=$env:IMGBB_API_KEY" } else { $ev = $ev.TrimEnd("`r","`n") + "`nIMGBB_API_KEY=$env:IMGBB_API_KEY`n" } }
    if (-not [string]::IsNullOrWhiteSpace($env:IMGCHEST_TOKEN)) {
      if ($ev -match '(?m)^IMGCHEST_TOKEN=') { $ev = $ev -replace '(?m)^IMGCHEST_TOKEN=.*$', "IMGCHEST_TOKEN=$env:IMGCHEST_TOKEN" } else { $ev = $ev.TrimEnd("`r","`n") + "`nIMGCHEST_TOKEN=$env:IMGCHEST_TOKEN`n" } }
    if (-not [string]::IsNullOrWhiteSpace($env:IMGPILE_KEY)) {
      if ($ev -match '(?m)^IMGPILE_KEY=') { $ev = $ev -replace '(?m)^IMGPILE_KEY=.*$', "IMGPILE_KEY=$env:IMGPILE_KEY" } else { $ev = $ev.TrimEnd("`r","`n") + "`nIMGPILE_KEY=$env:IMGPILE_KEY`n" } }
    # Streamtape needs BOTH halves: the FTP/API username and the password.
    # Without this pair the node falls back to whatever stale values happen to
    # be baked into the MINI_ENV blob, which silently keeps the old account.
    if (-not [string]::IsNullOrWhiteSpace($env:STREAMTAPE_LOGIN)) {
      if ($ev -match '(?m)^STREAMTAPE_LOGIN=') { $ev = $ev -replace '(?m)^STREAMTAPE_LOGIN=.*$', "STREAMTAPE_LOGIN=$env:STREAMTAPE_LOGIN" } else { $ev = $ev.TrimEnd("`r","`n") + "`nSTREAMTAPE_LOGIN=$env:STREAMTAPE_LOGIN`n" } }
    if (-not [string]::IsNullOrWhiteSpace($env:STREAMTAPE_KEY)) {
      if ($ev -match '(?m)^STREAMTAPE_KEY=') { $ev = $ev -replace '(?m)^STREAMTAPE_KEY=.*$', "STREAMTAPE_KEY=$env:STREAMTAPE_KEY" } else { $ev = $ev.TrimEnd("`r","`n") + "`nSTREAMTAPE_KEY=$env:STREAMTAPE_KEY`n" } }
    [System.IO.File]::WriteAllText("$repoDir\.env", $ev, (New-Object System.Text.UTF8Encoding($false)))
  }
} else { Write-Warning "[.env] MINI_ENV secret is empty" }

$freshEnv = Get-Content "$repoDir\.env" -Raw -ErrorAction SilentlyContinue
$envCookies = if ($freshEnv -match '(?m)^COOKIES=(.*)$') { $matches[1] } else { $null }
$envUA = if ($freshEnv -match '(?m)^USER_AGENT=(.*)$') { $matches[1] } else { $null }
if ($envCookies) {
  $c = Get-Content "$repoDir\.env" -Raw -ErrorAction SilentlyContinue
  if ($c) {
    $c = $c -replace '(?m)^COOKIES=.*$', "COOKIES=$envCookies"
    if ($envUA) { $c = $c -replace '(?m)^USER_AGENT=.*$', "USER_AGENT=$envUA" }
    [System.IO.File]::WriteAllText("$repoDir\.env", $c, (New-Object System.Text.UTF8Encoding($false)))
  }
}
# Merge Supabase credentials
if (-not [string]::IsNullOrWhiteSpace($env:SUPABASE_URL)) {
  $sb = Get-Content "$repoDir\.env" -Raw -ErrorAction SilentlyContinue
  if ($sb -eq $null) { $sb = "" }
  function SetOrAdd($k, $v) { if ($sb -match "(?m)^$k=") { $script:sb = $sb -replace "(?m)^$k=.*`$", "$k=$v" } else { $script:sb = $sb.TrimEnd("`r","`n") + "`n$k=$v`n" } }
  function GuardedSetOrAdd($k, $v) { if (-not [string]::IsNullOrWhiteSpace($v) -and $v -notmatch '^\-+$' -and $v.Length -gt 5) { SetOrAdd $k $v } }
  GuardedSetOrAdd "SUPABASE_URL" $env:SUPABASE_URL; GuardedSetOrAdd "SUPABASE_API_KEY" $env:SUPABASE_API_KEY; GuardedSetOrAdd "SUPABASE_SERVICE_ROLE_KEY" $env:SUPABASE_SERVICE_ROLE_KEY; GuardedSetOrAdd "DATABASE_PASSWORD" $env:DATABASE_PASSWORD
  [System.IO.File]::WriteAllText("$repoDir\.env", $sb, (New-Object System.Text.UTF8Encoding($false)))
}


# ========== WAIT for TS Connect + Cloudflared (parallel with builds + .env) ==========
Write-Host "[MAIN] Waiting for Tailscale connect + cloudflared..."; $null = [System.Console]::Out.Flush()
$null = $stJob | Wait-Job -Timeout 60
$null = $tsConnectJob | Wait-Job -Timeout 400
$tsIp = Receive-Job $tsConnectJob -ErrorAction SilentlyContinue
if (Test-Path $tailscaleIpFile) { $tsIp = Get-Content $tailscaleIpFile; echo "TAILSCALE_IP=$tsIp" >> $env:GITHUB_ENV; Write-Host "(OK) Tailscale: $tsIp" } else { Write-Warning "(TS) No Tailscale IP"; if (Test-Path $tsDiagFile) { Get-Content $tsDiagFile | ForEach-Object { Write-Host "(TS) $_" } } }
Receive-Job $stJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" }
Remove-Job $tsConnectJob, $stJob -Force -ErrorAction SilentlyContinue
$setupElapsed = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds() - [int64]$env:START_TIME
Write-Host "[MAIN] === Parallel setup complete in $setupElapsed s ==="
