param(
    [switch]$NoAppStart
)

$ErrorActionPreference = "Stop"
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ffmpegDir = "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Gyan.FFmpeg.Essentials_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-8.1.1-essentials_build\bin"
$cloudflaredDir = "C:\Program Files (x86)\cloudflared"

Write-Host "=== MiniDelectableService Setup ===" -ForegroundColor Cyan

# 1. Install FFmpeg via winget
Write-Host "[1/7] Installing FFmpeg..." -ForegroundColor Yellow
$ffmpeg = Get-Command ffmpeg -ErrorAction SilentlyContinue
if (-not $ffmpeg) {
    winget install Gyan.FFmpeg.Essentials --accept-package-agreements --accept-source-agreements 2>&1 | Out-Null
    if (-not (Test-Path $ffmpegDir\ffmpeg.exe)) {
        Write-Host "ERROR: FFmpeg install failed" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "  FFmpeg already installed" -ForegroundColor Green
}

# 2. Add ffmpeg to PATH
Write-Host "[2/7] Adding ffmpeg to PATH..." -ForegroundColor Yellow
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*$ffmpegDir*") {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;$ffmpegDir", "User")
    Write-Host "  Added to user PATH" -ForegroundColor Green
} else {
    Write-Host "  Already in PATH" -ForegroundColor Green
}
$env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [Environment]::GetEnvironmentVariable("Path", "User")

# 3. Install cloudflared
Write-Host "[3/7] Installing cloudflared..." -ForegroundColor Yellow
$cf = Get-Command cloudflared -ErrorAction SilentlyContinue
if (-not $cf) {
    winget install Cloudflare.cloudflared --accept-package-agreements --accept-source-agreements 2>&1 | Out-Null
    $env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [Environment]::GetEnvironmentVariable("Path", "User")
    $cf = Get-Command cloudflared -ErrorAction SilentlyContinue
    if (-not $cf) {
        Write-Host "ERROR: cloudflared install failed" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "  cloudflared already installed" -ForegroundColor Green
}

# 4. Install Go dependencies
Write-Host "[4/7] Installing Go dependencies..." -ForegroundColor Yellow
Set-Location -LiteralPath $ProjectDir
go mod download 2>&1 | Out-Null
Write-Host "  Go modules downloaded" -ForegroundColor Green

# 5. Build Go binary
Write-Host "[5/7] Building Go binary..." -ForegroundColor Yellow
go build -o chaturbate-dvr.exe . 2>&1 | Out-Null
Write-Host "  Build complete" -ForegroundColor Green

# 6. Install Python dependencies
Write-Host "[6/7] Installing Python dependencies..." -ForegroundColor Yellow
python -c "import tomllib; d=tomllib.load(open('pyproject.toml', 'rb')); print('\n'.join(d['project']['dependencies']))" | Set-Content -Path "$env:TEMP\requirements.txt" -Encoding ASCII
pip install -r "$env:TEMP\requirements.txt" 2>&1 | Out-Null
python -m playwright install chromium 2>&1 | Out-Null
Write-Host "  Python deps + Playwright installed" -ForegroundColor Green

# 7. Install Node.js dependencies
Write-Host "[7/7] Installing Node.js dependencies..." -ForegroundColor Yellow
npm ci 2>&1 | Out-Null
Write-Host "  Node.js deps installed" -ForegroundColor Green

# Copy .env from example if not exists
if (-not (Test-Path "$ProjectDir\.env")) {
    Copy-Item "$ProjectDir\.env.example" "$ProjectDir\.env"
    Write-Host "  Created .env from .env.example ? edit it with your API keys!" -ForegroundColor Yellow
}

Write-Host "`n=== Setup complete! ===" -ForegroundColor Cyan

if (-not $NoAppStart) {
    $env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [Environment]::GetEnvironmentVariable("Path", "User")
    $cfDir = (Get-Command cloudflared).Source | Split-Path

    Write-Host "Starting chaturbate-dvr in new terminal..." -ForegroundColor Green
    $dvrCmd = "'$ProjectDir\chaturbate-dvr.exe'"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$ProjectDir'; $dvrCmd" -WorkingDirectory $ProjectDir

    Start-Sleep -Seconds 3

    Write-Host "Starting Cloudflare tunnel in new terminal..." -ForegroundColor Green
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "& '$cloudflaredDir\cloudflared.exe' tunnel --url http://localhost:8080 --protocol http2" -WorkingDirectory $ProjectDir

    Write-Host "`nDVR: http://localhost:8080" -ForegroundColor Cyan
    Write-Host "Tunnel URL will appear in the tunnel terminal once connected." -ForegroundColor Cyan
} else {
    Write-Host "Run '.\chaturbate-dvr.exe' to start the app" -ForegroundColor Yellow
    Write-Host "Run 'cloudflared tunnel --url http://localhost:8080' for a tunnel" -ForegroundColor Yellow
}
