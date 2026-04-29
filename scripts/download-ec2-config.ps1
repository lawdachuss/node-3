# Download Configuration from EC2
# This script downloads your EC2 configuration files to local backup

param(
    [string]$EC2Host = "54.210.37.19",
    [string]$KeyPath = "aws-secrets/aws-key.pem",
    [string]$AppDir = "/home/ubuntu/goondvr"
)

$ErrorActionPreference = "Stop"

Write-Host "📥 Downloading Configuration from EC2" -ForegroundColor Cyan
Write-Host "======================================" -ForegroundColor Cyan
Write-Host ""

# Check if key file exists
if (-not (Test-Path $KeyPath)) {
    Write-Host "❌ SSH key not found at: $KeyPath" -ForegroundColor Red
    exit 1
}

# Create backup directory
$backupDir = "conf/ec2-backup-$(Get-Date -Format 'yyyy-MM-dd-HHmmss')"
New-Item -ItemType Directory -Path $backupDir -Force | Out-Null

Write-Host "📁 Backup directory: $backupDir" -ForegroundColor Green
Write-Host ""

# Download settings.json
Write-Host "⬇️  Downloading settings.json..." -ForegroundColor Yellow
scp -i $KeyPath ubuntu@${EC2Host}:$AppDir/conf/settings.json "$backupDir/settings.json"
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ settings.json downloaded" -ForegroundColor Green
} else {
    Write-Host "❌ Failed to download settings.json" -ForegroundColor Red
}

# Download channels.json
Write-Host "⬇️  Downloading channels.json..." -ForegroundColor Yellow
scp -i $KeyPath ubuntu@${EC2Host}:$AppDir/conf/channels.json "$backupDir/channels.json"
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ channels.json downloaded" -ForegroundColor Green
    
    # Count channels
    $channelsContent = Get-Content "$backupDir/channels.json" | ConvertFrom-Json
    $totalChannels = $channelsContent.Count
    $activeChannels = ($channelsContent | Where-Object { -not $_.is_paused }).Count
    $pausedChannels = ($channelsContent | Where-Object { $_.is_paused }).Count
    
    Write-Host ""
    Write-Host "📊 Channel Statistics:" -ForegroundColor Cyan
    Write-Host "  Total Channels: $totalChannels" -ForegroundColor White
    Write-Host "  Active: $activeChannels" -ForegroundColor Green
    Write-Host "  Paused: $pausedChannels" -ForegroundColor Yellow
} else {
    Write-Host "❌ Failed to download channels.json" -ForegroundColor Red
}

Write-Host ""
Write-Host "✅ Configuration downloaded to: $backupDir" -ForegroundColor Green
Write-Host ""
Write-Host "📝 Next steps:" -ForegroundColor Cyan
Write-Host "  1. Review the downloaded files"
Write-Host "  2. Edit channels.json to reduce active channels"
Write-Host "  3. Copy to conf/ directory"
Write-Host "  4. Upload with: .\scripts\upload-config.ps1"
Write-Host ""
