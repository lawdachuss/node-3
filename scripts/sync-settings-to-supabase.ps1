#!/usr/bin/env pwsh
# Sync settings.json to Supabase
# This script pushes local settings to Supabase for GitHub Actions to use

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "         SYNC SETTINGS TO SUPABASE                        " -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Check if settings.json exists
if (-not (Test-Path "settings.json")) {
    Write-Host "❌ Error: settings.json not found!" -ForegroundColor Red
    Write-Host "   Please ensure you're running this from the project root." -ForegroundColor Yellow
    exit 1
}

Write-Host "📄 Found settings.json" -ForegroundColor Green
Write-Host ""

# Run the Go script to push settings
Write-Host "🚀 Pushing settings to Supabase..." -ForegroundColor Yellow
Write-Host ""

$result = go run scripts/push_settings_to_supabase.go

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Green
    Write-Host "                    ✅ SUCCESS!                            " -ForegroundColor Green
    Write-Host "═══════════════════════════════════════════════════════════" -ForegroundColor Green
    Write-Host ""
    Write-Host "Settings have been synced to Supabase!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Cyan
    Write-Host "  1. GitHub Actions will now fetch settings from Supabase" -ForegroundColor White
    Write-Host "  2. No need to commit settings.json to Git anymore" -ForegroundColor White
    Write-Host "  3. Update settings via Web UI or run this script again" -ForegroundColor White
    Write-Host ""
    Write-Host "To verify settings in Supabase, run:" -ForegroundColor Yellow
    Write-Host "  go run scripts/verify_supabase_settings.go" -ForegroundColor White
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "❌ Failed to sync settings to Supabase!" -ForegroundColor Red
    Write-Host "   Check the error messages above for details." -ForegroundColor Yellow
    Write-Host ""
    exit 1
}
