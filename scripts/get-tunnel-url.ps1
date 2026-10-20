# Get the current tunnel URL from Supabase
# Usage: .\scripts\get-tunnel-url.ps1

$ErrorActionPreference = "Stop"

# Load environment variables from .env if it exists
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

$SUPABASE_URL = $env:SUPABASE_URL
$SUPABASE_API_KEY = $env:SUPABASE_API_KEY

if (-not $SUPABASE_URL -or -not $SUPABASE_API_KEY) {
    Write-Host "❌ Error: SUPABASE_URL and SUPABASE_API_KEY must be set" -ForegroundColor Red
    Write-Host "Set them in .env file or as environment variables" -ForegroundColor Yellow
    exit 1
}

Write-Host "🔍 Fetching latest tunnel URL from Supabase..." -ForegroundColor Cyan

try {
    # The operative tunnel registry is nodes.web_url — keep-alive.ps1 upserts
    # it every ~5 minutes per node. (The legacy sessions table 404s and
    # `tunnels` has no live writer; both were retired in plan D2.)
    $response = Invoke-RestMethod -Uri "$SUPABASE_URL/rest/v1/nodes?select=node_id,web_url,updated_at&web_url=not.is.null&order=updated_at.desc&limit=1" `
        -Method Get `
        -Headers @{
            "apikey" = $SUPABASE_API_KEY
            "Content-Type" = "application/json"
        }

    if ($response -and $response.Count -gt 0) {
        $tunnel = $response[0]
        Write-Host ""
        Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
        Write-Host "║                    🌐 WEB UI ACCESS                        ║" -ForegroundColor Green
        Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green
        Write-Host ""
        Write-Host "   $($tunnel.web_url)" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "   Node: $($tunnel.node_id)" -ForegroundColor Gray
        Write-Host "   Updated: $($tunnel.updated_at)" -ForegroundColor Gray
        Write-Host ""
        
        # Copy to clipboard if possible
        try {
            Set-Clipboard -Value $tunnel.web_url
            Write-Host "✅ URL copied to clipboard!" -ForegroundColor Green
        } catch {
            Write-Host "ℹ️  Copy the URL above to access the UI" -ForegroundColor Yellow
        }
    } else {
        Write-Host "❌ No node tunnel URLs found in Supabase" -ForegroundColor Red
        Write-Host "Make sure the GitHub Actions workflow has run at least once" -ForegroundColor Yellow
    }
} catch {
    Write-Host "❌ Error fetching tunnel URL: $_" -ForegroundColor Red
    Write-Host "Check your Supabase credentials and ensure the nodes table exists" -ForegroundColor Yellow
    exit 1
}
