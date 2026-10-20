# Quick script to open the UI in browser
# Usage: .\scripts\open-ui.ps1

$ErrorActionPreference = "Stop"

Write-Host "🔍 Finding your GoOnDVR Web UI..." -ForegroundColor Cyan
Write-Host ""

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
    Write-Host "❌ Supabase credentials not found" -ForegroundColor Red
    Write-Host ""
    Write-Host "Set them in .env file or run:" -ForegroundColor Yellow
    Write-Host '  $env:SUPABASE_URL = "your-url"' -ForegroundColor Cyan
    Write-Host '  $env:SUPABASE_API_KEY = "your-key"' -ForegroundColor Cyan
    exit 1
}

Write-Host "📡 Fetching tunnel URLs from Supabase..." -ForegroundColor Yellow

try {
    # The operative tunnel registry is nodes.web_url — keep-alive.ps1 upserts
    # it every ~5 minutes per node. (The legacy sessions table 404s and
    # `tunnels` has no live writer; both were retired in plan D2.)
    # Normalized to the old session-record shape so the probing loop below
    # keeps working: url / run_id / started_at.
    $nodeRows = Invoke-RestMethod -Uri "$SUPABASE_URL/rest/v1/nodes?select=node_id,web_url,updated_at&web_url=not.is.null&order=updated_at.desc&limit=10" `
        -Method Get `
        -Headers @{
            "apikey" = $SUPABASE_API_KEY
            "Content-Type" = "application/json"
        } -ErrorAction Stop

    $response = @()
    foreach ($n in $nodeRows) {
        $response += [pscustomobject]@{
            url        = $n.web_url
            run_id     = $n.node_id
            started_at = $n.updated_at
        }
    }

    if (-not $response -or $response.Count -eq 0) {
        Write-Host "❌ No node tunnel URLs found" -ForegroundColor Red
        Write-Host ""
        Write-Host "Make sure the GitHub Actions workflow is running:" -ForegroundColor Yellow
        Write-Host "  https://github.com/YOUR_USERNAME/YOUR_REPO/actions" -ForegroundColor Cyan
        exit 1
    }
    
    Write-Host "✅ Found $($response.Count) node tunnel URL(s)" -ForegroundColor Green
    Write-Host ""
    Write-Host "🔍 Testing accessibility..." -ForegroundColor Yellow
    Write-Host ""
    
    $accessibleUrl = $null
    $testedCount = 0
    
    foreach ($tunnel in $response) {
        $testedCount++
        $url = $tunnel.url
        
        Write-Host "[$testedCount/$($response.Count)] Testing: $url" -ForegroundColor Gray
        
        try {
            # Test with timeout
            $testResponse = Invoke-WebRequest -Uri $url -Method Head -TimeoutSec 8 -UseBasicParsing -ErrorAction Stop
            $statusCode = $testResponse.StatusCode
            
            if ($statusCode -eq 200) {
                Write-Host "   ✅ ACCESSIBLE (HTTP $statusCode)" -ForegroundColor Green
                $accessibleUrl = $url
                $accessibleTunnel = $tunnel
                break
            } else {
                Write-Host "   ⚠️  HTTP $statusCode" -ForegroundColor Yellow
            }
            
        } catch {
            $statusCode = $_.Exception.Response.StatusCode.value__
            if ($statusCode) {
                if ($statusCode -eq 502 -or $statusCode -eq 503 -or $statusCode -eq 504) {
                    Write-Host "   ❌ Gateway error (HTTP $statusCode)" -ForegroundColor Red
                } else {
                    Write-Host "   ❌ HTTP $statusCode" -ForegroundColor Red
                }
            } else {
                Write-Host "   ❌ Not accessible (timeout/connection failed)" -ForegroundColor Red
            }
        }
    }
    
    Write-Host ""
    
    if ($accessibleUrl) {
        Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Green
        Write-Host "║              ✅ ACCESSIBLE TUNNEL FOUND                    ║" -ForegroundColor Green
        Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Green
        Write-Host ""
        Write-Host "   $accessibleUrl" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "   Run ID: $($accessibleTunnel.run_id)" -ForegroundColor Gray
        Write-Host "   Started: $($accessibleTunnel.started_at)" -ForegroundColor Gray
        Write-Host ""
        
        # Copy to clipboard
        try {
            Set-Clipboard -Value $accessibleUrl
            Write-Host "✅ URL copied to clipboard!" -ForegroundColor Green
        } catch {
            # Clipboard may not be available
        }
        
        Write-Host ""
        Write-Host "🌐 Opening in your default browser..." -ForegroundColor Cyan
        Start-Process $accessibleUrl
        
    } else {
        Write-Host "╔════════════════════════════════════════════════════════════╗" -ForegroundColor Red
        Write-Host "║           ❌ NO ACCESSIBLE TUNNELS FOUND                   ║" -ForegroundColor Red
        Write-Host "╚════════════════════════════════════════════════════════════╝" -ForegroundColor Red
        Write-Host ""
        Write-Host "All tunnel URLs in the database are currently inaccessible." -ForegroundColor Yellow
        Write-Host ""
        Write-Host "Possible reasons:" -ForegroundColor White
        Write-Host "  • The GitHub Actions workflow is not running" -ForegroundColor Gray
        Write-Host "  • The workflow hasn't reached the tunnel setup step yet" -ForegroundColor Gray
        Write-Host "  • The tunnel is still propagating (wait 1-2 minutes)" -ForegroundColor Gray
        Write-Host "  • The application crashed after tunnel creation" -ForegroundColor Gray
        Write-Host ""
        Write-Host "What to do:" -ForegroundColor White
        Write-Host "  1. Check if workflow is running:" -ForegroundColor Gray
        Write-Host "     https://github.com/YOUR_USERNAME/YOUR_REPO/actions" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "  2. If running, wait 2-3 minutes and try again:" -ForegroundColor Gray
        Write-Host "     .\scripts\open-ui.ps1" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "  3. Check the workflow logs for errors" -ForegroundColor Gray
        Write-Host ""
        
        # Show the most recent URL anyway
        $latestUrl = $response[0].url
        Write-Host "Most recent tunnel URL (may not be accessible):" -ForegroundColor Yellow
        Write-Host "  $latestUrl" -ForegroundColor Gray
        Write-Host ""
        
        exit 1
    }
    
} catch {
    Write-Host "❌ Error: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "Check your Supabase credentials and network connection" -ForegroundColor Yellow
    exit 1
}
