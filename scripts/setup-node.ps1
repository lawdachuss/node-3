<#
.SYNOPSIS
    Sets up a new node repository for distributed shards mode.
.DESCRIPTION
    Creates a new GitHub repo (MiniDelectableService-<node-id>), configures secrets,
    and runs the initial migration to prepare it for pooled recording.
.PARAMETER NodeId
    The node identifier (e.g., "node-a", "node-b").
.PARAMETER SupabaseUrl
    The shared Supabase project URL.
.PARAMETER SupabaseKey
    The shared Supabase anon/service key.
.PARAMETER GithubToken
    GitHub personal access token with repo + secrets permissions.
.EXAMPLE
    .\scripts\setup-node.ps1 -NodeId "node-d" -SupabaseUrl "https://xyz.supabase.co" -SupabaseKey "eyJ..." -GithubToken "ghp_..."
#>

param(
    [Parameter(Mandatory = $true)]
    [string]$NodeId,

    [Parameter(Mandatory = $true)]
    [string]$SupabaseUrl,

    [Parameter(Mandatory = $true)]
    [string]$SupabaseKey,

    [Parameter(Mandatory = $true)]
    [string]$GithubToken
)

$ErrorActionPreference = "Stop"

# ── Validate NodeId format ──────────────────────────────────────────────
if ($NodeId -notmatch '^[a-zA-Z0-9_-]+$') {
    Write-Error "NodeId must be alphanumeric (dashes/underscores allowed)"
    exit 1
}

$RepoName = "MiniDelectableService-$NodeId"
$Org = "YOUR_ORG"  # Change to your GitHub org/username

Write-Host "=== Setting up node: $NodeId ===" -ForegroundColor Cyan
Write-Host "Repo: $Org/$RepoName" -ForegroundColor Cyan

# ── Step 1: Create repo via GitHub API ──────────────────────────────────
Write-Host "[1/5] Creating GitHub repository..." -ForegroundColor Yellow
$body = @{
    name        = $RepoName
    description = "MiniDelectableService node: $NodeId"
    auto_init   = $false
    @{private}  = $true
} | ConvertTo-Json

try {
    $null = Invoke-RestMethod -Uri "https://api.github.com/repos/$Org/$RepoName" -Method GET -Headers @{
        Authorization = "Bearer $GithubToken"
    } -ErrorAction Stop
    Write-Host "  Repo already exists, skipping creation." -ForegroundColor Green
} catch {
    $null = Invoke-RestMethod -Uri "https://api.github.com/orgs/$Org/repos" -Method POST -Headers @{
        Authorization = "Bearer $GithubToken"
        Accept        = "application/vnd.github+json"
    } -Body $body -ContentType "application/json"
    Write-Host "  Repo created: $RepoName" -ForegroundColor Green
}

# ── Step 2: Set GitHub Secrets ──────────────────────────────────────────
Write-Host "[2/5] Setting GitHub Secrets..." -ForegroundColor Yellow
$secrets = @{
    SUPABASE_URL          = $SupabaseUrl
    SUPABASE_API_KEY      = $SupabaseKey
    CHANNEL_POOL_MODE     = "pooled"
    NODE_ID               = $NodeId
}

foreach ($key in $secrets.Keys) {
    $secretBody = @{ encrypted_value = $secrets[$key] } | ConvertTo-Json
    $null = Invoke-RestMethod -Uri "https://api.github.com/repos/$Org/$RepoName/actions/secrets/$key" `
        -Method PUT `
        -Headers @{
            Authorization = "Bearer $GithubToken"
            Accept        = "application/vnd.github+json"
        } `
        -Body $secretBody `
        -ContentType "application/json"
    Write-Host "  Secret $key set." -ForegroundColor Green
}

# ── Step 3: Run Supabase migration ─────────────────────────────────────
Write-Host "[3/5] Running Supabase migration (migrate-v2.sql)..." -ForegroundColor Yellow
Write-Host "  Open your Supabase SQL editor and run the contents of database/migrate-v2.sql"
Write-Host "  URL: $SupabaseUrl" -ForegroundColor Gray

# ── Step 4: Link template repo ─────────────────────────────────────────
Write-Host "[4/5] Configuring sync..." -ForegroundColor Yellow
Write-Host "  Push to main branch of $Org/MiniDelectableService to auto-sync to $RepoName"
Write-Host "  The .github/workflows/sync-nodes.yml workflow handles this." -ForegroundColor Gray

# ── Step 5: Verify ─────────────────────────────────────────────────────
Write-Host "[5/5] Verification..." -ForegroundColor Yellow
Write-Host "  Repo:      https://github.com/$Org/$RepoName" -ForegroundColor Green
Write-Host "  Node ID:   $NodeId" -ForegroundColor Green
Write-Host "  Mode:      pooled" -ForegroundColor Green
Write-Host "" -ForegroundColor Cyan
Write-Host "=== Setup complete for $NodeId ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Trigger a manual sync: gh workflow run sync-nodes.yml"
Write-Host "  2. Run the secure-rdp workflow on $RepoName"
Write-Host "  3. Verify the node appears in the admin web UI at /nodes"
