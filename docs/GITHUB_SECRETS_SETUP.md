# GitHub Secrets Setup for Supabase Integration

## Overview

With Supabase integration, you only need **2 secrets** in GitHub instead of 10+. All settings are stored in Supabase and fetched automatically on deployment.

## Required GitHub Secrets

### 1. SUPABASE_URL
Your Supabase project URL

**Example:** `https://xhfbhgklqylmfmfjtgkq.supabase.co`

**Where to find it:**
1. Go to https://supabase.com/dashboard
2. Select your project
3. Go to Settings → API
4. Copy "Project URL"

### 2. SUPABASE_API_KEY
Your Supabase API key (anon or service role)

**Example:** `eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...`

**Where to find it:**
1. Go to https://supabase.com/dashboard
2. Select your project
3. Go to Settings → API
4. Copy "anon public" key (for read/write) or "service_role" key (for admin access)

**Recommendation:** Use `service_role` key for GitHub Actions to ensure full access.

## How to Add Secrets to GitHub

### Step 1: Go to Repository Settings
```
https://github.com/YOUR_USERNAME/YOUR_REPO/settings/secrets/actions
```

### Step 2: Click "New repository secret"

### Step 3: Add SUPABASE_URL
- **Name:** `SUPABASE_URL`
- **Value:** Your Supabase project URL
- Click "Add secret"

### Step 4: Add SUPABASE_API_KEY
- **Name:** `SUPABASE_API_KEY`
- **Value:** Your Supabase API key
- Click "Add secret"

## Verification

After adding secrets, verify they're set correctly:

1. Go to your repository
2. Click "Actions" tab
3. Click "Continuous Recording (24/7) with Web UI"
4. Click "Run workflow" → "Run workflow"
5. Check the "Verify Supabase credentials" step in the logs

You should see:
```
[SUCCESS] Supabase credentials found
[INFO] Settings and channels will be loaded from Supabase on startup
```

## What Gets Loaded from Supabase

When GitHub Actions starts, the app automatically fetches:

### Settings (from `app_settings` table)
- ✅ Cookies & User-Agent (Cloudflare bypass)
- ✅ Upload API keys (TurboViPlay, VOE.sx, Streamtape, ImgBB)
- ✅ FFmpeg settings (quality, preset, container)
- ✅ Disk monitoring thresholds
- ✅ Discord webhook URL
- ✅ Notification settings

### Channels (from `channels` table)
- ✅ All monitored channels
- ✅ Per-channel settings (resolution, framerate, etc)
- ✅ Pause/resume states
- ✅ Last stream timestamps

### Upload Records (to `video_uploads` table)
- ✅ Completed uploads are logged to Supabase
- ✅ Accessible via Web UI or Supabase dashboard

## Architecture Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    GitHub Actions Workflow                   │
│                                                              │
│  1. Checkout code                                           │
│  2. Verify SUPABASE_URL & SUPABASE_API_KEY secrets         │
│  3. Build Docker image                                      │
│  4. Start container with Supabase env vars                 │
│                                                              │
│     docker run -e SUPABASE_URL=${{ secrets.SUPABASE_URL }}  │
│                -e SUPABASE_API_KEY=${{ secrets... }}        │
│                                                              │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    GoOnDVR Application                       │
│                                                              │
│  On Startup:                                                │
│  1. Read SUPABASE_URL & SUPABASE_API_KEY from env          │
│  2. Initialize Supabase client                             │
│  3. Fetch settings from app_settings table                 │
│  4. Fetch channels from channels table                     │
│  5. Start monitoring all active channels                   │
│                                                              │
│  During Runtime:                                            │
│  - Save settings changes to Supabase                       │
│  - Save channel updates to Supabase                        │
│  - Log uploads to video_uploads table                      │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Benefits

### Before (Old Method)
```yaml
secrets:
  COOKIES: "..."
  USER_AGENT: "..."
  TURBOVIPLAY_API_KEY: "..."
  VOESX_API_KEY: "..."
  STREAMTAPE_LOGIN: "..."
  STREAMTAPE_API_KEY: "..."
  IMGBB_API_KEY: "..."
  DISCORD_WEBHOOK_URL: "..."
  SUPABASE_URL: "..."
  SUPABASE_API_KEY: "..."
  # 10+ secrets to manage! 😰
```

### After (New Method)
```yaml
secrets:
  SUPABASE_URL: "..."
  SUPABASE_API_KEY: "..."
  # Only 2 secrets! 🎉
```

### Advantages
✅ **Fewer secrets** - Only 2 instead of 10+  
✅ **Centralized config** - Update settings once in Supabase  
✅ **No code changes** - Update settings via Web UI  
✅ **Automatic sync** - Changes propagate to all deployments  
✅ **Secure** - Secrets never committed to Git  
✅ **Audit trail** - Supabase tracks all changes  

## Troubleshooting

### Error: "Supabase credentials not configured"

**Cause:** GitHub Secrets not set or named incorrectly

**Solution:**
1. Check secret names are exactly: `SUPABASE_URL` and `SUPABASE_API_KEY`
2. Verify secrets are set in repository settings
3. Re-run the workflow

### Error: "Failed to connect to Supabase"

**Cause:** Invalid Supabase URL or API key

**Solution:**
1. Verify URL format: `https://YOUR_PROJECT.supabase.co`
2. Verify API key is valid (copy from Supabase dashboard)
3. Test locally: `go run scripts/verify_supabase_settings.go`

### Error: "No settings found in Supabase"

**Cause:** Settings not pushed to Supabase yet

**Solution:**
```powershell
# Push settings to Supabase
.\scripts\sync-settings-to-supabase.ps1

# Verify they're there
go run scripts/verify_supabase_settings.go
```

### Workflow runs but no channels monitored

**Cause:** Channels not synced to Supabase

**Solution:**
```powershell
# The app automatically syncs channels to Supabase
# Just start the app once locally and it will sync
go run . --port 8080
```

## Security Best Practices

### ✅ Do:
- Use `service_role` key for GitHub Actions (full access)
- Use `anon` key for public Web UI (limited access)
- Enable Row Level Security (RLS) on Supabase tables
- Rotate API keys periodically
- Monitor Supabase audit logs

### ❌ Don't:
- Commit secrets to Git
- Share API keys publicly
- Use same key for dev and prod
- Disable RLS on sensitive tables

## Related Documentation

- [Settings Sync](SETTINGS_SYNC.md) - How settings sync works
- [Supabase Setup](SUPABASE_SETUP.md) - Database schema and setup
- [CI/CD Setup](CI_CD_SETUP.md) - GitHub Actions configuration

## Quick Reference

### Add Secrets
```
https://github.com/YOUR_USERNAME/YOUR_REPO/settings/secrets/actions
```

### Test Locally
```powershell
# Set environment variables
$env:SUPABASE_URL = "https://your-project.supabase.co"
$env:SUPABASE_API_KEY = "your-api-key"

# Run app
go run . --port 8080
```

### Verify Settings
```powershell
go run scripts/verify_supabase_settings.go
```

### Push Settings
```powershell
.\scripts\sync-settings-to-supabase.ps1
```
