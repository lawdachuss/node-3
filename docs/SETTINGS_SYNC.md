# Settings Synchronization with Supabase

## Overview

GoOnDVR now stores all application settings in Supabase, allowing GitHub Actions and EC2 deployments to fetch configuration from the cloud instead of relying on local files.

## Architecture

```
┌─────────────────┐
│  settings.json  │ (Local backup)
│   (Git ignored) │
└────────┬────────┘
         │
         │ Push/Pull
         ▼
┌─────────────────┐
│    Supabase     │ (Primary storage)
│  app_settings   │
│   table         │
└────────┬────────┘
         │
         │ Fetch on startup
         ▼
┌─────────────────┐
│ GitHub Actions  │
│   EC2 Instance  │
│   Local Dev     │
└─────────────────┘
```

## Benefits

✅ **No secrets in Git** - Settings are stored securely in Supabase  
✅ **Centralized config** - Update once, deploy everywhere  
✅ **Automatic sync** - GitHub Actions fetch latest settings on deploy  
✅ **Web UI updates** - Changes via dashboard are saved to Supabase  
✅ **Fallback support** - Local `settings.json` used if Supabase unavailable

## Settings Stored in Supabase

### Authentication
- `cookies` - Cloudflare bypass cookies
- `user_agent` - Browser user agent string

### Upload Configuration
- `enable_gofile_upload` - Enable multi-host uploads
- `turboviplay_api_key` - TurboViPlay API key
- `voesx_api_key` - VOE.sx API key
- `streamtape_login` - Streamtape login
- `streamtape_api_key` - Streamtape API key
- `imgbb_api_key` - ImgBB thumbnail hosting key

### FFmpeg Settings
- `finalize_mode` - Processing mode (none/remux/transcode)
- `ffmpeg_container` - Output container (mp4/mkv)
- `ffmpeg_encoder` - Video encoder (libx264/libx265/etc)
- `ffmpeg_quality` - Quality value (CRF)
- `ffmpeg_preset` - Encoding preset (fast/medium/slow)

### Disk Monitoring
- `disk_warning_percent` - Warning threshold (default: 96%)
- `disk_critical_percent` - Critical threshold (default: 98%)

### Notifications
- `discord_webhook_url` - Discord webhook for alerts
- `ntfy_url` - Ntfy server URL (optional)
- `ntfy_topic` - Ntfy topic (optional)
- `ntfy_token` - Ntfy auth token (optional)
- `notify_stream_online` - Send notifications when streams start
- `notify_cooldown_hours` - Hours between repeated alerts

### Cloudflare Protection
- `cf_channel_threshold` - CF blocks before per-channel alert
- `cf_global_threshold` - Channels blocked before global alert

### Supabase Credentials
- `supabase_url` - Supabase project URL
- `supabase_api_key` - Supabase API key (anon or service)

## Usage

### Push Settings to Supabase

```powershell
# Using PowerShell script (recommended)
.\scripts\sync-settings-to-supabase.ps1

# Or directly with Go
go run scripts/push_settings_to_supabase.go
```

### Verify Settings in Supabase

```powershell
go run scripts/verify_supabase_settings.go
```

### Update Settings

**Option 1: Via Web UI**
1. Open http://your-server:8080
2. Click "Settings" tab
3. Update values
4. Click "Save Settings"
5. Settings automatically saved to Supabase

**Option 2: Via Local File**
1. Edit `settings.json`
2. Run `.\scripts\sync-settings-to-supabase.ps1`
3. Settings pushed to Supabase

## How It Works

### Application Startup

1. **Check Supabase** - Try to load settings from Supabase
2. **Fallback to Local** - If Supabase unavailable, load `settings.json`
3. **Auto-migrate** - If loaded from local file, push to Supabase
4. **Initialize** - Apply settings to application

### Settings Priority

```
CLI flags > Environment variables > Supabase > settings.json > Defaults
```

### GitHub Actions Integration

GitHub Actions workflow automatically:
1. Fetches settings from Supabase on deployment
2. Uses environment variables for Supabase credentials
3. Falls back to local file if Supabase unavailable

## Database Schema

### Table: `app_settings`

```sql
CREATE TABLE app_settings (
  key TEXT PRIMARY KEY,
  value JSONB NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

### Row Format

```json
{
  "key": "global",
  "value": {
    "cookies": "...",
    "user_agent": "...",
    "enable_gofile_upload": true,
    ...
  },
  "updated_at": "2026-05-03T12:21:29Z"
}
```

## Security Considerations

⚠️ **Important Security Notes:**

1. **Supabase RLS** - Enable Row Level Security on `app_settings` table
2. **API Key** - Use service role key for write access, anon key for read
3. **Environment Variables** - Store Supabase credentials in GitHub Secrets
4. **Local Files** - Add `settings.json` to `.gitignore` (already done)
5. **Backup** - Keep encrypted backup of settings offline

## Troubleshooting

### Settings Not Syncing

```powershell
# Check Supabase connection
go run scripts/verify_supabase_settings.go

# Re-push settings
.\scripts\sync-settings-to-supabase.ps1
```

### GitHub Actions Can't Fetch Settings

1. Check GitHub Secrets contain:
   - `SUPABASE_URL`
   - `SUPABASE_API_KEY`
2. Verify Supabase API key has read access
3. Check workflow logs for error messages

### Local App Not Loading Settings

1. Check `settings.json` exists as fallback
2. Verify Supabase credentials in settings
3. Check network connectivity to Supabase

## Migration from Local Files

If you're migrating from local-only settings:

1. **Backup** - Copy `settings.json` to safe location
2. **Push** - Run `.\scripts\sync-settings-to-supabase.ps1`
3. **Verify** - Run `go run scripts/verify_supabase_settings.go`
4. **Test** - Restart app and confirm settings loaded
5. **Deploy** - Push to GitHub, Actions will use Supabase

## Best Practices

✅ **Do:**
- Keep `settings.json` as local backup
- Use Web UI for quick updates
- Verify settings after major changes
- Monitor Supabase usage/limits

❌ **Don't:**
- Commit `settings.json` to Git
- Share Supabase credentials publicly
- Hardcode secrets in code
- Disable local file fallback

## Related Documentation

- [Supabase Setup](SUPABASE_SETUP.md)
- [Configuration Management](CONFIGURATION_MANAGEMENT.md)
- [GitHub Actions Setup](CI_CD_SETUP.md)
