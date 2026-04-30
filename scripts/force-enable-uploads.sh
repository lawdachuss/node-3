#!/bin/bash

# Force enable uploads and verify it's working
# Run this on EC2 to ensure uploads are enabled

set -e

echo "🔧 Force Enabling Multi-Host Uploads"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

cd /home/ubuntu/goondvr || {
    echo "❌ Cannot find /home/ubuntu/goondvr directory!"
    exit 1
}

# Step 1: Stop container
echo "Step 1: Stopping container..."
docker stop goondvr || true
sleep 2

# Step 2: Backup and update settings
echo ""
echo "Step 2: Updating settings.json..."

# Create backup
if [ -f "conf/settings.json" ]; then
    cp conf/settings.json "conf/settings.json.backup.$(date +%Y%m%d_%H%M%S)"
    echo "✅ Backup created"
fi

# Ensure conf directory exists
mkdir -p conf

# Write settings with uploads enabled
cat > conf/settings.json <<'EOF'
{
  "cookies": "jZcKVhbRNIWIisSY0xRsgC_2J1RUc6BnEl.V7CdohIw-1777442158-1.2.1.1-tLXuiJSDY6mfuZ1_rP_i_OCKuYoK4IPTjESVDag73.qdehisLULg2WXXp_ui_GRv4YXjBsDU9Gl3I.AAX79Ka1R0W2hQfY7XIBNn_dnDNf_PbK6jJs2n5ixR5EycKo6BaEODQI30i0oFJY6YAhNb6dDN9tsT__AyMQsrNCpFqumvYNDACYrGadOfyi4T4YTqkWkMyscwEtkNTLt6QHAW5XZNIr7PdV0X9FN8TACzG2udofsiFJeadZNO7r2W24ot6cpQRRXNNWhZsfqsE8bNO6NR0i1Ulgcy_qsKPd72xmC0407ip0OS4Nbum9FS_bQWu3EMl_6106mQb9WSalICMw",
  "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Brave/1 Mobile/15E148 Safari/E7FBAF",
  "enable_gofile_upload": true,
  "enable_supabase": true,
  "turboviplay_api_key": "xizpCCPcnb",
  "voesx_api_key": "AF1YD2ExCqHrCSUjnwp9fPabywVRU1hwgFf8aKBzkx9gAV7S9Phdn9cS7ZAuWcN1",
  "streamtape_login": "ad687ba4675c26af3bd4",
  "streamtape_api_key": "WgMD3kVBWMsb66q",
  "supabase_url": "https://iktbuxgnnuebuoqaywev.supabase.co",
  "supabase_api_key": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlrdGJ1eGdubnVlYnVvcWF5d2V2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzY3NTcwMjcsImV4cCI6MjA5MjMzMzAyN30.Tl5VJdAnUSVzcbMd4k5IMqQltcJjvVUMR5fHoNO-BVw",
  "discord_webhook_url": "https://discord.com/api/webhooks/1497660499670863966/GjrVGaCdXCBgvYrnSM-pIepKHqSIA_HgyiIb7NM8rn4i9L5xHM7QlZJpqD60PAtQiOa-",
  "finalize_mode": "remux",
  "ffmpeg_container": "mp4",
  "ffmpeg_encoder": "libx264",
  "ffmpeg_quality": 18,
  "ffmpeg_preset": "slow",
  "disk_warning_percent": 80,
  "disk_critical_percent": 90
}
EOF

echo "✅ Settings updated with uploads enabled"

# Step 3: Verify file content
echo ""
echo "Step 3: Verifying settings.json..."
if grep -q '"enable_gofile_upload": true' conf/settings.json; then
    echo "✅ enable_gofile_upload is true"
else
    echo "❌ ERROR: enable_gofile_upload is not true!"
    cat conf/settings.json
    exit 1
fi

# Step 4: Check file permissions
echo ""
echo "Step 4: Checking file permissions..."
ls -la conf/settings.json
chmod 644 conf/settings.json
echo "✅ Permissions set to 644"

# Step 5: Ensure database directory exists
echo ""
echo "Step 5: Ensuring database directory exists..."
mkdir -p database
chmod 755 database
echo "✅ Database directory ready"

# Step 6: Start container
echo ""
echo "Step 6: Starting container..."
docker start goondvr || docker compose up -d

echo "⏳ Waiting for container to start..."
sleep 10

# Step 7: Verify container is running
echo ""
echo "Step 7: Verifying container status..."
if docker ps | grep -q goondvr; then
    echo "✅ Container is running"
else
    echo "❌ Container failed to start!"
    docker logs goondvr
    exit 1
fi

# Step 8: Check if settings are loaded
echo ""
echo "Step 8: Checking application logs for settings..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
docker logs --tail 50 goondvr
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Step 9: Test inside container
echo ""
echo "Step 9: Verifying settings inside container..."
docker exec goondvr cat /usr/src/app/conf/settings.json | grep enable_gofile_upload || {
    echo "⚠️  Settings file not found inside container!"
    echo "Checking container mounts..."
    docker inspect goondvr | grep -A 10 "Mounts"
}

# Step 10: Check for any existing videos
echo ""
echo "Step 10: Checking for existing videos..."
VIDEO_COUNT=$(find videos -type f \( -name "*.mp4" -o -name "*.ts" -o -name "*.mkv" \) 2>/dev/null | wc -l)
echo "📊 Video files found: $VIDEO_COUNT"

if [ $VIDEO_COUNT -gt 0 ]; then
    echo ""
    echo "⚠️  Found $VIDEO_COUNT existing video files"
    echo "These were recorded before uploads were enabled"
    echo ""
    echo "Options:"
    echo "1. Wait for new recordings (recommended)"
    echo "2. Manually upload existing files using web UI 'Upload Completed' button"
    echo "3. Delete old files: rm videos/*.mp4 videos/*.ts"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ SETUP COMPLETE!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📋 What happens next:"
echo "1. When a stream goes live, it will be recorded"
echo "2. When recording ends (stream offline or pause):"
echo "   → Video is converted to MP4"
echo "   → Uploaded to GoFile, TurboViPlay, VOE.sx, Streamtape"
echo "   → Thumbnail generated and uploaded"
echo "   → Saved to database"
echo "   → Local file DELETED to free space"
echo ""
echo "🔍 To monitor:"
echo "   docker logs -f goondvr"
echo ""
echo "🌐 Web UI:"
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null || echo "localhost")
echo "   http://${PUBLIC_IP}:8080"
echo ""
echo "📹 To test:"
echo "1. Wait for a channel to go live"
echo "2. Let it record for a few minutes"
echo "3. Pause the channel or wait for stream to end"
echo "4. Watch logs: docker logs -f goondvr"
echo "5. You should see upload messages"
echo "6. Check 'Videos' in web UI"
echo ""
