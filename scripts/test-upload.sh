#!/bin/bash

# Test upload functionality with a sample video
# This helps verify that uploads are working before waiting for a real recording

echo "🧪 Testing Upload Functionality"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

cd /home/ubuntu/goondvr || {
    echo "❌ Cannot find /home/ubuntu/goondvr directory!"
    exit 1
}

# Check if there are any video files
VIDEO_FILES=$(find videos -type f \( -name "*.mp4" -o -name "*.ts" -o -name "*.mkv" \) 2>/dev/null | head -1)

if [ -z "$VIDEO_FILES" ]; then
    echo "⚠️  No video files found to test with"
    echo ""
    echo "Options:"
    echo "1. Wait for a channel to record something"
    echo "2. Create a test video:"
    echo "   ffmpeg -f lavfi -i testsrc=duration=10:size=1280x720:rate=30 -pix_fmt yuv420p test_video.mp4"
    echo "   mv test_video.mp4 videos/"
    exit 1
fi

echo "📹 Found video file: $VIDEO_FILES"
echo ""

# Get file info
FILE_SIZE=$(du -h "$VIDEO_FILES" | cut -f1)
echo "📊 File size: $FILE_SIZE"
echo ""

# Check settings
echo "🔍 Checking settings..."
if ! grep -q '"enable_gofile_upload": true' conf/settings.json; then
    echo "❌ GoFile upload is not enabled!"
    echo "Run: bash scripts/force-enable-uploads.sh"
    exit 1
fi
echo "✅ GoFile upload is enabled"
echo ""

# Check API keys
echo "🔑 Checking API keys..."
HAS_TURBO=$(grep -o '"turboviplay_api_key": "[^"]*"' conf/settings.json | grep -v '""' | wc -l)
HAS_VOE=$(grep -o '"voesx_api_key": "[^"]*"' conf/settings.json | grep -v '""' | wc -l)
HAS_STREAM=$(grep -o '"streamtape_api_key": "[^"]*"' conf/settings.json | grep -v '""' | wc -l)

echo "  GoFile: ✅ (no API key needed)"
[ $HAS_TURBO -gt 0 ] && echo "  TurboViPlay: ✅" || echo "  TurboViPlay: ⚠️  (no API key)"
[ $HAS_VOE -gt 0 ] && echo "  VOE.sx: ✅" || echo "  VOE.sx: ⚠️  (no API key)"
[ $HAS_STREAM -gt 0 ] && echo "  Streamtape: ✅" || echo "  Streamtape: ⚠️  (no API key)"
echo ""

# Trigger upload via web UI API
echo "📤 Triggering upload via API..."
echo ""

RESPONSE=$(curl -s -X POST http://localhost:8080/api/upload/completed)
echo "Response: $RESPONSE"
echo ""

if echo "$RESPONSE" | grep -q "not enabled"; then
    echo "❌ Upload is not enabled in the running application!"
    echo ""
    echo "This means the container hasn't loaded the new settings."
    echo "Solution: Restart container"
    echo "  docker restart goondvr"
    exit 1
fi

if echo "$RESPONSE" | grep -q "started"; then
    echo "✅ Upload started!"
    echo ""
    echo "📋 Monitoring upload progress..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Follow logs for 60 seconds
    timeout 60 docker logs -f goondvr 2>&1 | grep -i "upload\|gofile\|turbo\|voe\|stream" || true
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "🔍 Checking results..."
    
    # Check if database was updated
    if [ -d "database" ]; then
        DB_COUNT=$(find database -name "recordings.json" 2>/dev/null | wc -l)
        echo "📊 Database records: $DB_COUNT"
        
        if [ $DB_COUNT -gt 0 ]; then
            echo "✅ Database updated!"
            echo ""
            echo "Latest record:"
            find database -name "recordings.json" -exec tail -20 {} \; | head -20
        fi
    fi
    
    # Check if video was deleted
    if [ ! -f "$VIDEO_FILES" ]; then
        echo "✅ Local video file was deleted (upload successful!)"
    else
        echo "⚠️  Local video file still exists"
        echo "This might mean:"
        echo "  - Upload is still in progress"
        echo "  - Upload failed"
        echo "  - Check logs: docker logs goondvr | grep -i error"
    fi
else
    echo "⚠️  Unexpected response from API"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 FINAL STATUS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Check videos in UI
echo "🌐 Checking videos via API..."
VIDEOS=$(curl -s http://localhost:8080/api/videos)
VIDEO_COUNT=$(echo "$VIDEOS" | grep -o '"count":[0-9]*' | grep -o '[0-9]*')

if [ -n "$VIDEO_COUNT" ] && [ "$VIDEO_COUNT" -gt 0 ]; then
    echo "✅ Found $VIDEO_COUNT video(s) in database!"
    echo ""
    echo "Sample:"
    echo "$VIDEOS" | head -50
else
    echo "⚠️  No videos found in database yet"
    echo "Response: $VIDEOS"
fi

echo ""
echo "✅ Test complete!"
echo ""
echo "Next steps:"
echo "1. Open web UI and click 'Videos' button"
echo "2. You should see uploaded videos"
echo "3. If not, check: docker logs goondvr | grep -i error"
