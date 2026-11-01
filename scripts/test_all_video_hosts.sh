#!/bin/bash

# Test all video upload hosts
echo "=== Testing All Video Upload Hosts ==="
echo "====================================="

# Load environment variables
if [ -f .env ]; then
    eval "$(grep -v '^#' .env | grep -v '^$' | sed 's/=/="/' | sed 's/$/"/' | sed 's/^/export /')"
fi

# Create a small test file
TEST_FILE="/tmp/test_video_host.mp4"
if [ ! -f "$TEST_FILE" ]; then
    # Create a tiny valid MP4 file (just header)
    printf '\x00\x00\x00\x1c\x66\x74\x79\x70\x69\x73\x6f\x6d\x00\x00\x02\x00' > "$TEST_FILE"
    printf '\x69\x73\x6f\x6d\x61\x76\x63\x31\x6d\x70\x34\x32' >> "$TEST_FILE"
fi

echo "Test file: $TEST_FILE ($(wc -c < "$TEST_FILE") bytes)"
echo ""

# Test each host
test_host() {
    local name="$1"
    local test_cmd="$2"
    local expected_pattern="$3"
    
    echo "Testing $name..."
    eval "$test_cmd" 2>&1 | tee /tmp/test_${name}.txt
    
    if grep -q "$expected_pattern" /tmp/test_${name}.txt 2>/dev/null; then
        echo "✅ $name: WORKING"
    else
        echo "❌ $name: FAILED"
    fi
    echo ""
}

# 1. GoFile (no API key needed)
test_host "GoFile" \
    "curl -s -X POST 'https://store1.gofile.io/uploadFile' -F 'file=@$TEST_FILE'" \
    "downloadPage"

# 2. Streamtape
if [ -n "$STREAMTAPE_LOGIN" ] && [ -n "$STREAMTAPE_API_KEY" ]; then
    test_host "Streamtape" \
        "curl -s 'https://streamtape.com/api/upload/server?login=$STREAMTAPE_LOGIN&key=$STREAMTAPE_API_KEY'" \
        "result"
else
    echo "Streamtape: ⚠️ Missing credentials (STREAMTAPE_LOGIN, STREAMTAPE_API_KEY)"
fi

# 3. Vidara
if [ -n "$VIDARA_KEY" ]; then
    test_host "Vidara" \
        "curl -s -X POST 'https://api.vidara.so/api/v1/upload/server' -H 'Authorization: Bearer $VIDARA_KEY'" \
        "url"
else
    echo "Vidara: ⚠️ Missing credentials (VIDARA_KEY)"
fi

# 4. Mixdrop
if [ -n "$MIXDROP_EMAIL" ] && [ -n "$MIXDROP_TOKEN" ]; then
    test_host "Mixdrop" \
        "curl -s 'https://mixdrop.co/api/upload/server?email=$MIXDROP_EMAIL&token=$MIXDROP_TOKEN'" \
        "result"
else
    echo "Mixdrop: ⚠️ Missing credentials (MIXDROP_EMAIL, MIXDROP_TOKEN)"
fi

# 5. VOE.sx
if [ -n "$VOE_KEY" ]; then
    test_host "VOE" \
        "curl -s 'https://voe.sx/api/upload/server?key=$VOE_KEY'" \
        "url"
else
    echo "VOE: ⚠️ Missing credentials (VOE_KEY)"
fi

# 6. AnonMP4 (no API key needed)
test_host "AnonMP4" \
    "curl -s -X POST 'https://anonmp4api.xyz/upload' -F 'file=@$TEST_FILE'" \
    "embed_url"

echo ""
echo "=== Summary ==="
echo "Check individual results above for detailed status."