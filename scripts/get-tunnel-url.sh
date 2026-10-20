#!/bin/bash
# Get the current tunnel URL from Supabase
# Usage: ./scripts/get-tunnel-url.sh

set -e

# Load environment variables from .env if it exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

if [ -z "$SUPABASE_URL" ] || [ -z "$SUPABASE_API_KEY" ]; then
    echo "❌ Error: SUPABASE_URL and SUPABASE_API_KEY must be set"
    echo "Set them in .env file or as environment variables"
    exit 1
fi

echo "🔍 Fetching latest tunnel URL from Supabase..."

# The operative tunnel registry is nodes.web_url — keep-alive.ps1 upserts
# it every ~5 minutes per node. (The legacy sessions table 404s and
# `tunnels` has no live writer; both were retired in plan D2.)
RESPONSE=$(curl -s -X GET "$SUPABASE_URL/rest/v1/nodes?select=node_id,web_url,updated_at&web_url=not.is.null&order=updated_at.desc&limit=1" \
    -H "apikey: $SUPABASE_API_KEY" \
    -H "Content-Type: application/json")

TUNNEL_URL=$(echo "$RESPONSE" | grep -oP '"web_url":"https://[^"]+' | head -1 | cut -d'"' -f4)
NODE_ID=$(echo "$RESPONSE" | grep -oP '"node_id":"[^"]+' | head -1 | cut -d'"' -f4)
STARTED_AT=$(echo "$RESPONSE" | grep -oP '"updated_at":"[^"]+' | head -1 | cut -d'"' -f4)

if [ -n "$TUNNEL_URL" ]; then
    echo ""
    echo "╔════════════════════════════════════════════════════════════╗"
    echo "║                    🌐 WEB UI ACCESS                        ║"
    echo "╚════════════════════════════════════════════════════════════╝"
    echo ""
    echo "   $TUNNEL_URL"
    echo ""
    echo "   Node: $NODE_ID"
    echo "   Updated: $STARTED_AT"
    echo ""
else
    echo "❌ No node tunnel URLs found in Supabase"
    echo "Make sure the GitHub Actions workflow has run at least once"
    exit 1
fi
