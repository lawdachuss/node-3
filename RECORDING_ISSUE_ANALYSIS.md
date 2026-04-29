# 🔍 Recording Issue Analysis

## Problem

Channels are live but not being recorded.

## Root Cause

**FlareSolverr is overwhelmed** with too many requests.

### Evidence

1. **268 channels configured** on EC2 (vs 3 locally)
2. **FlareSolverr queue depth: 55-65 tasks** (should be near 0)
3. **All channels detected as "offline"** even though they're live
4. **No ffmpeg processes running** - no actual recording happening
5. **FlareSolverr taking 80-120 seconds** per Cloudflare challenge

### Why This Happens

Chaturbate uses Cloudflare protection. For each channel check:
1. Application requests channel status
2. Request goes through FlareSolverr
3. FlareSolverr solves Cloudflare challenge (80-120 seconds)
4. Returns result to application

With 268 channels checking every minute:
- **268 requests per minute** = 4.5 requests/second
- **Each request takes 80-120 seconds**
- **Queue builds up faster than it can be processed**

Result: Channels appear offline because FlareSolverr can't keep up.

---

## Solutions

### Option 1: Reduce Number of Channels (Immediate Fix)

**Recommended for quick resolution**

Reduce to 20-30 actively monitored channels:

```powershell
# Edit channels locally
notepad conf/channels.json

# Keep only your priority channels
# Set others to "is_paused": true

# Upload to EC2
.\scripts\upload-config.ps1
```

**Impact:**
- ✅ Immediate improvement
- ✅ FlareSolverr queue will clear
- ✅ Active channels will record properly
- ❌ Won't monitor all 268 channels

### Option 2: Add More FlareSolverr Instances (Scalable)

**Best for monitoring many channels**

Run multiple FlareSolverr containers with load balancing:

1. **Update docker-compose.yml:**

```yaml
services:
  flaresolverr-1:
    image: ghcr.io/flaresolverr/flaresolverr:latest
    container_name: flaresolverr-1
    ports:
      - "8191:8191"
    # ... rest of config

  flaresolverr-2:
    image: ghcr.io/flaresolverr/flaresolverr:latest
    container_name: flaresolverr-2
    ports:
      - "8192:8191"
    # ... rest of config

  flaresolverr-3:
    image: ghcr.io/flaresolverr/flaresolverr:latest
    container_name: flaresolverr-3
    ports:
      - "8193:8191"
    # ... rest of config
```

2. **Configure application to use multiple instances** (if supported)

**Impact:**
- ✅ Can handle 3x more channels
- ✅ Better reliability
- ❌ Uses more CPU/memory
- ❌ Requires code changes if app doesn't support multiple instances

### Option 3: Increase Check Interval (Moderate Fix)

**Balance between coverage and performance**

If the application supports it, increase the interval between channel checks from 1 minute to 2-3 minutes.

**Impact:**
- ✅ Reduces FlareSolverr load by 50-66%
- ✅ Can monitor more channels
- ❌ Slower to detect when channels go live
- ❌ May miss short streams

### Option 4: Upgrade EC2 Instance (Resource Fix)

**If FlareSolverr is CPU-bound**

Current instance might not have enough CPU for FlareSolverr's browser automation.

Upgrade to a larger instance type:
- Current: t2.medium (2 vCPU, 4GB RAM)
- Upgrade to: t2.large (2 vCPU, 8GB RAM) or c5.large (2 vCPU, 4GB RAM, better CPU)

**Impact:**
- ✅ Faster Cloudflare solving
- ✅ Can handle more concurrent requests
- ❌ Higher AWS costs

---

## Recommended Immediate Action

### Step 1: Reduce Channels (Quick Win)

1. **Download current channels from EC2:**
```powershell
scp -i aws-secrets/aws-key.pem ubuntu@54.210.37.19:/home/ubuntu/goondvr/conf/channels.json conf/channels-ec2-backup.json
```

2. **Edit and keep only priority channels:**
```powershell
notepad conf/channels-ec2-backup.json
# Keep 20-30 most important channels
# Set others to "is_paused": true
```

3. **Save as channels.json and upload:**
```powershell
Copy-Item conf/channels-ec2-backup.json conf/channels.json
.\scripts\upload-config.ps1
```

4. **Verify FlareSolverr queue clears:**
```bash
ssh -i aws-secrets/aws-key.pem ubuntu@54.210.37.19 "cd /home/ubuntu/goondvr && sudo docker compose logs flaresolverr --tail=20"
```

### Step 2: Monitor and Verify

Wait 2-3 minutes, then check:

```bash
# Check if channels are now detected as online
ssh -i aws-secrets/aws-key.pem ubuntu@54.210.37.19 "cd /home/ubuntu/goondvr && sudo docker compose logs recorder --tail=50 | grep 'online\|recording'"

# Check for active recordings
ssh -i aws-secrets/aws-key.pem ubuntu@54.210.37.19 "ls -lh /home/ubuntu/goondvr/videos/*.mp4"

# Check FlareSolverr queue
ssh -i aws-secrets/aws-key.pem ubuntu@54.210.37.19 "cd /home/ubuntu/goondvr && sudo docker compose logs flaresolverr | grep queue | tail -5"
```

---

## Long-Term Solution

### Implement Tiered Monitoring

**Priority Tiers:**
- **Tier 1 (Always Monitor):** 10-15 top priority channels - check every 1 minute
- **Tier 2 (Regular Monitor):** 30-40 channels - check every 3 minutes  
- **Tier 3 (Occasional Monitor):** Remaining channels - check every 10 minutes

This requires application code changes but would allow monitoring all channels efficiently.

---

## Current Status

### What's Working
✅ Application is running
✅ Configuration is loaded
✅ Supabase is enabled
✅ FlareSolverr is solving challenges
✅ Web UI is accessible

### What's Not Working
❌ Too many channels (268)
❌ FlareSolverr queue overloaded (55-65 tasks)
❌ Channels detected as offline
❌ No recordings happening

### Key Metrics
- **Channels Configured:** 268
- **FlareSolverr Queue:** 55-65 tasks
- **Challenge Solve Time:** 80-120 seconds
- **Active Recordings:** 0
- **Completed Recordings:** 1 (mysunderland_2026-04-29_18-34-40.mp4)

---

## Verification Commands

### Check FlareSolverr Queue
```bash
ssh -i aws-secrets/aws-key.pem ubuntu@54.210.37.19 "cd /home/ubuntu/goondvr && sudo docker compose logs flaresolverr | grep queue | tail -10"
```

### Check Channel Status
```bash
ssh -i aws-secrets/aws-key.pem ubuntu@54.210.37.19 "cd /home/ubuntu/goondvr && sudo docker compose logs recorder | grep -E 'online|offline' | tail -20"
```

### Check Active Recordings
```bash
ssh -i aws-secrets/aws-key.pem ubuntu@54.210.37.19 "ls -lh /home/ubuntu/goondvr/videos/*.mp4 2>/dev/null || echo 'No active recordings'"
```

### Check Container Resources
```bash
ssh -i aws-secrets/aws-key.pem ubuntu@54.210.37.19 "sudo docker stats --no-stream"
```

---

## Next Steps

1. **Immediate:** Reduce channels to 20-30 priority ones
2. **Short-term:** Monitor FlareSolverr queue and recording success
3. **Medium-term:** Consider adding more FlareSolverr instances
4. **Long-term:** Implement tiered monitoring or upgrade instance

---

**Issue Identified:** 2026-04-29  
**Status:** Root cause found - FlareSolverr overload  
**Recommended Action:** Reduce channel count to 20-30
