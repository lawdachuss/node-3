# How to Get Fresh Chaturbate Cookies

If channels show as offline when they're actually live, your cookies are likely expired. Here's how to get fresh ones:

## Method 1: Browser DevTools (Recommended)

### Chrome/Brave/Edge:

1. **Open Chaturbate** in your browser: https://chaturbate.com
2. **Log in** to your account (if you have one)
3. **Open DevTools**: Press `F12` or `Ctrl+Shift+I`
4. **Go to Application tab** (or Storage in Firefox)
5. **Click Cookies** → `https://chaturbate.com`
6. **Find the cookie** named `cf_clearance` or similar
7. **Copy ALL cookies** in this format:
   ```
   cookie1=value1; cookie2=value2; cookie3=value3
   ```

### Firefox:

1. Open Chaturbate: https://chaturbate.com
2. Press `F12` to open DevTools
3. Go to **Storage** tab
4. Click **Cookies** → `https://chaturbate.com`
5. Copy all cookies in the format above

## Method 2: Use Cookie Editor Extension

1. **Install Extension:**
   - Chrome: [Cookie Editor](https://chrome.google.com/webstore/detail/cookie-editor/hlkenndednhfkekhgcdicdfddnkalmdm)
   - Firefox: [Cookie Editor](https://addons.mozilla.org/en-US/firefox/addon/cookie-editor/)

2. **Visit Chaturbate:** https://chaturbate.com

3. **Click the extension icon**

4. **Click "Export"** → Copy the cookies

5. **Format them** as: `name1=value1; name2=value2`

## Method 3: Use the Script (Automated)

Run this PowerShell script:

```powershell
.\scripts\get-fresh-cookies.ps1
```

This will:
1. Open Chaturbate in your browser
2. Wait for you to log in
3. Extract cookies automatically
4. Update your settings.json

## Important Cookies to Get:

The most important cookies are:
- `cf_clearance` - Cloudflare clearance
- `__cf_bm` - Cloudflare bot management
- `affkey` - Affiliate key
- `agreeterms` - Terms agreement
- Any session cookies

## Update Your Settings:

Once you have the cookies, update your `settings.json`:

```json
{
  "cookies": "cf_clearance=xxx; __cf_bm=yyy; affkey=zzz; ...",
  ...
}
```

Then update the GitHub secret `SETTINGS_JSON` with the new content.

## How Often to Update:

- **Cloudflare cookies** expire every 24-48 hours
- **Session cookies** may last longer
- Update whenever channels show offline but are actually live

## Alternative: Use Byparr

Instead of manual cookies, you can use Byparr (included in the workflow) which automatically bypasses Cloudflare. However, it's slower and may not work 100% of the time.

The workflow already includes Byparr, but fresh cookies are more reliable!

---

**Last Updated:** 2026-05-03
