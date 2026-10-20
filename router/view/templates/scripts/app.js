// ════════════════════════════════════════════════════
// CHANNEL SEARCH
// ════════════════════════════════════════════════════
function filterChannels(query) {
    var q = query.toLowerCase().trim();
    var items = document.querySelectorAll('.channel-item');
    items.forEach(function(item) {
        var username = (item.dataset.username || '').toLowerCase();
        if (!q || username.indexOf(q) !== -1) {
            item.style.display = '';
        } else {
            item.style.display = 'none';
        }
    });
    // Also hide section labels when searching
    document.querySelectorAll('[data-channel-section]').forEach(function(label) {
        label.style.display = q ? 'none' : '';
    });
}

// ════════════════════════════════════════════════════
// DARK MODE
// ════════════════════════════════════════════════════
function toggleDarkMode() {
    var isDark = document.documentElement.classList.toggle('dark');
    localStorage.setItem('darkMode', isDark ? 'true' : 'false');
}

var _deleteChannelTarget = '';
function openDeleteChannelDialog(username) {
    _deleteChannelTarget = username;
    document.getElementById('delete-channel-name').textContent = username;
    var link = document.getElementById('delete-channel-confirm-link');
    link.href = '/stop_channel/' + encodeURIComponent(username);
    document.getElementById('delete-channel-dialog').showModal();
}

// ════════════════════════════════════════════════════
// CHANNEL SELECTION
// ════════════════════════════════════════════════════
function selectChannel(username) {
    document.querySelectorAll('.channel-item').forEach(function(el) { el.classList.remove('selected'); });
    document.querySelectorAll('.channel-detail').forEach(function(el) { el.classList.add('hidden'); el.classList.remove('flex'); });
    var item = document.querySelector('.channel-item[data-username="' + username + '"]');
    if (item) item.classList.add('selected');
    var detail = document.querySelector('.channel-detail[data-channel="' + username + '"]');
    if (detail) { detail.classList.remove('hidden'); detail.classList.add('flex'); }
    var noSel = document.getElementById('no-selection');
    if (noSel) noSel.classList.add('hidden');
    localStorage.setItem('selectedChannel', username);
}

(function() {
    var last = localStorage.getItem('selectedChannel');
    if (last && document.querySelector('.channel-detail[data-channel="' + last + '"]')) {
        selectChannel(last);
    } else {
        var first = document.querySelector('.channel-item');
        if (first) selectChannel(first.dataset.username);
    }
})();

// ════════════════════════════════════════════════════
// LOG RENDERER — parses "15:04 [LEVEL] message" format
// ════════════════════════════════════════════════════

// Parse raw log string into parts
function parseLog(raw) {
    var m = raw.match(/^(\d{2}:\d{2})\s+\[(\w+)\]\s+([\s\S]*)$/);
    if (m) return { time: m[1], level: m[2].toUpperCase(), msg: m[3] };
    return { time: '', level: 'INFO', msg: raw };
}

// Map message prefix → { type, color }
var LOG_MSG_RULES = [
    // Recording lifecycle
    { re: /^starting to record/i,              type: 'play',      color: '#34d399' },
    { re: /^channel resumed/i,                 type: 'play',      color: '#34d399' },
    { re: /^channel (?:paused|stopped)/i,      type: 'stop',      color: '#fb7185' },
    // Upload
    { re: /^upload:/i,                         type: 'upload',    color: '#7dd3fc' },
    { re: /^upload (?:complete|started|failed|retrying)/i, type: 'upload', color: '#7dd3fc' },
    { re: /^queued for upload/i,              type: 'upload',    color: '#7dd3fc' },
    { re: /^file deleted from/i,               type: 'upload',    color: '#7dd3fc' },
    { re: /^vidhide:/i,                        type: 'upload',    color: '#fb923c' },
    { re: /^streamwish:/i,                     type: 'upload',    color: '#67e8f9' },
    { re: /^seekstreaming:/i,                  type: 'upload',    color: '#f0abfc' },
    { re: /^upnshare:/i,                       type: 'upload',    color: '#6ee7b7' },
    { re: /^vidara:/i,                         type: 'upload',    color: '#fda4af' },
    { re: /^doodstream:/i,                     type: 'upload',    color: '#f97316' },

    // Compress
    { re: /^compress:/i,                       type: 'compress',  color: '#c084fc' },
    { re: /^encoding/i,                        type: 'compress',  color: '#c084fc' },
    // Mux
    { re: /^mux:/i,                            type: 'mux',       color: '#22d3ee' },
    { re: /^merging/i,                         type: 'mux',       color: '#22d3ee' },
    // Thumbnail / preview
    { re: /^thumbnail:/i,                      type: 'thumbnail', color: '#f472b6' },
    { re: /^preview:/i,                        type: 'thumbnail', color: '#f472b6' },
    { re: /^sprite:/i,                         type: 'thumbnail', color: '#f472b6' },
    { re: /^generating (?:thumbnail|preview)/i, type: 'thumbnail', color: '#f472b6' },
    // File / output
    { re: /^delete:/i,                         type: 'delete',    color: '#f87171' },
    { re: /^removed/i,                         type: 'delete',    color: '#f87171' },
    { re: /^output-dir:/i,                     type: 'file',      color: '#fbbf24' },
    { re: /^file:/i,                           type: 'file',      color: '#fbbf24' },
    { re: /^max filesize or duration/i,        type: 'file',      color: '#fbbf24' },
    { re: /^new file created/i,                type: 'file',      color: '#fbbf24' },
    { re: /^saved/i,                           type: 'file',      color: '#fbbf24' },
    { re: /^writing/i,                         type: 'file',      color: '#fbbf24' },
    // Status / stream quality
    { re: /^stream quality/i,                  type: 'status',    color: '#2dd4bf' },
    { re: /^channel status:/i,                 type: 'status',    color: '#2dd4bf' },
    { re: /^channel is /i,                     type: 'status',    color: '#2dd4bf' },
    { re: /^status:/i,                         type: 'status',    color: '#2dd4bf' },
    { re: /^detected separate audio/i,         type: 'mux',       color: '#22d3ee' },
    { re: /^channel was paused/i,              type: 'status',    color: '#a78bfa' },
    { re: /^connection/i,                      type: 'status',    color: '#2dd4bf' },
    { re: /^reconnect/i,                       type: 'status',    color: '#fbbf24' },
    { re: /^waiting for/i,                     type: 'status',    color: '#a1a1aa' },
    { re: /^checking/i,                        type: 'status',    color: '#a1a1aa' },
    { re: /^polling/i,                         type: 'status',    color: '#a1a1aa' },
    { re: /^scanning/i,                        type: 'status',    color: '#2dd4bf' },
    { re: /^(?:stream is|stream)(?: now)? (?:online|offline|live|ended)/i, type: 'status', color: '#fde047' },
    { re: /^no (?:stream|video|playlist)/i,    type: 'status',    color: '#f87171' },
    { re: /^found (?:stream|video|playlist)/i, type: 'status',    color: '#34d399' },
    // Cloudflare
    { re: /^cf[_ ]?clearance/i,                type: 'cf',        color: '#fb923c' },
    { re: /^cloudflare/i,                      type: 'cf',        color: '#fb923c' },
    { re: /^cookie/i,                          type: 'cf',        color: '#fb923c' },
    // Progress (suppress / dim)
    { re: /^duration:/i,                       type: 'info',      color: '#52525b' },
];

var LOG_LEVEL_STYLE = {
    'ERROR': { text: '#ef4444', msgColor: '#f87171', type: 'error' },
    'WARN':  { text: '#f59e0b', msgColor: '#fbbf24', type: 'warn'  },
    'INFO':  { text: '#2e2e34', msgColor: null,      type: null    },
};

function getLogMeta(level, msg) {
    var ls = LOG_LEVEL_STYLE[level] || LOG_LEVEL_STYLE['INFO'];
    // ERROR and WARN always win — use their own color/type regardless of message prefix
    if (ls.type === 'error' || ls.type === 'warn') return { type: ls.type, msgColor: ls.msgColor, levelStyle: ls };
    // Otherwise check message prefix rules
    for (var i = 0; i < LOG_MSG_RULES.length; i++) {
        if (LOG_MSG_RULES[i].re.test(msg)) {
            return { type: LOG_MSG_RULES[i].type, msgColor: LOG_MSG_RULES[i].color, levelStyle: ls };
        }
    }
    return { type: 'info', msgColor: '#52525b', levelStyle: ls };
}

// Highlight keywords within a message string (returns HTML).
// IMPORTANT: bracket/host badges MUST be done first (before any other
// replacement injects HTML containing ']'), otherwise later regexes
// match across HTML attribute boundaries and corrupt the output.
function highlightMsg(msg, baseColor) {
    // Escape HTML first
    var safe = msg.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

    // ── STEP 1: Bracket badges (must be first — other rules inject ']') ──
    // Upload host names in brackets [GoFile], [VOE.sx] etc → per-host colored badge
    var HOST_COLORS = {
        'GoFile':       { fg: '#34d399', bg: 'rgba(52,211,153,0.1)'    },  // green
        'VOE.sx':       { fg: '#f472b6', bg: 'rgba(244,114,182,0.1)'   },  // pink
        'VoeSX':        { fg: '#f472b6', bg: 'rgba(244,114,182,0.1)'   },  // pink
        'TurboViPlay':  { fg: '#a78bfa', bg: 'rgba(167,139,250,0.1)'   },  // violet
        'GitHub':       { fg: '#e4e4e7', bg: 'rgba(228,228,231,0.08)'  },  // white
        'Streamtape':   { fg: '#fbbf24', bg: 'rgba(251,191,36,0.1)'    },  // amber
        'Mixdrop':      { fg: '#c084fc', bg: 'rgba(192,132,252,0.1)'   },  // purple
        'VidHide':      { fg: '#fb923c', bg: 'rgba(251,146,60,0.12)'   },  // orange
        'StreamWish':   { fg: '#67e8f9', bg: 'rgba(103,232,249,0.1)'   },  // cyan
        'SeekStreaming':{ fg: '#f0abfc', bg: 'rgba(240,171,252,0.1)'   },  // light purple
        'UPnShare':     { fg: '#6ee7b7', bg: 'rgba(110,231,183,0.12)'  },  // green
        'DoodStream':   { fg: '#f97316', bg: 'rgba(249,115,22,0.12)'   },  // orange
    };
    safe = safe.replace(/\[(GoFile|VOE\.sx|VoeSX|TurboViPlay|GitHub|Streamtape|Mixdrop|VidHide|StreamWish|SeekStreaming|UPnShare|DoodStream)\]/g,
        function(m, host) {
            var c = HOST_COLORS[host] || { fg: '#94a3b8', bg: 'rgba(148,163,184,0.1)' };
            return '<span style="color:' + c.fg + ';background:' + c.bg + ';padding:0 4px;border-radius:3px;font-size:10px;font-weight:600">[' + host + ']</span>';
        });

    // ── STEP 2: Everything else (safe — no ']' in injected HTML) ──
    // Backtick strings → orange mono
    safe = safe.replace(/`([^`]+)`/g, '<span style="color:#fb923c;font-family:ui-monospace,monospace;background:rgba(251,146,60,0.08);padding:0 3px;border-radius:3px">$1</span>');
    // Status keywords → colored
    safe = safe.replace(/\b(success(?:ful|fully)?|complete(?:d)?|done|finished)\b/gi,
        '<span style="color:#34d399;font-weight:600">$1</span>');
    safe = safe.replace(/\b(fail(?:ed|ing)?|error|err)\b/gi,
        '<span style="color:#f87171;font-weight:600">$1</span>');
    safe = safe.replace(/\b(warn(?:ing)?|caution)\b/gi,
        '<span style="color:#fbbf24;font-weight:600">$1</span>');
    safe = safe.replace(/\b(ignored)\b/gi,
        '<span style="color:#a1a1aa;font-weight:600">$1</span>');
    // Action keywords → teal
    safe = safe.replace(/\b(start(?:ed|ing)?|stopp(?:ed|ing)?|creat(?:ed|ing)?|remov(?:ed|ing)?|delet(?:ed|ing)?|resum(?:ed|ing)?|paus(?:ed|ing)?|queu(?:ed|ing)?|upload(?:ed|ing)?|download(?:ed|ing)?|merg(?:ed|ing)?|encod(?:ed|ing)?|compress(?:ed|ing)?|finished|skipp(?:ed|ing)?)\b/gi,
        '<span style="color:#2dd4bf;font-weight:600">$1</span>');
    // Queue-related highlights → teal
    safe = safe.replace(/\b(queue|queuing|queued)\b/gi,
        '<span style="color:#34d399;font-weight:600">$1</span>');
    // Found / discovered → green
    safe = safe.replace(/\b(found|discovered|detected|located)\b/gi,
        '<span style="color:#34d399;font-weight:600">$1</span>');
    // "No" matches → red
    safe = safe.replace(/\b(no|none|empty|missing)\b/gi,
        '<span style="color:#f87171;font-weight:600">$1</span>');
    // Online / offline / live → status colored
    safe = safe.replace(/\b(online|offline|live|ended)\b/gi,
        '<span style="color:#fde047;font-weight:600">$1</span>');
    // Host: prefix → dim label
    safe = safe.replace(/\bhost:\s*(\S+)/gi,
        'host:<span style="color:#a78bfa;font-weight:600">$1</span>');
    // Encoder/tech names → violet
    safe = safe.replace(/\b(NVENC|AMF|QSV|VideoToolbox|CPU|libx264|h264_nvenc|h264_amf|h264_qsv|ffmpeg|ffprobe|streamlink|yt-dlp|hls|rtmp)\b/gi,
        '<span style="color:#c084fc;font-weight:600">$1</span>');
    // URLs → dim link-style
    safe = safe.replace(/(https?:\/\/[^\s<]+)/g,
        '<span style="color:#60a5fa;text-decoration:underline;text-underline-offset:2px;opacity:0.8">$1</span>');
    // File names (e.g. foo.mp4, bar.ts) → amber mono
    safe = safe.replace(/\b([\w.\-]+\.(?:ts|mp4|mkv|webm|avi|mov|m3u8|jpg|png))\b/g, '<span style="color:#fbbf24;font-family:ui-monospace,monospace">$1</span>');
    // Bitrate values (e.g. 4500 kbps, 2.5 Mbps) → pink
    safe = safe.replace(/\b(\d+(?:\.\d+)?)\s*(kbps|mbps|bps)\b/gi, '<span style="color:#f472b6;font-weight:600">$1 $2</span>');
    // Sizes: 1.2 MB / 512 KB → cyan
    safe = safe.replace(/(\d+(?:\.\d+)?)\s*(KB|MB|GB|TB|B)\b/g, '<span style="color:#22d3ee;font-weight:600">$1 $2</span>');
    // Durations HH:MM:SS or MM:SS → cyan
    safe = safe.replace(/\b(\d{1,2}:\d{2}(?::\d{2})?)\b/g, '<span style="color:#22d3ee;font-weight:600">$1</span>');
    // Resolution Np (e.g. 1080p, 720p) → violet
    safe = safe.replace(/\b(\d{3,4})p\b/g, '<span style="color:#a78bfa;font-weight:600">$1p</span>');
    // FPS → violet
    safe = safe.replace(/\b(\d+)\s*(?:fps|@\s*\d+fps)/g, function(m) { return '<span style="color:#a78bfa;font-weight:600">' + m + '</span>'; });
    // Viewer counts → teal
    safe = safe.replace(/\b(\d+)\s+viewers?\b/g, '<span style="color:#2dd4bf;font-weight:600">$1 viewers</span>');
    // Percentages → yellow
    safe = safe.replace(/\b(\d+(?:\.\d+)?%)/g, '<span style="color:#fde047;font-weight:600">$1</span>');
    // Numbers in parens like (attempt 3) → dim yellow
    safe = safe.replace(/\(attempt (\d+)\)/g, '<span style="color:#a16207">(attempt $1)</span>');
    // Retry countdown → dim
    safe = safe.replace(/retry in (\d+) min/g, 'retry in <span style="color:#71717a">$1 min</span>');
    safe = safe.replace(/try again in (\d+) min(?:\(s\))?/g, 'try again in <span style="color:#71717a">$1 min</span>');
    return safe;
}

// Build the full HTML for one log line
function renderLogLine(raw) {
    if (!raw || !raw.trim()) return null;
    var p = parseLog(raw.trim());
    var meta = getLogMeta(p.level, p.msg);
    var ls = meta.levelStyle;
    var msgColor = meta.msgColor || '#3f3f46';

    var html = '<div class="log-line log-type-' + meta.type + '">';
    // Timestamp
    if (p.time) {
        html += '<span class="log-time" style="color:#a16207">' + p.time + '</span>';
    }
    // Level as badge with background (ERROR → red bg, WARN → amber bg, INFO → subtle gray bg)
    var badgeClass = ls.type === 'error' ? 'log-level-badge-error'
        : ls.type === 'warn' ? 'log-level-badge-warn'
        : 'log-level-badge-info';
    html += '<span class="log-level ' + badgeClass + '" style="color:' + ls.text + ';font-size:10px;font-weight:600;letter-spacing:0.3px">[' + p.level + ']</span>';
    // Message
    html += '<span class="log-msg" style="color:' + msgColor + '">' + highlightMsg(p.msg, msgColor) + '</span>';
    html += '</div>';
    return html;
}

// Per-container line count cache to avoid repeated querySelectorAll
var _logLineCount = {};
// Debounce timers for count updates
var _countTimer = {};
function appendLog(container, rawText, animate) {
    var html = renderLogLine(rawText);
    if (!html) return null;

    var id = container.id;
    if (!_logLineCount[id]) _logLineCount[id] = container.querySelectorAll('.log-line').length;

    // Trim excess lines (keep last 500) — remove in batches when threshold exceeded
    if (_logLineCount[id] >= 500) {
        var excess = container.querySelectorAll('.log-line');
        var removeCount = excess.length - 499;
        for (var i = 0; i < removeCount; i++) excess[i].remove();
        _logLineCount[id] = 499;
    }

    var tmp = document.createElement('div');
    tmp.innerHTML = html;
    var line = tmp.firstElementChild;
    if (!line) return null;

    if (animate) {
        line.classList.add('log-new');
        setTimeout(function() { line.classList.remove('log-new'); }, 300);
    }

    container.appendChild(line);
    _logLineCount[id]++;
    return line;
}

// Apply current active filter to a single line
function applyLineFilter(container, line) {
    var panel = container.closest('.flex-col');
    if (!panel) return;
    var activeBtn = panel.querySelector('.log-filter-btn.active, .log-filter-dot.active');
    if (!activeBtn) return;
    var filter = activeBtn.dataset.filter;
    if (!filter || filter === 'all') {
        line.classList.remove('log-filtered');
    } else if (line.classList.contains('log-type-' + filter)) {
        line.classList.remove('log-filtered');
    } else {
        line.classList.add('log-filtered');
    }
}

// Update entry count display — debounced so we don't thrash the DOM on every append
function updateLogCount(container) {
    var id = container.id;
    clearTimeout(_countTimer[id]);
    _countTimer[id] = setTimeout(function() {
        var username = id.replace('-log-container', '');
        var cnt = document.getElementById(username + '-log-count');
        if (!cnt) return;
        var total   = container.querySelectorAll('.log-line').length;
        var visible = container.querySelectorAll('.log-line:not(.log-filtered)').length;
        cnt.textContent = visible === total ? total + ' entries' : visible + ' / ' + total;
    }, 250);
}

// ════════════════════════════════════════════════════
// RECORDING COUNTER
// ════════════════════════════════════════════════════
function updateRecordingCounter() {
    var count = document.querySelectorAll('.channel-item[data-priority="0"]').length;
    var paused = document.querySelectorAll('.channel-item[data-priority="1"]').length;
    var el = document.getElementById('recording-counter');
    if (el) {
        var text = count > 0 ? count + ' recording' + (count > 1 ? 's' : '') : '0 recording';
        if (paused > 0) text += ' / ' + paused + ' paused';
        el.textContent = text;
    }
}
updateRecordingCounter();

var CHANNEL_GROUPS = {
    0: 'Recording',
    1: 'Paused',
    2: 'Reconnecting',
    3: 'Offline'
};
var _sidebarRefreshPending = false;

// Re-sort the sidebar channel list by data-priority so paused channels
// always appear below active channels and above offline channels.
function sortChannelList() {
    var list = document.getElementById('channel-list');
    if (!list) return;
    list.querySelectorAll('[data-channel-section]').forEach(function(el) { el.remove(); });
    var items = Array.from(list.querySelectorAll('.channel-item'));
    items.sort(function(a, b) {
        var pa = parseInt(a.dataset.priority || '9', 10);
        var pb = parseInt(b.dataset.priority || '9', 10);
        if (pa !== pb) return pa - pb;
        var ua = (a.dataset.username || '').toLowerCase();
        var ub = (b.dataset.username || '').toLowerCase();
        return ua < ub ? -1 : ua > ub ? 1 : 0;
    });
    var frag = document.createDocumentFragment();
    var lastPriority = null;
    items.forEach(function(item) {
        var priority = parseInt(item.dataset.priority || '9', 10);
        if (priority !== lastPriority) {
            var label = document.createElement('div');
            label.className = 'channel-section-label';
            label.dataset.channelSection = String(priority);
            label.textContent = CHANNEL_GROUPS[priority] || 'Other';
            frag.appendChild(label);
            lastPriority = priority;
        }
        frag.appendChild(item);
    });
    list.appendChild(frag);
}

var _lastSidebarRefresh = 0;
function scheduleSidebarRefresh() {
    if (_sidebarRefreshPending) return;
    var now = Date.now();
    if (now - _lastSidebarRefresh < 1000) return; // throttle to 1 Hz
    _lastSidebarRefresh = now;
    _sidebarRefreshPending = true;
    requestAnimationFrame(function() {
        _sidebarRefreshPending = false;
        updateRecordingCounter();
        sortChannelList();
    });
}
scheduleSidebarRefresh();

// ════════════════════════════════════════════════════
// SSE MESSAGE HANDLER — single listener for all events
// ════════════════════════════════════════════════════
document.addEventListener('htmx:sseMessage', function(e) {
    var eventName = e.detail.type || '';

    // ── Global upload state (broadcast from Manager.PublishUploadState) ──
    if (eventName === 'upload') {
        try {
            var data = JSON.parse(e.detail.data || '{}');
            window.__uploadState = data;
            renderUploadState(data);
        } catch(_) {}
        return;
    }

    var parts     = eventName.split('-');
    if (parts.length < 2) return;

    var suffix   = parts[parts.length - 1];          // "info" or "log"
    var username = parts.slice(0, -1).join('-');      // everything before last "-"

    // ── INFO panel updated via sse-swap → sync sidebar dot/badge ──
    if (suffix === 'info') {
        // Give HTMX a tick to finish its sse-swap DOM update
        setTimeout(function() {
            var infoEl = document.querySelector('[sse-swap="' + username + '-info"]');
            if (!infoEl) return;
            var statusEl = infoEl.querySelector('[data-status]');
            if (!statusEl) return;

            var status     = statusEl.dataset.status;
            var roomStatus = statusEl.dataset.roomStatus || '';
            var dot  = document.querySelector('[data-status-dot="' + username + '"]');
            var text = document.querySelector('[data-status-text="' + username + '"]');
            var item = document.querySelector('.channel-item[data-username="' + username + '"]');

            if (dot) {
                if (status === 'recording') {
                    dot.className = 'w-2 h-2 rounded-full block bg-emerald-400 rec-pulse';
                    if (item) item.dataset.priority = '0';
                } else if (status === 'connecting') {
                    dot.className = 'w-2 h-2 rounded-full block bg-amber-400';
                    if (item) item.dataset.priority = '2';
                } else if (status === 'paused') {
                    dot.className = 'w-2 h-2 rounded-full block bg-amber-400';
                    if (item) item.dataset.priority = '1';
                } else {
                    dot.className = 'w-2 h-2 rounded-full block bg-zinc-600';
                    if (item) item.dataset.priority = '3';
                }
            }
            if (text) {
                if (status === 'recording') {
                    text.textContent = 'Rec';
                    text.className = 'text-[9px] font-bold uppercase tracking-widest shrink-0 text-emerald-500';
                } else if (status === 'connecting') {
                    text.textContent = 'Reconnecting';
                    text.className = 'text-[9px] font-bold uppercase tracking-widest shrink-0 text-amber-400';
                } else if (status === 'paused') {
                    text.textContent = 'Paused';
                    text.className = 'text-[9px] font-bold uppercase tracking-widest shrink-0 text-amber-400';
                } else {
                    text.textContent = roomStatus || 'Offline';
                    text.className = 'text-[9px] font-bold uppercase tracking-widest shrink-0 text-zinc-600';
                }
            }
            scheduleSidebarRefresh();
        }, 50);
        return;
    }

    // ── LOG line appended in JS — sse-swap NOT used on log container ──
    if (suffix === 'log') {
        var rawText = e.detail.data || '';
        if (!rawText.trim()) return;

        var container = document.getElementById(username + '-log-container');
        if (!container) return;

        var line = appendLog(container, rawText, true);
        if (line) {
            applyLineFilter(container, line);
            updateLogCount(container);
        }

        // Auto-scroll if enabled — use rAF to avoid forced reflow on each append
        var panel = container.closest('.flex-1.min-w-0');
        var toggle = panel && panel.querySelector('.log-scroll-toggle');
        if (!toggle || toggle.checked) {
            requestAnimationFrame(function() { container.scrollTop = container.scrollHeight; });
        }
    }
});

// ════════════════════════════════════════════════════
// FILTER BUTTONS
// ════════════════════════════════════════════════════
function applyFilter(bar, filter) {
    bar.querySelectorAll('.log-filter-btn, .log-filter-dot').forEach(function(b) { b.classList.remove('active'); });
    var container = bar.querySelector('.log-container');
    if (!container) return;
    container.querySelectorAll('.log-line').forEach(function(line) {
        if (filter === 'all' || line.classList.contains('log-type-' + filter)) {
            line.classList.remove('log-filtered');
        } else {
            line.classList.add('log-filtered');
        }
    });
    updateLogCount(container);
}

document.querySelectorAll('.log-filter-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
        var bar = this.closest('.flex-col');
        applyFilter(bar, this.dataset.filter);
        this.classList.add('active');
    });
});

document.querySelectorAll('.log-filter-dot').forEach(function(btn) {
    btn.addEventListener('click', function() {
        var bar = this.closest('.flex-col');
        applyFilter(bar, this.dataset.filter);
        this.classList.add('active');
    });
});

document.querySelectorAll('.log-filter-clear').forEach(function(btn) {
    btn.addEventListener('click', function() {
        var container = this.closest('.flex-col').querySelector('.log-container');
        if (container) { container.innerHTML = ''; updateLogCount(container); }
    });
});

// ════════════════════════════════════════════════════
// INIT — render existing [data-raw] log lines on load
// ════════════════════════════════════════════════════
document.querySelectorAll('.log-container').forEach(function(container) {
    // Re-render each placeholder div that has a data-raw attribute
    container.querySelectorAll('[data-raw]').forEach(function(placeholder) {
        var raw = placeholder.getAttribute('data-raw');
        if (!raw) { placeholder.remove(); return; }
        var html = renderLogLine(raw);
        if (html) {
            var tmp = document.createElement('div');
            tmp.innerHTML = html;
            var line = tmp.firstElementChild;
            if (line) container.insertBefore(line, placeholder);
        }
        placeholder.remove();
    });

    // Scroll to bottom
    requestAnimationFrame(function() { container.scrollTop = container.scrollHeight; });

    // Initial count
    updateLogCount(container);
});

// ── Upload queue modal ──
var _uploadQueueInterval = null;

function openUploadQueue() {
    var overlay = document.getElementById('upload-queue-overlay');
    var modal = document.getElementById('upload-queue-modal');
    if (!overlay || !modal) return;
    overlay.style.display = 'block';
    modal.style.display = 'block';
    fetchUploadQueue();
    if (_uploadQueueInterval) clearInterval(_uploadQueueInterval);
    _uploadQueueInterval = setInterval(fetchUploadQueue, 3000);
}

function closeUploadQueue() {
    var overlay = document.getElementById('upload-queue-overlay');
    var modal = document.getElementById('upload-queue-modal');
    if (overlay) overlay.style.display = 'none';
    if (modal) modal.style.display = 'none';
    if (_uploadQueueInterval) {
        clearInterval(_uploadQueueInterval);
        _uploadQueueInterval = null;
    }
}

function fetchUploadQueue() {
    Promise.all([
        fetch('/api/uploads').then(function(r) { return r.json(); }),
        fetch('/api/orphans').then(function(r) { return r.json(); })
    ]).then(function(results) {
        var data = results[0];
        var orphansResp = results[1];
        var body = document.getElementById('upload-queue-body');
        if (!body) return;
        var html = '';
        // Active uploads
        var active = (data && data.active) || [];
        if (active.length > 0) {
            html += '<div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:0.12em;color:#34d399;margin-bottom:8px">Active Uploads (' + active.length + ')</div>';
            for (var i = 0; i < active.length; i++) {
                var c = active[i];
                var pct = Math.min(Math.max(c.progress || 0, 0), 100);
                html += renderQueueItem(c, pct);
            }
        }
        // Pending
        var pending = (data && data.pending) || [];
        if (pending.length > 0) {
            if (active.length > 0) html += '<div style="height:1px;background:rgba(255,255,255,0.06);margin:12px 0"></div>';
            html += '<div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:0.12em;color:#fbbf24;margin-bottom:8px">Pending (' + pending.length + ')</div>';
            for (var i = 0; i < pending.length; i++) {
                var p = pending[i];
                html += '<div style="padding:8px 10px;border-radius:8px;background:rgba(255,255,255,0.03);margin-bottom:4px">'
                    +   '<div style="display:flex;align-items:center;gap:6px;font-size:10px;color:#e4e4e7;font-weight:600">'
                    +     '<span style="width:5px;height:5px;border-radius:50%;background:#fbbf24;flex-shrink:0"></span>'
                    +     esc(p.channel || '?')
                    +   '</div>'
                    +   '<div style="font-size:9px;color:#71717a;font-family:ui-monospace,monospace;margin-top:2px">' + esc(p.filename || '—') + '</div>'
                    +   '<div style="font-size:8px;color:#52525b;margin-top:2px">stage: ' + esc(p.stage || '—') + '</div>'
                    +   (p.failed ? '<div style="font-size:8px;color:#ef4444;margin-top:2px">failed: ' + esc(p.error || '') + '</div>' : '')
                    + '</div>';
            }
        }
        // History (recently completed / failed)
        var history = (data && data.history) || [];
        var completed = history.filter(function(h) { return !h.failed; });
        var failed = history.filter(function(h) { return h.failed; });
        if (failed.length > 0) {
            if (active.length > 0 || pending.length > 0) html += '<div style="height:1px;background:rgba(255,255,255,0.06);margin:12px 0"></div>';
            html += '<div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:0.12em;color:#ef4444;margin-bottom:8px">Failed (' + failed.length + ')</div>';
            for (var i = 0; i < failed.length; i++) {
                var f = failed[i];
                html += '<div style="padding:8px 10px;border-radius:8px;background:rgba(255,255,255,0.03);margin-bottom:4px">'
                    +   '<div style="display:flex;align-items:center;gap:6px;font-size:10px;color:#e4e4e7;font-weight:600">'
                    +     '<span style="width:5px;height:5px;border-radius:50%;background:#ef4444;flex-shrink:0"></span>'
                    +     esc(f.channel || '?') + ' &middot; ' + esc(f.filename || '—')
                    +   '</div>'
                    +   '<div style="font-size:8px;color:#ef4444;margin-top:2px">' + esc(f.error || 'unknown error') + '</div>'
                    +   '<div style="font-size:8px;color:#52525b;margin-top:1px">stage: ' + esc(f.stage || '—') + '</div>'
                    + '</div>';
            }
        }
        if (completed.length > 0) {
            if (active.length > 0 || pending.length > 0 || failed.length > 0) html += '<div style="height:1px;background:rgba(255,255,255,0.06);margin:12px 0"></div>';
            html += '<div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:0.12em;color:#34d399;margin-bottom:8px">Completed (' + completed.length + ')</div>';
            for (var i = 0; i < completed.length; i++) {
                var d = completed[i];
                html += '<div style="padding:8px 10px;border-radius:8px;background:rgba(255,255,255,0.03);margin-bottom:4px">'
                    +   '<div style="display:flex;align-items:center;gap:6px;font-size:10px;color:#e4e4e7;font-weight:600">'
                    +     '<span style="width:5px;height:5px;border-radius:50%;background:#34d399;flex-shrink:0"></span>'
                    +     esc(d.channel || '?') + ' &middot; ' + esc(d.filename || '—')
                    +   '</div>'
                    +   '<div style="font-size:8px;color:#52525b;margin-top:2px">stage: ' + esc(d.stage || '—') + '</div>'
                    + '</div>';
            }
        }
        // Orphaned
        var orphans = (orphansResp && orphansResp.orphans) || [];
        if (orphans.length > 0) {
            if (active.length > 0 || pending.length > 0 || failed.length > 0 || completed.length > 0) html += '<div style="height:1px;background:rgba(255,255,255,0.06);margin:12px 0"></div>';
            html += '<div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:0.12em;color:#f87171;margin-bottom:8px">Orphaned (' + orphans.length + ')</div>';
            for (var i = 0; i < orphans.length; i++) {
                var o = orphans[i];
                html += '<div style="padding:8px 10px;border-radius:8px;background:rgba(255,255,255,0.03);margin-bottom:4px">'
                    +   '<div style="display:flex;align-items:center;gap:6px">'
                    +     '<span style="width:5px;height:5px;border-radius:50%;background:#f87171;flex-shrink:0"></span>'
                    +     '<span style="font-size:10px;color:#e4e4e7;font-weight:600">' + esc(o.filename || '?') + '</span>'
                    +   '</div>'
                    +   '<div style="font-size:8px;color:#52525b;margin-top:2px">' + esc(orphanPathShort(o.path || '')) + ' &middot; ' + (o.age || '') + ' &middot; ' + fmtBytes(o.size || 0) + '</div>'
                    + '</div>';
            }
            html += '<button type="button" data-action="retry-orphans" style="margin-top:8px;width:100%;padding:8px;border-radius:8px;background:rgba(239,68,68,0.1);border:1px solid rgba(239,68,68,0.2);color:#f87171;font-size:11px;font-weight:500;cursor:pointer">Retry All Orphans</button>';
        }
        if (active.length === 0 && pending.length === 0 && failed.length === 0 && completed.length === 0 && orphans.length === 0) {
            html = '<div style="color:#52525b;font-size:11px;text-align:center;padding:24px">No activity</div>';
        }
        body.innerHTML = html;
    }).catch(function() {});
}

function orphanPathShort(p) {
    var parts = p.replace(/\\/g, '/').split('/');
    return parts.length > 1 ? parts[parts.length - 2] + '/' + parts[parts.length - 1] : p;
}

function retryOrphans() {
    fetch('/api/orphans').then(function(r) { return r.json(); }).then(function(resp) {
        var orphans = (resp && resp.orphans) || [];
        if (orphans.length === 0) { alert('No orphaned files found.'); return; }
        var paths = orphans.map(function(o) { return o.path; });
        // Split into chunks of 5 so we don't overwhelm the server
        var chunkSize = 5;
        for (var i = 0; i < paths.length; i += chunkSize) {
            var chunk = paths.slice(i, i + chunkSize);
            (function(c) {
                fetch('/api/orphans/retry', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ paths: c })
                }).then(function(r) { return r.json(); }).then(function(result) {
                    var results = result.results || [];
                    for (var j = 0; j < results.length; j++) {
                        var r = results[j];
                        console.log('orphan ' + r.path + ': ' + r.status + (r.error ? ' (' + r.error + ')' : ''));
                    }
                }).catch(function(e) { console.error('orphan retry error:', e); });
            })(chunk);
        }
        alert('Retrying ' + orphans.length + ' orphaned file(s) in background.');
        // Refresh the queue after a few seconds
        setTimeout(fetchUploadQueue, 3000);
    }).catch(function() {});
}

function renderQueueItem(c, pct) {
    var barColor = pct < 30 ? '#3b82f6' : pct < 70 ? '#8b5cf6' : '#34d399';
    var hostLabel = (c.host_count || 0) + '/' + (c.host_total || '?');
    var sizeLabel = '';
    if (c.bytes_total > 0) sizeLabel = fmtBytes(c.bytes_current || 0) + ' / ' + fmtBytes(c.bytes_total);
    var speedLabel = c.speed || '';
    var html = '<div style="padding:8px 10px;border-radius:8px;background:rgba(255,255,255,0.03);margin-bottom:4px">'
        +   '<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">'
        +     '<span style="width:5px;height:5px;border-radius:50%;background:' + barColor + ';flex-shrink:0"></span>'
        +     '<span style="font-size:10px;color:#e4e4e7;font-weight:600">' + esc(c.channel || '?') + '</span>'
        +     '<span style="font-size:8px;color:#52525b;margin-left:auto">' + hostLabel + '</span>'
        +   '</div>'
        +   '<div style="font-size:9px;color:#71717a;font-family:ui-monospace,monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:2px">' + esc(c.filename || '—') + '</div>'
        +   '<div style="font-size:8px;color:#52525b;margin-bottom:4px">' + esc(c.status || '—')
        +     (speedLabel ? ' <span style="color:#34d399">' + speedLabel + '</span>' : '')
        +     (sizeLabel ? ' <span style="color:#71717a">' + sizeLabel + '</span>' : '')
        +   '</div>'
        +   '<div style="height:3px;border-radius:3px;background:rgba(255,255,255,0.05);overflow:hidden;margin-bottom:4px">'
        +     '<div style="height:100%;border-radius:3px;background:linear-gradient(90deg,' + barColor + ',' + (pct < 70 ? '#8b5cf6' : '#34d399') + ');width:' + pct + '%;transition:width 0.5s"></div>'
        +   '</div>';
    if (c.hosts && c.hosts.length > 0) {
        html += '<div style="display:flex;flex-direction:column;gap:3px;padding:4px 0 0 0">';
        for (var j = 0; j < c.hosts.length; j++) {
            var h = c.hosts[j];
            var hBarColor, hLabel, hLabelColor;
            if (h.status === 'done') { hBarColor = '#34d399'; hLabel = '✓'; hLabelColor = '#34d399'; }
            else if (h.status === 'failed') { hBarColor = '#ef4444'; hLabel = '✗'; hLabelColor = '#ef4444'; }
            else if (h.status === 'uploading') {
                var hp = Math.min(Math.max(h.progress || 0, 0), 100);
                hBarColor = hp < 30 ? '#60a5fa' : hp < 70 ? '#a78bfa' : '#34d399';
                hLabel = Math.round(hp) + '%';
                hLabelColor = '#a1a1aa';
            } else { hBarColor = '#3f3f46'; hLabel = '•'; hLabelColor = '#52525b'; }
            var hPct = Math.min(Math.max(h.progress || 0, 0), 100);
            var hSize = '';
            if (h.bytes_total > 0) hSize = fmtBytes(h.bytes_current || 0) + '/' + fmtBytes(h.bytes_total);
            var hSpeed = h.speed ? '<span style="color:#34d399;font-size:8px">' + h.speed + '</span>' : '';
            html += '<div style="display:flex;align-items:center;gap:4px">'
                +   '<span style="font-size:8px;color:' + hLabelColor + ';font-weight:700;width:16px;text-align:right;flex-shrink:0">' + hLabel + '</span>'
                +   '<span style="font-size:8px;color:#71717a;font-weight:600;width:48px;flex-shrink:0">' + esc(h.host || '') + '</span>'
                +   '<div style="flex:1;height:2px;border-radius:2px;background:rgba(255,255,255,0.05);overflow:hidden;min-width:40px">'
                +     '<div style="height:100%;border-radius:2px;background:' + hBarColor + ';width:' + hPct + '%;transition:width 0.5s"></div>'
                +   '</div>'
                +   hSpeed
                +   (hSize ? '<span style="font-size:7px;color:#52525b;font-family:ui-monospace,monospace;white-space:nowrap;width:60px;text-align:right">' + hSize + '</span>' : '')
                + '</div>';
        }
        html += '</div>';
    }
    html += '</div>';
    return html;
}

// ── Upload state renderer (called from SSE "upload" event) ──
var _lastUploadState = '';

function renderUploadState(s) {
    // Skip if nothing changed (cheap JSON compare)
    var key = JSON.stringify(s || {});
    if (key === _lastUploadState) return;
    _lastUploadState = key;

    var activeCount = 0;
    if (s && s.active && s.channels) {
        for (var i = 0; i < s.channels.length; i++) {
            if (s.channels[i].filename) activeCount++;
        }
    }

    // Badge
    var badge = document.getElementById('session-badge-upload');
    if (badge) {
        if (!s || !s.active || !s.channels || s.channels.length === 0) {
            badge.style.display = 'none';
        } else {
            badge.style.display = 'inline-flex';
            var dot = badge.querySelector('.upload-dot');
            if (dot) dot.classList.add('upload-dot-pulse');
            var bc = document.getElementById('session-badge-upload-count');
            if (bc) bc.textContent = activeCount;
        }
    }

    // Upload bar
    var bar = document.getElementById('upload-bar');
    var barList = document.getElementById('upload-bar-list');
    var barCount = document.getElementById('upload-bar-count');
    if (bar && barList && barCount) {
        if (!s || !s.active || !s.channels || s.channels.length === 0) {
            bar.style.display = 'none';
            return;
        }
        bar.style.display = 'block';
        barCount.textContent = activeCount + ' active';

        var html = '';
        for (var i = 0; i < s.channels.length; i++) {
            var c = s.channels[i];
            var pct = Math.min(Math.max(c.progress || 0, 0), 100);
            var hostLabel = (c.host_count || 0) + '/' + (c.host_total || '?');
            var sizeLabel = '';
            if (c.bytes_total > 0) {
                sizeLabel = fmtBytes(c.bytes_current || 0) + ' / ' + fmtBytes(c.bytes_total);
            }
            var speedLabel = c.speed || '';
            var barColor = pct < 30 ? '#3b82f6' : pct < 70 ? '#8b5cf6' : '#34d399';

            html += '<div style="padding:8px 10px;border-radius:8px;background:rgba(255,255,255,0.03)">'
                  +   '<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">'
                  +     '<span style="width:5px;height:5px;border-radius:50%;background:' + barColor + ';flex-shrink:0"></span>'
                  +     '<span style="font-size:10px;color:#e4e4e7;font-weight:600">' + esc(c.channel || '?') + '</span>'
                  +     '<span style="font-size:8px;color:#52525b;margin-left:auto">' + hostLabel + '</span>'
                  +   '</div>'
                  +   '<div style="font-size:9px;color:#71717a;font-family:ui-monospace,monospace;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:2px">' + esc(c.filename || '—') + '</div>'
                  +   '<div style="font-size:8px;color:#52525b;margin-bottom:4px">' + esc(c.status || '—')
                  +     (speedLabel ? ' <span style="color:#34d399">' + speedLabel + '</span>' : '')
                  +     (sizeLabel ? ' <span style="color:#71717a">' + sizeLabel + '</span>' : '')
                  +   '</div>'
                  +   '<div style="height:3px;border-radius:3px;background:rgba(255,255,255,0.05);overflow:hidden;margin-bottom:4px">'
                  +     '<div style="height:100%;border-radius:3px;background:linear-gradient(90deg,' + barColor + ',' + (pct < 70 ? '#8b5cf6' : '#34d399') + ');width:' + pct + '%;transition:width 0.5s"></div>'
                  +   '</div>';

            // Per-host rows
            if (c.hosts && c.hosts.length > 0) {
                html += '<div style="display:flex;flex-direction:column;gap:3px;padding:4px 0 0 0">';
                for (var j = 0; j < c.hosts.length; j++) {
                    var h = c.hosts[j];
                    var hBarColor, hLabel, hLabelColor;
                    if (h.status === 'done') {
                        hBarColor = '#34d399';
                        hLabel = '✓';
                        hLabelColor = '#34d399';
                    } else if (h.status === 'failed') {
                        hBarColor = '#ef4444';
                        hLabel = '✗';
                        hLabelColor = '#ef4444';
                    } else if (h.status === 'uploading') {
                        var hp = Math.min(Math.max(h.progress || 0, 0), 100);
                        hBarColor = hp < 30 ? '#60a5fa' : hp < 70 ? '#a78bfa' : '#34d399';
                        hLabel = Math.round(hp) + '%';
                        hLabelColor = '#a1a1aa';
                    } else {
                        hBarColor = '#3f3f46';
                        hLabel = '•';
                        hLabelColor = '#52525b';
                    }
                    var hPct = Math.min(Math.max(h.progress || 0, 0), 100);
                    var hSize = '';
                    if (h.bytes_total > 0) {
                        hSize = fmtBytes(h.bytes_current || 0) + '/' + fmtBytes(h.bytes_total);
                    }
                    var hSpeed = h.speed ? '<span style="color:#34d399;font-size:8px">' + h.speed + '</span>' : '';
                    html += '<div style="display:flex;align-items:center;gap:4px">'
                          +   '<span style="font-size:8px;color:' + hLabelColor + ';font-weight:700;width:16px;text-align:right;flex-shrink:0">' + hLabel + '</span>'
                          +   '<span style="font-size:8px;color:#71717a;font-weight:600;width:48px;flex-shrink:0">' + esc(h.host || '') + '</span>'
                          +   '<div style="flex:1;height:2px;border-radius:2px;background:rgba(255,255,255,0.05);overflow:hidden;min-width:40px">'
                          +     '<div style="height:100%;border-radius:2px;background:' + hBarColor + ';width:' + hPct + '%;transition:width 0.5s"></div>'
                          +   '</div>'
                          +   hSpeed
                          +   (hSize ? '<span style="font-size:7px;color:#52525b;font-family:ui-monospace,monospace;white-space:nowrap;width:60px;text-align:right">' + hSize + '</span>' : '')
                          + '</div>';
                }
                html += '</div>';
            }

            html += '</div>';
        }
        barList.innerHTML = html;
    }
}

function fmtBytes(b) {
    if (!b || b === 0) return '0 B';
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = 0;
    var v = b;
    while (v >= 1024 && i < units.length - 1) {
        v /= 1024;
        i++;
    }
    return (i === 0 ? v : v.toFixed(1)) + ' ' + units[i];
}

function esc(s) {
    if (!s) return '';
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── Session countdown ──
(function() {
    var dataEl = document.getElementById('session-data');
    var deadline = dataEl ? parseInt(dataEl.dataset.deadline) || 0 : 0;
    var totalSec = dataEl ? parseInt(dataEl.dataset.duration) || 1 : 1;
    var badgeTime = document.getElementById('session-badge-time');
    var cardTime = document.getElementById('session-card-time');
    var cardProgress = document.getElementById('session-card-progress');
    var cardSub = document.getElementById('session-card-sub');
    if (!badgeTime || !cardTime || !cardProgress) return;
    function tick() {
        var now = Math.floor(Date.now() / 1000);
        var rem = deadline - now;
        if (!deadline || rem <= 0) {
            var txt = deadline ? 'Processing...' : '--';
            badgeTime.textContent = txt;
            cardTime.textContent = txt;
            cardProgress.style.width = '100%';
            cardProgress.style.background = 'linear-gradient(90deg, #f59e0b, #f97316)';
            if (cardSub) cardSub.textContent = deadline ? 'Processing' : '';
            return;
        }
        if (cardSub) cardSub.textContent = '';
        var h = Math.floor(rem / 3600);
        var m = Math.floor((rem % 3600) / 60);
        var s = rem % 60;
        var pad = function(n) { return n < 10 ? '0' + n : n; };
        badgeTime.textContent = h + 'h ' + pad(m) + 'm';
        cardTime.textContent = h + 'h ' + pad(m) + 'm ' + pad(s) + 's';
        cardProgress.style.width = ((totalSec - rem) / totalSec * 100) + '%';
        cardProgress.style.background = 'linear-gradient(90deg, #d4a053, #e8c06a)';
    }
    tick();
    setInterval(tick, 1000);

    window.toggleSessionCard = function() {
        document.getElementById('session-card').classList.toggle('hidden');
    };
    window.confirmStopSession = function() {
        if (!confirm('Stop all channels and start processing now?')) return;
        fetch('/api/session/stop', { method: 'POST' });
        // Auto-open the session card so user sees processing start
        var card = document.getElementById('session-card');
        if (card) card.classList.remove('hidden');
    };
    document.addEventListener('click', function(e) {
        var fl = document.getElementById('session-floating');
        if (fl && !fl.contains(e.target)) document.getElementById('session-card')?.classList.add('hidden');
    });

    // Lightweight upload bar poll — fetches from API every 4s (no SSE dependency).
    // This replaces the old 2-second fallback that relied on window.__uploadState
    // and the info-panel upload detection which caused flickering.
    setInterval(function() {
        fetch('/api/uploads').then(function(r) { return r.json(); }).then(function(data) {
            if (!data) return;
            window.__uploadState = data;
            renderUploadState(data);
        }).catch(function(){});
    }, 4000);
})();

function getConfiguredDomain() {
    var el = document.getElementById('add-channel-site');
    return (el && el.dataset.domain) || 'https://chaturbate.com/';
}

function updateDomainPrefix() {
    var site = document.getElementById('add-channel-site');
    var prefix = document.getElementById('domain-prefix');
    if (!site || !prefix) return;
    if (site.value === 'stripchat') {
        prefix.textContent = 'https://stripchat.com/';
    } else {
        prefix.textContent = getConfiguredDomain();
    }
}

// ════════════════════════════════════════════════════
// EVENT DELEGATION — replaces all inline onclick handlers so the
// UI keeps working even when inline JS / event-handler attributes
// are blocked (CSP, proxy rewriting, etc.).
// ════════════════════════════════════════════════════
document.addEventListener('click', function(e) {
    var el = e.target && e.target.closest ? e.target.closest('[data-action], .channel-item') : null;
    if (!el) return;

    if (el.classList.contains('channel-item')) {
        selectChannel(el.dataset.username);
        return;
    }

    var action = el.getAttribute('data-action');
    if (!action) return;

    switch (action) {
        case 'open-settings':
            document.getElementById('settings-dialog').showModal();
            break;
        case 'open-create':
            document.getElementById('create-dialog').showModal();
            break;
        case 'toggle-dark':
            toggleDarkMode();
            break;
        case 'confirm-stop-session':
            confirmStopSession();
            break;
        case 'open-upload-queue':
            openUploadQueue();
            break;
        case 'close-upload-queue':
            closeUploadQueue();
            break;
        case 'retry-orphans':
            retryOrphans();
            break;
        case 'close-dialog':
            var dlg = el.closest('dialog');
            if (dlg) dlg.close();
            break;
        case 'close-delete-dialog':
            document.getElementById('delete-channel-dialog').close();
            break;
        case 'history-back':
            history.back();
            break;
        case 'navigate':
            if (el.dataset.href) window.location.href = el.dataset.href;
            break;
        case 'delete-channel':
            openDeleteChannelDialog(el.dataset.username);
            break;
    }
});

var _channelSearch = document.getElementById('channel-search');
if (_channelSearch) {
    _channelSearch.addEventListener('input', function() { filterChannels(this.value); });
}

var _siteSelect = document.getElementById('add-channel-site');
if (_siteSelect) {
    _siteSelect.addEventListener('change', updateDomainPrefix);
}

// Hide broken thumbnails (replaces the inline onerror on thumb images)
document.addEventListener('error', function(e) {
    var img = e.target;
    if (!img || img.tagName !== 'IMG') return;
    if (!img.closest || !img.closest('.thumb-wrap')) return;
    img.style.display = 'none';
    var p = img.parentElement;
    if (p) {
        p.style.display = 'flex';
        p.style.alignItems = 'center';
        p.style.justifyContent = 'center';
    }
}, true);

// ════════════════════════════════════════════════════
// SETTINGS DIALOG — save, then show upload-key verdicts
// The form posts to /update_config, which verifies every
// credential this save added or rotated before responding.
// Submitting via fetch keeps the dialog open so a rotated
// key that fails shows its real error where it was typed.
// ════════════════════════════════════════════════════
var _settingsForm = document.getElementById('settings-form');
if (_settingsForm) {
    _settingsForm.addEventListener('submit', function(e) {
        e.preventDefault();
        saveSettings(_settingsForm);
    });
}

function settingsVerdictColor(status) {
    if (status === 'healthy') return '#34d399';
    if (status === 'dead') return '#f87171';
    if (status === 'quota' || status === 'unknown') return '#fbbf24';
    return '#a1a1aa';
}

function settingsVerdictText(v) {
    var msg = v.message ? ' — ' + v.message : '';
    switch (v.status) {
        case 'healthy': return 'upload verified';
        case 'dead': return 'failed' + msg;
        case 'quota': return 'account capped' + msg;
        case 'unknown': return 'could not verify' + msg;
        default: return 'saved (no check available)' + msg;
    }
}

function settingsResultBox() {
    var box = document.getElementById('settings-result');
    if (!box) return null;
    box.innerHTML = '';
    box.className = 'mx-4 sm:mx-6 mb-4 rounded-xl px-4 py-3 text-xs leading-relaxed';
    return box;
}

function renderSettingsResult(keys) {
    var box = settingsResultBox();
    if (!box) return;
    if (!keys || !keys.length) {
        box.style.color = '#a1a1aa';
        box.textContent = 'Settings saved.';
        return;
    }
    keys.forEach(function(v, i) {
        var line = document.createElement('div');
        if (i > 0) line.style.marginTop = '6px';
        var who = document.createElement('span');
        who.style.color = '#d4d4d8';
        who.textContent = (v.label || v.host || 'Key') + (v.display ? ' ' + v.display : '') + ' — ';
        var verdict = document.createElement('span');
        verdict.style.color = settingsVerdictColor(v.status);
        verdict.textContent = settingsVerdictText(v);
        line.appendChild(who);
        line.appendChild(verdict);
        box.appendChild(line);
    });
}

function renderSettingsError(msg) {
    var box = settingsResultBox();
    if (!box) return;
    box.style.color = '#f87171';
    box.textContent = 'Could not save: ' + msg;
}

function saveSettings(form) {
    var btn = form.querySelector('button[type="submit"]');
    var label = btn ? btn.textContent : '';
    if (btn) {
        btn.disabled = true;
        btn.textContent = 'Saving…';
    }
    var box = settingsResultBox();
    if (box) {
        box.style.color = '#a1a1aa';
        box.textContent = 'Saving and verifying keys…';
    }
    fetch(form.action, {
        method: 'POST',
        headers: { 'Accept': 'application/json' },
        body: new FormData(form)
    }).then(function(r) {
        return r.json().then(function(d) {
            if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status));
            return d;
        });
    }).then(function(d) {
        renderSettingsResult(d.keys || []);
    }).catch(function(err) {
        renderSettingsError(err && err.message ? err.message : 'unknown error');
    }).finally(function() {
        if (btn) {
            btn.disabled = false;
            btn.textContent = label;
        }
    });
}
