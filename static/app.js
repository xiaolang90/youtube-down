// ── State ──
let currentVideo = null;
let selectedFormat = null;
let sseConnections = {};  // taskId -> EventSource
let taskStartTimes = {};  // taskId -> ms timestamp when first saw downloading
let taskFilesizes = {};   // taskId -> bytes (from metadata)
let taskQueuedAt = {};    // taskId -> ms timestamp when task was enqueued
let taskElapsedTimers = {}; // taskId -> setInterval handle for "已等待 Xs"
let taskHasProgress = {}; // taskId -> bool, true once a real %-progress event arrived
let taskPhaseText = {};   // taskId -> last known phase_text (for reconnects / display)

const PHASE_TEXT_FALLBACK = {
    queued: '排队中',
    preparing: '准备中',
    pot_check: '检查 PO Token 服务',
    pot: '获取 PO Token',
    starting_ytdlp: '启动 yt-dlp',
    extracting: '解析视频信息',
    player: '获取播放器数据',
    manifest: '拉取媒体清单',
    formats: '准备下载格式',
    starting: '开始下载',
    retry: '重试中…',
    pot_refresh: '刷新 PO Token',
    downloading_video: '下载视频流',
    downloading_audio: '下载音频流',
    merging: '合并音视频'
};

function clearElapsedTimer(taskId) {
    if (taskElapsedTimers[taskId]) {
        clearInterval(taskElapsedTimers[taskId]);
        delete taskElapsedTimers[taskId];
    }
}
let historyPage = 1;

// ── Format helpers ──
function formatDuration(sec) {
    sec = Math.max(0, Math.round(sec));
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    return m > 0 ? `${m}min${s}s` : `${s}s`;
}

function formatSpeed(bytesPerSec) {
    if (!isFinite(bytesPerSec) || bytesPerSec <= 0) return '-';
    const MB = 1024 * 1024, KB = 1024;
    if (bytesPerSec >= MB) return (bytesPerSec / MB).toFixed(2) + 'MB/s';
    if (bytesPerSec >= KB) return (bytesPerSec / KB).toFixed(2) + 'KB/s';
    return bytesPerSec.toFixed(0) + 'B/s';
}

// ── Tabs ──
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
        if (btn.dataset.tab === 'history') loadHistory();
    });
});

// ── URL Input: Enter key ──
document.getElementById('url-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') parseVideo();
});

// ── Parse Video ──
async function parseVideo() {
    const url = document.getElementById('url-input').value.trim();
    if (!url) return showToast('请输入视频链接', 'error');

    const btn = document.getElementById('parse-btn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span>';

    // Hide previous results
    hide('video-info');
    hide('format-section');
    hide('download-btn-section');
    currentVideo = null;
    selectedFormat = null;

    // Staged hint messages while /api/parse runs
    const stages = [
        { at: 0,     text: '正在连接 YouTube...' },
        { at: 2000,  text: '正在获取视频信息...' },
        { at: 8000,  text: '首次解析较慢,正在解 JS 签名...' },
        { at: 15000, text: '网络似乎有点慢,继续等待...' }
    ];
    let toastEl = showToast(stages[0].text, 'info', 0);
    const timers = stages.slice(1).map(s =>
        setTimeout(() => { if (toastEl) toastEl.textContent = s.text; }, s.at)
    );

    try {
        const res = await api('/api/parse', { url });
        if (res.code !== 0) throw new Error(res.message);
        currentVideo = res.data;
        showVideoInfo(res.data);
        showFormats(res.data.formats);
    } catch (e) {
        showToast(e.message || '解析失败', 'error');
    } finally {
        timers.forEach(clearTimeout);
        if (toastEl) toastEl.remove();
        btn.disabled = false;
        btn.textContent = '解析';
    }
}

function showVideoInfo(data) {
    document.getElementById('video-thumbnail').src = data.thumbnail;
    document.getElementById('video-title').textContent = data.title;
    document.getElementById('video-uploader').textContent = data.uploader;
    document.getElementById('video-duration').textContent = data.duration_str;
    show('video-info');
}

function showFormats(formats) {
    const grid = document.getElementById('format-grid');
    grid.innerHTML = '';
    formats.forEach(f => {
        const card = document.createElement('div');
        card.className = 'format-card bg-dark-700 border border-dark-600 rounded-lg p-3 text-center';
        card.dataset.formatId = f.format_id;
        card.innerHTML = `
            <div class="text-sm font-semibold text-white">${f.resolution}</div>
            <div class="text-xs text-gray-500 mt-1">${f.ext}${f.filesize_str ? ' · ' + f.filesize_str : ''}</div>
            ${f.vcodec ? `<div class="text-xs text-gray-600 mt-0.5">${f.vcodec}${f.acodec ? '+' + f.acodec : ''}</div>` : ''}
        `;
        card.addEventListener('click', () => selectFormat(card, f));
        grid.appendChild(card);
    });
    show('format-section');
}

function selectFormat(card, format) {
    document.querySelectorAll('.format-card').forEach(c => c.classList.remove('selected'));
    card.classList.add('selected');
    selectedFormat = format;
    show('download-btn-section');
}

// ── Start Download ──
async function startDownload() {
    if (!currentVideo || !selectedFormat) return showToast('请先选择清晰度', 'error');

    const btn = document.getElementById('download-btn');
    btn.disabled = true;

    try {
        const res = await api('/api/download', {
            url: currentVideo.url,
            format_id: selectedFormat.format_id,
            title: currentVideo.title,
            thumbnail: currentVideo.thumbnail,
            uploader: currentVideo.uploader,
            duration: currentVideo.duration,
            format_note: selectedFormat.note,
            filesize: selectedFormat.filesize
        });
        if (res.code !== 0) throw new Error(res.message);

        showToast('下载任务已创建', 'success');
        addQueueItem(res.data.id, currentVideo.title, selectedFormat.note, selectedFormat.filesize);
        connectSSE(res.data.id);
    } catch (e) {
        showToast(e.message || '创建下载失败', 'error');
    } finally {
        btn.disabled = false;
    }
}

// ── Download Queue ──
function addQueueItem(taskId, title, formatNote, filesize) {
    if (filesize) taskFilesizes[taskId] = filesize;
    taskQueuedAt[taskId] = Date.now();
    taskHasProgress[taskId] = false;
    taskPhaseText[taskId] = '排队中';
    document.getElementById('queue-empty').style.display = 'none';
    const list = document.getElementById('queue-list');
    const item = document.createElement('div');
    item.id = 'queue-' + taskId;
    item.className = 'bg-dark-800 border border-dark-600 rounded-lg p-3';
    item.innerHTML = `
        <div class="flex items-center justify-between mb-2">
            <div class="min-w-0 flex-1 mr-3">
                <p class="text-sm text-white truncate">${escHtml(title)}</p>
                <p class="text-xs text-gray-500">${escHtml(formatNote || '')}</p>
            </div>
            <div class="flex items-center gap-2 flex-shrink-0">
                <button class="queue-reveal hidden text-xs text-gray-400 hover:text-blue-400 transition" title="打开所在目录" onclick="revealFile('${taskId}')">📂 打开位置</button>
                <span class="queue-status text-xs px-2 py-0.5 rounded-full bg-blue-500/20 text-blue-400">等待中</span>
                <button class="queue-cancel text-xs text-gray-500 hover:text-red-400 transition" onclick="cancelTask('${taskId}')">取消</button>
                <button class="queue-resume hidden text-xs text-blue-400 hover:text-blue-300 transition" onclick="resumeTask('${taskId}')">继续下载</button>
                <button class="queue-delete hidden text-xs text-gray-500 hover:text-red-400 transition" onclick="deleteQueueTask('${taskId}')">删除</button>
            </div>
        </div>
        <div class="w-full bg-dark-700 rounded-full h-2 mb-1.5 overflow-hidden">
            <div class="queue-progress progress-fill indeterminate h-2 rounded-full" style="width: 100%"></div>
        </div>
        <div class="flex justify-between text-xs text-gray-500">
            <span class="queue-percent">排队中</span>
            <span class="queue-detail">已等待 0s</span>
        </div>
        <div class="queue-actions mt-2 hidden">
            <a class="queue-download-link text-xs text-blue-400 hover:text-blue-300" href="#" target="_blank">💾 保存到本地</a>
        </div>
    `;
    list.prepend(item);
    updateQueueCount();

    // Tick "已等待 Xs" until real progress arrives or the task ends.
    clearElapsedTimer(taskId);
    taskElapsedTimers[taskId] = setInterval(() => {
        if (taskHasProgress[taskId]) { clearElapsedTimer(taskId); return; }
        const el = document.querySelector('#queue-' + taskId + ' .queue-detail');
        if (!el) { clearElapsedTimer(taskId); return; }
        const secs = Math.round((Date.now() - (taskQueuedAt[taskId] || Date.now())) / 1000);
        el.textContent = '已等待 ' + secs + 's';
    }, 1000);
}

function updateQueueItem(taskId, data) {
    const item = document.getElementById('queue-' + taskId);
    if (!item) return;

    const statusEl = item.querySelector('.queue-status');
    const progressEl = item.querySelector('.queue-progress');
    const percentEl = item.querySelector('.queue-percent');
    const detailEl = item.querySelector('.queue-detail');
    const cancelEl = item.querySelector('.queue-cancel');
    const actionsEl = item.querySelector('.queue-actions');
    const linkEl = item.querySelector('.queue-download-link');

    // Phase update (cold-start or merging). Keep the bar indeterminate until
    // we see a real %-progress line with a non-zero value and speed.
    if (data.phase) {
        const text = data.phase_text || PHASE_TEXT_FALLBACK[data.phase] || data.phase;
        taskPhaseText[taskId] = text;
        if (!taskHasProgress[taskId] || data.phase === 'merging') {
            progressEl.classList.add('indeterminate');
            progressEl.style.width = '100%';
            percentEl.textContent = text;
        }
    }

    const hasRealProgress = data.progress !== undefined
        && data.progress > 0
        && (data.speed || data.eta);
    const isMerging = data.phase === 'merging';

    if (hasRealProgress && !isMerging) {
        taskHasProgress[taskId] = true;
        clearElapsedTimer(taskId);
        progressEl.classList.remove('indeterminate');
        const p = Math.min(100, Math.max(0, data.progress));
        progressEl.style.width = p + '%';
        percentEl.textContent = p.toFixed(1) + '%';
    } else if (data.progress !== undefined && taskHasProgress[taskId] && !isMerging) {
        // Already past cold start: normal progress update (may not always
        // include speed/eta, e.g. the last 100% frame).
        const p = Math.min(100, Math.max(0, data.progress));
        progressEl.style.width = p + '%';
        progressEl.classList.remove('indeterminate');
        percentEl.textContent = p.toFixed(1) + '%';
    }

    if (data.speed || data.eta) {
        // Speed/ETA presence ⇒ yt-dlp is actively downloading even if the
        // percentage is still 0.0 (typical for the first 30-60s of a large
        // HLS-segmented file). Kill the "已等待 Xs" timer so it doesn't
        // clobber the speed display every second.
        taskHasProgress[taskId] = true;
        clearElapsedTimer(taskId);
        const parts = [];
        if (data.speed) parts.push(data.speed);
        if (data.eta) parts.push('ETA ' + data.eta);
        detailEl.textContent = parts.join(' · ');
    }

    if (data.status) {
        const statusMap = {
            'pending': { text: '等待中', cls: 'bg-gray-500/20 text-gray-400' },
            'downloading': { text: '下载中', cls: 'bg-blue-500/20 text-blue-400 animate-pulse-blue' },
            'completed': { text: '已完成', cls: 'bg-green-500/20 text-green-400' },
            'failed': { text: '失败', cls: 'bg-red-500/20 text-red-400' },
            'cancelled': { text: '已取消', cls: 'bg-gray-500/20 text-gray-400' }
        };
        const s = statusMap[data.status] || statusMap.pending;
        statusEl.className = 'queue-status text-xs px-2 py-0.5 rounded-full ' + s.cls;
        statusEl.textContent = s.text;

        // Record start time on first downloading event
        if (data.status === 'downloading' && !taskStartTimes[taskId]) {
            taskStartTimes[taskId] = Date.now();
        }

        if (data.status === 'completed') {
            progressEl.classList.remove('indeterminate');
            progressEl.style.width = '100%';
            percentEl.textContent = '100%';
            clearElapsedTimer(taskId);
            progressEl.classList.add('completed');
            cancelEl.classList.add('hidden');
            const revealEl = item.querySelector('.queue-reveal');
            if (revealEl) revealEl.classList.remove('hidden');
            if (data.filename) {
                linkEl.href = '/api/file/' + taskId;
                actionsEl.classList.remove('hidden');
            }
            // Replace speed/eta line with total duration + avg speed
            const endMs = data._completedAtMs || Date.now();
            const startMs = taskStartTimes[taskId];
            const bytes = taskFilesizes[taskId];
            if (startMs && bytes && endMs > startMs) {
                const durSec = (endMs - startMs) / 1000;
                const avg = bytes / durSec;
                detailEl.textContent = `总耗时: ${formatDuration(durSec)} · 平均速度: ${formatSpeed(avg)}`;
            } else {
                detailEl.textContent = '';
            }
        } else if (data.status === 'failed') {
            progressEl.classList.remove('indeterminate');
            clearElapsedTimer(taskId);
            progressEl.classList.add('failed');
            cancelEl.classList.add('hidden');
            const deleteEl = item.querySelector('.queue-delete');
            const resumeEl = item.querySelector('.queue-resume');
            if (deleteEl) deleteEl.classList.remove('hidden');
            if (resumeEl) resumeEl.classList.remove('hidden');
            if (data.error) detailEl.textContent = data.error;
        } else if (data.status === 'cancelled') {
            progressEl.classList.remove('indeterminate');
            clearElapsedTimer(taskId);
            cancelEl.classList.add('hidden');
            const deleteEl = item.querySelector('.queue-delete');
            const resumeEl = item.querySelector('.queue-resume');
            if (deleteEl) deleteEl.classList.remove('hidden');
            if (resumeEl) resumeEl.classList.remove('hidden');
        }
    }
}

function updateQueueCount() {
    const count = document.querySelectorAll('[id^="queue-"]').length - 1; // minus empty msg
    document.getElementById('queue-count').textContent = count > 0 ? count + ' 个任务' : '';
}

// ── SSE ──
function connectSSE(taskId) {
    if (sseConnections[taskId]) return;
    const es = new EventSource('/api/download/' + taskId + '/progress');

    es.addEventListener('progress', e => {
        const data = JSON.parse(e.data);
        updateQueueItem(taskId, data);
        if (data.status === 'completed' || data.status === 'failed' || data.status === 'cancelled') {
            es.close();
            delete sseConnections[taskId];
        }
    });

    es.addEventListener('ping', () => { /* heartbeat, ignore */ });

    es.onerror = () => {
        es.close();
        delete sseConnections[taskId];
    };

    sseConnections[taskId] = es;
}

// ── Clear All Queue ──
async function clearAllQueue() {
    if (!confirm('确定清空所有下载记录？\n这将取消正在进行的任务，删除数据库记录和已下载的文件。')) return;
    try {
        const res = await fetch('/api/downloads/all', { method: 'DELETE' });
        const data = await res.json();
        if (data.code !== 0) throw new Error(data.message);
        // Close all active SSE connections
        Object.values(sseConnections).forEach(es => es.close());
        sseConnections = {};
        // Clear the queue UI
        document.getElementById('queue-list').innerHTML =
            '<p id="queue-empty" class="text-center text-gray-600 text-sm py-8">暂无下载任务</p>';
        updateQueueCount();
        showToast(data.message || '已清空', 'success');
    } catch (e) {
        showToast(e.message || '清空失败', 'error');
    }
}

// ── Delete a single queue task ──
async function deleteQueueTask(taskId) {
    if (!confirm('确定删除此任务？')) return;
    try {
        const res = await fetch('/api/download/' + taskId, { method: 'DELETE' });
        const data = await res.json();
        if (data.code !== 0) throw new Error(data.message);
        if (sseConnections[taskId]) {
            sseConnections[taskId].close();
            delete sseConnections[taskId];
        }
        const item = document.getElementById('queue-' + taskId);
        if (item) item.remove();
        updateQueueCount();
        const list = document.getElementById('queue-list');
        if (list.querySelectorAll('[id^="queue-"]').length === 0) {
            document.getElementById('queue-empty').style.display = '';
        }
        showToast('已删除', 'success');
    } catch (e) {
        showToast(e.message || '删除失败', 'error');
    }
}

// ── Reveal file in OS file manager ──
async function revealFile(taskId) {
    try {
        const res = await fetch('/api/file/' + taskId + '/reveal', { method: 'POST' });
        const data = await res.json();
        if (data.code !== 0) throw new Error(data.message);
    } catch (e) {
        showToast(e.message || '打开失败', 'error');
    }
}

// ── Cancel ──
async function cancelTask(taskId) {
    try {
        await api('/api/download/' + taskId + '/cancel', {}, 'POST');
        updateQueueItem(taskId, { status: 'cancelled', progress: 0 });
    } catch (e) {
        showToast('取消失败', 'error');
    }
}

// ── Resume (for cancelled / failed tasks) ──
async function resumeTask(taskId) {
    try {
        const data = await api('/api/download/' + taskId + '/resume', {}, 'POST');
        if (data.code !== 0) { showToast(data.message || '继续下载失败', 'error'); return; }

        // Reset per-task UI state so the card looks like a fresh pending task.
        const item = document.getElementById('queue-' + taskId);
        if (item) {
            const resumeEl = item.querySelector('.queue-resume');
            const deleteEl = item.querySelector('.queue-delete');
            const cancelEl = item.querySelector('.queue-cancel');
            const progressEl = item.querySelector('.queue-progress');
            const detailEl = item.querySelector('.queue-detail');
            if (resumeEl) resumeEl.classList.add('hidden');
            if (deleteEl) deleteEl.classList.add('hidden');
            if (cancelEl) cancelEl.classList.remove('hidden');
            if (progressEl) progressEl.classList.remove('failed');
            if (detailEl) detailEl.textContent = '已等待 0s';
        }
        taskQueuedAt[taskId] = Date.now();
        taskHasProgress[taskId] = false;
        taskStartTimes[taskId] = null;
        taskPhaseText[taskId] = '排队中';
        clearElapsedTimer(taskId);
        taskElapsedTimers[taskId] = setInterval(() => {
            if (taskHasProgress[taskId]) { clearElapsedTimer(taskId); return; }
            const el = document.querySelector('#queue-' + taskId + ' .queue-detail');
            if (!el) { clearElapsedTimer(taskId); return; }
            const secs = Math.round((Date.now() - (taskQueuedAt[taskId] || Date.now())) / 1000);
            el.textContent = '已等待 ' + secs + 's';
        }, 1000);

        updateQueueItem(taskId, { status: 'pending', progress: 0, phase: 'queued', phase_text: '排队中' });
        connectSSE(taskId);
        showToast('已继续', 'success');
    } catch (e) {
        showToast('继续下载失败', 'error');
    }
}

// ── History ──
async function loadHistory() {
    const search = document.getElementById('history-search').value.trim();
    try {
        const res = await fetch(`/api/history?page=${historyPage}&per_page=20&q=${encodeURIComponent(search)}`);
        const data = await res.json();
        if (data.code !== 0) return;
        renderHistory(data.data);
    } catch (e) {
        console.error(e);
    }
}

function renderHistory(data) {
    const list = document.getElementById('history-list');
    const emptyHtml = '<p id="history-empty" class="text-center text-gray-600 text-sm py-8">暂无下载记录</p>';

    if (!data.items.length) {
        list.innerHTML = emptyHtml;
        document.getElementById('history-pagination').innerHTML = '';
        return;
    }

    list.innerHTML = data.items.map(t => {
        const statusMap = {
            completed: '<span class="text-green-400">✅ 完成</span>',
            failed: '<span class="text-red-400">❌ 失败</span>',
            cancelled: '<span class="text-gray-400">🚫 取消</span>',
            downloading: '<span class="text-blue-400">⬇️ 下载中</span>',
            pending: '<span class="text-gray-400">⏳ 等待</span>'
        };
        return `
        <div class="bg-dark-800 border border-dark-600 rounded-lg p-3 flex items-center justify-between gap-3">
            <div class="min-w-0 flex-1">
                <p class="text-sm text-white truncate">${escHtml(t.title || '未知')}</p>
                <div class="flex items-center gap-3 mt-1 text-xs text-gray-500">
                    ${statusMap[t.status] || t.status}
                    <span>${t.format_note || ''}</span>
                    <span>${t.created_at || ''}</span>
                </div>
            </div>
            <div class="flex gap-2 flex-shrink-0">
                ${t.status === 'completed' && t.filename ? `<button onclick="revealFile('${t.id}')" class="text-xs text-gray-400 hover:text-blue-400">📂 位置</button>` : ''}
                ${t.status === 'completed' && t.filename ? `<a href="/api/file/${t.id}" class="text-xs text-blue-400 hover:text-blue-300">💾 下载</a>` : ''}
                <button onclick="deleteTask('${t.id}')" class="text-xs text-gray-500 hover:text-red-400">🗑</button>
            </div>
        </div>`;
    }).join('');

    // Pagination
    const totalPages = Math.ceil(data.total / data.per_page);
    const pag = document.getElementById('history-pagination');
    if (totalPages <= 1) { pag.innerHTML = ''; return; }
    let html = '';
    for (let i = 1; i <= totalPages; i++) {
        html += `<button onclick="historyPage=${i};loadHistory()" class="px-3 py-1 rounded text-xs ${i === data.page ? 'bg-blue-600 text-white' : 'bg-dark-700 text-gray-400 hover:text-white'}">${i}</button>`;
    }
    pag.innerHTML = html;
}

async function deleteTask(taskId) {
    if (!confirm('确定删除此记录？')) return;
    try {
        await fetch('/api/download/' + taskId, { method: 'DELETE' });
        loadHistory();
        showToast('已删除', 'success');
    } catch (e) {
        showToast('删除失败', 'error');
    }
}

// ── Settings: health check ──
async function loadHealth() {
    try {
        const res = await fetch('/api/health');
        const data = await res.json();
        if (data.code !== 0) return;
        const fmt = (h) => `${h.ok ? '✅' : '❌'} ${h.version}`;
        document.getElementById('health-ytdlp').textContent = fmt(data.data.ytdlp);
        document.getElementById('health-node').textContent = fmt(data.data.node);
        document.getElementById('health-pot').textContent = fmt(data.data.pot_provider);
        if (data.data.ffmpeg) {
            document.getElementById('health-ffmpeg').textContent = fmt(data.data.ffmpeg);
        }
    } catch (e) {}
}
loadHealth();

// ── Load active tasks on page load ──
(async () => {
    try {
        const res = await fetch('/api/active');
        const data = await res.json();
        if (data.code === 0 && data.data.length) {
            data.data.forEach(t => {
                addQueueItem(t.id, t.title, t.format_note, t.filesize);
                // Reconstruct duration for already-completed tasks loaded after refresh
                if (t.status === 'completed' && t.created_at && t.completed_at) {
                    const startMs = new Date(t.created_at.replace(' ', 'T')).getTime();
                    const endMs = new Date(t.completed_at.replace(' ', 'T')).getTime();
                    if (isFinite(startMs) && isFinite(endMs) && endMs > startMs) {
                        taskStartTimes[t.id] = startMs;
                        taskFilesizes[t.id] = t.filesize;
                        // Pass endMs so updateQueueItem uses it as completion timestamp
                        updateQueueItem(t.id, {
                            status: t.status,
                            progress: t.progress || 0,
                            filename: t.filename,
                            error: t.error,
                            _completedAtMs: endMs
                        });
                        return;
                    }
                }
                updateQueueItem(t.id, {
                    status: t.status,
                    progress: t.progress || 0,
                    speed: t.speed,
                    eta: t.eta,
                    filename: t.filename,
                    error: t.error
                });
                if (t.status === 'downloading' || t.status === 'pending') {
                    connectSSE(t.id);
                }
            });
        }
    } catch (e) {}
})();

// ── Utilities ──
async function api(url, body, method = 'POST') {
    const res = await fetch(url, {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
    return res.json();
}

function show(id) { document.getElementById(id).classList.remove('hidden'); }
function hide(id) { document.getElementById(id).classList.add('hidden'); }
function escHtml(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function showToast(msg, type = 'info', duration = 3000) {
    const container = document.getElementById('toast-container');
    const colors = { success: 'bg-green-600', error: 'bg-red-600', info: 'bg-blue-600' };
    const toast = document.createElement('div');
    toast.className = `toast ${colors[type] || colors.info} text-white text-sm px-4 py-2.5 rounded-lg shadow-lg`;
    toast.textContent = msg;
    container.appendChild(toast);
    if (duration > 0) {
        setTimeout(() => { toast.style.opacity = '0'; toast.style.transition = 'opacity 0.3s'; setTimeout(() => toast.remove(), 300); }, duration);
    }
    return toast;
}
