import subprocess
import json
import re
import os
import threading
import time
from collections import OrderedDict, deque
from config import YT_DLP_PATH, DOWNLOAD_DIR, ARIA2C_PATH, ARIA2C_CONNECTIONS, FFMPEG_PATH, load_settings
from logger import get_logger

# Auto-refresh PO token when in-flight speed degrades. Pot-provider's
# integrityToken gets "aged" over time and googlevideo starts throttling
# the session. Refresh policy: kill yt-dlp, invalidate pot caches, respawn
# yt-dlp (which resumes via the default .part continue behavior). Constants
# tuned against the 100M chunk size; 30 s window matches the long-session
# throttle onset described in yt-dlp-speed-journey.md ch.4.
SPEED_WINDOW_SEC = 30.0
SPEED_SLOW_ABS_MIBPS = 2.0      # absolute floor — any slower triggers refresh
SPEED_DECAY_FRAC = 0.5          # relative floor — < 0.5 * initial_avg triggers
REFRESH_COOLDOWN_SEC = 60.0     # minimum gap between two refresh attempts
REFRESH_MAX_PER_STAGE = 3       # hard cap on auto-refreshes per yt-dlp stage

_SPEED_PARSE_RE = re.compile(r'^([\d.]+)\s*(B|KiB|MiB|GiB)/s$')
_SPEED_UNIT_TO_MIB = {'B': 1 / 1048576.0, 'KiB': 1 / 1024.0, 'MiB': 1.0, 'GiB': 1024.0}


def _parse_speed_mibps(speed_str):
    """Convert a yt-dlp speed string like '5.45MiB/s' to MiB/s float.
    Returns None if the string is unparsable (e.g. 'Unknown B/s')."""
    if not speed_str:
        return None
    m = _SPEED_PARSE_RE.match(speed_str.strip())
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    factor = _SPEED_UNIT_TO_MIB.get(m.group(2))
    if factor is None:
        return None
    return val * factor

log = get_logger()

# ── Metadata cache ──────────────────────────────────
_METADATA_CACHE: "OrderedDict[str, tuple[float, dict]]" = OrderedDict()
_METADATA_TTL_SEC = 600  # 10 minutes
_METADATA_CACHE_MAX = 200
_METADATA_LOCK = threading.Lock()

_VIDEO_ID_RE = re.compile(r'(?:v=|/shorts/|youtu\.be/|/embed/)([\w-]{11})')


def _extract_video_id(url: str):
    m = _VIDEO_ID_RE.search(url)
    return m.group(1) if m else None


def _cache_get(video_id: str):
    with _METADATA_LOCK:
        entry = _METADATA_CACHE.get(video_id)
        if entry is None:
            return None
        ts, data = entry
        if time.time() - ts > _METADATA_TTL_SEC:
            _METADATA_CACHE.pop(video_id, None)
            return None
        _METADATA_CACHE.move_to_end(video_id)
        return data


def _cache_put(video_id: str, data: dict):
    with _METADATA_LOCK:
        _METADATA_CACHE[video_id] = (time.time(), data)
        _METADATA_CACHE.move_to_end(video_id)
        while len(_METADATA_CACHE) > _METADATA_CACHE_MAX:
            _METADATA_CACHE.popitem(last=False)

# Known YouTube audio-only itag IDs. Any format_id matching one of these
# (optionally with -drc suffix) is handled by bare yt-dlp single connection.
# Rationale: bench_audio_parallelism.py proved that googlevideo applies a
# hard per-connection playback-rate throttle to audio streams the moment it
# sees ≥2 parallel TCP connections. Observed:
#   x=1 (bare yt-dlp)   → 4140 KiB/s  ← cold-start bucket, fast
#   x=2 (aria2c 2 conn) →   82 KiB/s  ← per-conn locked to ~40 KiB/s
#   x=16                →  ~640 KiB/s ← (16 × 40, seen in production)
# Going multi-connection on audio is a 50x negative optimization.
_AUDIO_ITAGS = {'139', '140', '141', '171', '249', '250', '251'}


def is_audio_only_format(format_id):
    """True if format_id is a bare audio-only YouTube itag."""
    if not format_id or '+' in format_id:
        return False
    # Strip optional -drc (dynamic range compression) suffix
    base = format_id.split('-', 1)[0]
    return base in _AUDIO_ITAGS


# Progress regex patterns
PROGRESS_RE = re.compile(
    r'\[download\]\s+([\d.]+)%\s+of\s+~?\s*([\d.]+\s*\w+)\s+at\s+([\d.]+\s*\w+/s)\s+ETA\s+(\S+)'
)
PROGRESS_SIMPLE_RE = re.compile(r'\[download\]\s+([\d.]+)%')
# aria2c summary format: [#abc 45MiB/100MiB(45%) CN:16 DL:5.2MiB ETA:10s]
ARIA2C_RE = re.compile(
    r'\[#\w+\s+[\d.]+\w+/([\d.]+\w+)\((\d+)%\).*?DL:([\d.]+\w+)(?:\s+ETA:(\S+))?\]'
)
DEST_RE = re.compile(r'\[download\] Destination:\s+(.+)')
MERGE_RE = re.compile(r'\[Merger\]|Finalpath:\s*(.+)|has already been downloaded')
ERROR_RE = re.compile(r'ERROR:\s*(.+)')

# Cold-start phase detection. Order matters: first match wins. Each entry is
# (substring_or_regex, phase_code, human_text). Substrings are cheaper and
# sufficient for the patterns yt-dlp emits.
_PHASE_RULES = [
    ('youtubepot-bgutilhttp', 'pot', '获取 PO Token'),
    ('Fetching PO Token',     'pot', '获取 PO Token'),
    ('bgutil',                'pot', '获取 PO Token'),
    ('Downloading webpage',   'extracting', '解析视频信息'),
    ('Downloading tv ',       'player', '获取播放器数据'),
    ('Downloading ios ',      'player', '获取播放器数据'),
    ('Downloading android ',  'player', '获取播放器数据'),
    ('Downloading web ',      'player', '获取播放器数据'),
    ('player API JSON',       'player', '获取播放器数据'),
    ('Downloading player',    'player', '获取播放器数据'),
    ('Downloading m3u8',      'manifest', '拉取媒体清单'),
    ('Downloading MPD',       'manifest', '拉取媒体清单'),
    ('Downloading manifest',  'manifest', '拉取媒体清单'),
    ('[download] Destination:', 'starting', '开始下载'),
    ('[Merger]',              'merging', '合并音视频'),
    ('[FixupM3u8]',           'merging', '合并音视频'),
    ('Deleting original file', 'merging', '合并音视频'),
    ('Retrying',              'retry', '重试中…'),
    ('Sleeping',              'retry', '重试中…'),
    # [info] must come late so more specific rules (player/webpage) win first.
    ('[info]',                'formats', '准备下载格式'),
]


def classify_phase(line):
    """Map a yt-dlp stdout/stderr line to a (phase_code, phase_text) tuple,
    or None if the line doesn't correspond to a tracked cold-start phase."""
    for needle, phase, text in _PHASE_RULES:
        if needle in line:
            return phase, text
    return None


def _cookie_args():
    """Build yt-dlp cookie arguments from settings."""
    settings = load_settings()
    mode = settings.get('cookie_mode', 'none')
    if mode == 'browser':
        browser = settings.get('cookie_browser', 'chrome')
        return ['--cookies-from-browser', browser]
    elif mode == 'file':
        cookie_file = settings.get('cookie_file', '')
        if cookie_file and os.path.exists(cookie_file):
            return ['--cookies', cookie_file]
    return []


def fetch_metadata(url):
    """Run yt-dlp --dump-json and return parsed metadata with processed formats."""
    video_id = _extract_video_id(url)
    if video_id:
        cached = _cache_get(video_id)
        if cached is not None:
            log.info(f"fetch_metadata cache hit | video_id={video_id}")
            return cached

    cmd = [
        YT_DLP_PATH, '--dump-json', '--no-playlist', '--no-warnings',
        '-v',  # needed to emit [youtube]/[info] phase lines on stderr in dump-json mode
        '--js-runtimes', 'node',
        '--extractor-args', 'youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416',
    ] + _cookie_args() + [url]
    t0 = time.monotonic()
    log.info(f"fetch_metadata start | video_id={video_id} | url={url}")

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1
    )
    stdout_lines = []
    stderr_lines = []
    phase_re = re.compile(r'^\[(?:youtube|info|jsc:|download)')

    def _reader(stream, target):
        for line in iter(stream.readline, ''):
            s = line.rstrip('\n')
            target.append(s)
            if phase_re.match(s):
                rel = time.monotonic() - t0
                log.info(f"  +{rel:5.2f}s {s}")
        stream.close()

    t_out = threading.Thread(target=_reader, args=(proc.stdout, stdout_lines), daemon=True)
    t_err = threading.Thread(target=_reader, args=(proc.stderr, stderr_lines), daemon=True)
    t_out.start()
    t_err.start()

    try:
        proc.wait(timeout=120)
    except subprocess.TimeoutExpired:
        proc.kill()
        dt = time.monotonic() - t0
        log.error(f"fetch_metadata timeout after {dt:.2f}s | video_id={video_id}")
        raise Exception('解析超时(120s),网络可能有问题')

    t_out.join(timeout=2)
    t_err.join(timeout=2)
    dt = time.monotonic() - t0

    if proc.returncode != 0:
        err_text = '\n'.join(stderr_lines)
        error_msg = err_text
        for line in stderr_lines:
            if 'ERROR' in line:
                error_msg = line
                break
        log.warning(f"fetch_metadata failed in {dt:.2f}s | video_id={video_id} | err={error_msg[:200]}")
        raise Exception(error_msg or '无法获取视频信息')

    log.info(f"fetch_metadata done in {dt:.2f}s | video_id={video_id}")

    # Find the JSON payload (yt-dlp --dump-json emits a single JSON line on stdout)
    json_line = next((l for l in stdout_lines if l.startswith('{')), None)
    if not json_line:
        raise Exception('无法解析 yt-dlp 输出(未找到 JSON)')
    data = json.loads(json_line)

    duration = data.get('duration') or 0
    minutes, seconds = divmod(int(duration), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        duration_str = f'{hours}:{minutes:02d}:{seconds:02d}'
    else:
        duration_str = f'{minutes}:{seconds:02d}'

    meta = {
        'title': data.get('title', '未知标题'),
        'thumbnail': data.get('thumbnail', ''),
        'duration': duration,
        'duration_str': duration_str,
        'uploader': data.get('uploader') or data.get('channel') or '未知',
        'url': data.get('webpage_url', url),
        'formats': process_formats(data.get('formats', []))
    }
    if video_id:
        _cache_put(video_id, meta)
    return meta


def _format_size(size_bytes):
    if not size_bytes:
        return ''
    for unit in ('B', 'KiB', 'MiB', 'GiB'):
        if abs(size_bytes) < 1024:
            return f'{size_bytes:.1f} {unit}'
        size_bytes /= 1024
    return f'{size_bytes:.1f} TiB'


def _resolution_label(f):
    h = f.get('height')
    if not h:
        return None
    if h >= 2160:
        return '2160p (4K)'
    elif h >= 1440:
        return '1440p (2K)'
    elif h >= 1080:
        return '1080p'
    elif h >= 720:
        return '720p'
    elif h >= 480:
        return '480p'
    elif h >= 360:
        return '360p'
    elif h >= 240:
        return '240p'
    else:
        return f'{h}p'


def process_formats(raw_formats):
    """Convert yt-dlp format list into user-friendly grouped options."""
    # Separate formats
    combined = []  # has both video + audio
    video_only = []
    audio_only = []

    for f in raw_formats:
        vcodec = f.get('vcodec', 'none')
        acodec = f.get('acodec', 'none')
        if vcodec == 'none' and acodec == 'none':
            continue
        # Skip formats without a direct URL (e.g. SABR-restricted on YouTube)
        if not f.get('url'):
            continue
        if vcodec != 'none' and acodec != 'none':
            combined.append(f)
        elif vcodec != 'none':
            video_only.append(f)
        else:
            audio_only.append(f)

    # Find best audio track for merging
    best_audio = None
    if audio_only:
        best_audio = max(audio_only, key=lambda x: x.get('abr') or x.get('tbr') or 0)

    results = []
    seen_resolutions = set()

    # Process video-only + best audio (these are usually higher quality)
    if best_audio:
        # Sort by height descending
        video_only.sort(key=lambda x: x.get('height') or 0, reverse=True)
        for vf in video_only:
            label = _resolution_label(vf)
            if not label or label in seen_resolutions:
                continue
            seen_resolutions.add(label)

            combined_id = f"{vf['format_id']}+{best_audio['format_id']}"
            vcodec_short = (vf.get('vcodec') or '').split('.')[0]
            acodec_short = (best_audio.get('acodec') or '').split('.')[0]
            ext = vf.get('ext', 'mp4')
            vsize = vf.get('filesize') or vf.get('filesize_approx') or 0
            asize = best_audio.get('filesize') or best_audio.get('filesize_approx') or 0
            total_size = (vsize + asize) if (vsize and asize) else None

            results.append({
                'format_id': combined_id,
                'resolution': label,
                'ext': ext,
                'height': vf.get('height', 0),
                'filesize': total_size,
                'filesize_str': _format_size(total_size),
                'vcodec': vcodec_short,
                'acodec': acodec_short,
                'fps': vf.get('fps'),
                'note': f"{label} {ext} ({vcodec_short}+{acodec_short})",
                'type': 'video'
            })

    # Process pre-combined formats (lower priority, fill gaps)
    combined.sort(key=lambda x: x.get('height') or 0, reverse=True)
    for f in combined:
        label = _resolution_label(f)
        if not label or label in seen_resolutions:
            continue
        seen_resolutions.add(label)

        vcodec_short = (f.get('vcodec') or '').split('.')[0]
        acodec_short = (f.get('acodec') or '').split('.')[0]
        fsize = f.get('filesize') or f.get('filesize_approx')

        results.append({
            'format_id': f['format_id'],
            'resolution': label,
            'ext': f.get('ext', 'mp4'),
            'height': f.get('height', 0),
            'filesize': fsize,
            'filesize_str': _format_size(fsize),
            'vcodec': vcodec_short,
            'acodec': acodec_short,
            'fps': f.get('fps'),
            'note': f"{label} {f.get('ext', 'mp4')} ({vcodec_short}+{acodec_short})",
            'type': 'video'
        })

    # Add best audio-only option
    if best_audio:
        asize = best_audio.get('filesize') or best_audio.get('filesize_approx')
        acodec_short = (best_audio.get('acodec') or '').split('.')[0]
        abr = best_audio.get('abr') or best_audio.get('tbr') or 0
        results.append({
            'format_id': best_audio['format_id'],
            'resolution': f'仅音频 ({int(abr)}kbps)',
            'ext': best_audio.get('ext', 'm4a'),
            'height': 0,
            'filesize': asize,
            'filesize_str': _format_size(asize),
            'vcodec': '',
            'acodec': acodec_short,
            'fps': None,
            'note': f"仅音频 {acodec_short} {int(abr)}kbps",
            'type': 'audio'
        })

    # Sort: video by height desc, audio at end
    results.sort(key=lambda x: (x['type'] == 'audio', -(x.get('height') or 0)))
    return results


def parse_progress_line(line):
    """Parse a yt-dlp stdout line into progress data."""
    # Full progress line
    m = PROGRESS_RE.search(line)
    if m:
        return {
            'progress': float(m.group(1)),
            'filesize_str': m.group(2).strip(),
            'speed': m.group(3).strip(),
            'eta': m.group(4).strip(),
            'status': 'downloading'
        }

    # Simple percentage only
    m = PROGRESS_SIMPLE_RE.search(line)
    if m:
        return {
            'progress': float(m.group(1)),
            'status': 'downloading'
        }

    # aria2c summary line
    m = ARIA2C_RE.search(line)
    if m:
        return {
            'progress': float(m.group(2)),
            'filesize_str': m.group(1).strip(),
            'speed': m.group(3).strip() + '/s',
            'eta': (m.group(4) or '').strip(),
            'status': 'downloading'
        }

    # Merge line — only used to capture the final filepath for run_download.
    # Do NOT emit status='completed' here: that is the authoritative event
    # the worker fires after yt-dlp exits, carrying the 'filename' field.
    # Emitting it early (without filename) makes the SSE client close the
    # stream before the real completion event arrives.
    m = MERGE_RE.search(line)
    if m:
        filepath = m.group(1) if m.group(1) else None
        return {
            'progress': 100,
            'filepath': filepath
        }

    return None


def _run_ytdlp_stage(task_id, url, format_id, output_template, use_aria2c,
                     stage_phase, stage_phase_text, on_progress, cancel_event):
    """
    Run one yt-dlp invocation for a single (non-merged) format_id.
    Returns (ok: bool, filepath_or_None, error_str_or_None).

    Progress lines are forwarded via on_progress with phase_text forced to
    `stage_phase_text` so the UI shows what substage is running. `stage_phase`
    is the stable phase code used for dedup.
    """
    cmd = [
        YT_DLP_PATH,
        '-f', format_id,
        '--newline',
        '--no-playlist',
        '--windows-filenames',
        '--restrict-filenames',
        '--retries', '10',
        '--fragment-retries', '10',
        '--socket-timeout', '30',
        '--concurrent-fragments', '16',
        # Google GFE rejects aria2c's TLS ClientHello regardless of backend
        # (AppleTLS/GnuTLS/OpenSSL all fail at handshake). curl_cffi's
        # impersonated Chrome fingerprint passes. See yt-dlp issue #9706:
        # --impersonate only applies to yt-dlp's own requests, NOT to
        # external downloaders like aria2c — so we must also avoid
        # --downloader aria2c for any googlevideo stream.
        '--impersonate', 'chrome',
        '--js-runtimes', 'node',
        '--extractor-args', 'youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416',
        '-o', output_template,
    ]
    if use_aria2c and ARIA2C_PATH:
        # Kept for future non-Google downloads. No caller currently passes
        # use_aria2c=True because aria2c can't handshake to googlevideo.
        cmd += [
            '--downloader', ARIA2C_PATH,
            '--downloader-args',
            f'aria2c:-x {ARIA2C_CONNECTIONS} -s 4096 -k 1M '
            f'--summary-interval=1 --console-log-level=warn',
        ]
    else:
        # 100 MB chunks: safe because the speed-monitoring auto-refresh loop
        # below will catch long-session throttle and respawn yt-dlp with a
        # fresh PO token, sidestepping the "big chunks get throttled" problem
        # we saw in yt-dlp-web ch.4-5.
        cmd += ['--http-chunk-size', '100M']
    cmd += _cookie_args() + [url]

    # Emit the stage phase up front so the UI switches immediately, before
    # any progress line arrives. Kept outside the respawn loop so a
    # mid-stage refresh doesn't erase the caller-set stage label.
    on_progress({'phase': stage_phase, 'phase_text': stage_phase_text, 'progress': 0})

    # Stage-level state that must survive across respawns.
    final_filepath = [None]
    refresh_count = 0
    last_refresh_at = 0.0
    initial_avg_mibps = None  # locked after first SPEED_WINDOW_SEC of samples

    while True:
        log.info(f"[{task_id}] [{stage_phase}] spawning yt-dlp"
                 f"{' (respawn #' + str(refresh_count) + ' after pot refresh)' if refresh_count else ''}"
                 f": {' '.join(cmd)}")
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1
        )
        log.debug(f"[{task_id}] [{stage_phase}] yt-dlp pid={process.pid}")

        # Per-process state: reset on each respawn.
        stderr_lines = []
        # Startup phase tracking: between yt-dlp spawn and the "[download]
        # Destination:" line, yt-dlp goes through several extractor substages
        # (Fetching PO Token → Downloading player API → Downloading manifest …).
        # We surface those to the UI via classify_phase(). Once the real
        # download starts, we flip in_download_stage and stop honoring
        # classify_phase so the phase_text stays pinned to stage_phase_text.
        in_download_stage = [False]
        last_startup_phase = [None]
        # Sliding window of (timestamp, MiB/s) samples for auto-refresh.
        speed_samples = deque()
        trigger_refresh = False

        def _maybe_emit_startup_phase(text_line):
            if in_download_stage[0]:
                return
            result = classify_phase(text_line)
            if result is None:
                return
            phase_code, phase_text = result
            # '[download] Destination:' is handled by DEST_RE below; skip the
            # classify_phase 'starting' rule to avoid a redundant flip.
            if phase_code == 'starting':
                return
            if phase_code == last_startup_phase[0]:
                return
            last_startup_phase[0] = phase_code
            on_progress({'phase': phase_code, 'phase_text': phase_text})

        def _read_stderr():
            for line in iter(process.stderr.readline, ''):
                s = line.strip()
                if s:
                    stderr_lines.append(s)
                    log.debug(f"[{task_id}] [{stage_phase}] stderr: {s}")
                    _maybe_emit_startup_phase(s)
            process.stderr.close()

        stderr_thread = threading.Thread(target=_read_stderr, daemon=True)
        stderr_thread.start()

        try:
            for line in iter(process.stdout.readline, ''):
                if cancel_event.is_set():
                    log.info(f"[{task_id}] [{stage_phase}] cancel_event set, terminating")
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    return False, None, '用户取消'

                line = line.strip()
                if not line:
                    continue
                log.debug(f"[{task_id}] [{stage_phase}] stdout: {line}")

                _maybe_emit_startup_phase(line)

                m = DEST_RE.match(line)
                if m:
                    final_filepath[0] = m.group(1).strip()
                    log.info(f"[{task_id}] [{stage_phase}] destination: {final_filepath[0]}")
                    in_download_stage[0] = True
                    on_progress({'phase': stage_phase, 'phase_text': stage_phase_text, 'progress': 0})
                    continue

                progress = parse_progress_line(line)
                if progress:
                    if progress.get('filepath'):
                        final_filepath[0] = progress['filepath']
                    # Stamp this stage's phase_text on every progress update
                    # so the UI never reverts to the stale "pot refresh" text.
                    progress['phase'] = stage_phase
                    progress['phase_text'] = stage_phase_text
                    on_progress(progress)

                    # Auto-refresh speed monitor: only track after real
                    # download has started and we have a parseable speed.
                    if in_download_stage[0] and progress.get('speed'):
                        mibps = _parse_speed_mibps(progress['speed'])
                        if mibps is not None:
                            now = time.monotonic()
                            speed_samples.append((now, mibps))
                            # Prune samples older than the window.
                            while speed_samples and (now - speed_samples[0][0]) > SPEED_WINDOW_SEC:
                                speed_samples.popleft()

                            current_avg = sum(s[1] for s in speed_samples) / len(speed_samples)
                            window_span = now - speed_samples[0][0]

                            # Lock the baseline once we have a full window.
                            if initial_avg_mibps is None and window_span >= SPEED_WINDOW_SEC - 1.0:
                                initial_avg_mibps = current_avg
                                log.info(f"[{task_id}] [{stage_phase}] "
                                         f"initial {SPEED_WINDOW_SEC:.0f}s avg locked: "
                                         f"{initial_avg_mibps:.2f} MiB/s")

                            # Only evaluate triggers once the baseline exists,
                            # the cooldown has elapsed, and we still have
                            # retries left. We require a full window of fresh
                            # samples so a brief dip doesn't fire.
                            if (initial_avg_mibps is not None
                                    and window_span >= SPEED_WINDOW_SEC - 1.0
                                    and refresh_count < REFRESH_MAX_PER_STAGE
                                    and (now - last_refresh_at) > REFRESH_COOLDOWN_SEC):
                                below_relative = current_avg < SPEED_DECAY_FRAC * initial_avg_mibps
                                below_absolute = current_avg < SPEED_SLOW_ABS_MIBPS
                                if below_relative or below_absolute:
                                    reason = []
                                    if below_relative:
                                        reason.append(
                                            f"{current_avg:.2f} < {SPEED_DECAY_FRAC:.0%} of initial "
                                            f"{initial_avg_mibps:.2f}")
                                    if below_absolute:
                                        reason.append(f"{current_avg:.2f} < {SPEED_SLOW_ABS_MIBPS} MiB/s")
                                    log.warning(
                                        f"[{task_id}] [{stage_phase}] speed degraded "
                                        f"({'; '.join(reason)}) → auto-refresh PO token")
                                    trigger_refresh = True
                                    last_refresh_at = now
                                    break  # exit stdout loop; handled below

            # Exited stdout loop. Drain stderr and reap the process.
            process.stdout.close()
            stderr_thread.join(timeout=10)

            if trigger_refresh:
                # Kill the current yt-dlp, refresh pot caches, loop to respawn.
                # yt-dlp's default --continue behavior picks up the .part file.
                try:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                except Exception:
                    pass

                try:
                    import pot_provider
                    ok_ref = pot_provider.invalidate_caches()
                    log.info(f"[{task_id}] [{stage_phase}] mid-stage pot refresh -> {ok_ref}")
                except Exception as e:
                    log.warning(f"[{task_id}] [{stage_phase}] pot refresh failed: {e}")

                refresh_count += 1
                initial_avg_mibps = None  # rebaseline against fresh token
                on_progress({
                    'phase': 'pot_refresh',
                    'phase_text': f'自动刷新 PO Token (#{refresh_count})',
                })
                continue  # respawn yt-dlp

            process.wait()
            log.info(f"[{task_id}] [{stage_phase}] yt-dlp exited rc={process.returncode}")

            if process.returncode != 0:
                stderr_text = '\n'.join(stderr_lines)
                m = ERROR_RE.search(stderr_text)
                error_msg = m.group(1) if m else stderr_text.strip()[:200]
                log.error(f"[{task_id}] [{stage_phase}] yt-dlp error: {error_msg}")
                return False, None, error_msg or '下载失败'

            # Normal successful exit.
            return True, final_filepath[0], None

        except Exception as e:
            log.exception(f"[{task_id}] [{stage_phase}] exception while reading yt-dlp output")
            try:
                process.kill()
            except OSError:
                pass
            return False, None, str(e)


def _mux_streams(task_id, video_path, audio_path, on_progress, cancel_event):
    """
    ffmpeg -c copy mux video + audio into one container. Returns
    (ok, final_path, error). Picks mp4 when both inputs are mp4-family,
    otherwise mkv (accepts vp9/av1/opus/etc.).
    """
    base = os.path.basename(video_path)
    # Strip the ".video.<ext>" suffix we added at download time.
    m = re.match(r'(.+)\.video\.[^.]+$', base)
    stem = m.group(1) if m else os.path.splitext(base)[0]

    v_ext = os.path.splitext(video_path)[1].lower()
    a_ext = os.path.splitext(audio_path)[1].lower()
    if v_ext == '.mp4' and a_ext in ('.m4a', '.mp4'):
        final_ext = '.mp4'
    else:
        final_ext = '.mkv'
    final_path = os.path.join(DOWNLOAD_DIR, stem + final_ext)

    on_progress({'phase': 'merging', 'phase_text': '合并音视频', 'progress': 100})
    cmd = [
        FFMPEG_PATH, '-y',
        '-i', video_path,
        '-i', audio_path,
        '-c', 'copy',
        '-map', '0:v:0',
        '-map', '1:a:0',
        '-loglevel', 'error',
        final_path,
    ]
    log.info(f"[{task_id}] [mux] ffmpeg: {' '.join(cmd)}")
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
    except FileNotFoundError:
        return False, None, 'ffmpeg 未安装,无法合并音视频'

    while proc.poll() is None:
        if cancel_event.is_set():
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
            return False, None, '用户取消'
        time.sleep(0.2)

    _, err = proc.communicate()
    if proc.returncode != 0:
        log.error(f"[{task_id}] [mux] ffmpeg rc={proc.returncode} err={err[:400]}")
        return False, None, f'ffmpeg 合并失败: {err.strip()[:200] or proc.returncode}'

    # Success: remove the intermediate files to save disk.
    for p in (video_path, audio_path):
        try:
            os.remove(p)
        except OSError as e:
            log.warning(f"[{task_id}] [mux] failed to remove {p}: {e}")

    log.info(f"[{task_id}] [mux] final: {final_path}")
    return True, final_path, None


def run_download(task_id, url, format_id, on_progress, cancel_event):
    """
    Run yt-dlp download. Calls on_progress(data_dict) for each update.
    cancel_event is a threading.Event that signals cancellation.
    Returns (success: bool, result: dict).

    Merged formats (e.g. "616+140") are split into two sequential stages:
    video with aria2c multi-connection, then audio with bare yt-dlp single
    connection. This avoids the googlevideo audio-stream throttle/SSL-reset
    that kicks in the instant ≥2 parallel TCPs hit an audio itag.
    """
    # Audio-only selection: one bare stage.
    if is_audio_only_format(format_id):
        log.info(f"[{task_id}] audio-only format {format_id} — single bare stage")
        tmpl = os.path.join(DOWNLOAD_DIR, '%(title)s [%(id)s].%(ext)s')
        ok, path, err = _run_ytdlp_stage(
            task_id, url, format_id, tmpl,
            use_aria2c=False,
            stage_phase='downloading_audio',
            stage_phase_text='下载音频流',
            on_progress=on_progress, cancel_event=cancel_event,
        )
        if not ok:
            return False, {'error': err or '下载失败'}
        return True, {'filename': _resolve_filename(path)}

    # Non-merged video-only format: one bare yt-dlp stage with impersonate.
    if '+' not in format_id:
        log.info(f"[{task_id}] single-format {format_id} — one impersonate stage")
        tmpl = os.path.join(DOWNLOAD_DIR, '%(title)s [%(id)s].%(ext)s')
        ok, path, err = _run_ytdlp_stage(
            task_id, url, format_id, tmpl,
            use_aria2c=False,
            stage_phase='downloading_video',
            stage_phase_text='下载视频流',
            on_progress=on_progress, cancel_event=cancel_event,
        )
        if not ok:
            return False, {'error': err or '下载失败'}
        return True, {'filename': _resolve_filename(path)}

    # Merged format: video stage → PO refresh → audio stage → mux.
    # Both stages use yt-dlp impersonate (no aria2c — Google TLS fingerprint
    # blocks it). Mid-stage PO refresh still matters to reset the googlevideo
    # throttle state machine before the audio session starts.
    video_id, audio_id = format_id.split('+', 1)
    log.info(f"[{task_id}] merged format {format_id} → split: "
             f"video={video_id} + audio={audio_id} (both impersonate chrome)")

    video_tmpl = os.path.join(DOWNLOAD_DIR, '%(title)s [%(id)s].video.%(ext)s')
    ok, video_path, err = _run_ytdlp_stage(
        task_id, url, video_id, video_tmpl,
        use_aria2c=False,
        stage_phase='downloading_video',
        stage_phase_text='下载视频流',
        on_progress=on_progress, cancel_event=cancel_event,
    )
    if not ok:
        return False, {'error': err or '视频下载失败'}
    if not video_path or not os.path.exists(video_path):
        return False, {'error': '视频流下载完成但未找到文件'}

    # Refresh pot-provider's integrityToken before the audio stage. After a
    # long video download the minter cache is aged and googlevideo will
    # throttle the audio session to ~30 KiB/s per connection.
    try:
        import pot_provider
        ok_ref = pot_provider.invalidate_caches()
        log.info(f"[{task_id}] pot refresh between stages -> {ok_ref}")
        on_progress({'phase': 'pot_refresh', 'phase_text': '刷新 PO Token (音频阶段)'})
    except Exception as e:
        log.warning(f"[{task_id}] pot refresh failed: {e}")

    audio_tmpl = os.path.join(DOWNLOAD_DIR, '%(title)s [%(id)s].audio.%(ext)s')
    ok, audio_path, err = _run_ytdlp_stage(
        task_id, url, audio_id, audio_tmpl,
        use_aria2c=False,
        stage_phase='downloading_audio',
        stage_phase_text='下载音频流',
        on_progress=on_progress, cancel_event=cancel_event,
    )
    if not ok:
        return False, {'error': err or '音频下载失败'}
    if not audio_path or not os.path.exists(audio_path):
        return False, {'error': '音频流下载完成但未找到文件'}

    ok, final_path, err = _mux_streams(
        task_id, video_path, audio_path, on_progress, cancel_event
    )
    if not ok:
        return False, {'error': err or '合并失败'}

    return True, {'filename': os.path.basename(final_path)}


def _resolve_filename(hinted_path):
    """Return basename from hinted_path if it exists, else most-recent file in DOWNLOAD_DIR."""
    if hinted_path and os.path.exists(hinted_path):
        return os.path.basename(hinted_path)
    try:
        files = [(f, os.path.getmtime(os.path.join(DOWNLOAD_DIR, f)))
                 for f in os.listdir(DOWNLOAD_DIR)
                 if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))]
    except OSError:
        return None
    if not files:
        return None
    return max(files, key=lambda x: x[1])[0]
