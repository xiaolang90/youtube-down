import subprocess
import json
import re
import os
import threading
import time
from collections import OrderedDict
from config import YT_DLP_PATH, DOWNLOAD_DIR, HTTP_CHUNK_SIZE, ARIA2C_PATH, ARIA2C_CONNECTIONS, load_settings
from logger import get_logger

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


def run_download(task_id, url, format_id, on_progress, cancel_event):
    """
    Run yt-dlp download. Calls on_progress(data_dict) for each update.
    cancel_event is a threading.Event that signals cancellation.
    Returns (success: bool, result: dict).
    """
    output_template = os.path.join(DOWNLOAD_DIR, '%(title)s [%(id)s].%(ext)s')
    cmd = [
        YT_DLP_PATH,
        '-f', format_id,
        '--newline',
        '--no-playlist',
        '--windows-filenames',
        '--restrict-filenames',
        '--retries', '10',
        '--fragment-retries', '10',
        '--js-runtimes', 'node',
        '--extractor-args', 'youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416',
        '-o', output_template,
    ]
    audio_only = is_audio_only_format(format_id)
    if audio_only:
        # Audio-only: SKIP aria2c, bare yt-dlp single TCP connection.
        # googlevideo enforces a hard per-connection playback-rate throttle on
        # audio the instant it sees ≥2 parallel TCPs (~40 KiB/s per connection).
        # bench_audio_parallelism.py measured x=1 → 4140 KiB/s vs x=2 → 82 KiB/s.
        # Single connection + fresh PO is the only path to multi-MiB/s audio.
        log.info(f"[{task_id}] audio-only format detected ({format_id}) — "
                 f"skipping aria2c, using bare yt-dlp")
    elif ARIA2C_PATH:
        # Video (or merged): aria2c 16x with medium split (10 MB pieces).
        # Piece count = min(-s, file_size / (2 * -k)). For a 20 GB file with
        # -s 2048 -k 1M: min(2048, 10240) = 2048 → piece ≈ 10 MB.
        #
        # This is the sweet spot balancing two opposing forces:
        #   - TCP slow start: each new connection needs ~1-3 MB of data to
        #     fully open its congestion window. Pieces smaller than 5 MB
        #     spend 50%+ of their life in slow-start ramp-up.
        #   - googlevideo long-session throttle: connections alive more than
        #     ~30 s start getting rate-limited toward playback bitrate.
        # 10 MB pieces at ~600 KiB/s per conn = ~17 s TCP lifetime. Long enough
        # for TCP to reach full throughput, short enough to escape throttle.
        #
        # Iteration log (MEASURED):
        #   -s 16 -k 1M           → 1.25 GB pieces, TCP lives 30 min, decays
        #                           to 820 KiB/s. Total fail.
        #   -s 256 -k 10M + keep-alive=false → 80 MB pieces, decays to 5.3 MiB/s.
        #   -s 256 --lowest-speed=500K → death spiral abort→retry→abort, exit 5.
        #   -s 2048 -k 1M         → 10 MB pieces, TCP ~17 s, STABLE 6.4-6.9 MiB/s
        #                           for 16 min+ ✅ CURRENT BEST.
        #   -s 8192 -k 256K       → rejected by aria2c (min-split-size floor 1M).
        #   -s 8192 -k 1M         → 2.5 MB pieces, TCP ~4 s, DROPS to 5.2 MiB/s
        #                           and still declining. Too small: TCP slow
        #                           start dominates, most of each connection
        #                           never reaches full throughput.
        cmd += [
            '--downloader', ARIA2C_PATH,
            '--downloader-args',
            f'aria2c:-x {ARIA2C_CONNECTIONS} -s 4096 -k 1M '
            f'--summary-interval=1 --console-log-level=warn',
        ]
    else:
        cmd += ['--http-chunk-size', HTTP_CHUNK_SIZE]
    cmd += _cookie_args() + [url]

    log.info(f"[{task_id}] spawning yt-dlp: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1
    )
    log.debug(f"[{task_id}] yt-dlp pid={process.pid}")

    final_filepath = None
    stderr_lines = []
    # Last phase emitted, shared by stdout loop and stderr thread so we dedupe
    # across both streams. Mutated from two threads but only via assignment of
    # a single slot, which is atomic in CPython.
    last_phase = [None]
    # Track [download] Destination: occurrences. For merged formats like
    # vp9+mp4a, yt-dlp emits two Destination lines (first video, second audio).
    # When we see the second one, we refresh pot-provider's PO token cache so
    # the audio phase starts with a fresh integrityToken — after 30+ minutes
    # of video downloading, the PO session is aged and googlevideo will throttle
    # the audio phase to ~30 KiB/s per connection.
    dest_seen = [0]
    pot_refreshed_mid = [False]

    def _emit_phase(line):
        hit = classify_phase(line)
        if not hit:
            return
        phase, text = hit
        if last_phase[0] == phase:
            return
        last_phase[0] = phase
        on_progress({'phase': phase, 'phase_text': text})
        log.info(f"[{task_id}] phase -> {phase} ({text})")

    # Read stderr in a separate thread to prevent pipe buffer deadlock
    def _read_stderr():
        for line in iter(process.stderr.readline, ''):
            s = line.strip()
            if s:
                stderr_lines.append(s)
                log.debug(f"[{task_id}] stderr: {s}")
                _emit_phase(s)
        process.stderr.close()

    stderr_thread = threading.Thread(target=_read_stderr, daemon=True)
    stderr_thread.start()

    try:
        for line in iter(process.stdout.readline, ''):
            if cancel_event.is_set():
                log.info(f"[{task_id}] cancel_event set, terminating yt-dlp")
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                return False, {'error': '用户取消'}

            line = line.strip()
            if not line:
                continue
            log.debug(f"[{task_id}] stdout: {line}")

            progress = parse_progress_line(line)
            if progress:
                if progress.get('filepath'):
                    final_filepath = progress['filepath']
                    log.info(f"[{task_id}] final filepath detected: {final_filepath}")
                    # MERGE_RE hit: surface as merging phase so the UI stops
                    # pretending it's still at 99% during mux/fixup.
                    if last_phase[0] != 'merging':
                        last_phase[0] = 'merging'
                        progress = {**progress, 'phase': 'merging', 'phase_text': '合并音视频'}
                        log.info(f"[{task_id}] phase -> merging (合并音视频)")
                on_progress(progress)
            else:
                _emit_phase(line)

            # Check for destination line to get filename
            m = DEST_RE.match(line)
            if m:
                final_filepath = m.group(1).strip()
                log.info(f"[{task_id}] destination: {final_filepath}")
                dest_seen[0] += 1
                # On the 2nd Destination line (audio phase of merged format),
                # refresh pot-provider caches so the upcoming audio PO token
                # is minted from a fresh BotGuard integrityToken.
                if dest_seen[0] == 2 and not pot_refreshed_mid[0]:
                    pot_refreshed_mid[0] = True
                    try:
                        import pot_provider
                        ok = pot_provider.invalidate_caches()
                        log.info(f"[{task_id}] mid-process pot-provider "
                                 f"invalidate_caches (audio phase) -> {ok}")
                        on_progress({
                            'phase': 'pot_refresh',
                            'phase_text': '刷新 PO Token (音频阶段)'
                        })
                    except Exception as e:
                        log.warning(f"[{task_id}] mid-process pot refresh failed: {e}")

        process.stdout.close()
        stderr_thread.join(timeout=10)
        process.wait()
        log.info(f"[{task_id}] yt-dlp exited with returncode={process.returncode}")

        if process.returncode != 0:
            stderr_text = '\n'.join(stderr_lines)
            m = ERROR_RE.search(stderr_text)
            error_msg = m.group(1) if m else stderr_text.strip()[:200]
            log.error(f"[{task_id}] yt-dlp error: {error_msg}")
            return False, {'error': error_msg or '下载失败'}

    except Exception as e:
        log.exception(f"[{task_id}] exception while reading yt-dlp output")
        try:
            process.kill()
        except OSError:
            pass
        return False, {'error': str(e)}

    # Determine final filename
    filename = None
    if final_filepath and os.path.exists(final_filepath):
        filename = os.path.basename(final_filepath)
    else:
        # Scan downloads dir for most recently modified file
        files = [(f, os.path.getmtime(os.path.join(DOWNLOAD_DIR, f)))
                 for f in os.listdir(DOWNLOAD_DIR)
                 if os.path.isfile(os.path.join(DOWNLOAD_DIR, f))]
        if files:
            filename = max(files, key=lambda x: x[1])[0]

    return True, {'filename': filename}
