import json
import os
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from flask import Flask, request, jsonify, Response, send_from_directory
from flask_cors import CORS

from config import DOWNLOAD_DIR, MAX_CONCURRENT, HOST, PORT, load_settings, save_settings
from models import init_db, create_task, update_task, get_task, list_tasks, delete_task, delete_all_tasks, mark_stale_downloads
from downloader import fetch_metadata, run_download
from logger import get_logger
import pot_provider

log = get_logger()

app = Flask(__name__,
            static_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static'))
CORS(app)

executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT)
# task_id -> {"cancel_event": threading.Event, "sse_queues": [queue.Queue, ...]}
active_tasks = {}
active_lock = threading.Lock()


def _notify(task_id, data):
    """Push data to all SSE listeners for a task."""
    with active_lock:
        info = active_tasks.get(task_id)
        if info:
            for q in info['sse_queues']:
                try:
                    q.put_nowait(data)
                except queue.Full:
                    pass


def _download_worker(task_id, url, format_id):
    """Background worker that runs a download and updates DB + SSE."""
    log.info(f"[{task_id}] worker started | url={url} | format_id={format_id}")
    _notify(task_id, {'phase': 'preparing', 'phase_text': '准备中'})
    _notify(task_id, {'phase': 'pot_check', 'phase_text': '检查 PO Token 服务'})
    if not pot_provider.ensure_running():
        log.warning(f"[{task_id}] pot-provider not reachable; proceeding anyway (POT generation may fail)")
    # Refresh integrityToken before every download. A stale minter cache is
    # the dominant cause of the googlevideo single-connection throttle we
    # see drop to ~40 KiB/s; clearing it brings the rate back to ~3.6 MiB/s.
    _notify(task_id, {'phase': 'pot_refresh', 'phase_text': '刷新 PO Token'})
    pot_provider.invalidate_caches()
    update_task(task_id, status='downloading', progress=0)
    _notify(task_id, {'status': 'downloading', 'progress': 0,
                      'phase': 'starting_ytdlp', 'phase_text': '启动 yt-dlp'})
    log.debug(f"[{task_id}] status -> downloading")

    cancel_event = None
    with active_lock:
        info = active_tasks.get(task_id)
        if info:
            cancel_event = info['cancel_event']

    if not cancel_event:
        cancel_event = threading.Event()

    last_logged_pct = [-10.0]

    def on_progress(data):
        updates = {}
        if 'progress' in data:
            updates['progress'] = data['progress']
        if 'speed' in data:
            updates['speed'] = data['speed']
        if 'eta' in data:
            updates['eta'] = data['eta']
        if updates:
            update_task(task_id, **updates)
        _notify(task_id, {**data})
        # Throttle progress logging to every 10%
        p = data.get('progress')
        if p is not None and p - last_logged_pct[0] >= 10:
            last_logged_pct[0] = p
            log.info(f"[{task_id}] progress {p:.1f}% speed={data.get('speed','-')} eta={data.get('eta','-')}")

    try:
        success, result = run_download(task_id, url, format_id, on_progress, cancel_event)
    except Exception as e:
        log.exception(f"[{task_id}] run_download raised unexpected exception")
        update_task(task_id, status='failed', error=str(e))
        _notify(task_id, {'status': 'failed', 'error': str(e)})
        _notify(task_id, None)
        with active_lock:
            active_tasks.pop(task_id, None)
        return

    if cancel_event.is_set():
        log.info(f"[{task_id}] cancelled by user")
        update_task(task_id, status='cancelled')
        _notify(task_id, {'status': 'cancelled', 'progress': 0})
    elif success:
        filename = result.get('filename', '')
        log.info(f"[{task_id}] completed | filename={filename}")
        update_task(
            task_id,
            status='completed',
            progress=100,
            filename=filename,
            completed_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
        _notify(task_id, {'status': 'completed', 'progress': 100, 'filename': filename})
    else:
        error_msg = result.get('error', '未知错误')
        log.error(f"[{task_id}] failed | error={error_msg}")
        update_task(task_id, status='failed', error=error_msg)
        _notify(task_id, {'status': 'failed', 'error': error_msg})

    # Signal end to SSE listeners
    _notify(task_id, None)

    # Cleanup
    with active_lock:
        active_tasks.pop(task_id, None)
    log.debug(f"[{task_id}] worker exited, active_tasks cleaned")


# ── Routes ──────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/api/parse', methods=['POST'])
def parse_video():
    data = request.get_json()
    url = (data or {}).get('url', '').strip()
    if not url:
        return jsonify({'code': 1, 'message': '请输入视频链接', 'data': None})
    try:
        meta = fetch_metadata(url)
        return jsonify({'code': 0, 'message': 'success', 'data': meta})
    except Exception as e:
        return jsonify({'code': 1, 'message': str(e), 'data': None})


@app.route('/api/download', methods=['POST'])
def start_download():
    data = request.get_json() or {}
    url = data.get('url', '').strip()
    format_id = data.get('format_id', 'best')
    title = data.get('title', '')
    thumbnail = data.get('thumbnail', '')
    uploader = data.get('uploader', '')
    duration = data.get('duration', 0)
    format_note = data.get('format_note', '')
    filesize = data.get('filesize')

    if not url:
        log.warning("download request rejected: missing url")
        return jsonify({'code': 1, 'message': '缺少视频链接', 'data': None})

    task_id = create_task(url, title, thumbnail, uploader, duration, format_id, format_note, filesize)
    log.info(f"[{task_id}] task created | title={title!r} | format={format_id} ({format_note})")

    with active_lock:
        active_tasks[task_id] = {
            'cancel_event': threading.Event(),
            'sse_queues': []
        }

    _notify(task_id, {'phase': 'queued', 'phase_text': '排队中'})
    executor.submit(_download_worker, task_id, url, format_id)
    log.debug(f"[{task_id}] submitted to executor")
    return jsonify({'code': 0, 'message': 'success', 'data': {'id': task_id}})


@app.route('/api/download/<task_id>/progress')
def stream_progress(task_id):
    q = queue.Queue(maxsize=100)

    with active_lock:
        info = active_tasks.get(task_id)
        if info:
            info['sse_queues'].append(q)
        else:
            # Task may already be done, send current state
            task = get_task(task_id)
            if task:
                def done_gen():
                    yield f"event: progress\ndata: {json.dumps({'status': task['status'], 'progress': task['progress'] or 0})}\n\n"
                return Response(done_gen(), mimetype='text/event-stream',
                                headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})
            return jsonify({'code': 1, 'message': '任务不存在'}), 404

    def generate():
        try:
            while True:
                try:
                    data = q.get(timeout=30)
                except queue.Empty:
                    # Heartbeat
                    yield "event: ping\ndata: {}\n\n"
                    continue

                if data is None:
                    break
                yield f"event: progress\ndata: {json.dumps(data)}\n\n"
        finally:
            with active_lock:
                info = active_tasks.get(task_id)
                if info and q in info['sse_queues']:
                    info['sse_queues'].remove(q)

    return Response(generate(), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/api/download/<task_id>/cancel', methods=['POST'])
def cancel_download(task_id):
    with active_lock:
        info = active_tasks.get(task_id)
        if info:
            info['cancel_event'].set()
            return jsonify({'code': 0, 'message': '已取消'})

    # Task not active, just update DB
    task = get_task(task_id)
    if task and task['status'] in ('pending', 'downloading'):
        update_task(task_id, status='cancelled')
        return jsonify({'code': 0, 'message': '已取消'})

    return jsonify({'code': 1, 'message': '任务不存在或无法取消'})


@app.route('/api/download/<task_id>', methods=['DELETE'])
def remove_download(task_id):
    task = get_task(task_id)
    if not task:
        return jsonify({'code': 1, 'message': '任务不存在'})

    # Delete file if exists
    if task.get('filename'):
        filepath = os.path.join(DOWNLOAD_DIR, task['filename'])
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except OSError:
                pass

    delete_task(task_id)
    return jsonify({'code': 0, 'message': '已删除'})


@app.route('/api/downloads/all', methods=['DELETE'])
def clear_all_downloads():
    """Cancel all active tasks, delete all DB records, and remove downloaded files."""
    # Cancel any running tasks
    with active_lock:
        for task_id, info in list(active_tasks.items()):
            info['cancel_event'].set()

    filenames = delete_all_tasks()
    removed = 0
    for fn in filenames:
        try:
            fp = os.path.join(DOWNLOAD_DIR, fn)
            if os.path.exists(fp):
                os.remove(fp)
                removed += 1
        except OSError:
            pass
    log.info(f"cleared all downloads: {len(filenames)} db records, {removed} files removed")
    return jsonify({'code': 0, 'message': f'已清空 {len(filenames)} 条记录'})


@app.route('/api/history')
def history():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 20, type=int)
    search = request.args.get('q', '')
    result = list_tasks(page=page, per_page=per_page, search=search)
    return jsonify({'code': 0, 'data': result})


@app.route('/api/file/<task_id>')
def serve_file(task_id):
    task = get_task(task_id)
    if not task or not task.get('filename'):
        return jsonify({'code': 1, 'message': '文件不存在'}), 404
    filepath = os.path.join(DOWNLOAD_DIR, task['filename'])
    if not os.path.exists(filepath):
        return jsonify({'code': 1, 'message': '文件不存在'}), 404
    return send_from_directory(DOWNLOAD_DIR, task['filename'], as_attachment=True)


@app.route('/api/file/<task_id>/reveal', methods=['POST'])
def reveal_file(task_id):
    """Reveal the downloaded file in the OS file manager (Finder on macOS)."""
    import subprocess
    import sys
    task = get_task(task_id)
    if not task or not task.get('filename'):
        return jsonify({'code': 1, 'message': '文件不存在'}), 404
    filepath = os.path.join(DOWNLOAD_DIR, task['filename'])
    if not os.path.exists(filepath):
        return jsonify({'code': 1, 'message': '文件不存在'}), 404
    try:
        if sys.platform == 'darwin':
            subprocess.Popen(['open', '-R', filepath])
        elif sys.platform.startswith('win'):
            subprocess.Popen(['explorer', '/select,', filepath])
        else:
            subprocess.Popen(['xdg-open', os.path.dirname(filepath)])
        return jsonify({'code': 0, 'message': 'ok'})
    except Exception as e:
        log.warning(f"reveal_file failed: {e}")
        return jsonify({'code': 1, 'message': str(e)}), 500


@app.route('/api/active')
def active_downloads():
    """Return list of currently active/recent tasks for the download queue display."""
    result = list_tasks(page=1, per_page=50)
    # Filter to show only relevant tasks
    items = [t for t in result['items'] if t['status'] in ('pending', 'downloading', 'completed', 'failed', 'cancelled')]
    return jsonify({'code': 0, 'data': items})


@app.route('/api/settings', methods=['GET'])
def get_settings():
    return jsonify({'code': 0, 'data': load_settings()})


@app.route('/api/health')
def health_check():
    """Return status of yt-dlp, node.js, and the POT provider server."""
    import subprocess as sp
    from config import YT_DLP_PATH, ARIA2C_PATH

    def _run(cmd):
        try:
            r = sp.run(cmd, capture_output=True, text=True, timeout=5)
            return r.stdout.strip().splitlines()[0] if r.stdout else None
        except Exception:
            return None

    ytdlp = _run([YT_DLP_PATH, '--version'])
    node = _run(['node', '--version'])
    aria2c = _run([ARIA2C_PATH, '--version']) if ARIA2C_PATH else None

    # Self-heal attempt before reporting status
    pot_provider.ensure_running()

    pot_ok = False
    pot_info = None
    try:
        import urllib.request, json as _json
        with urllib.request.urlopen('http://127.0.0.1:4416/ping', timeout=1) as r:
            pot_ok = r.status == 200
            pot_info = _json.loads(r.read().decode()).get('version')
    except Exception:
        pass

    return jsonify({'code': 0, 'data': {
        'ytdlp': {'ok': bool(ytdlp), 'version': ytdlp or '未检测到'},
        'node': {'ok': bool(node), 'version': node or '未检测到'},
        'pot_provider': {'ok': pot_ok, 'version': pot_info or ('运行中' if pot_ok else '未运行')},
        'aria2c': {'ok': bool(aria2c), 'version': aria2c or '未检测到 (已回退到 http-chunk-size)'},
    }})


@app.route('/api/settings', methods=['PUT'])
def update_settings():
    data = request.get_json() or {}
    settings = load_settings()
    for k in ('cookie_mode', 'cookie_browser', 'cookie_file'):
        if k in data:
            settings[k] = data[k]
    save_settings(settings)
    return jsonify({'code': 0, 'message': '设置已保存'})


if __name__ == '__main__':
    init_db()
    mark_stale_downloads()
    pot_provider.start()
    print(f"  YouTube 视频下载器已启动: http://localhost:{PORT}")
    app.run(host=HOST, port=PORT, debug=False, threaded=True)
