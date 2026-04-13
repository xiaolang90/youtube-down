"""
Manage the bgutil-ytdlp-pot-provider Node.js server lifecycle.

The server listens on 127.0.0.1:4416 and is used by the yt-dlp bgutil HTTP
plugin to generate PO tokens required by YouTube for GVS (web/web_safari
clients). Without it, downloads of HLS/DASH formats return HTTP 403 on
every fragment.
"""
import atexit
import os
import socket
import subprocess
import time
import urllib.request

from logger import get_logger

log = get_logger()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POT_DIR = os.path.join(BASE_DIR, 'pot-provider')
POT_ENTRY = os.path.join(POT_DIR, 'build', 'main.js')
POT_PORT = 4416
POT_PING_URL = f'http://127.0.0.1:{POT_PORT}/ping'

_process = None
_last_start_attempt = 0.0
_START_COOLDOWN_SEC = 10.0


def _port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex(('127.0.0.1', port)) == 0


def _ping():
    try:
        with urllib.request.urlopen(POT_PING_URL, timeout=1) as r:
            return r.status == 200
    except Exception:
        return False


def start():
    """Start the bgutil POT provider server if not already running."""
    global _process, _last_start_attempt

    if _ping():
        log.info(f"pot-provider: already running on :{POT_PORT}")
        return

    # Reap dead child before deciding what to do
    if _process is not None and _process.poll() is not None:
        log.warning(f"pot-provider: previous child exited with code {_process.returncode}")
        _process = None

    if _port_in_use(POT_PORT):
        log.warning(f"pot-provider: port {POT_PORT} busy but /ping failed; skipping start (foreign process?)")
        return

    if not os.path.exists(POT_ENTRY):
        log.error(f"pot-provider: build not found at {POT_ENTRY}. Run 'cd pot-provider && npm install && npx tsc'.")
        return

    _last_start_attempt = time.time()
    log.info(f"pot-provider: starting node {POT_ENTRY}")
    try:
        _process = subprocess.Popen(
            ['node', POT_ENTRY],
            cwd=POT_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        log.error("pot-provider: 'node' not found in PATH; install Node.js")
        return

    # Wait up to 10s for server to become reachable
    for i in range(20):
        if _ping():
            log.info(f"pot-provider: ready (pid={_process.pid}) after {i*0.5:.1f}s")
            atexit.register(stop)
            return
        time.sleep(0.5)

    log.error("pot-provider: failed to become reachable within 10s")


def ensure_running():
    """
    Best-effort self-heal called before each download (and by /api/health).
    Returns True if the server is reachable after the attempt.
    Throttled: won't retry start() more than once per cooldown window.
    """
    if _ping():
        return True

    # Reap dead child so status reflects reality
    global _process
    if _process is not None and _process.poll() is not None:
        log.warning(f"pot-provider: detected dead child (code={_process.returncode}), will restart")
        _process = None

    since = time.time() - _last_start_attempt
    if since < _START_COOLDOWN_SEC:
        log.debug(f"pot-provider: ensure_running throttled ({since:.1f}s < {_START_COOLDOWN_SEC}s)")
        return False

    start()
    return _ping()


def invalidate_caches():
    """
    Clear pot-provider's internal integrityToken + PO token caches.

    Empirically, the googlevideo single-connection download rate is anchored
    on the integrityToken: a token that's been minting PO tokens for a while
    ends up in a throttled state (~40 KiB/s in the worst case). Clearing
    _minterCache forces the next POT mint to re-run the BotGuard challenge
    round-trip, producing a fresh integrityToken that googlevideo treats as
    a cold-start session → rate jumps back to ~3700 KiB/s.

    Called before each download from _download_worker. Best-effort: if
    pot-provider is unreachable or returns non-2xx, log and move on;
    worst case the user sees the pre-refresh throttled rate.
    """
    url = f'http://127.0.0.1:{POT_PORT}/invalidate_caches'
    req = urllib.request.Request(url, data=b'', method='POST')
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            log.info(f"pot-provider: invalidate_caches -> HTTP {r.status}")
            return True
    except Exception as e:
        log.warning(f"pot-provider: invalidate_caches failed: {e}")
        return False


def stop():
    global _process
    if _process is None:
        return
    if _process.poll() is None:
        log.info(f"pot-provider: stopping (pid={_process.pid})")
        _process.terminate()
        try:
            _process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _process.kill()
    _process = None
