#!/usr/bin/env python3
"""Test aria2c under Clash Verge TUN mode (L3 transparent proxy).

Hypothesis:
  aria2c's TLS handshake to googlevideo fails in HTTP CONNECT proxy mode
  (JA3/JA4 fingerprint gets mangled or proxied differently). TUN mode
  intercepts at L3 — aria2c sends raw TCP, Clash forwards transparently,
  so the TLS ClientHello reaches googlevideo unmodified.

Method:
  1. Detect proxy env vars; strip them so aria2c goes direct (TUN intercepts)
  2. Use yt-dlp to extract a fresh googlevideo stream URL (tv client + PO token)
  3. Run 5 variants, each downloading ~30s worth of data then stopping:
     a) aria2c direct (no proxy env, TUN should intercept)     — THE MAIN TEST
     b) aria2c direct -x1 (single connection)                  — isolate multi-conn
     c) yt-dlp + aria2c as --downloader (no proxy env)         — full pipeline
     d) yt-dlp HttpFD + --impersonate chrome (current prod)    — baseline
     e) yt-dlp HttpFD + tv client (latest change)              — latest prod

Usage:
    # First: enable Clash Verge TUN mode in the GUI
    python3 scripts/test_tun_aria2c.py [URL] [--format 137+251]
    python3 scripts/test_tun_aria2c.py --only aria2c-tun
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import YT_DLP_PATH, ARIA2C_PATH, ARIA2C_CONNECTIONS  # noqa: E402

DEFAULT_URL = 'https://www.youtube.com/watch?v=OZytLLasceA'
DEFAULT_FORMAT = '137'
WORK_DIR = '/tmp/test_tun_aria2c'
SAMPLE_DURATION = 30  # seconds per variant — enough to measure speed
MAX_TIMEOUT = 120     # hard kill if variant hangs

# aria2c args matching production downloader.py
ARIA2C_BASE_ARGS = (
    f'-x {ARIA2C_CONNECTIONS} -s 4096 -k 1M '
    f'--summary-interval=1 --console-log-level=notice '
    f'--max-tries=3 --retry-wait=3 '
    f'--timeout=60 --connect-timeout=30 '
    f'--lowest-speed-limit=10K'
)
ARIA2C_SINGLE_ARGS = (
    f'-x 1 -s 1 -k 1M '
    f'--summary-interval=1 --console-log-level=notice '
    f'--max-tries=3 --retry-wait=3 '
    f'--timeout=60 --connect-timeout=30'
)


def clean_proxy_env():
    """Return a copy of os.environ with proxy vars removed.

    Under TUN mode, traffic is intercepted at the network layer.
    Keeping http_proxy/https_proxy would make aria2c use HTTP CONNECT,
    which defeats the whole point of TUN.
    """
    env = os.environ.copy()
    for key in list(env.keys()):
        if 'proxy' in key.lower() or key in ('ALL_PROXY', 'all_proxy'):
            print(f'  [env] unsetting {key}={env[key][:40]}...')
            del env[key]
    return env


def check_tun_active():
    """Heuristic: try a direct TCP connection to google.com:443 without
    proxy env vars. If it succeeds, TUN is likely active (or we have
    direct internet). Either way, the test is meaningful."""
    import socket
    env = clean_proxy_env()
    try:
        s = socket.create_connection(('www.google.com', 443), timeout=5)
        s.close()
        return True
    except Exception as e:
        return False


def extract_stream_url(url, fmt, player_client='tv,web'):
    """Use yt-dlp to get the direct googlevideo stream URL."""
    cmd = [
        YT_DLP_PATH, '--dump-json', '--no-playlist', '--no-warnings',
        '-f', fmt,
        '--js-runtimes', 'node',
        '--extractor-args', 'youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416',
        '--extractor-args', f'youtube:player_client={player_client}',
        '--cookies-from-browser', 'chrome',
        url,
    ]
    print(f'  [extract] Running yt-dlp to get stream URL (player_client={player_client})...')
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        print(f'  [extract] FAILED rc={proc.returncode}')
        print(f'  stderr: {proc.stderr[-500:]}')
        return None

    info = json.loads(proc.stdout)
    stream_url = info.get('url')
    if not stream_url:
        # Merged format — get the video stream URL from requested_formats
        fmts = info.get('requested_formats', [])
        if fmts:
            stream_url = fmts[0].get('url')
    if stream_url:
        print(f'  [extract] Got URL: {stream_url[:80]}...')
    return stream_url


def run_variant(name, cmd, env, work_dir, sample_duration=SAMPLE_DURATION):
    """Run a download command for sample_duration seconds, measure speed."""
    out_dir = os.path.join(work_dir, name)
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir, exist_ok=True)

    print(f'\n{"="*70}\n[{name}] START\n  cmd: {" ".join(cmd[:6])}...\n{"="*70}')

    output_lines = []
    t0 = time.monotonic()
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, env=env, cwd=out_dir,
        )
        last_print = 0
        while True:
            line = proc.stdout.readline()
            if not line and proc.poll() is not None:
                break
            line = line.rstrip()
            if not line:
                continue
            output_lines.append(line)
            if len(output_lines) > 300:
                output_lines.pop(0)

            now = time.monotonic()
            elapsed = now - t0
            is_important = any(k in line.lower() for k in
                             ['error', 'exception', 'fail', 'abort',
                              'handshake', 'tls', 'ssl', 'exit'])
            if is_important or now - last_print > 3:
                print(f'  [{name}] +{elapsed:5.1f}s {line[:160]}')
                last_print = now

            if elapsed > sample_duration and not is_important:
                print(f'  [{name}] {sample_duration}s sample complete, stopping...')
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                break

            if elapsed > MAX_TIMEOUT:
                print(f'  [{name}] TIMEOUT {MAX_TIMEOUT}s, killing')
                proc.kill()
                proc.wait()
                break

        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        rc = proc.returncode
    except KeyboardInterrupt:
        proc.kill()
        rc = -2
    except Exception as e:
        print(f'  [{name}] Exception: {e}')
        rc = -1
        output_lines.append(str(e))

    elapsed = time.monotonic() - t0

    # Measure downloaded bytes
    total_bytes = 0
    for root, _, files in os.walk(out_dir):
        for f in files:
            total_bytes += os.path.getsize(os.path.join(root, f))

    speed_mibps = (total_bytes / 1024 / 1024) / elapsed if elapsed > 0 else 0

    # Check for TLS errors in output
    tls_errors = [l for l in output_lines if any(k in l.lower() for k in
                  ['handshake', 'tls', 'ssl', 'certificate', 'CUID'])]

    print(f'\n[{name}] RESULT:')
    print(f'  rc={rc}  wall={elapsed:.1f}s  bytes={total_bytes:,}  speed={speed_mibps:.2f} MiB/s')
    if tls_errors:
        print(f'  TLS issues found ({len(tls_errors)} lines):')
        for l in tls_errors[:5]:
            print(f'    {l[:160]}')
    print(f'  Last 10 output lines:')
    for l in output_lines[-10:]:
        print(f'    {l[:160]}')

    # Cleanup
    shutil.rmtree(out_dir, ignore_errors=True)

    return {
        'name': name,
        'rc': rc,
        'wall_sec': elapsed,
        'bytes': total_bytes,
        'speed_mibps': speed_mibps,
        'tls_errors': len(tls_errors),
        'ok': total_bytes > 10000,  # at least 10KB downloaded
    }


def main():
    ap = argparse.ArgumentParser(description='Test aria2c under TUN mode')
    ap.add_argument('url', nargs='?', default=DEFAULT_URL)
    ap.add_argument('--format', default=DEFAULT_FORMAT)
    ap.add_argument('--only', choices=[
        'aria2c-tun', 'aria2c-single', 'ytdlp-aria2c',
        'ytdlp-httpfd', 'ytdlp-tv',
    ])
    ap.add_argument('--duration', type=int, default=SAMPLE_DURATION,
                    help='Seconds per variant (default 30)')
    args = ap.parse_args()

    sample_duration = args.duration

    print('='*70)
    print('aria2c TUN mode test')
    print('='*70)

    # Check prerequisites
    if not ARIA2C_PATH or not os.path.exists(ARIA2C_PATH):
        sys.exit(f'aria2c not found: {ARIA2C_PATH!r}')
    print(f'aria2c:  {ARIA2C_PATH}')

    # Show aria2c TLS backend
    ver = subprocess.run([ARIA2C_PATH, '--version'], capture_output=True, text=True)
    for line in ver.stdout.split('\n')[:3]:
        print(f'  {line}')

    print(f'yt-dlp:  {YT_DLP_PATH}')
    print(f'URL:     {args.url}')
    print(f'Format:  {args.format}')
    print(f'Sample:  {SAMPLE_DURATION}s per variant')

    # Check proxy state
    proxy_vars = {k: v for k, v in os.environ.items()
                  if 'proxy' in k.lower() or k in ('ALL_PROXY', 'all_proxy')}
    if proxy_vars:
        print(f'\nProxy env vars detected (will be stripped for aria2c-tun variants):')
        for k, v in proxy_vars.items():
            print(f'  {k}={v[:50]}...')
    else:
        print(f'\nNo proxy env vars — traffic must be going through TUN or direct.')

    # Check TUN
    print('\nChecking direct TCP connectivity (TUN heuristic)...')
    tun_ok = check_tun_active()
    print(f'  Direct TCP to google.com:443: {"OK" if tun_ok else "FAILED"}')
    if not tun_ok:
        print('  WARNING: Direct TCP failed. TUN may not be active.')
        print('  Tests will proceed but aria2c variants will likely fail.')

    no_proxy_env = clean_proxy_env()

    # Extract stream URL for direct aria2c tests
    print('\n--- Extracting googlevideo stream URL ---')
    stream_url = extract_stream_url(args.url, args.format, player_client='tv,web')
    if not stream_url:
        print('Failed to extract stream URL. Trying with default client...')
        stream_url = extract_stream_url(args.url, args.format, player_client='web')
    if not stream_url:
        sys.exit('Cannot extract stream URL. Check yt-dlp / PO token / cookies.')

    os.makedirs(WORK_DIR, exist_ok=True)

    # --- Define variants ---
    variants = []

    # A) aria2c direct, no proxy (TUN intercepts), multi-connection
    out_a = os.path.join(WORK_DIR, 'aria2c-tun', 'video.mp4')
    variants.append(('aria2c-tun', [
        ARIA2C_PATH,
        *ARIA2C_BASE_ARGS.split(),
        '-o', out_a,
        stream_url,
    ], no_proxy_env))

    # B) aria2c direct, single connection
    out_b = os.path.join(WORK_DIR, 'aria2c-single', 'video.mp4')
    variants.append(('aria2c-single', [
        ARIA2C_PATH,
        *ARIA2C_SINGLE_ARGS.split(),
        '-o', out_b,
        stream_url,
    ], no_proxy_env))

    # C) yt-dlp + aria2c as downloader (no proxy)
    out_c = os.path.join(WORK_DIR, 'ytdlp-aria2c', '%(id)s.%(ext)s')
    variants.append(('ytdlp-aria2c', [
        YT_DLP_PATH,
        '-f', args.format,
        '--newline', '--no-playlist', '--no-warnings',
        '--js-runtimes', 'node',
        '--extractor-args', 'youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416',
        '--extractor-args', 'youtube:player_client=tv,web',
        '--cookies-from-browser', 'chrome',
        '--downloader', ARIA2C_PATH,
        '--downloader-args', f'aria2c:{ARIA2C_BASE_ARGS}',
        '-o', out_c,
        args.url,
    ], no_proxy_env))

    # D) yt-dlp HttpFD + impersonate chrome (current production)
    out_d = os.path.join(WORK_DIR, 'ytdlp-httpfd', '%(id)s.%(ext)s')
    variants.append(('ytdlp-httpfd', [
        YT_DLP_PATH,
        '-f', args.format,
        '--newline', '--no-playlist', '--no-warnings',
        '--js-runtimes', 'node',
        '--extractor-args', 'youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416',
        '--impersonate', 'chrome',
        '--http-chunk-size', '100M',
        '--socket-timeout', '30',
        '--cookies-from-browser', 'chrome',
        '-o', out_d,
        args.url,
    ], None))  # None = inherit current env (with proxy)

    # E) yt-dlp HttpFD + tv client (latest change)
    out_e = os.path.join(WORK_DIR, 'ytdlp-tv', '%(id)s.%(ext)s')
    variants.append(('ytdlp-tv', [
        YT_DLP_PATH,
        '-f', args.format,
        '--newline', '--no-playlist', '--no-warnings',
        '--js-runtimes', 'node',
        '--extractor-args', 'youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416',
        '--extractor-args', 'youtube:player_client=tv,web',
        '--impersonate', 'chrome',
        '--http-chunk-size', '100M',
        '--socket-timeout', '30',
        '--cookies-from-browser', 'chrome',
        '-o', out_e,
        args.url,
    ], None))  # inherit current env

    if args.only:
        variants = [v for v in variants if v[0] == args.only]
        if not variants:
            sys.exit(f'Unknown variant: {args.only}')

    # --- Run ---
    results = []
    for name, cmd, env in variants:
        r = run_variant(name, cmd, env, WORK_DIR, sample_duration=sample_duration)
        results.append(r)

    # --- Summary ---
    print(f'\n\n{"="*70}')
    print(f'SUMMARY  (sampled {sample_duration}s per variant)')
    print(f'{"="*70}')
    print(f'  {"Status":<6} {"Variant":<16} {"Speed":>10} {"Bytes":>14} {"TLS Err":>8} {"RC":>4}')
    print(f'  {"-"*6} {"-"*16} {"-"*10} {"-"*14} {"-"*8} {"-"*4}')
    for r in results:
        status = ' OK ' if r['ok'] else 'FAIL'
        print(f'  {status:<6} {r["name"]:<16} {r["speed_mibps"]:>8.2f}M/s'
              f' {r["bytes"]:>14,} {r["tls_errors"]:>8} {r["rc"]:>4}')

    # Highlight the key question
    aria2c_result = next((r for r in results if r['name'] == 'aria2c-tun'), None)
    if aria2c_result:
        print(f'\n>>> KEY RESULT: aria2c under TUN = '
              f'{"SUCCESS" if aria2c_result["ok"] else "FAILED"}'
              f' ({aria2c_result["speed_mibps"]:.2f} MiB/s)')
        if aria2c_result['ok']:
            httpfd = next((r for r in results if r['name'] in ('ytdlp-tv', 'ytdlp-httpfd')), None)
            if httpfd and httpfd['ok']:
                ratio = aria2c_result['speed_mibps'] / httpfd['speed_mibps'] if httpfd['speed_mibps'] > 0 else 0
                print(f'    vs HttpFD: {ratio:.1f}x '
                      f'(aria2c {aria2c_result["speed_mibps"]:.2f} vs '
                      f'HttpFD {httpfd["speed_mibps"]:.2f} MiB/s)')
                if ratio > 1.5:
                    print(f'    → aria2c multi-connection IS faster. '
                          f'Consider switching back to aria2c in downloader.py.')
                else:
                    print(f'    → aria2c works but not significantly faster. '
                          f'HttpFD + impersonate is fine.')
        else:
            print(f'    → TUN mode did NOT fix aria2c. '
                  f'The issue is likely JA3 fingerprinting, not proxy tunnel.')


if __name__ == '__main__':
    main()
