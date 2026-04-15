#!/usr/bin/env python3
"""Reproduce aria2c exit-code-1 on a long video, and test a candidate fix.

Context:
  Production log shows aria2c aborting mid-download on a 4h / ~1 GB 1080p
  video ("CLAUDE CODE FULL COURSE 4 HOURS"), with CUID errors and final
  "aria2c exited with code 1". Current args in downloader.py are:
    -x 16 -s 4096 -k 1M --summary-interval=1 --console-log-level=warn
  Suspected cause: no retry/timeout tolerance — one bad connection or
  transient 403 from googlevideo kills the whole download.

Method:
  Run yt-dlp end-to-end with the real --downloader aria2c pipeline, but
  inject two different --downloader-args variants:
    1. "baseline"  — exact current production args
    2. "candidate" — baseline + retries / timeouts / lowest-speed-limit
  For each variant, record: wall time, exit code, last ~15 lines of stderr,
  final file size if any. Downloads go to /tmp and are deleted after.

Usage:
    python3 scripts/test_aria2c_long_video.py <URL> [--format 137]
    # Defaults to the failing video URL if none provided.

Note:
  This is a LONG test. 1 GB at 5 MiB/s ≈ 3.5 min per variant if healthy.
  Unhealthy variant may fail in seconds or hang for minutes — script
  enforces a per-variant wall-clock timeout.
"""
import argparse
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from config import YT_DLP_PATH, ARIA2C_PATH  # noqa: E402

DEFAULT_URL = 'https://www.youtube.com/watch?v=lIrvMJOE4Go'  # the failing 4h video
DEFAULT_FORMAT = '137'  # 1080p mp4 video-only, same itag as the failing log
WORK_DIR = '/tmp/test_aria2c_long'
PER_VARIANT_TIMEOUT = 900  # 15 min — enough for a healthy 1 GB download

BASELINE_ARGS = (
    f'aria2c:-x 16 -s 4096 -k 1M '
    f'--summary-interval=1 --console-log-level=warn'
)
CANDIDATE_ARGS = (
    f'aria2c:-x 16 -s 4096 -k 1M '
    f'--summary-interval=1 --console-log-level=warn '
    f'--max-tries=0 --retry-wait=3 '
    f'--timeout=60 --connect-timeout=30 '
    f'--lowest-speed-limit=50K'
)
# Bypass certificate verification — tests whether the SSL/TLS handshake
# failure is a cert-chain issue.
NOCERT_ARGS = (
    f'aria2c:-x 16 -s 4096 -k 1M '
    f'--summary-interval=1 --console-log-level=warn '
    f'--check-certificate=false'
)
# Single-connection aria2c — tests whether -x 16 itself is what googlevideo
# rejects at the TLS layer.
SINGLECONN_ARGS = (
    f'aria2c:-x 1 -s 1 '
    f'--summary-interval=1 --console-log-level=warn'
)


def run_variant(name, downloader_args, url, fmt):
    """downloader_args=None means bare yt-dlp (no --downloader)."""
    out_dir = os.path.join(WORK_DIR, name)
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir, exist_ok=True)
    tmpl = os.path.join(out_dir, '%(id)s.%(ext)s')

    cmd = [
        YT_DLP_PATH,
        '-f', fmt,
        '--newline',
        '--no-playlist',
        '--no-warnings',
        '--retries', '1',
        '--fragment-retries', '1',
        '--js-runtimes', 'node',
        '--extractor-args', 'youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416',
        '--cookies-from-browser', 'chrome',
        '-o', tmpl,
    ]
    if downloader_args is not None:
        cmd += ['--downloader', ARIA2C_PATH, '--downloader-args', downloader_args]
    else:
        cmd += ['--http-chunk-size', '10M']
    cmd += [url]

    print(f'\n{"="*70}\n[{name}] START\n  args: {downloader_args}\n{"="*70}')
    t0 = time.monotonic()
    stderr_tail = []
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1,
        )
        last_print = 0
        for line in iter(proc.stdout.readline, ''):
            line = line.rstrip()
            if not line:
                continue
            stderr_tail.append(line)
            if len(stderr_tail) > 200:
                stderr_tail.pop(0)

            # Throttle per-line printing to keep the terminal readable.
            now = time.monotonic()
            is_important = ('ERROR' in line or 'Exception' in line
                            or 'aborted' in line or 'exit' in line.lower())
            if is_important or now - last_print > 2:
                dt = now - t0
                print(f'  [{name}] +{dt:6.1f}s {line[:180]}')
                last_print = now

            if now - t0 > PER_VARIANT_TIMEOUT:
                print(f'  [{name}] TIMEOUT after {PER_VARIANT_TIMEOUT}s, killing')
                proc.kill()
                break

        proc.wait()
        rc = proc.returncode
    except KeyboardInterrupt:
        proc.kill()
        rc = -2
    dt = time.monotonic() - t0

    files = []
    if os.path.isdir(out_dir):
        for f in os.listdir(out_dir):
            p = os.path.join(out_dir, f)
            if os.path.isfile(p):
                files.append((f, os.path.getsize(p)))
    total_bytes = sum(s for _, s in files)

    print(f'\n[{name}] DONE rc={rc} wall={dt:.1f}s bytes={total_bytes:,}')
    print(f'[{name}] last stderr lines:')
    for line in stderr_tail[-15:]:
        print(f'    {line[:200]}')

    # Clean up the file to save disk.
    shutil.rmtree(out_dir, ignore_errors=True)

    return {
        'name': name,
        'rc': rc,
        'wall_sec': dt,
        'bytes': total_bytes,
        'ok': rc == 0 and total_bytes > 0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('url', nargs='?', default=DEFAULT_URL)
    ap.add_argument('--format', default=DEFAULT_FORMAT)
    ap.add_argument('--only', choices=['baseline', 'candidate', 'nocert', 'single', 'bare'],
                    help='Run only one variant')
    args = ap.parse_args()

    if not ARIA2C_PATH or not os.path.exists(ARIA2C_PATH):
        sys.exit(f'aria2c not found: {ARIA2C_PATH!r}')
    if not os.path.exists(YT_DLP_PATH) and not shutil.which(YT_DLP_PATH):
        sys.exit(f'yt-dlp not found: {YT_DLP_PATH!r}')

    print(f'URL:     {args.url}')
    print(f'FORMAT:  {args.format}')
    print(f'aria2c:  {ARIA2C_PATH}')
    print(f'yt-dlp:  {YT_DLP_PATH}')

    all_variants = [
        ('baseline',  BASELINE_ARGS),
        ('candidate', CANDIDATE_ARGS),
        ('nocert',    NOCERT_ARGS),
        ('single',    SINGLECONN_ARGS),
        ('bare',      None),  # no aria2c at all
    ]
    if args.only:
        variants = [v for v in all_variants if v[0] == args.only]
    else:
        variants = all_variants

    results = [run_variant(n, a, args.url, args.format) for n, a in variants]

    print('\n\n' + '='*70 + '\nSUMMARY\n' + '='*70)
    for r in results:
        status = 'OK ' if r['ok'] else 'FAIL'
        print(f'  {status}  {r["name"]:10s} rc={r["rc"]} '
              f'wall={r["wall_sec"]:6.1f}s bytes={r["bytes"]:>14,}')


if __name__ == '__main__':
    main()
