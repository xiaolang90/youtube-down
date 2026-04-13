#!/usr/bin/env python3
"""Test whether audio downloads scale with aria2c parallelism or hit a hard cap.

Hypothesis under test:
  Does googlevideo apply a single hard "playback-rate" throttle to the entire
  audio stream regardless of how many parallel TCP connections we use?
  (Like what we saw with yt-dlp --download-sections + ffmpeg streaming.)

Method:
  Run N sequential variants, each a fresh full audio-only download of the
  same URL. Fresh PO token before each (via /invalidate_caches) to eliminate
  the "aged minter" confound. Vary -x (concurrent connections) from 1 to 32.

Interpretation:
  - Rate flat across all variants → hard session/stream cap, parallelism is useless
  - Rate scales linearly with -x     → per-connection cap, parallelism helps
  - Plateau at some -x (e.g. 8)      → session-level cap around that total rate

Usage:
    python3 scripts/bench_audio_parallelism.py <URL> [--format 251-drc]
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request

YT_DLP = os.path.expanduser("~/Library/Python/3.14/bin/yt-dlp")
ARIA2C = "/usr/local/bin/aria2c"
POT_URL = "http://127.0.0.1:4416"
WORK_BASE = "/tmp/bench_audio_par"
DEFAULT_URL = "https://www.youtube.com/watch?v=QoQBzR1NIqI"
COOLDOWN = 10  # seconds between variants

# -x values to test. 1 = bare yt-dlp (no aria2c). Others = aria2c with that many conns.
X_VALUES = [1, 2, 4, 8, 16, 24]


def refresh_pot():
    req = urllib.request.Request(
        POT_URL + "/invalidate_caches", data=b"", method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status
    except Exception as e:
        return f"err: {e}"


def pot_alive():
    try:
        with urllib.request.urlopen(POT_URL + "/ping", timeout=2) as r:
            return r.status == 200
    except Exception:
        return False


def build_cmd(url, fmt, x, work_dir):
    base = [
        YT_DLP, "-f", fmt, "--no-playlist", "--newline",
        "--js-runtimes", "node",
        "--extractor-args", f"youtubepot-bgutilhttp:base_url={POT_URL}",
        "--cookies-from-browser", "chrome",
        "-o", "chunk.%(ext)s",
    ]
    if x == 1:
        # bare yt-dlp, single TCP connection, no aria2c
        return base + [url]
    return base + [
        "--downloader", ARIA2C,
        "--downloader-args",
        # aria2c's min-split-size has a hard floor of 1 MiB; 256K would
        # fail with errorCode=28 before aria2c even starts. 1M is legal.
        f"aria2c:-x {x} -s 8192 -k 1M "
        f"--summary-interval=1 --console-log-level=warn",
        url,
    ]


def run_variant(i, x, url, fmt):
    work = os.path.join(WORK_BASE, f"x{x:02d}")
    if os.path.exists(work):
        shutil.rmtree(work)
    os.makedirs(work, exist_ok=True)

    cmd = build_cmd(url, fmt, x, work)
    log_path = os.path.join(work, "_run.log")
    t0 = time.monotonic()
    with open(log_path, "wb") as lf:
        proc = subprocess.Popen(
            cmd, cwd=work, stdout=lf, stderr=subprocess.STDOUT
        )
        try:
            proc.wait(timeout=600)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    elapsed = time.monotonic() - t0

    total = 0
    for f in os.listdir(work):
        p = os.path.join(work, f)
        if os.path.isfile(p) and not f.startswith("_") and not f.endswith(".aria2"):
            total += os.path.getsize(p)
    rate = total / elapsed / 1024 if elapsed > 0 else 0
    return total, elapsed, rate, proc.returncode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", nargs="?", default=DEFAULT_URL)
    parser.add_argument("--format", default="251-drc")
    parser.add_argument("--x-values", default=",".join(map(str, X_VALUES)),
                        help="comma-separated list of -x values to test")
    args = parser.parse_args()
    x_vals = [int(v) for v in args.x_values.split(",")]

    if not os.path.exists(YT_DLP):
        sys.exit(f"yt-dlp not found: {YT_DLP}")
    if not pot_alive():
        sys.exit(f"pot-provider not reachable at {POT_URL}/ping")
    if not os.path.exists(ARIA2C) and any(x > 1 for x in x_vals):
        sys.exit(f"aria2c not found: {ARIA2C}")

    os.makedirs(WORK_BASE, exist_ok=True)
    print(f"url    : {args.url}")
    print(f"format : {args.format}")
    print(f"x vals : {x_vals}")
    print(f"(fresh PO token before each run, {COOLDOWN}s cooldown between)\n")

    results = []
    for i, x in enumerate(x_vals):
        label = "bare yt-dlp (1 TCP)" if x == 1 else f"aria2c -x {x}"
        print(f"=== Variant {i+1}/{len(x_vals)}: {label} ===")
        status = refresh_pot()
        print(f"  invalidate_caches -> {status}")
        time.sleep(2)
        total, elapsed, rate, rc = run_variant(i, x, args.url, args.format)
        ok = "OK" if rc == 0 else f"rc={rc}"
        print(f"  -> {total / 1024 / 1024:7.2f} MB  in {elapsed:5.1f}s  "
              f"({rate:7.0f} KiB/s)  {ok}")
        results.append((x, total, elapsed, rate, rc))
        if i < len(x_vals) - 1:
            time.sleep(COOLDOWN)

    print("\n======== SUMMARY ========")
    print(f"{'x':>4}  {'MB':>9}  {'sec':>7}  {'KiB/s':>10}  {'vs x=1':>10}  status")
    base_rate = None
    for x, total, elapsed, rate, rc in results:
        if base_rate is None and rate > 0:
            base_rate = rate
        ratio = f"{rate / base_rate:.2f}x" if base_rate else "-"
        ok = "OK" if rc == 0 else f"rc={rc}"
        print(f"{x:>4}  {total/1024/1024:>8.2f}  {elapsed:>7.1f}  "
              f"{rate:>10.0f}  {ratio:>10}  {ok}")


if __name__ == "__main__":
    main()
