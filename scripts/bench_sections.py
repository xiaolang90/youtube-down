#!/usr/bin/env python3
"""Validate yt-dlp --download-sections for chunked large-file downloads.

Downloads N small time-range chunks of the same video sequentially,
calling pot-provider /invalidate_caches between chunks. Each yt-dlp
invocation is a fresh session → fresh cpn → fresh PO token mint.

Hypothesis: if googlevideo's "long-session playback-rate throttle" is
keyed on (IP, cpn, session_duration), each chunk should be treated as
a brand-new session and hit cold-start speeds (several MiB/s) even
when the continuous download has been throttled to ~1x playback rate.

Usage:
    python3 scripts/bench_sections.py <URL> [--format FMT] [--chunks N]
                                      [--chunk-duration SEC] [--stride SEC]
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
import urllib.request

YT_DLP = os.path.expanduser("~/Library/Python/3.14/bin/yt-dlp")
POT_URL = "http://127.0.0.1:4416"
WORK = "/tmp/bench_sections"


def seconds_to_hms(s):
    h, rem = divmod(int(s), 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}"


def refresh_pot():
    req = urllib.request.Request(
        POT_URL + "/invalidate_caches", data=b"", method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status
    except Exception as e:
        print(f"  warn: invalidate_caches failed: {e}")
        return None


def download_section(i, url, fmt, start_sec, dur_sec):
    work = os.path.join(WORK, f"chunk_{i:02d}")
    if os.path.exists(work):
        shutil.rmtree(work)
    os.makedirs(work, exist_ok=True)

    start_str = seconds_to_hms(start_sec)
    end_str = seconds_to_hms(start_sec + dur_sec)
    section = f"*{start_str}-{end_str}"

    cmd = [
        YT_DLP, "-f", fmt, "--no-playlist", "--newline",
        "--js-runtimes", "node",
        "--extractor-args", f"youtubepot-bgutilhttp:base_url={POT_URL}",
        "--cookies-from-browser", "chrome",
        "--download-sections", section,
        "-o", "chunk.%(ext)s",
        url,
    ]

    log_path = os.path.join(work, "_yt-dlp.log")
    t0 = time.monotonic()
    with open(log_path, "wb") as logf:
        proc = subprocess.Popen(
            cmd, cwd=work, stdout=logf, stderr=subprocess.STDOUT,
        )
        try:
            proc.wait(timeout=180)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    elapsed = time.monotonic() - t0

    # Sum sizes of all files except the log
    total = 0
    for f in os.listdir(work):
        p = os.path.join(work, f)
        if os.path.isfile(p) and not f.startswith("_"):
            total += os.path.getsize(p)

    rate = total / elapsed / 1024 if elapsed > 0 else 0
    return total, elapsed, rate, proc.returncode, log_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "url", nargs="?",
        default="https://www.youtube.com/watch?v=QoQBzR1NIqI",
        help="YouTube URL (default: QoQBzR1NIqI, a known 4h video)",
    )
    parser.add_argument("--format", default="251-drc",
                        help="yt-dlp -f spec (default: 251-drc audio-only)")
    parser.add_argument("--chunks", type=int, default=5,
                        help="how many chunks to test")
    parser.add_argument("--chunk-duration", type=int, default=60,
                        help="seconds of video per chunk")
    parser.add_argument("--stride", type=int, default=600,
                        help="seconds between chunk starts (600 = every 10min mark)")
    args = parser.parse_args()

    if not os.path.exists(YT_DLP):
        sys.exit(f"yt-dlp not found at {YT_DLP}")

    os.makedirs(WORK, exist_ok=True)
    print(f"url            : {args.url}")
    print(f"format         : {args.format}")
    print(f"chunks         : {args.chunks}")
    print(f"chunk duration : {args.chunk_duration}s")
    print(f"stride         : {args.stride}s (chunks sampled every {args.stride}s mark)\n")

    results = []
    for i in range(args.chunks):
        start_sec = i * args.stride
        print(f"=== Chunk {i+1}/{args.chunks}: "
              f"{seconds_to_hms(start_sec)}-{seconds_to_hms(start_sec + args.chunk_duration)} ===")
        status = refresh_pot()
        print(f"  invalidate_caches -> HTTP {status}")
        time.sleep(0.5)
        got, elapsed, rate, rc, log_path = download_section(
            i, args.url, args.format, start_sec, args.chunk_duration
        )
        ok = "OK" if rc == 0 else f"FAIL rc={rc}"
        print(f"  -> {got/1024/1024:7.2f} MB  in {elapsed:5.1f}s  "
              f"({rate:7.0f} KiB/s)  {ok}")
        if rc != 0:
            print(f"  ⚠ see log: {log_path}")
        results.append((i + 1, start_sec, got, elapsed, rate, rc))

    print("\n======== SUMMARY ========")
    print(f"{'chunk':<6}{'start':>10}{'MB':>9}{'sec':>8}{'KiB/s':>12}{'status':>8}")
    for i, s, got, e, r, rc in results:
        ok = "OK" if rc == 0 else f"rc={rc}"
        print(f"{i:<6}{seconds_to_hms(s):>10}{got/1024/1024:>8.2f} "
              f"{e:>7.1f} {r:>11.0f} {ok:>8}")


if __name__ == "__main__":
    main()
