#!/usr/bin/env python3
"""Pollution induction loop: hammer a URL with aria2c 16x and track rate.

Repeatedly downloads the target URL via yt-dlp + aria2c (16 connections)
and logs per-iteration average rate. Stops on:
  - aggregate rate drops below STOP_RATE_KIB (throttle detected)
  - MAX_MINUTES elapsed (time cap)
  - Ctrl-C

Outputs a history table so you can see the rate progression over time.
"""
import argparse
import os
import re
import shutil
import signal
import subprocess
import sys
import time

DEFAULT_URL  = "https://www.youtube.com/watch?v=QoQBzR1NIqI"
URL          = DEFAULT_URL
FORMAT       = "251-drc"
YT_DLP       = os.path.expanduser("~/Library/Python/3.14/bin/yt-dlp")
ARIA2C       = "/usr/local/bin/aria2c"
POT_URL      = "http://127.0.0.1:4416"
WORK_DIR     = "/tmp/pollute_loop"
CONNECTIONS  = 16
STOP_RATE_KIB = 1500   # if iteration avg drops below this -> throttle reached
MAX_MINUTES  = 15

ARIA_RE = re.compile(r'DL:([\d.]+)([KMG]?i?B)')


def build_cmd():
    return [
        YT_DLP, "-f", FORMAT, "--no-playlist", "--newline",
        "--js-runtimes", "node",
        "--extractor-args", f"youtubepot-bgutilhttp:base_url={POT_URL}",
        "--cookies-from-browser", "chrome",
        "-o", "%(id)s.%(ext)s",
        "--downloader", ARIA2C,
        "--downloader-args",
        f"aria2c:-x {CONNECTIONS} -s {CONNECTIONS} -k 1M --summary-interval=2 --console-log-level=warn",
        URL,
    ]


def dir_max_file_bytes(path):
    """Return max file size in dir (since aria2c may write .part and .aria2 metadata)."""
    total = 0
    for f in os.listdir(path):
        p = os.path.join(path, f)
        if os.path.isfile(p) and not f.startswith('.'):
            sz = os.path.getsize(p)
            # Skip aria2 control file which is tiny
            if f.endswith('.aria2'):
                continue
            if sz > total:
                total = sz
    return total


def run_iteration(i, log_file):
    if os.path.exists(WORK_DIR):
        shutil.rmtree(WORK_DIR)
    os.makedirs(WORK_DIR, exist_ok=True)

    cmd = build_cmd()
    t0 = time.monotonic()
    last_sample_print = 0.0
    samples = []

    proc = subprocess.Popen(
        cmd, cwd=WORK_DIR,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )

    try:
        for line in iter(proc.stdout.readline, ''):
            now = time.monotonic()
            line = line.rstrip()
            m = ARIA_RE.search(line)
            if m:
                val = float(m.group(1))
                unit = m.group(2)
                mul = {'B': 1, 'KB': 1000, 'KiB': 1024,
                       'MB': 1_000_000, 'MiB': 1024*1024,
                       'GB': 1_000_000_000, 'GiB': 1024**3}.get(unit, 1)
                bps = val * mul
                kib = bps / 1024
                samples.append((now - t0, kib))
                # Print ~once per 5s
                if now - last_sample_print >= 5:
                    last_sample_print = now
                    msg = f"    [iter {i} T+{now-t0:5.1f}s] DL {kib:7.0f} KiB/s"
                    print(msg, flush=True)
                    log_file.write(msg + "\n")
                    log_file.flush()
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        raise

    elapsed = time.monotonic() - t0
    got = dir_max_file_bytes(WORK_DIR)
    avg_kib = got / elapsed / 1024 if elapsed > 0 else 0
    return avg_kib, elapsed, got, samples


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--max-minutes", type=int, default=MAX_MINUTES)
    parser.add_argument("--stop-rate", type=int, default=STOP_RATE_KIB,
                        help="KiB/s; stop when iter avg drops below this")
    args = parser.parse_args()

    global URL
    URL = args.url

    os.makedirs(os.path.dirname(WORK_DIR) or "/tmp", exist_ok=True)
    log_path = f"{WORK_DIR}.log"
    with open(log_path, "w") as log_file:
        print(f"target URL : {URL}")
        print(f"aria2c -x  : {CONNECTIONS}")
        print(f"stop rate  : {args.stop_rate} KiB/s")
        print(f"time cap   : {args.max_minutes} min")
        print(f"log file   : {log_path}\n")

        start = time.monotonic()
        iteration = 0
        history = []
        try:
            while True:
                elapsed_total = time.monotonic() - start
                if elapsed_total > args.max_minutes * 60:
                    print(f"\n[STOP] reached {args.max_minutes} min cap")
                    break
                iteration += 1
                hdr = f"\n=== Iter {iteration}  (T+{elapsed_total/60:5.2f} min) ==="
                print(hdr, flush=True)
                log_file.write(hdr + "\n")
                log_file.flush()

                avg, dur, got, samples = run_iteration(iteration, log_file)
                result = (f"  -> {got/1024/1024:.1f} MB in {dur:.1f}s  "
                          f"avg {avg:.0f} KiB/s")
                print(result, flush=True)
                log_file.write(result + "\n")
                log_file.flush()
                history.append((iteration, elapsed_total, avg, got))

                if avg < args.stop_rate:
                    print(f"\n[STOP] iter avg {avg:.0f} < {args.stop_rate} KiB/s — throttle reached!")
                    break
        except KeyboardInterrupt:
            print("\n[INTERRUPTED]")

        print("\n======== HISTORY ========")
        print(f"{'iter':>4} {'T+min':>7} {'MB':>8} {'avg KiB/s':>12}")
        for i, t, r, b in history:
            print(f"{i:>4} {t/60:>6.2f} {b/1024/1024:>8.1f} {r:>12.0f}")
        log_file.write("\n======== HISTORY ========\n")
        for i, t, r, b in history:
            log_file.write(f"{i:>4} {t/60:>6.2f}m {b/1024/1024:>7.1f}MB {r:>10.0f} KiB/s\n")


if __name__ == "__main__":
    main()
