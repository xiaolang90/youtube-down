#!/usr/bin/env python3
"""Tail the active log and print TRUE cumulative average rate.

aria2c's `DL:X` field is a noisy short-window rolling average that
oscillates 10x within seconds and misleads you about the real throughput.
This script instead computes `(bytes_now - bytes_start) / (time_now - time_start)`
from the aria2c progress lines in the log — the actual throughput.

It also prints a short-window rate (last 60s) so you can see whether the
download is currently accelerating, steady, or decaying.

Usage:
    python3 scripts/watch_progress.py [LOG_PATH]

If LOG_PATH is omitted, auto-detects the most recent log/app-*.log file.
"""
import glob
import os
import re
import sys
import time
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'log')

# stdout: [#013f82 1.3GiB/20GiB(6%) CN:16 DL:454KiB ETA:12h33m4s]
# The units can be any of B, KiB, MiB, GiB, TiB (aria2c uses binary IEC).
LINE_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
    r'.*stdout:\s*\[#\w+\s+'
    r'([\d.]+)([KMGT]?i?B)/([\d.]+)([KMGT]?i?B)'
    r'\((\d+)%\)\s+CN:(\d+)'
)

UNIT = {
    'B': 1, 'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3, 'TiB': 1024**4,
    'KB': 1000, 'MB': 1000**2, 'GB': 1000**3, 'TB': 1000**4,
}


def parse_size(val, unit):
    return float(val) * UNIT.get(unit, 1)


def human(bps):
    if bps >= 1024**2:
        return f"{bps / 1024**2:6.2f} MiB/s"
    if bps >= 1024:
        return f"{bps / 1024:6.1f} KiB/s"
    return f"{bps:6.0f}   B/s"


def human_gib(b):
    if b >= 1024**3:
        return f"{b / 1024**3:5.2f} GiB"
    if b >= 1024**2:
        return f"{b / 1024**2:5.1f} MiB"
    return f"{b / 1024:5.0f} KiB"


def pick_log(arg):
    if arg:
        return arg
    files = sorted(
        glob.glob(os.path.join(LOG_DIR, 'app-*.log')),
        key=os.path.getmtime,
        reverse=True,
    )
    if not files:
        sys.exit(f"no log files found under {LOG_DIR}")
    return files[0]


def main():
    log_path = pick_log(sys.argv[1] if len(sys.argv) > 1 else None)
    print(f"tailing {log_path}\n")
    print(f"{'wall':<9} {'T+':<9} {'downloaded':<16} {'%':>5}  "
          f"{'cumulative':>12}  {'last 60s':>12}  {'CN':>3}")
    print("-" * 80)

    start_ts = None
    start_bytes = None
    last_print = 0.0
    samples = []  # list of (ts_datetime, downloaded_bytes)

    with open(log_path) as f:
        f.seek(0, os.SEEK_END)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.3)
                continue

            m = LINE_RE.match(line)
            if not m:
                continue

            ts_str, val, unit, tot_val, tot_unit, pct, cn = m.groups()
            ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
            downloaded = parse_size(val, unit)
            total = parse_size(tot_val, tot_unit)

            if start_ts is None:
                start_ts = ts
                start_bytes = downloaded
                total_at_start = total
                samples.append((ts, downloaded))
                print(f"[START] {ts.strftime('%H:%M:%S')}  "
                      f"baseline {human_gib(downloaded)} / {human_gib(total)}")
                continue

            # When yt-dlp switches from video file to audio file (merged format)
            # the total changes and downloaded drops to 0. Reset our baseline
            # so cumulative rate isn't poisoned by the cross-file math.
            if total != total_at_start:
                print(f"[FILE SWITCH] {ts.strftime('%H:%M:%S')}  "
                      f"new file: {human_gib(total)}  (resetting baseline)")
                start_ts = ts
                start_bytes = downloaded
                total_at_start = total
                samples = [(ts, downloaded)]
                continue

            samples.append((ts, downloaded))
            # Trim samples older than 120s so short-window stays bounded
            cutoff = ts.timestamp() - 120
            samples[:] = [s for s in samples if s[0].timestamp() >= cutoff]

            now = time.monotonic()
            if now - last_print < 10:
                continue
            last_print = now

            elapsed = (ts - start_ts).total_seconds()
            if elapsed <= 0:
                continue

            cum_bps = (downloaded - start_bytes) / elapsed

            # short-window rate over the last ~60 seconds of samples
            window = [s for s in samples if (ts - s[0]).total_seconds() <= 60]
            if len(window) >= 2:
                w_dt = (window[-1][0] - window[0][0]).total_seconds()
                w_db = window[-1][1] - window[0][1]
                short_bps = w_db / w_dt if w_dt > 0 else 0
            else:
                short_bps = 0

            mm, ss = divmod(int(elapsed), 60)
            print(f"{ts.strftime('%H:%M:%S')} "
                  f"T+{mm:3d}m{ss:02d}s "
                  f"{human_gib(downloaded):>9}/{human_gib(total):<6} "
                  f"{float(pct):>4.1f}%  "
                  f"{human(cum_bps):>12}  "
                  f"{human(short_bps):>12}  "
                  f"{cn:>3}",
                  flush=True)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
