#!/usr/bin/env python3
"""Factor-isolation benchmark for YouTube single-connection throttle.

Runs 4 sequential cells (baseline, fresh_video, fresh_ip, fresh_pot) using
bare yt-dlp with a single HTTP connection (no aria2c, no http-chunk-size)
to expose the raw googlevideo per-connection rate. Compares each cell to
the baseline to see which factor (PO token / video / IP) dominates.

Cell ordering is fixed: 0 -> B -> C -> A, because PO-token refresh is
irreversible within the experiment window.

Usage:
    python3 scripts/bench_factor_isolation.py <FRESH_URL> [FRESH_URL2 ...]

The first FRESH_URL is used for the "fresh_video" cell. Additional URLs
are currently unused but accepted to match plan (future 2^3 extension).
"""
import argparse
import os
import shutil
import subprocess
import sys
import time
import urllib.request

OLD_URL  = "https://www.youtube.com/watch?v=QoQBzR1NIqI"
FORMAT   = "251-drc"
YT_DLP   = os.path.expanduser("~/Library/Python/3.14/bin/yt-dlp")
POT_URL  = "http://127.0.0.1:4416"
DURATION = 60
COOLDOWN = 30
TEST_DIR = "/tmp/factor_isolation"

COMMON = [
    YT_DLP, "-f", FORMAT, "--no-playlist", "--newline",
    "--js-runtimes", "node",
    "--extractor-args", f"youtubepot-bgutilhttp:base_url={POT_URL}",
    "--cookies-from-browser", "chrome",
    "-o", "%(id)s.%(ext)s",
    # Intentionally no --downloader and no --http-chunk-size: we want a
    # single unbroken HTTP GET so the googlevideo token-bucket speed is
    # the only thing we measure.
]


def dir_bytes(path, exclude):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            p = os.path.join(root, f)
            if p in exclude:
                continue
            try:
                total += os.path.getsize(p)
            except OSError:
                pass
    return total


def run_yt_dlp(work_dir, url):
    cmd = COMMON + [url]
    out_path = os.path.join(work_dir, "_stdout.log")
    err_path = os.path.join(work_dir, "_stderr.log")
    print("  cmd: " + " ".join(cmd))
    t0 = time.monotonic()
    with open(out_path, "wb") as outf, open(err_path, "wb") as errf:
        proc = subprocess.Popen(cmd, cwd=work_dir, stdout=outf, stderr=errf)
        try:
            proc.wait(timeout=DURATION)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
    elapsed = time.monotonic() - t0
    got = dir_bytes(work_dir, exclude={out_path, err_path})
    rate = got / elapsed / 1024
    return got, elapsed, rate


def measure_cell(name, url):
    work = os.path.join(TEST_DIR, name)
    if os.path.exists(work):
        shutil.rmtree(work)
    os.makedirs(work, exist_ok=True)
    got, elapsed, rate = run_yt_dlp(work, url)
    print(f"  -> {got/1024/1024:.2f} MB in {elapsed:.1f}s  ({rate:.1f} KiB/s)")
    return got, elapsed, rate


def refresh_pot_token():
    """Clear pot-provider integrity/token caches. Next POT mint will redo
    the BotGuard challenge round-trip to YouTube."""
    req = urllib.request.Request(
        POT_URL + "/invalidate_caches",
        data=b"",
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            print(f"  pot-provider /invalidate_caches -> HTTP {r.status}")
    except Exception as e:
        print(f"  WARN: invalidate_caches failed: {e}")
    time.sleep(2)


def wait_for_user(prompt):
    print(prompt)
    try:
        input("  按 Enter 继续 > ")
    except EOFError:
        pass


def pot_health():
    try:
        with urllib.request.urlopen(POT_URL + "/ping", timeout=2) as r:
            return r.status == 200
    except Exception:
        return False


def print_summary(results):
    print("\n======== SUMMARY ========")
    print(f"{'cell':<14}{'downloaded':>13}{'elapsed':>12}{'rate':>16}{'vs baseline':>15}")
    base_rate = results.get("baseline", (0, 0, 0))[2] or 1e-9
    for name in ("baseline", "fresh_video", "fresh_ip", "fresh_pot"):
        if name not in results:
            continue
        got, elapsed, rate = results[name]
        ratio = f"{rate/base_rate:.2f}x" if name != "baseline" else "(base)"
        print(f"{name:<14}{got/1024/1024:10.2f} MB{elapsed:10.1f}s{rate:12.1f} KiB/s{ratio:>15}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("fresh_urls", nargs="+", help="从未下载过的 YouTube URL，至少 1 个")
    parser.add_argument("--old-url", default=OLD_URL)
    args = parser.parse_args()

    if not os.path.exists(YT_DLP):
        sys.exit(f"yt-dlp not found at {YT_DLP}")
    if not pot_health():
        sys.exit(f"pot-provider not reachable at {POT_URL}/ping")

    os.makedirs(TEST_DIR, exist_ok=True)
    print(f"old URL   : {args.old_url}")
    print(f"fresh URL : {args.fresh_urls[0]}")
    print(f"format    : {FORMAT}")
    print(f"window    : {DURATION}s per cell, {COOLDOWN}s cooldown between\n")

    results = {}

    print(">>> Cell 0: baseline (old PO, old video, old IP)")
    results["baseline"] = measure_cell("baseline", args.old_url)
    time.sleep(COOLDOWN)

    print(f"\n>>> Cell B: fresh video ({args.fresh_urls[0]})")
    results["fresh_video"] = measure_cell("fresh_video", args.fresh_urls[0])
    time.sleep(COOLDOWN)

    wait_for_user("\n>>> Cell C: 请【切换到新 IP】（VPN / 热点），切换完成后")
    results["fresh_ip"] = measure_cell("fresh_ip", args.old_url)
    wait_for_user("\n>>> Cell C 完成，请【切回原 IP】")
    time.sleep(COOLDOWN)

    print("\n>>> Cell A: fresh PO token")
    refresh_pot_token()
    results["fresh_pot"] = measure_cell("fresh_pot", args.old_url)

    print_summary(results)


if __name__ == "__main__":
    main()
