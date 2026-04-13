#!/usr/bin/env python3
"""Benchmark audio-only YouTube download strategies against CDN throttle."""
import os, shutil, subprocess, time

URL      = "https://www.youtube.com/watch?v=QoQBzR1NIqI"
FORMAT   = "251-drc"
YT_DLP   = os.path.expanduser("~/Library/Python/3.14/bin/yt-dlp")
POT_ARG  = "youtubepot-bgutilhttp:base_url=http://127.0.0.1:4416"
TEST_DIR = "/tmp/audio_bench"
DURATION = 60  # seconds per variant (must cover cold-start metadata + pot token)

COMMON = [
    YT_DLP, "-f", FORMAT, "--no-playlist", "--newline",
    "--js-runtimes", "node",
    "--extractor-args", POT_ARG,
    "--cookies-from-browser", "chrome",
    "-o", "%(id)s.%(ext)s",
]

VARIANTS = [
    ("baseline_100M", ["--http-chunk-size", "100M"]),
    ("small_1M",      ["--http-chunk-size", "1M"]),
    ("aria2c_x16",    ["--downloader", "aria2c",
                       "--downloader-args",
                       "aria2c:-x 16 -s 16 -k 1M --summary-interval=1"]),
]


def dir_bytes(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def run_variant(name, extra):
    work = os.path.join(TEST_DIR, name)
    if os.path.exists(work):
        shutil.rmtree(work)
    os.makedirs(work, exist_ok=True)
    cmd = COMMON + extra + [URL]
    print(f"\n=== {name} ===")
    print("  " + " ".join(cmd))
    out_path = os.path.join(work, "_stdout.log")
    err_path = os.path.join(work, "_stderr.log")
    t0 = time.monotonic()
    with open(out_path, "wb") as outf, open(err_path, "wb") as errf:
        proc = subprocess.Popen(cmd, cwd=work, stdout=outf, stderr=errf)
        try:
            proc.wait(timeout=DURATION)
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
    elapsed = time.monotonic() - t0
    got = (dir_bytes(work)
           - os.path.getsize(out_path)
           - os.path.getsize(err_path))
    rate = got / elapsed / 1024
    print(f"  -> {got/1024/1024:.2f} MB in {elapsed:.1f}s  ({rate:.1f} KiB/s)")
    return got, elapsed, rate


def main():
    os.makedirs(TEST_DIR, exist_ok=True)
    print(f"URL     = {URL}")
    print(f"format  = {FORMAT}")
    print(f"window  = {DURATION}s per variant")
    results = []
    for name, extra in VARIANTS:
        got, elapsed, rate = run_variant(name, extra)
        results.append((name, got, elapsed, rate))

    print("\n======== SUMMARY ========")
    print(f"{'variant':<18} {'downloaded':>13} {'elapsed':>10} {'rate':>16}")
    for name, got, elapsed, rate in results:
        print(f"{name:<18} {got/1024/1024:10.2f} MB {elapsed:8.1f}s {rate:12.1f} KiB/s")


if __name__ == "__main__":
    main()
