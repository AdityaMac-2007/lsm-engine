#!/usr/bin/env python3
import argparse
import json
import os
import random
import shutil
import sqlite3
import subprocess
import tempfile
import time


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, int(round((len(sorted_vals) - 1) * p)))
    return sorted_vals[idx]


def run_sqlite_bench(num_records: int, num_probes: int):
    tmp_dir = tempfile.mkdtemp(prefix="sqlite_wal_bench_")
    db_path = os.path.join(tmp_dir, "bench.db")
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v BLOB) WITHOUT ROWID;")

        val = b"V" * 64
        batch_size = 50_000
        t0 = time.perf_counter()
        for start in range(0, num_records, batch_size):
            end = min(num_records, start + batch_size)
            conn.executemany(
                "INSERT INTO kv (k, v) VALUES (?, ?);",
                ((f"k_{i:010}", val) for i in range(start, end)),
            )
            conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        write_secs = time.perf_counter() - t0

        db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        wal_path = db_path + "-wal"
        wal_size = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
        logical_bytes = num_records * (12 + 64)
        waf = (db_size + wal_size) / logical_bytes if logical_bytes else 0.0

        rng = random.Random(42)
        pos_lat = []
        for _ in range(num_probes):
            k = f"k_{rng.randrange(num_records):010}"
            ts = time.perf_counter()
            conn.execute("SELECT v FROM kv WHERE k = ?;", (k,)).fetchone()
            pos_lat.append((time.perf_counter() - ts) * 1e6)
        pos_lat.sort()

        neg_lat = []
        for i in range(num_probes):
            k = f"z_missing_{i:010}"
            ts = time.perf_counter()
            conn.execute("SELECT v FROM kv WHERE k = ?;", (k,)).fetchone()
            neg_lat.append((time.perf_counter() - ts) * 1e6)
        neg_lat.sort()

        conn.close()
        return {
            "records": num_records,
            "write_secs": write_secs,
            "write_ops_per_sec": num_records / write_secs,
            "db_bytes": db_size,
            "waf": waf,
            "present_us": [
                percentile(pos_lat, 0.50),
                percentile(pos_lat, 0.95),
                percentile(pos_lat, 0.99),
            ],
            "absent_us": [
                percentile(neg_lat, 0.50),
                percentile(neg_lat, 0.95),
                percentile(neg_lat, 0.99),
            ],
        }
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def write_svg_chart(results, out_path):
    lsm = results["lsm"]
    sql = results["sqlite_wal"]
    w, h = 960, 340
    max_ops = max(lsm["write_ops_per_sec"], sql["write_ops_per_sec"], 1.0)
    max_waf = max(lsm["waf"], sql["waf"], 1.0)
    max_lat = max(
        max(lsm["absent_bloom_us"]),
        max(lsm["absent_nobloom_us"]),
        max(sql["absent_us"]),
        1.0,
    )

    def bar(x, y, bw, bh, color, label, val_str):
        return (
            f'<rect x="{x}" y="{y:.1f}" width="{bw}" height="{bh:.1f}" fill="{color}" rx="3"/>'
            f'<text x="{x + bw/2:.1f}" y="{y - 6:.1f}" font-size="11" text-anchor="middle" fill="#1e293b">{val_str}</text>'
            f'<text x="{x + bw/2:.1f}" y="295" font-size="11" text-anchor="middle" fill="#475569">{label}</text>'
        )

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" style="font-family: -apple-system, sans-serif; background: #ffffff;">',
        f'<text x="160" y="30" font-size="14" font-weight="bold" text-anchor="middle" fill="#0f172a">Write Throughput (ops/s)</text>',
        f'<text x="480" y="30" font-size="14" font-weight="bold" text-anchor="middle" fill="#0f172a">Absent-Key p99 Latency (µs)</text>',
        f'<text x="800" y="30" font-size="14" font-weight="bold" text-anchor="middle" fill="#0f172a">Write Amplification (WAF)</text>',
    ]

    chart_h = 200.0
    base_y = 270.0

    # Throughput
    bh1 = (lsm["write_ops_per_sec"] / max_ops) * chart_h
    bh2 = (sql["write_ops_per_sec"] / max_ops) * chart_h
    parts.append(bar(75, base_y - bh1, 65, bh1, "#2563eb", "LSM-Tree", f'{lsm["write_ops_per_sec"]:,.0f}'))
    parts.append(bar(175, base_y - bh2, 65, bh2, "#64748b", "SQLite WAL", f'{sql["write_ops_per_sec"]:,.0f}'))

    # Absent p99 Latency
    lat1 = (lsm["absent_bloom_us"][2] / max_lat) * chart_h
    lat2 = (lsm["absent_nobloom_us"][2] / max_lat) * chart_h
    lat3 = (sql["absent_us"][2] / max_lat) * chart_h
    parts.append(bar(365, base_y - lat1, 55, lat1, "#16a34a", "Bloom ON", f'{lsm["absent_bloom_us"][2]:.1f}'))
    parts.append(bar(450, base_y - lat2, 55, lat2, "#dc2626", "Bloom OFF", f'{lsm["absent_nobloom_us"][2]:.1f}'))
    parts.append(bar(535, base_y - lat3, 55, lat3, "#64748b", "SQLite WAL", f'{sql["absent_us"][2]:.1f}'))

    # WAF
    waf1 = (lsm["waf"] / max_waf) * chart_h
    waf2 = (sql["waf"] / max_waf) * chart_h
    parts.append(bar(715, base_y - waf1, 65, waf1, "#2563eb", "LSM-Tree", f'{lsm["waf"]:.2f}x'))
    parts.append(bar(815, base_y - waf2, 65, waf2, "#64748b", "SQLite WAL", f'{sql["waf"]:.2f}x'))

    parts.append("</svg>")
    with open(out_path, "w") as f:
        f.write("\n".join(parts))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=10_000_000)
    parser.add_argument("--probes", type=int, default=5_000)
    parser.add_argument("--out-dir", default=".")
    args = parser.parse_args()

    env = os.environ.copy()
    env["PATH"] = os.path.expanduser("~/.cargo/bin") + ":" + env.get("PATH", "")
    env["CARGO_TARGET_DIR"] = "/tmp/lsm-target"

    print(f"Building release binary and running LSM-Tree benchmark ({args.records:,} records)...")
    subprocess.run(["cargo", "build", "--release", "--bin", "bench"], check=True, env=env)
    lsm_out = subprocess.check_output(
        ["/tmp/lsm-target/release/bench", "--records", str(args.records), "--probes", str(args.probes)],
        env=env,
        text=True,
    )
    lsm = json.loads(lsm_out)

    print(f"Running SQLite (WAL mode) benchmark ({args.records:,} records)...")
    sql = run_sqlite_bench(args.records, args.probes)

    results = {"lsm": lsm, "sqlite_wal": sql}
    json_path = os.path.join(args.out_dir, "bench_results.json")
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)

    svg_path = os.path.join(args.out_dir, "benchmark_comparison.svg")
    write_svg_chart(results, svg_path)
    print(f"Saved {json_path} and {svg_path}")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
