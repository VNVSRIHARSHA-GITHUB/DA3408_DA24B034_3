"""
verify_parity.py -- do the Spark and Ray outputs match exactly?

Compares two output folders (each with  trips/  and  hourly_speed/  Parquet folders).


Usage:
  python verify_parity.py                                   # 3-month subset (spark_3month vs ray_3month)
  python verify_parity.py --spark output/spark_clean --ray output/ray_clean --tag full
"""
import argparse
import glob
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
MASK = (1 << 64) - 1
BATCH_ROWS = 250_000
REL_TOL = 1e-9


def mix(x):
    """splitmix64 finalizer on a uint64 ndarray (wraps silently on overflow)."""
    with np.errstate(over="ignore"):
        x = x ^ (x >> np.uint64(30))
        x = x * np.uint64(0xBF58476D1CE4E5B9)
        x = x ^ (x >> np.uint64(27))
        x = x * np.uint64(0x94D049BB133111EB)
        x = x ^ (x >> np.uint64(31))
    return x


def to_u64(arr):
    """Column -> uint64 array with a representation that is identical across Spark and Ray."""
    t = arr.type
    is_str = pa.types.is_string(t) or pa.types.is_large_string(t)
    if arr.null_count:
        arr = pc.fill_null(arr, pa.scalar("" if is_str else 0, type=t))
    if is_str:
        d = arr.dictionary_encode()
        idx = d.indices.to_numpy(zero_copy_only=False).astype(np.int64)
        vals = np.array(d.dictionary.to_pylist(), dtype=object)
        hv = pd.util.hash_array(vals) if len(vals) else np.zeros(0, dtype=np.uint64)
        return np.asarray(hv, dtype=np.uint64)[idx]
    if pa.types.is_timestamp(t):                      # -> epoch microseconds (tz ignored)
        raw = arr.cast(pa.int64()).to_numpy(zero_copy_only=False).astype(np.int64)
        if t.unit == "ns":
            raw = raw // 1000
        elif t.unit == "ms":
            raw = raw * 1000
        elif t.unit == "s":
            raw = raw * 1_000_000
        return raw.view(np.uint64)
    if pa.types.is_date32(t):
        return arr.cast(pa.int32()).to_numpy(zero_copy_only=False).astype(np.int64).view(np.uint64)
    if pa.types.is_floating(t):
        return arr.to_numpy(zero_copy_only=False).astype(np.float64).view(np.uint64)
    if pa.types.is_integer(t):
        return arr.to_numpy(zero_copy_only=False).astype(np.int64).view(np.uint64)
    raise TypeError(f"unsupported column type {t}")


def numeric_stats(arr):
    """min / max / sum for int and float columns (None for others)."""
    t = arr.type
    if not (pa.types.is_integer(t) or pa.types.is_floating(t)):
        return None
    a = arr.to_numpy(zero_copy_only=False)
    a = a[~np.isnan(a)] if a.dtype.kind == "f" else a
    if a.size == 0:
        return None
    return float(a.min()), float(a.max()), float(a.sum(dtype=np.float64))


def scan(folder):
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise SystemExit(f"no .parquet files in {folder}")
    schema = pq.ParquetFile(files[0]).schema_arrow
    cols = sorted(schema.names)
    acc = {"files": len(files), "rows": 0, "types": {n: str(schema.field(n).type) for n in cols},
           "cols": cols, "nulls": {c: 0 for c in cols}, "colsum": {c: 0 for c in cols},
           "min": {}, "max": {}, "sum": {}, "row1": 0, "row2": 0}
    for f in files:
        pf = pq.ParquetFile(f)
        for rb in pf.iter_batches(batch_size=BATCH_ROWS, columns=cols):
            n = rb.num_rows
            acc["rows"] += n
            h = np.zeros(n, dtype=np.uint64)
            for i, c in enumerate(cols):
                arr = rb.column(c)
                acc["nulls"][c] += arr.null_count
                u = to_u64(arr)
                ch = mix(u ^ np.uint64((i + 1) * 0x9E3779B97F4A7C15 & MASK))
                acc["colsum"][c] = (acc["colsum"][c] + int(ch.sum(dtype=np.uint64))) & MASK
                h = mix(h ^ ch)
                s = numeric_stats(arr)
                if s:
                    lo, hi, sm = s
                    acc["min"][c] = min(acc["min"].get(c, lo), lo)
                    acc["max"][c] = max(acc["max"].get(c, hi), hi)
                    acc["sum"][c] = acc["sum"].get(c, 0.0) + sm
            acc["row1"] = (acc["row1"] + int(h.sum(dtype=np.uint64))) & MASK
            acc["row2"] = (acc["row2"] + int(mix(h ^ np.uint64(0xA5A5A5A5A5A5A5A5)).sum(dtype=np.uint64))) & MASK
    return acc


def compare_rows(a, b, report):
    ok = True

    def check(name, cond, detail=""):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'PASS' if cond else 'FAIL'}] {name} {detail}")
        report[name] = bool(cond)

    print(f"Spark: {a['files']} files, {a['rows']:,} rows | Ray: {b['files']} files, {b['rows']:,} rows")
    check("same columns", a["cols"] == b["cols"], f"(spark-only={sorted(set(a['cols']) - set(b['cols']))}, "
                                                   f"ray-only={sorted(set(b['cols']) - set(a['cols']))})")
    check("row count", a["rows"] == b["rows"])
    common = [c for c in a["cols"] if c in b["cols"]]
    check("null counts equal", all(a["nulls"][c] == b["nulls"][c] for c in common),
          f"(spark total={sum(a['nulls'].values())}, ray total={sum(b['nulls'].values())})")
    bad = [c for c in common if a["colsum"][c] != b["colsum"][c]]
    check("per-column checksums equal", not bad, f"(differing: {bad})" if bad else "")
    check("whole-row checksums equal", a["row1"] == b["row1"] and a["row2"] == b["row2"])
    drift = []
    for c in common:
        if c in a["sum"] and c in b["sum"]:
            for k in ("min", "max", "sum"):
                x, y = a[k][c], b[k][c]
                if abs(x - y) > REL_TOL * max(1.0, abs(x), abs(y)):
                    drift.append((c, k, x, y))
    check("numeric min/max/sum within 1e-9", not drift, f"(differences: {drift[:5]})" if drift else "")
    diff_types = {c: (a["types"][c], b["types"][c]) for c in common if a["types"][c] != b["types"][c]}
    if diff_types:
        print("  [INFO] storage types differ (values still compared on a common representation):")
        for c, (x, y) in diff_types.items():
            print(f"         {c}: spark={x}  ray={y}")
    return ok


def compare_hourly(sdir, rdir, report):
    s = pq.read_table(sdir).to_pandas()
    r = pq.read_table(rdir).to_pandas()
    keys = ["pu_borough", "pickup_hour"]
    ok = True

    def check(name, cond, detail=""):
        nonlocal ok
        ok &= bool(cond)
        print(f"  [{'PASS' if cond else 'FAIL'}] {name} {detail}")
        report[name] = bool(cond)

    check("hourly: same columns", sorted(s.columns) == sorted(r.columns),
          f"(spark={sorted(s.columns)}, ray={sorted(r.columns)})")
    if sorted(s.columns) != sorted(r.columns):
        return False
    s = s.sort_values(keys).reset_index(drop=True)
    r = r.sort_values(keys).reset_index(drop=True)
    check("hourly: same number of groups", len(s) == len(r), f"({len(s)} vs {len(r)})")
    if len(s) != len(r):
        return False
    check("hourly: same group keys", (s[keys].astype(str).values == r[keys].astype(str).values).all())
    check("hourly: trip counts identical", (s["trips"].astype("int64").values == r["trips"].astype("int64").values).all())
    for c in ("avg_speed_mph", "avg_trip_distance"):
        x, y = s[c].astype("float64").values, r[c].astype("float64").values
        rel = float(np.max(np.abs(x - y) / np.maximum(1.0, np.abs(x)))) if len(x) else 0.0
        check(f"hourly: {c} within 1e-9", rel <= REL_TOL, f"(max relative diff {rel:.2e})")
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--spark", default=str(ROOT / "output/spark_3month"))
    ap.add_argument("--ray", default=str(ROOT / "output/ray_3month"))
    ap.add_argument("--tag", default="subset")
    args = ap.parse_args()

    t0 = time.perf_counter()
    report = {"spark_dir": args.spark, "ray_dir": args.ray}
    print("=== trips (row level) ===")
    ok_rows = compare_rows(scan(os.path.join(args.spark, "trips")), scan(os.path.join(args.ray, "trips")), report)
    print("=== hourly_speed (aggregate) ===")
    ok_hour = compare_hourly(os.path.join(args.spark, "hourly_speed"), os.path.join(args.ray, "hourly_speed"), report)

    ok = ok_rows and ok_hour
    print(f"\nOVERALL: {'PARITY OK' if ok else 'PARITY FAILED'}   ({time.perf_counter() - t0:.0f} s)")
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    report["parity_ok"] = bool(ok)
    with open(out / f"parity_{args.tag}.json", "w") as f:
        json.dump(report, f, indent=2)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
