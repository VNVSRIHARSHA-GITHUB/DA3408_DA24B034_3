"""
ray_clean.py -- DA3408 A3, Ray Data side. Same pipeline and rules as spark_clean.py.

  1. Ingestion      : ray.data.read_parquet (9 columns only)
  2. Normalization  : force ONE canonical 9-column schema right after ingestion
  3. Cleansing      : null drop, sanity filters, exact-duplicate removal, timestamp formatting
  4. Transformation : join with the zone lookup (pickup + dropoff), the SAME Python function
                      for average speed, speed sanity filter
  5. Export         : row-level Parquet + hourly-speed aggregate Parquet

Needs a running Ray cluster (head + 2 workers started with `ray start`).
Run:
  python ray_clean.py --input "data/trips/yellow_tripdata_2023-0[2-4].parquet" \
                      --output-dir output/ray_3month --tag subset          # small test
  python ray_clean.py                                                      # all 35 files
  python ray_clean.py --hold 90                                            # keep dashboard job visible
  python ray_clean.py --udf-benchmark --runs 3                             # UDF test on exported Parquet
"""
import argparse
import glob
import json
import shutil
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import ray
from ray.data.aggregate import Count, Mean
from ray.data.context import DataContext, ShuffleStrategy

ROOT = Path(__file__).resolve().parent
ZONE_CSV = str(ROOT / "data/lookup/taxi_zone_lookup.csv")

# ---- Stage 2 contract: column -> Arrow type (order matters; same as Spark) ----
SCHEMA = {
    "VendorID": pa.int32(),
    "tpep_pickup_datetime": pa.timestamp("us"),
    "tpep_dropoff_datetime": pa.timestamp("us"),
    "trip_distance": pa.float64(),
    "PULocationID": pa.int32(),
    "DOLocationID": pa.int32(),
    "payment_type": pa.int64(),
    "fare_amount": pa.float64(),
    "total_amount": pa.float64(),
}
COLS = list(SCHEMA)
TARGET_SCHEMA = pa.schema(list(SCHEMA.items()))

# ---- Cleansing constants (identical to spark_clean.py) ------------------------
PICKUP_MIN_DT = datetime(2023, 2, 1)     # inclusive
PICKUP_MAX_DT = datetime(2026, 1, 1)     # exclusive
MIN_DURATION_US = 60 * 1_000_000
MAX_DURATION_US = 24 * 3600 * 1_000_000
MAX_SPEED_MPH = 100.0
PICKUP, DROPOFF = "tpep_pickup_datetime", "tpep_dropoff_datetime"

FINAL_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
    "PULocationID", "DOLocationID", "payment_type", "fare_amount", "total_amount",
    "pickup_date", "pickup_hour", "trip_duration_min",
    "pu_borough", "pu_zone", "do_borough", "do_zone", "avg_speed_mph",
]


# ---- the custom Python function: EXACTLY the one used in spark_clean.py ---------
def speed_mph(distance, duration_min):
    if distance is None or duration_min is None or duration_min <= 0:
        return None
    return round(distance / (duration_min / 60.0), 3)


# ---- Stage 2: schema normalization (immediately after ingestion) ---------------
def normalize_batch(tbl: pa.Table) -> pa.Table:
    return tbl.select(COLS).cast(TARGET_SCHEMA)


# ---- Stage 3: cleansing ----------------------------------------------------------
def _duration_us(tbl: pa.Table):
    return pc.subtract(pc.cast(tbl[DROPOFF], pa.int64()), pc.cast(tbl[PICKUP], pa.int64()))


def cleanse_batch(tbl: pa.Table, num_buckets: int) -> pa.Table:
    """Null drop + row-wise sanity filters, then tag each row with a dedupe bucket.
    Identical rows have identical pickup timestamps, so they always share a bucket."""
    tbl = tbl.drop_null()                                              # 3a. nulls
    lo = pa.scalar(PICKUP_MIN_DT, type=pa.timestamp("us"))
    hi = pa.scalar(PICKUP_MAX_DT, type=pa.timestamp("us"))
    dur = _duration_us(tbl)
    mask = pc.and_(pc.greater_equal(tbl[PICKUP], lo), pc.less(tbl[PICKUP], hi))
    mask = pc.and_(mask, pc.greater_equal(dur, MIN_DURATION_US))
    mask = pc.and_(mask, pc.less_equal(dur, MAX_DURATION_US))
    mask = pc.and_(mask, pc.greater(tbl["trip_distance"], 0.0))
    mask = pc.and_(mask, pc.greater_equal(tbl["fare_amount"], 0.0))
    mask = pc.and_(mask, pc.greater_equal(tbl["total_amount"], 0.0))
    tbl = tbl.filter(mask)                                             # 3b. sanity filters
    secs = pc.cast(tbl[PICKUP], pa.int64()).to_numpy() // 1_000_000
    return tbl.append_column("_bucket", pa.array(secs % num_buckets, type=pa.int64()))


def dedupe_group(tbl: pa.Table) -> pa.Table:
    """3c. exact-duplicate removal over all 9 columns, inside one bucket."""
    tbl = tbl.select(COLS)
    return tbl.group_by(COLS, use_threads=False).aggregate([]).select(COLS)


# ---- Stage 4: transformation -------------------------------------------------------
def load_zones():
    # PyArrow treats strings like "N/A" as null by default; Spark keeps them as text.
    # Disable that so both frameworks see the same lookup values.
    opts = pacsv.ConvertOptions(
        column_types={"LocationID": pa.int32(), "Borough": pa.string(), "Zone": pa.string()},
        null_values=[], strings_can_be_null=False)
    z = pacsv.read_csv(ZONE_CSV, convert_options=opts)
    pu = pa.table({"PULocationID": z["LocationID"], "pu_borough": z["Borough"], "pu_zone": z["Zone"]})
    do = pa.table({"DOLocationID": z["LocationID"], "do_borough": z["Borough"], "do_zone": z["Zone"]})
    return pu, do


def derive_and_transform(tbl: pa.Table, pu: pa.Table, do: pa.Table) -> pa.Table:
    pickup = tbl[PICKUP]
    dur_us = pc.cast(_duration_us(tbl), pa.float64())
    tbl = tbl.append_column("pickup_date", pc.cast(pickup, pa.date32()))            # 3d. timestamp formatting
    tbl = tbl.append_column("pickup_hour", pc.cast(pc.hour(pickup), pa.int32()))
    tbl = tbl.append_column("trip_duration_min", pc.divide(dur_us, 60_000_000.0))
    tbl = tbl.join(pu, keys="PULocationID", join_type="inner", use_threads=False)  # pickup zone
    tbl = tbl.join(do, keys="DOLocationID", join_type="inner", use_threads=False)  # dropoff zone
    dist = tbl["trip_distance"].to_pylist()
    mins = tbl["trip_duration_min"].to_pylist()
    speeds = pa.array([speed_mph(d, m) for d, m in zip(dist, mins)], type=pa.float64())  # Python UDF
    tbl = tbl.append_column("avg_speed_mph", speeds)
    tbl = tbl.filter(pc.less_equal(tbl["avg_speed_mph"], MAX_SPEED_MPH))           # rule 7 (after the UDF)
    return tbl.select(FINAL_COLUMNS)


def build_pipeline(paths, pu, do, buckets):
    ds = ray.data.read_parquet(paths, columns=COLS)                                        # 1. ingestion
    ds = ds.map_batches(normalize_batch, batch_format="pyarrow", batch_size=None)          # 2. normalization
    ds = ds.map_batches(cleanse_batch, batch_format="pyarrow", batch_size=None,
                        fn_kwargs={"num_buckets": buckets})                                # 3. null + filters
    ds = ds.groupby("_bucket").map_groups(dedupe_group, batch_format="pyarrow")            # 3c. duplicates
    ds = ds.map_batches(derive_and_transform, batch_format="pyarrow", batch_size=None,
                        fn_kwargs={"pu": pu, "do": do})                                    # 4. join + UDF
    return ds


def log_result(record):
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    with open(out / "ray_runs.jsonl", "a") as f:
        f.write(json.dumps(record) + "\n")


def udf_benchmark(trips_dir, runs):
    """Same speed calculation over the exported Parquet: row-wise Python function vs vectorized."""
    def py_udf(tbl):
        d, m = tbl["trip_distance"].to_pylist(), tbl["trip_duration_min"].to_pylist()
        return pa.table({"s": pa.array([speed_mph(a, b) for a, b in zip(d, m)], type=pa.float64())})

    def vectorized(tbl):
        d, m = tbl["trip_distance"].to_numpy(), tbl["trip_duration_min"].to_numpy()
        return pa.table({"s": pa.array(np.round(d / (m / 60.0), 3))})

    ds = ray.data.read_parquet(trips_dir, columns=["trip_distance", "trip_duration_min"])
    results = {}
    for name, fn in {"python_udf": py_udf, "vectorized": vectorized}.items():
        times = []
        for _ in range(runs + 1):                       # first run = warm-up, discarded
            t0 = time.perf_counter()
            total = ds.map_batches(fn, batch_format="pyarrow", batch_size=None).sum("s")
            times.append(round(time.perf_counter() - t0, 2))
        results[name] = times[1:]
        print(f"{name:12} runs={times[1:]}  (warm-up {times[0]} s)  checksum={total:.3f}")
    py, vec = min(results["python_udf"]), min(results["vectorized"])
    print(f"Row-wise Python vs vectorized (best python_udf - best vectorized): {py - vec:.2f} s  ({py / vec:.1f}x)")
    log_result({"kind": "udf_benchmark", "trips_dir": trips_dir, **results})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(ROOT / "data/trips/*.parquet"))
    ap.add_argument("--output-dir", default=str(ROOT / "output/ray_clean"))
    ap.add_argument("--buckets", type=int, default=256, help="dedupe buckets (rows in one bucket must fit in memory)")
    ap.add_argument("--hold", type=int, default=0, help="seconds to stay connected after the run")
    ap.add_argument("--udf-benchmark", action="store_true")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    trips_dir = str(Path(args.output_dir) / "trips")
    hourly_dir = str(Path(args.output_dir) / "hourly_speed")

    ray.init(address="auto", ignore_reinit_error=True)
    DataContext.get_current().shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE_V2
    print("Ray cluster resources:", ray.cluster_resources())

    if args.udf_benchmark:
        udf_benchmark(trips_dir, args.runs)
        ray.shutdown()
        return

    paths = sorted(glob.glob(args.input))
    if not paths:
        raise SystemExit(f"no files match {args.input}")
    print(f"{len(paths)} input files")
    pu, do = load_zones()

    for d in (trips_dir, hourly_dir):          # Ray's writer does not overwrite; clear first (untimed)
        shutil.rmtree(d, ignore_errors=True)

    # ======================= timed region: ingestion -> export =======================
    t0 = time.perf_counter()
    build_pipeline(paths, pu, do, args.buckets).write_parquet(trips_dir)   # export (row level)
    t1 = time.perf_counter()
    (ray.data.read_parquet(trips_dir, columns=["pu_borough", "pickup_hour", "avg_speed_mph", "trip_distance"])
        .groupby(["pu_borough", "pickup_hour"])
        .aggregate(Count(alias_name="trips"),
                   Mean("avg_speed_mph", alias_name="avg_speed_mph"),
                   Mean("trip_distance", alias_name="avg_trip_distance"))
        .write_parquet(hourly_dir))
    t2 = time.perf_counter()
    # =================================================================================

    rows = ray.data.read_parquet(trips_dir).count()          # untimed
    print(f"rows exported          : {rows:,}")
    print(f"wall-clock total       : {t2 - t0:.1f} s")
    print(f"  pipeline + export    : {t1 - t0:.1f} s")
    print(f"  hourly aggregate     : {t2 - t1:.1f} s")
    log_result({"kind": "pipeline", "tag": args.tag, "input": args.input, "rows": rows,
                "seconds_total": round(t2 - t0, 2), "seconds_pipeline_export": round(t1 - t0, 2),
                "seconds_hourly_agg": round(t2 - t1, 2)})

    if args.hold:
        print(f"Holding {args.hold}s so you can screenshot the dashboard...")
        time.sleep(args.hold)
    ray.shutdown()


if __name__ == "__main__":
    main()
