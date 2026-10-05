"""
spark_clean.py 

  1. Ingestion      : read yellow-taxi Parquet files
  2. Normalization  : force ONE canonical 9-column schema right after ingestion
  3. Cleansing      : null drop, sanity filters, duplicate removal, timestamp formatting
  4. Transformation : broadcast join with the zone lookup (pickup + dropoff),
                      Python UDF for average speed, speed sanity filter
  5. Export         : row-level Parquet + hourly-speed aggregate Parquet

Run (Spark master + 2 workers must be up):
  python spark_clean.py --input "data/trips/yellow_tripdata_2023-0[2-4].parquet"   # small test
  python spark_clean.py                                    # all 35 files (timed benchmark run)
  python spark_clean.py --hold 120                         # keep Spark UI :4040 open 120 s
  python spark_clean.py --diagnostics                      # rule hit counts (untimed, extra pass)
  python spark_clean.py --udf-benchmark --runs 3           # UDF overhead, reads the exported Parquet
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession, functions as F  # noqa: E402
from pyspark.sql.types import DoubleType               # noqa: E402

ROOT = Path(__file__).resolve().parent
MASTER = "spark://127.0.0.1:7077"
ZONE_CSV = str(ROOT / "data/lookup/taxi_zone_lookup.csv")

# ---- Stage 2 contract: column -> Spark type (order matters) -----------------
SCHEMA = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "trip_distance": "double",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "long",
    "fare_amount": "double",
    "total_amount": "double",
}

# ---- Cleansing rule constants (must be identical in ray_clean.py) -----------
PICKUP_MIN = "2023-02-01 00:00:00"   # inclusive
PICKUP_MAX = "2026-01-01 00:00:00"   # exclusive
MIN_DURATION_US = 60 * 1_000_000            # 1 minute
MAX_DURATION_US = 24 * 3600 * 1_000_000     # 24 hours
MAX_SPEED_MPH = 100.0                       # applied AFTER the UDF

PICKUP, DROPOFF = "tpep_pickup_datetime", "tpep_dropoff_datetime"

FINAL_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
    "PULocationID", "DOLocationID", "payment_type", "fare_amount", "total_amount",
    "pickup_date", "pickup_hour", "trip_duration_min",
    "pu_borough", "pu_zone", "do_borough", "do_zone", "avg_speed_mph",
]


def build_spark(app_name="spark_clean"):
    return (
        SparkSession.builder.appName(app_name)
        .master(MASTER)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "4")
        .config("spark.cores.max", "8")                # 2 executors x 4 cores
        .config("spark.sql.shuffle.partitions", "16")  # 2 x total cores
        .config("spark.sql.session.timeZone", "UTC")   # no DST surprises in hour/date
        .config("spark.pyspark.python", sys.executable)
        .getOrCreate()
    )


# ---- Stage 1: ingestion ------------------------------------------------------
def ingest(spark, input_glob):
    # Only the needed columns are selected, so the 2025-only cbd_congestion_fee
    # column can never leak in.
    return spark.read.parquet(input_glob).select(*SCHEMA.keys())


# ---- Stage 2: schema normalization (immediately after ingestion) -------------
def normalize(df):
    return df.select([F.col(c).cast(t).alias(c) for c, t in SCHEMA.items()])


# ---- Stage 3: cleansing -------------------------------------------------------
def duration_us():
    return F.unix_micros(F.col(DROPOFF)) - F.unix_micros(F.col(PICKUP))


def rule_conditions():
    """Each rule as a 'row is GOOD' condition (used by the filters and diagnostics)."""
    dur = duration_us()
    return {
        "pickup_in_range": (F.col(PICKUP) >= F.lit(PICKUP_MIN).cast("timestamp"))
        & (F.col(PICKUP) < F.lit(PICKUP_MAX).cast("timestamp")),
        "duration_1min_to_24h": (dur >= MIN_DURATION_US) & (dur <= MAX_DURATION_US),
        "trip_distance_positive": F.col("trip_distance") > 0,
        "fare_nonnegative": F.col("fare_amount") >= 0,
        "total_nonnegative": F.col("total_amount") >= 0,
    }


def cleanse(df):
    df = df.dropna(how="any", subset=list(SCHEMA.keys()))      # 3a. nulls
    for cond in rule_conditions().values():                    # 3b. sanity filters
        df = df.filter(cond)
    df = df.dropDuplicates()                                   # 3c. exact duplicates (9 cols)
    return (                                                   # 3d. timestamp formatting
        df.withColumn("pickup_date", F.to_date(F.col(PICKUP)))
        .withColumn("pickup_hour", F.hour(F.col(PICKUP)))
        .withColumn("trip_duration_min", duration_us() / F.lit(60_000_000.0))
    )


# ---- Stage 4: transformation --------------------------------------------------
def speed_mph(distance, duration_min):
    """The custom Python function (row-at-a-time). Same logic is used in ray_clean.py."""
    if distance is None or duration_min is None or duration_min <= 0:
        return None
    return round(distance / (duration_min / 60.0), 3)


speed_udf = F.udf(speed_mph, DoubleType())


def native_speed_col():
    """Built-in-expression equivalent of the UDF (only used by --udf-benchmark)."""
    return F.round(F.col("trip_distance") / (F.col("trip_duration_min") / F.lit(60.0)), 3)


def load_zones(spark):
    z = (spark.read.option("header", True).csv(ZONE_CSV)
         .select(F.col("LocationID").cast("int").alias("LocationID"), "Borough", "Zone"))
    pu = z.select(F.col("LocationID").alias("PULocationID"),
                  F.col("Borough").alias("pu_borough"), F.col("Zone").alias("pu_zone"))
    do = z.select(F.col("LocationID").alias("DOLocationID"),
                  F.col("Borough").alias("do_borough"), F.col("Zone").alias("do_zone"))
    return pu, do


def transform(spark, df):
    pu, do = load_zones(spark)
    df = (df.join(F.broadcast(pu), "PULocationID", "inner")      # pickup zone
            .join(F.broadcast(do), "DOLocationID", "inner"))     # dropoff zone
    df = df.withColumn("avg_speed_mph", speed_udf(F.col("trip_distance"), F.col("trip_duration_min")))
    df = df.filter(F.col("avg_speed_mph") <= MAX_SPEED_MPH)       # rule 7 (after the UDF)
    return df.select(*FINAL_COLUMNS)


def hourly_speed(trips):
    return (trips.groupBy("pu_borough", "pickup_hour")
            .agg(F.count(F.lit(1)).alias("trips"),
                 F.avg("avg_speed_mph").alias("avg_speed_mph"),
                 F.avg("trip_distance").alias("avg_trip_distance"))
            .orderBy("pu_borough", "pickup_hour"))


def diagnostics(df_norm):
    aggs = [F.count(F.lit(1)).alias("rows_after_normalization")]
    for c in SCHEMA:
        aggs.append(F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}"))
    for name, good in rule_conditions().items():
        aggs.append(F.sum((~good).cast("int")).alias(f"violates_{name}"))
    return df_norm.agg(*aggs).first().asDict()


def log_result(record):
    out = ROOT / "results"
    out.mkdir(exist_ok=True)
    with open(out / "spark_runs.jsonl", "a") as f:
        f.write(json.dumps(record) + "\n")


def udf_benchmark(spark, trips_dir, runs):
    """UDF overhead: same aggregation over the exported Parquet, Python UDF vs built-in expression."""
    df = spark.read.parquet(trips_dir).select("trip_distance", "trip_duration_min")
    variants = {
        "python_udf": speed_udf(F.col("trip_distance"), F.col("trip_duration_min")),
        "native_expr": native_speed_col(),
    }
    results = {}
    for name, col in variants.items():
        times = []
        for _ in range(runs + 1):                       # first run = warm-up, discarded
            t0 = time.perf_counter()
            total = df.agg(F.sum(col)).first()[0]
            times.append(round(time.perf_counter() - t0, 2))
        results[name] = times[1:]
        print(f"{name:12} runs={times[1:]}  (warm-up {times[0]} s)  checksum={total:.3f}")
    py, nat = min(results["python_udf"]), min(results["native_expr"])
    print(f"UDF overhead (best python_udf - best native_expr): {py - nat:.2f} s  ({py / nat:.1f}x)")
    log_result({"kind": "udf_benchmark", "trips_dir": trips_dir, **results})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(ROOT / "data/trips/*.parquet"))
    ap.add_argument("--output-dir", default=str(ROOT / "output/spark_clean"))
    ap.add_argument("--diagnostics", action="store_true", help="count rule violations (untimed)")
    ap.add_argument("--hold", type=int, default=0, help="seconds to keep the app (UI :4040) alive")
    ap.add_argument("--udf-benchmark", action="store_true", help="UDF overhead test on exported Parquet")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--tag", default="", help="label stored with the run result")
    args = ap.parse_args()

    trips_dir = os.path.join(args.output_dir, "trips")
    hourly_dir = os.path.join(args.output_dir, "hourly_speed")

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark UI:", spark.sparkContext.uiWebUrl)

    if args.udf_benchmark:
        udf_benchmark(spark, trips_dir, args.runs)
        spark.stop()
        return

    if args.diagnostics:
        print("--- diagnostics (not timed) ---")
        print(json.dumps(diagnostics(normalize(ingest(spark, args.input))), indent=2))

    # ======================= timed region: ingestion -> export =======================
    t0 = time.perf_counter()
    trips = transform(spark, cleanse(normalize(ingest(spark, args.input))))
    trips.write.mode("overwrite").parquet(trips_dir)                     # export (row level)
    t1 = time.perf_counter()
    hourly_speed(spark.read.parquet(trips_dir)).write.mode("overwrite").parquet(hourly_dir)
    t2 = time.perf_counter()
    # =================================================================================

    rows = spark.read.parquet(trips_dir).count()       # untimed
    print(f"rows exported          : {rows:,}")
    print(f"wall-clock total       : {t2 - t0:.1f} s")
    print(f"  pipeline + export    : {t1 - t0:.1f} s")
    print(f"  hourly aggregate     : {t2 - t1:.1f} s")
    log_result({"kind": "pipeline", "tag": args.tag, "input": args.input, "rows": rows,
                "seconds_total": round(t2 - t0, 2), "seconds_pipeline_export": round(t1 - t0, 2),
                "seconds_hourly_agg": round(t2 - t1, 2)})

    if args.hold:
        print(f"Holding {args.hold}s so you can screenshot the Spark UI...")
        time.sleep(args.hold)
    spark.stop()


if __name__ == "__main__":
    main()
