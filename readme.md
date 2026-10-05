# DA3408 Assignment 3 — Spark vs. Ray: The Data Engineering Duel

**Student:** Sri Harsha V  
**Roll Number:** DA24B034  
**Course:** DA3408 — AIOps

This assignment implements the same large-scale data preprocessing pipeline using **Apache Spark** and **Ray Data**, and compares their execution time, resource utilization, and Python transformation overhead.

AI assistance was used and is declared in [`AI_DISCLOSURE.md`](AI_DISCLOSURE.md).

---

## Setup

Create/activate the Python virtual environment and install the required dependencies:

```bash
source ~/projects/.venv/bin/activate
pip install -r requirements.txt
```

The benchmark environment used for this assignment was:

| Component | Version |
|---|---|
| OS | Ubuntu 26.04 LTS |
| Python | 3.14.4 |
| Java | OpenJDK 17.0.20 |
| Apache Spark | 4.2.0 |
| PySpark | 4.2.0 |
| Ray | 2.58.0 |
| CPU | 12 logical cores |
| RAM | 14.9 GiB (4 GiB swap) |
| Disk | NVMe SSD |

Both clusters run on this single laptop, so all "nodes" share the same CPU, memory and disk.

---

## Dataset

The benchmark uses the **NYC Yellow Taxi Trip Data** together with the **Taxi Zone Lookup** dataset. The data is not committed to this repository; download it from the NYC TLC trip record data page and place it as shown below.

### Yellow Taxi Trip Data

The final benchmark uses **35 monthly Parquet files**:

- 2023: February–December — 11 files
- 2024: January–December — 12 files
- 2025: January–December — 12 files

Total:

- **35 Parquet files**
- **125,135,782 raw rows**
- Approximately **2 GB**

December 2022 and January 2023 were excluded from the final benchmark because their schemas/physical column types were inconsistent with the later files. The 2025 files contain one extra column (`cbd_congestion_fee`); both pipelines read only the nine columns they need, so this does not matter.

The raw trip data was not modified.

### Taxi Zone Lookup

```text
data/lookup/taxi_zone_lookup.csv
```

The lookup table contains 265 location records with:

- `LocationID`
- `Borough`
- `Zone`
- `service_zone`

It is used to enrich both pickup and dropoff locations.

---

## Pipeline

The same logical preprocessing pipeline is implemented in both frameworks.

```text
NYC Taxi Parquet files
        |
        v
     Ingestion  (9 columns, cast to one fixed schema)
        |
        v
     Cleansing
        |
        +--> Null filtering
        +--> Timestamp validation
        +--> Trip duration validation (1 min to 24 h)
        +--> Distance validation
        +--> Monetary-value validation
        +--> Duplicate removal
        |
        v
   Location Lookup Joins
        |
        +--> Pickup location
        +--> Dropoff location
        |
        v
   Derived Features
        |
        +--> Trip duration
        +--> Pickup date/hour
        +--> Average speed (custom Python function)
        |
        v
   Speed filtering (> 100 mph removed)
        |
        v
      Parquet
        |
        +--> Row-level output
        |
        +--> Hourly aggregate
```

The final benchmark produced **117,707,950 rows** after preprocessing (94.06% of the raw rows).

How the two implementations differ: Spark uses `dropDuplicates` and a broadcast join. Ray groups rows into 256 buckets by pickup second (identical rows always share a bucket), removes duplicates inside each bucket after a hash shuffle, and joins the 265-row lookup inside each batch with PyArrow. The cleansing rules and the Python speed function are identical.

---

## Layout

Files in this repository:

```text
DA3408_DA24B034_3/
│
├── spark_clean.py                          # Spark implementation
├── ray_clean.py                            # Ray Data implementation
├── verify_parity.py                        # Spark/Ray output verification
├── requirements.txt
├── readme.md
├── AI_DISCLOSURE.md                        # AI usage disclosure
├── .gitignore
│
├── report/
│   └── AIOPS_3-1.pdf                       # Benchmark report
├── results/                                # Benchmark measurements (timings, parity, top logs)
├── logs/                                   # Baseline memory, OOM checks, top logs for the Ray runs
└── screenshots/
    ├── spark/                              # Spark Master UI (2 workers) and Application UI
    └── ray/                                # Ray Dashboard cluster view
```

Not committed (generated or large): `data/` (input Parquet files and lookup CSV), `output/` (generated Parquet), `ray_spill/` and `spark_tmp/` (runtime spill directories), and the Ray session logs.

---

# Spark

## Spark Cluster Configuration

The Spark cluster consists of:

```text
Spark Master
    |
    +---- Worker 1 — 4 cores, 3 GiB
    |
    +---- Worker 2 — 4 cores, 3 GiB
```

Total worker resources: **8 cores, 6 GiB worker memory**. The application runs 2 executors × 4 cores with 2 GiB each.

Spark Master: `spark://127.0.0.1:7077`  
Spark Master UI: `http://127.0.0.1:8080`  
Spark Application UI: `http://127.0.0.1:4040`

The Spark cluster configuration and network setup were performed manually.

## Running Spark

Set `SPARK_HOME`:

```bash
export SPARK_HOME=/home/sriharsha/projects/.venv/lib/python3.14/site-packages/pyspark
```

Start the Spark Master:

```bash
SPARK_LOCAL_IP=127.0.0.1 \
$SPARK_HOME/bin/spark-class \
org.apache.spark.deploy.master.Master \
--host 127.0.0.1 \
--port 7077 \
--webui-port 8080
```

Start Worker 1:

```bash
SPARK_LOCAL_IP=127.0.0.1 \
SPARK_LOCAL_DIRS=$HOME/projects/DA3408_DA24B034_3/spark_tmp/worker1 \
$SPARK_HOME/bin/spark-class \
org.apache.spark.deploy.worker.Worker \
spark://127.0.0.1:7077 \
--cores 4 \
--memory 3g \
--webui-port 8081 \
--work-dir /tmp/spark-worker1
```

Start Worker 2:

```bash
SPARK_LOCAL_IP=127.0.0.1 \
SPARK_LOCAL_DIRS=$HOME/projects/DA3408_DA24B034_3/spark_tmp/worker2 \
$SPARK_HOME/bin/spark-class \
org.apache.spark.deploy.worker.Worker \
spark://127.0.0.1:7077 \
--cores 4 \
--memory 3g \
--webui-port 8082 \
--work-dir /tmp/spark-worker2
```

Verify the cluster at `http://127.0.0.1:8080`. It should show **2 ALIVE workers** and **8 total cores**.

Run the Spark pipeline:

```bash
python spark_clean.py --tag run1
```

Additional benchmark runs:

```bash
python spark_clean.py --tag run2
python spark_clean.py --tag run3
```

Spark UDF benchmark (reads the exported Parquet):

```bash
python spark_clean.py --udf-benchmark --runs 3
```

---

# Ray

## Ray Cluster Configuration

The Ray cluster consists of:

```text
Ray Head (0 CPUs)
    |
    +---- Worker 1 — 4 CPUs, 1 GB object store
    |
    +---- Worker 2 — 4 CPUs, 1 GB object store
```

The head node is configured with zero task CPUs, giving **8 worker CPUs**. Ray Data uses hash shuffle V2 (200 partitions) and 256 duplicate-removal buckets. Object spilling goes to an NVMe directory, because `/tmp` is a RAM-backed `tmpfs` on this machine.

Ray Dashboard: `http://127.0.0.1:8265`

## Running Ray

Start the Ray head:

```bash
ulimit -n 65535

ray start --head \
--port=6379 \
--num-cpus=0 \
--dashboard-host=127.0.0.1 \
--object-spilling-directory="$HOME/projects/DA3408_DA24B034_3/ray_spill"
```

Start Worker 1:

```bash
ulimit -n 65535

ray start \
--address=10.101.203.203:6379 \
--num-cpus=4 \
--object-store-memory=1000000000 \
--object-spilling-directory="$HOME/projects/DA3408_DA24B034_3/ray_spill"
```

Start Worker 2:

```bash
ulimit -n 65535

ray start \
--address=10.101.203.203:6379 \
--num-cpus=4 \
--object-store-memory=1000000000 \
--object-spilling-directory="$HOME/projects/DA3408_DA24B034_3/ray_spill"
```

> `10.101.203.203` is the head-node IP used during the benchmark. If the cluster is recreated on a different machine/network, use the IP reported by the Ray head.

Verify the cluster:

```bash
ray status
```

The dashboard should show **3 active nodes, 1 head, 2 workers, and 8 CPUs**.

Run the Ray pipeline:

```bash
python ray_clean.py --tag run1
```

Ray UDF benchmark (reads the exported Parquet):

```bash
python ray_clean.py --udf-benchmark --runs 3
```

## Procedure used for each counted Ray run

Ray ran close to its memory limit on this 14.9 GiB machine, so every counted run followed the same procedure:

1. `ray stop`, then empty `/tmp/ray` and the spill directory.
2. Flush swap (`sudo swapoff -a && sudo swapon -a`) and confirm swap is 0.
3. Start the head and the two workers (commands above) and record `free -m` and `ray status`.
4. Log `top -b -d 1` during the run.
5. Run `ray_clean.py` with no tuning flags.
6. Before any cleanup, search all Ray logs for memory-pressure kills (`grep -rl "memory pressure" /tmp/ray/session_latest/logs/`) and run the parity check. The result was 0 kills for every counted run.

---

# Output Verification

The Spark and Ray outputs are compared using:

```bash
python verify_parity.py \
    --spark output/spark_clean \
    --ray output/ray_clean \
    --tag full
```

The verification checks:

- Row count
- Column names
- Null counts
- Per-column checksums
- Whole-row checksums
- Numeric minimum/maximum/sum
- Hourly aggregate keys
- Hourly trip counts
- Aggregate floating-point values

Each of the three counted Ray runs was compared with the Spark output:

```text
Spark rows: 117,707,950
Ray rows:   117,707,950   (all three counted runs)

OVERALL: PARITY OK
```

The hourly aggregates have the same 192 groups and identical trip counts; the averages differ by at most 3.1e-13 (relative), which is floating-point noise. The `results/parity_*.json` files hold each check.

---

# Benchmark Results

Three counted benchmark runs were performed for each framework. Wall-clock time runs from ingestion to export. Peak CPU is `us`+`sy` and peak memory is the whole-machine "used" value, both from `top` with one-second sampling.

## Spark

| Run | Total | Pipeline + Export | Hourly | Peak CPU | Peak Memory |
|---|---:|---:|---:|---:|---:|
| Run 1 | 261.1 s | 256.5 s | 4.6 s | 99.4% | 10.69 GiB |
| Run 2 | 266.3 s | 261.5 s | 4.8 s | 99.5% | 10.99 GiB |
| Run 3 | 253.6 s | 248.8 s | 4.7 s | 99.6% | 10.88 GiB |
| **Mean** | **260.3 s** | **255.6 s** | **4.7 s** | **99.5%** | **10.85 GiB** |

## Ray

| Run | Total | Pipeline + Export | Hourly | Peak CPU | Peak Memory |
|---|---:|---:|---:|---:|---:|
| Run 1 | 149.0 s | 118.6 s | 30.3 s | 72.7% | 13.21 GiB |
| Run 2 | 147.8 s | 119.2 s | 28.7 s | 79.8% | 13.21 GiB |
| Run 3 | 146.7 s | 115.2 s | 31.5 s | 78.3% | 13.22 GiB |
| **Mean** | **147.8 s** | **117.7 s** | **30.2 s** | **76.9%** | **13.21 GiB** |

## Ray development runs (not counted)

Earlier full-data Ray runs behaved differently while the setup was being fixed:

| Run | Total | Memory-pressure kills |
|---|---:|---|
| Warm-up (shuffle V2 + NVMe spill) | 157.1 s | yes (2 workers, tasks retried) |
| Warm-up with a shuffle-concurrency option | 231.3 s | none in the driver output |
| Same option, swap not flushed | 230.8 s | none |
| Same option, swap flushed | 159.4 s | yes (all three Ray nodes) |

The two ~231 s runs and the ~150 s runs differ by about 80 s, and I could not explain the gap from the data I recorded. Peak whole-machine memory stayed around 13.2–13.5 GiB in these runs and swap filled its 4 GiB. The option was later found not to be used by the shuffle in this plan and was removed from the code (see the tuning note). The report discusses these runs.

---

# Python UDF Benchmark

The custom average-speed transformation was benchmarked separately, over the exported data (117.7 M rows): three timed runs after a discarded warm-up.

## Spark

| Method | Best Time |
|---|---:|
| Python UDF | 51.36 s |
| Built-in expression | 9.65 s |

Python UDF overhead: **41.71 s (5.3× relative to the built-in expression)**.

## Ray

| Method | Best Time |
|---|---:|
| Row-wise Python function | 28.95 s |
| Vectorized NumPy | 3.23 s |

Python overhead: **25.72 s (9.0× relative to the vectorized implementation)**.

The row-wise Python implementation produced the same checksum in Spark and Ray (1,299,468,753.321). The Spark built-in and Ray NumPy versions each differ slightly from that and from each other, so they are not claimed to be numerically equivalent. The same Python function ran 1.8× faster in Ray than as a Spark UDF.

---

# Performance Tuning Note

The following optimizations were suggested by an AI assistant, reviewed, and kept (see [`AI_DISCLOSURE.md`](AI_DISCLOSURE.md)).

### Spark

- Selected only the required nine columns during ingestion.
- Applied cleansing filters before the shuffle-heavy duplicate removal.
- Broadcast the small Taxi Zone Lookup table.
- Used UTC timestamp handling.
- Used 16 shuffle partitions for the 8 Spark cores.

### Ray

- Used Ray Data hash shuffle V2.
- Used bucket-based duplicate handling.
- Increased the open-file limit to `65535`.
- Configured NVMe-backed object spilling to handle the large shuffle workload.

A shuffle-concurrency option was also suggested, tested during development, and removed: inspection of the Ray 2.58 execution path showed that the executed hash-shuffle plan does not use it. It is not in the final code, and the runs that used it are not part of the counted results.

During development, Ray experienced memory-pressure failures on some full-data runs. These were investigated by examining Ray logs and system-level resource usage. The final procedure completed all three counted runs without memory-pressure kills.

---

# Cluster Evidence

## Spark

`screenshots/spark/spark_master_2_workers.png`

Shows:

- 2 ALIVE Spark workers
- 4 cores per worker
- 3 GiB memory per worker
- 8 total cores

Additional Spark UI screenshots are available under `screenshots/spark/`.

## Ray

`screenshots/ray/ray_dashboard_cluster.png`

Shows:

- 3 active Ray nodes
- 1 head node
- 2 worker nodes
- 8 CPUs

Additional Ray Dashboard evidence is available under `screenshots/ray/`.

---

# Files

| Requirement | Location |
|---|---|
| Spark implementation | [`spark_clean.py`](spark_clean.py) |
| Ray implementation | [`ray_clean.py`](ray_clean.py) |
| Spark/Ray parity verification | [`verify_parity.py`](verify_parity.py) |
| Spark cluster evidence | [`screenshots/spark/`](screenshots/spark/) |
| Ray cluster evidence | [`screenshots/ray/`](screenshots/ray/) |
| Benchmark results | [`results/`](results/) |
| Memory baselines, OOM checks, `top` logs | [`logs/`](logs/) |
| Final report | [`report/`](report/) |
| AI usage disclosure | [`AI_DISCLOSURE.md`](AI_DISCLOSURE.md) |

---

# Conclusion

For the measured full-data pipeline, the mean total execution time was:

```text
Spark : 260.3 s
Ray   : 147.8 s
```

Ray was approximately **1.76× faster** for the measured end-to-end pipeline, and it is the winner for this task on speed. The comparison should be read in the context of the single-machine cluster, the different execution models, and the resource limits of the machine.

Spark showed:

- Higher CPU utilization (99.5% vs. 76.9% peak).
- Lower peak memory usage (10.85 vs. 13.21 GiB), and it ran without any special tuning.
- Much faster hourly aggregation (4.7 s vs. 30.2 s).

Ray showed:

- Lower peak CPU utilization.
- Faster overall preprocessing and export.
- A faster row-wise Python transformation (28.95 s vs. 51.36 s for the same function).
- Higher memory use: it ran close to its own kill threshold with swap full, and getting a full run to complete needed extra setup (file limit, NVMe spill, one fixed run procedure).

For an **AI-first** project I would choose Ray (Python-native tasks, faster row-wise Python, and ML libraries), provided memory is planned for. For a **BI-first** project I would choose Spark (SQL/DataFrames, built-in expressions, predictable memory behavior). These conclusions come from one dataset on one machine and may not carry over to a multi-node cluster.

Detailed methodology, benchmark analysis, UDF analysis, parity results, and limitations are provided in the final report.