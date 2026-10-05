# DA3408 Assignment 3 — Spark vs. Ray: The Data Engineering Duel

**Student:** Sri Harsha V  
**Roll Number:** DA24B034  
**Course:** DA3408 — AIOps

This assignment implements the same large-scale data preprocessing pipeline using **Apache Spark** and **Ray Data**, and compares their execution time, resource utilization, and Python transformation overhead.

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
| RAM | 14.9 GiB |

---

## Dataset

The benchmark uses the **NYC Yellow Taxi Trip Data** together with the **Taxi Zone Lookup** dataset.

### Yellow Taxi Trip Data

The final benchmark uses **35 monthly Parquet files**:

- 2023: February–December — 11 files
- 2024: January–December — 12 files
- 2025: January–December — 12 files

Total:

- **35 Parquet files**
- **125,135,782 raw rows**
- Approximately **2 GB**

December 2022 and January 2023 were excluded from the final benchmark because their schemas/physical column types were inconsistent with the later files.

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
     Ingestion
        |
        v
     Cleansing
        |
        +--> Null filtering
        +--> Timestamp validation
        +--> Trip duration validation
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
        +--> Average speed
        |
        v
   Speed filtering
        |
        v
      Parquet
        |
        +--> Row-level output
        |
        +--> Hourly aggregate
```

The final benchmark produced **117,707,950 rows** after preprocessing.

---

## Layout

```text
DA3408_DA24B034_3/
│
├── data/
│   ├── trips/                              # 35 NYC Taxi Parquet files
│   └── lookup/
│       └── taxi_zone_lookup.csv            # Taxi zone lookup
│
├── output/                                 # Generated Parquet outputs
├── results/                                # Benchmark measurements
├── screenshots/
│   ├── spark/
│   │   ├── spark_master_2_workers.png
│   │   ├── spark_jobs_completed.png
│   │   ├── spark_jobs_execution.png
│   │   ├── spark_master_completed_apps.png
│   │   ├── spark_run1.png
│   │   ├── spark_run2.png
│   │   └── spark_run3_udf_benchmark.png
│   └── ray/
│       ├── ray_dashboard_cluster.png
│       └── ...
│
├── report/
│   └── report.pdf
├── spark_clean.py                          # Spark implementation
├── ray_clean.py                            # Ray Data implementation
├── verify_parity.py                        # Spark/Ray output verification
├── requirements.txt
├── README.md
└── ...
```

Large raw datasets and temporary runtime/spill directories are not required to be committed to GitHub.

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

Total worker resources: **8 cores, 6 GiB worker memory**.

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

---

# Ray

## Ray Cluster Configuration

The Ray cluster consists of:

```text
Ray Head
    |
    +---- Worker 1 — 4 CPUs
    |
    +---- Worker 2 — 4 CPUs
```

The head node is configured with zero task CPUs, giving **8 worker CPUs**.

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

The final full-data verification produced:

```text
Spark rows: 117,707,950
Ray rows:   117,707,950

OVERALL: PARITY OK
```

---

# Benchmark Results

Three counted benchmark runs were performed for each framework. Peak CPU and memory were measured using `top` with one-second sampling.

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

---

# Python UDF Benchmark

The custom average-speed transformation was benchmarked separately.

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

The row-wise Python implementation produced the same checksum in Spark and Ray.

---

# Performance Tuning Note

The following optimizations were used during implementation.

### Spark

- Selected only the required nine columns during ingestion.
- Applied cleansing filters before downstream processing.
- Broadcast the small Taxi Zone Lookup table.
- Used UTC timestamp handling.
- Used 16 shuffle partitions for the available Spark cores.

### Ray

- Used Ray Data Hash Shuffle V2.
- Used bucket-based duplicate handling.
- Increased the open-file limit to `65535`.
- Configured NVMe-backed object spilling to handle the large shuffle workload.

During development, Ray experienced memory-pressure failures on some full-data runs. These were investigated by examining Ray logs and system-level resource usage. The final benchmark procedure completed all three counted runs without memory-pressure kills.

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
| Final report | [`report/`](report/) |

---

# Conclusion

For the measured full-data pipeline, the mean total execution time was:

```text
Spark : 260.3 s
Ray   : 147.8 s
```

Ray was approximately **1.76× faster** for the measured end-to-end pipeline.

However, the comparison should be interpreted in the context of the single-machine cluster environment, the different execution models, and the resource constraints of the machine.

Spark showed:

- Higher CPU utilization.
- Lower peak memory usage.
- Much faster hourly aggregation.

Ray showed:

- Lower peak CPU utilization.
- Faster overall preprocessing and export.
- Strong performance for the Python-based transformation.

The final choice therefore depends on the workload. Ray was faster for this Python-heavy preprocessing pipeline, while Spark demonstrated advantages in built-in aggregation performance and lower memory usage.

Detailed methodology, benchmark analysis, UDF analysis, parity results, and limitations are provided in the final report.