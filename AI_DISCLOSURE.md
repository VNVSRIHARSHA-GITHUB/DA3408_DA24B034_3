# AI Usage Disclosure

AI assistance was used in accordance with the assignment's Fair Use of AI guidelines.

## Tools used
Claude (Anthropic), through the claude.ai chat interface.

## How it was used
- Understanding Spark and Ray concepts.
- Translating pipeline logic between PySpark and Ray Data.
- Debugging implementation issues (Ray file-descriptor errors, out-of-memory worker kills, RAM-backed `/tmp`).
- Discussing possible performance optimizations.
- Interpreting benchmark and runtime behavior.
- Assistance with command syntax (`grep`/`awk` over `top` and Ray logs) and documentation, including drafting the
  LaTeX benchmark report from numbers I measured.

The **cluster configuration and network setup were performed manually**. The Ray run settings I applied (higher
open-file limit, NVMe spill directory, hash shuffle V2) were identified with AI help while debugging failed runs, and
are listed in the report's Performance Tuning Note.

All benchmark results reported in this repository were obtained from actual executions of the Spark and Ray
pipelines. No synthetic benchmark numbers were generated.

Performance-related suggestions were tested before being incorporated into the final implementation. One suggested
option (limiting concurrent shuffle finalizations) turned out not to be used by the Ray shuffle in this plan, so it was
removed and the runs that used it are not part of the counted results.

## Performance tuning suggestions (from the assistant, reviewed and kept)
- **Early column selection and normalization:** read only nine columns and cast them right after ingestion, which cuts
  memory and avoids the cross-file schema difference.
- **Filters before duplicate removal:** the row-wise filters run before the shuffle-heavy duplicate removal, giving the
  same output with less data shuffled.
- **Spark broadcast join:** the 265-row zone table is broadcast to the executors, so the large trip table is not
  shuffled for the join.
- **UTC session time zone and 16 shuffle partitions:** stable date/hour values and sensible parallelism on the 8
  allocated Spark cores.
- **Ray run settings:** the higher open-file limit, NVMe spill directory and hash shuffle V2 were applied after Ray's
  error messages and disk checks showed that the initial configuration was insufficient for the full dataset.
- **Shuffle-concurrency option, tried and removed:** the assistant suggested limiting concurrent shuffle finalizations.
  I tested it during development, but inspection of the Ray 2.58 execution path showed that the executed hash-shuffle
  plan does not use it. It was removed from the final code and excluded from the counted benchmark results.

## Impact
The assistant sped up the PySpark/Ray translation and the debugging of Ray's memory and file-limit failures, and
helped present the results. The cluster setup, benchmark runs, measurements and conclusions are my own.