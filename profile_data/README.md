# S3 Spark Input Profiler

This folder contains a lightweight profiler for large Spark/EMR/Glue inputs stored in S3.

The goal is to answer:

- Are my largest tables laid out well for Spark?
- Do I have too few files, too many small files, or huge unsplittable files?
- Should I repartition or rewrite the data before the main job?
- What is a reasonable starting point for driver memory, driver cores, executor count, executor cores, executor memory, executor overhead, and partition count?

This script is intentionally **S3-only**. It uses `boto3` to inspect file layout and object sizes. It does not start Spark and does not read full data files.

## Why This Exists

Spark parallelism depends heavily on partitions. Large data does not automatically mean good parallelism.

Bad layouts include:

- One huge `.csv.gz` file.
- A small number of giant files.
- Thousands or millions of tiny files.
- Uneven partition directories, such as one date folder holding most of the data.

Good layouts usually include:

- Parquet or ORC.
- Many reasonably sized files.
- Splittable compression.
- Balanced partition directories.
- File sizes commonly around 128 MB to 1 GB, depending on workload.

## Files

| File | Purpose |
| --- | --- |
| `profile_s3_spark_inputs.py` | Main profiler script |
| `sample_tables.txt` | Example table input file |
| `README.md` | Usage guide |

## Input Format

Create a text file with one table per line:

```text
customers,s3://my-bucket/raw/customers/
transactions,s3://my-bucket/raw/transactions/
events,s3://my-bucket/raw/events/
```

Format:

```text
table_name,s3://bucket/prefix/
```

Blank lines and lines starting with `#` are ignored.

## Basic Usage

From this folder:

```bash
python3 profile_s3_spark_inputs.py \
  --tables-file sample_tables.txt \
  --output-dir profile_output \
  --worker-nodes 6 \
  --worker-cores 16 \
  --worker-memory-gb 64
```

With AWS profile/region:

```bash
AWS_PROFILE=my-profile AWS_REGION=us-east-1 \
python3 profile_s3_spark_inputs.py \
  --tables-file tables.txt \
  --output-dir profile_output \
  --worker-nodes 6 \
  --worker-cores 16 \
  --worker-memory-gb 64
```

Or pass region explicitly:

```bash
python3 profile_s3_spark_inputs.py \
  --tables-file tables.txt \
  --region us-east-1 \
  --output-dir profile_output \
  --worker-nodes 6 \
  --worker-cores 16 \
  --worker-memory-gb 64
```

## Outputs

The script writes:

| Output | Purpose |
| --- | --- |
| `table_profile_report.md` | Human-readable Markdown report |
| `table_profile_report.csv` | Spreadsheet-friendly summary |
| `spark_recommendations.json` | Machine-readable recommendations |

## What It Checks

For each S3 table path, it calculates:

- total data size
- number of files
- average file size
- largest file size
- largest file key
- file extensions
- compression indicators
- likely format
- splittability risk
- estimated Spark input partitions
- recommended partition counts using 256 MB and 512 MB targets

## What It Recommends

The profiler flags patterns like:

| Finding | Meaning | Likely Action |
| --- | --- | --- |
| One or few huge `.gz` files | Spark may not split input well | Split/decompress/re-export first |
| Few files vs many cluster cores | Low parallelism | Repartition after read if readable |
| Many tiny files | Scheduling/listing overhead | Compact into larger Parquet files |
| Parquet/ORC with healthy file sizes | Generally good | Validate Spark UI for skew/spill |

## Spark Sizing Logic

The script gives a starting point, not a guarantee.

Inputs:

```text
worker nodes
cores per worker
memory per worker
executor cores
reserved memory per node
memory overhead fraction
```

Example:

```bash
--worker-nodes 6 \
--worker-cores 16 \
--worker-memory-gb 64 \
--executor-cores 4
```

The script estimates:

```text
executors per node = worker cores / executor cores
total executors = worker nodes * executors per node
executor memory = remaining node memory / executors per node, minus overhead
driver memory = based on table count, file count, and total input size
driver cores = based on table count and file count
shuffle partitions = bounded by data size and cluster cores
```

The report includes suggested values for:

| Spark Setting | What It Controls |
| --- | --- |
| `spark.driver.cores` | CPU for the Spark driver |
| `spark.driver.memory` | Driver memory for planning, metadata, scheduling, and driver-side work |
| `spark.executor.instances` | Number of executors |
| `spark.executor.cores` | Cores per executor |
| `spark.executor.memory` | Heap memory per executor |
| `spark.executor.memoryOverhead` | Off-heap/container overhead memory |
| `spark.default.parallelism` | Default RDD/task parallelism |
| `spark.sql.shuffle.partitions` | Number of shuffle partitions for Spark SQL operations |

Important:

```text
Driver memory does not fix executor OOM caused by large input partitions or large shuffles.
If the data layout is bad, fix file layout and partitioning first.
```

For real EMR tuning, validate with:

- Spark UI stages and task counts
- executor memory usage
- spill to disk
- GC time
- failed executors
- YARN container killed messages
- S3 read throughput

## Repartition Guidance

If Spark can read the data:

```python
df = spark.read.parquet("s3://bucket/raw/table/")
df = df.repartition(400)
df.write.mode("overwrite").parquet("s3://bucket/curated/table/")
```

If Spark cannot read efficiently because the input is one huge unsplittable file:

```text
repartition() will not fix the initial read bottleneck.
Split, decompress, or ask the upstream system to re-export the data first.
```

Example:

```text
Bad:
  s3://bucket/raw/table/big_file.csv.gz

Better:
  s3://bucket/raw/table/part-00001.csv
  s3://bucket/raw/table/part-00002.csv
  ...

Best for repeated analytics:
  s3://bucket/curated/table/part-00001.parquet
  s3://bucket/curated/table/part-00002.parquet
  ...
```

## Caveats and Limitations

This script gives **starting recommendations**, not final Spark tuning. It profiles S3 layout and cluster shape, but it does not execute your Spark transformations.

What the script can estimate reasonably:

- whether file layout has enough parallelism
- whether files look splittable or risky
- rough input partition count
- rough executor and driver starting values
- whether data should likely be split, compacted, or rewritten

What the script does **not** know:

- row counts
- data skew by key
- join keys
- actual shuffle volume
- Parquet row group details
- compression ratio
- transformation complexity
- executor runtime behavior
- broadcast join feasibility
- UDF cost
- downstream write cost
- S3 throttling or network bottlenecks

Important caveats:

- A table can have a healthy file layout but still fail because of skewed joins or large aggregations.
- Driver memory helps with planning, metadata, and scheduling; it does not fix executor OOM from large partitions.
- `repartition()` helps only after Spark can read the input. It does not fix the initial read of a huge unsplittable `.gz` file.
- Recommended `spark.sql.shuffle.partitions` is a starting point. Join and aggregation-heavy jobs may need different values.
- Executor sizing depends on instance type, EMR release, YARN overhead, dynamic allocation, and workload behavior.
- If dynamic allocation is enabled, fixed `--num-executors` may not be the final runtime executor count.

Use this as a first-pass layout profiler. For real tuning, combine the report with:

- Spark UI stages and task duration
- Spark event logs
- spill metrics
- executor lost/OOM messages
- GC time
- shuffle read/write size
- skewed task analysis
- YARN resource usage

## Practical Workflow

1. Put your 3 largest table S3 paths in `tables.txt`.
2. Run this profiler.
3. Review `table_profile_report.md`.
4. Fix obvious layout problems first.
5. Run the Spark job.
6. Validate task counts, spills, skew, and memory in Spark UI.
7. Tune repartitioning, joins, and executor sizing based on runtime evidence.

## Example Command for 6 x 64 GB Cluster

```bash
python3 profile_s3_spark_inputs.py \
  --tables-file tables.txt \
  --output-dir profile_output \
  --worker-nodes 6 \
  --worker-cores 16 \
  --worker-memory-gb 64 \
  --executor-cores 4 \
  --target-partition-mb 256
```

## Interpreting One Common Result

If the report says:

```text
High risk layout: one or more gzip/zip files may be unsplittable.
Few files for cluster size.
```

Then do not assume Spark `repartition()` will solve the problem. The initial read may already be one large task.

Preferred fix:

```text
Split/decompress/re-export as multiple files, then write curated Parquet.
```

## AWS Permissions Needed

The caller needs S3 list permissions for the profiled prefixes:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:ListBucket"
  ],
  "Resource": "arn:aws:s3:::my-bucket"
}
```

If prefixes are restricted, include a condition for the allowed prefixes.
