# One big file or files

> A 500 GB dataset shows only 1 or a small number of partitions, how does the user repartition it as they cannot read
> into pyspark

So there are two cases.

## Case 1: Spark Can Read It, But Partitions Are Too Few

Example:

```text
One large uncompressed CSV
One large Parquet file with many row groups
Few large input files
```

Spark can read it, but parallelism is low.

Then you can do:

```python
df = spark.read.csv("s3://bucket/raw/file.csv", header=True)

df = df.repartition(400)

df.write.mode("overwrite").parquet("s3://bucket/curated/")
```

Here:

```python
repartition(400)
```

works because Spark was able to read the data first.

## Case 2: Spark Cannot Read It Efficiently

Example:

```text
One huge .gz CSV file
One huge unsplittable compressed file
Read task OOMs before DataFrame is created
```

Then this will not help:

```python
df = spark.read.csv("s3://bucket/raw/file.csv.gz")
df = df.repartition(400)
```

because Spark cannot get past the read stage efficiently.

In that case, the user must fix the input before Spark repartitioning helps.

Options:

### Option 1: Ask Source System to Deliver Better Files

Best option if possible:

```text
Instead of one 500 GB file:
deliver 500 files of 1 GB each
or 2000 files of 256 MB each
or deliver Parquet/ORC
```

### Option 2: Decompress and Split Before Spark

If the file is `.gz`, decompress it first and split it into multiple files.

Example flow:

```text
big_file.csv.gz
  -> decompress
  -> split into many CSV files
  -> Spark reads many files
  -> write Parquet
```

This can be done using:

- source system
- EMR step using shell tools
- AWS Glue/Python job
- AWS Batch
- custom EC2 job
- distributed tool if available

### Option 3: Use a Splittable Compression Format

Instead of gzip, use:

```text
bzip2
LZO with index
Snappy with Parquet/ORC
```

Best practice is usually:

```text
Parquet or ORC + Snappy
```

### Option 4: Rewrite Once Into a Better Layout

Once Spark can read the data, rewrite it:

```python
df = spark.read.csv("s3://bucket/raw/split-files/", header=True)

df.repartition(400)
.write
.mode("overwrite")
.parquet("s3://bucket/curated/my_table/")
```

After that, future Spark jobs read the curated Parquet dataset efficiently.

## Rule

```text
If Spark can read it, use repartition().
If Spark cannot read it, fix/split/decompress the input first.
```

Better wording for the note:

> If Spark shows only one or a few partitions, repartitioning helps only after Spark can successfully read the data. For
> a huge unsplittable file such as a single `.gz` CSV, the read itself may be the bottleneck or failure point. In that
> case, split/decompress/re-export the file first, then use Spark to write a properly partitioned Parquet/ORC dataset.