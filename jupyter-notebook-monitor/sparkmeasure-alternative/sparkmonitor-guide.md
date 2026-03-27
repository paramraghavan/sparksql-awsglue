# sparkmonitor — User Guide

> **For data modellers using Jupyter notebooks on the EMR cluster.**
> You do not need to understand Spark internals to use this tool.

---

## What is sparkmonitor?

When you run a cell in Jupyter that processes data with Spark, a lot happens behind the scenes — data is read from S3, split into pieces, moved between machines, sorted, joined, and written back. Any one of those steps can quietly become a bottleneck that makes your job 10x slower than it needs to be, or in the worst case crash the cluster for everyone.

**sparkmonitor watches what Spark actually did when your cell ran** and translates it into plain English. If it spots a problem it tells you exactly what happened, why it matters, and shows you the code to fix it — no Spark knowledge required.

It works by reading Spark's own built-in monitoring data (available on every EMR cluster at port 4040). There is nothing to install and nothing to configure. You add one line to Cell 1 of your notebook, and then prefix any cell you care about with `%%measure`.

---

## What sparkmonitor checks for

| Problem | What it means in plain English |
|---|---|
| **Data spilled to disk** | Your job ran out of memory and had to use the hard drive. This is 10–100× slower than memory and can crash the job. |
| **Memory pressure** | RAM is running low. The next run with more data will likely spill to disk. |
| **Large network shuffle** | A join or groupBy moved hundreds of megabytes of data between machines. A common cause of slow jobs. |
| **Under-parallelised work** | Only a handful of the cluster's cores are being used. The rest are sitting idle. |
| **High garbage collection** | Python UDFs (custom functions) are making the JVM spend more time cleaning up than computing. |
| **Slow cell with no data reads** | Spark is silently replaying an expensive chain of steps because a DataFrame was never cached. |

For every problem it detects, sparkmonitor shows you a **copy-paste code fix** inside the report.

---

## How to use it

### Step 1 — Add this as Cell 1 of every notebook

```python
%run s3://your-bucket/tools/sparkmonitor.py
```

When this runs successfully you will see a green banner:

```
⚡ sparkmonitor is ready
Connected to Spark on port 4040.
```

If you see a yellow warning instead, re-run the cell — Spark may still be starting up.

---

### Step 2 — Add `%%measure` to any cell you want to profile

Put `%%measure` as the very first line of the cell. Your code goes underneath exactly as normal — nothing else changes.

**Without measurement (normal):**
```python
df = spark.read.parquet("s3://your-bucket/sales/")
df.filter(df.year == 2024).groupBy("region").agg({"revenue": "sum"}).show()
```

**With measurement:**
```python
%%measure
df = spark.read.parquet("s3://your-bucket/sales/")
df.filter(df.year == 2024).groupBy("region").agg({"revenue": "sum"}).show()
```

The cell runs exactly as before. The performance report appears automatically underneath the output.

---

### Step 3 — Read the report

The report has three parts.

**1. The health banner** — the first thing to look at:

| Banner | Meaning |
|---|---|
| 🟢 Green | No issues detected. The cell ran efficiently. |
| 🟡 Yellow | One or more warnings. Advice cards are shown below. |
| 🔴 Red | A serious issue that is slowing or could crash the job. Fix immediately. |

**2. The metrics table** — a collapsible section showing the key numbers from the run. Hover over any row to see a plain-English explanation of what that metric means. Rows highlighted in yellow are the ones worth paying attention to.

**3. Advice cards** — one card per issue detected. Each card contains:

- **What happened** — what Spark actually did, using the real numbers from your run
- **Why it matters** — the impact on speed or cluster health
- **What to do** — a plain-English instruction
- **Copy this fix** — a code block you can paste directly into your notebook

---

## Example reports

### A healthy cell (no issues)

```
⚡ sparkmonitor   ·   3 stages   ·   8.2 s
────────────────────────────────────────────
🟢  Looks healthy — no major issues detected

▸ Full metrics
  Total time                   8.2 s
  Parallel tasks used          384
  Data read from S3 / HDFS     2.1 GB
  Network shuffle              0 B
  Spilled to disk              None ✓
  Spilled from memory          None ✓
  GC time                      0.4 s  (1% of run time)
  Peak executor memory         1.2 GB
```

---

### A cell with a caching problem (red alert)

```
⚡ sparkmonitor   ·   5 stages   ·   4 min 12 s
────────────────────────────────────────────────
🔴  Issues found — see the advice cards below

🔴  Your data spilled to disk

  WHAT HAPPENED    Spark ran out of memory and had to write 2.1 GB
                   to the cluster's hard drive to continue.

  WHY IT MATTERS   Reading from disk is 10–100× slower than reading
                   from memory. This is one of the most common reasons
                   a job takes much longer than expected. If enough data
                   spills, the job can crash entirely.

  WHAT TO DO       Add .cache() on the DataFrame you reuse most, right
                   after you build it. This keeps it in memory so Spark
                   never needs to recompute or reload it.

  Copy this fix into your notebook:

  ┌─────────────────────────────────────────────────────────┐
  │  # Find the DataFrame used in multiple cells:           │
  │  df = df.cache()                                        │
  │  df.count()  # this forces the cache to load now        │
  │                                                         │
  │  # From this point, all .show(), .count(), .groupBy()   │
  │  # on df will read from memory — not from S3 again.     │
  └─────────────────────────────────────────────────────────┘
```

---

## The most common fixes

### Fix 1 — Cache a DataFrame you use more than once

If your notebook uses the same DataFrame in multiple cells (for example, building it in Cell 2 and then filtering, plotting, or aggregating it in Cells 3, 4, and 5), Spark re-reads it from S3 every single time unless you cache it.

```python
# In the cell where you first build your DataFrame:
df = (
    spark.read.parquet("s3://your-bucket/data/")
    .filter(df.year == 2024)
)
df = df.cache()    # keep this DataFrame in cluster memory
df.count()         # this line triggers the cache to load now

# All later cells that use df will be instant
```

**When to use it:** any time the same DataFrame appears in more than one cell, or when a cell is slow but the data has not changed.

---

### Fix 2 — Broadcast a small table in a join

When you join a large table to a small one (a lookup table, a reference list, a set of filter values), Spark by default shuffles the large table across the network. Using `broadcast()` sends the small table to every machine instead, which is much cheaper.

```python
from pyspark.sql.functions import broadcast

# Before — causes a large network shuffle:
result = transactions.join(country_codes, "country_id")

# After — no shuffle; country_codes is sent to every node:
result = transactions.join(broadcast(country_codes), "country_id")
```

**When to use it:** when one of the tables in a join is small (under about 100 MB). If sparkmonitor reports a large shuffle, this is usually the first thing to try.

---

### Fix 3 — Repartition your data before heavy operations

If sparkmonitor reports very few parallel tasks on a large dataset, Spark is not splitting the work across enough machines.

```python
# Add this right after reading your data:
df = spark.read.parquet("s3://your-bucket/large-dataset/")
df = df.repartition(200)   # 200 is a safe starting point

# Then continue as normal:
result = df.filter(...).groupBy(...).agg(...)
```

**When to use it:** when sparkmonitor reports fewer than 10 tasks on data larger than 100 MB, or when the cluster feels like it is not doing much.

---

### Fix 4 — Replace Python UDFs with built-in functions

Python UDFs (functions you define yourself with `@udf` or `lambda`) are convenient but slow. Spark has hundreds of built-in functions that do the same things much faster.

```python
# SLOW — Python UDF:
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
clean_udf = udf(lambda x: x.strip().upper(), StringType())
df = df.withColumn("name", clean_udf("name"))

# FAST — built-in functions:
from pyspark.sql.functions import upper, trim
df = df.withColumn("name", upper(trim("name")))
```

Common built-in functions: `upper`, `lower`, `trim`, `split`, `regexp_replace`, `to_date`, `datediff`, `coalesce`, `when`, `col`, `lit`, `concat`, `substring`.

**When to use it:** when sparkmonitor reports high GC time, or when you are using `@udf` and the cell is slow.

---

## Frequently asked questions

**Do I need to add `%%measure` to every cell?**

No. Add it only to cells that are slow or that you suspect might be causing problems. A good starting point is any cell that reads data, does a join, or takes longer than 10 seconds.

**Will `%%measure` slow down my cell?**

No. It reads Spark's own monitoring data after the cell finishes. The cell itself runs at exactly the same speed.

**What if the report shows no issues but my cell is still slow?**

The data volume itself may be the bottleneck — you are simply processing a lot of data and it takes time. sparkmonitor checks for *inefficiencies*, not raw data size. If no issues are found, the run time is probably close to optimal for your dataset.

**What if I see a yellow or red banner but I do not understand the fix?**

Show the advice card to a colleague or the platform team. The card contains the full context — what happened, why, and what to change — so anyone can help you apply the fix even without Spark expertise.

**Can I use `%%measure` on cells that use plain Python (no Spark)?**

Yes — the cell will still run normally, and sparkmonitor will report that no Spark work ran. This is not an error; it just means the cell did not trigger any Spark computation.

---

## Quick reference card

```
┌─────────────────────────────────────────────────────────────────┐
│  sparkmonitor — quick reference                                  │
├─────────────────────────────────────────────────────────────────┤
│  Cell 1 of every notebook:                                       │
│    %run s3://your-bucket/tools/sparkmonitor.py                   │
│                                                                  │
│  To profile a cell:                                              │
│    %%measure          ← first line of the cell                   │
│    df.groupBy(...).show()                                        │
│                                                                  │
│  What the banners mean:                                          │
│    🟢 Green   = no issues, cell ran efficiently                  │
│    🟡 Yellow  = warnings, review advice cards                    │
│    🔴 Red     = serious issue, apply fix before re-running       │
│                                                                  │
│  Most common fixes:                                              │
│    Spill to disk   →  df = df.cache(); df.count()                │
│    Large shuffle   →  big.join(broadcast(small), 'key')          │
│    Too few tasks   →  df = df.repartition(200)                   │
│    High GC time    →  replace @udf with built-in functions       │
└─────────────────────────────────────────────────────────────────┘
```

---

*sparkmonitor reads Spark's built-in REST API. No external packages required.*
