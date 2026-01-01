## Why Table2 Needs Replication - Explained with Example

You're right to question this! Let me explain with a concrete example.

### The Salting Problem

**Original situation (WITHOUT salting):**

```
Table1 (570M rows with CUSIP='ABC', DATE='2024-01-01')
┌──────┬──────────────┬─────────┐
│ CUSIP│ EFFECTIVEDATE│ amount  │
├──────┼──────────────┼─────────┤
│ ABC  │ 2024-01-01   │ 100     │  ← All 570M rows
│ ABC  │ 2024-01-01   │ 200     │     go to ONE partition
│ ABC  │ 2024-01-01   │ 300     │     (SKEW!)
│ ...  │ ...          │ ...     │
│ ABC  │ 2024-01-01   │ 999     │  (570 million rows)
└──────┴──────────────┴─────────┘

Table2 (10K rows with same key)
┌──────┬──────────────┬─────────┐
│ CUSIP│ EFFECTIVEDATE│ price   │
├──────┼──────────────┼─────────┤
│ ABC  │ 2024-01-01   │ 50.00   │  ← All 10K rows
│ ABC  │ 2024-01-01   │ 50.10   │     also in ONE partition
│ ...  │ ...          │ ...     │
└──────┴──────────────┴─────────┘

JOIN → 570M × 10K = 5.7 TRILLION comparisons in ONE executor! 💥
```

### The Salting Solution

**Goal**: Split this massive join across multiple partitions

**Step 1: Salt Table1 (RANDOM salt)**

```python
table1_salted = table1.withColumn(
    "salt",
    (F.rand() * 100).cast("int")  # Random 0-99
)
```

Result:

```
Table1 with RANDOM salt
┌──────┬──────────────┬────────┬──────┐
│ CUSIP│ EFFECTIVEDATE│ amount │ salt │
├──────┼──────────────┼────────┼──────┤
│ ABC  │ 2024-01-01   │ 100    │  37  │  ← Random salt
│ ABC  │ 2024-01-01   │ 200    │  82  │  ← Random salt
│ ABC  │ 2024-01-01   │ 300    │   5  │  ← Random salt
│ ABC  │ 2024-01-01   │ 400    │  37  │  ← Random salt
│ ...  │ ...          │ ...    │ ...  │
└──────┴──────────────┴────────┴──────┘

Now distributed across ~100 partitions (by salt value)
Each partition has ~5.7M rows (570M / 100)
```

**Step 2: The Problem - How to Join?**

Table1 rows now have **random** salt values. For the join to work:

- Table1 row with `(CUSIP=ABC, DATE=2024-01-01, salt=37)` needs to join with...
- Table2 row with `(CUSIP=ABC, DATE=2024-01-01, salt=37)` ← **same salt!**

But Table2 doesn't know which salt values Table1 will randomly pick!

### Why Replication is Necessary

Since Table1 uses **random** salts (0-99), Table2 needs to have copies with **ALL possible salts** (0-99):

```python
table2_salted = table2.withColumn(
    "salt",
    F.explode(F.array([F.lit(i) for i in range(100)]))  # ALL salts 0-99
)
```

Result:

```
Table2 REPLICATED with ALL salt values
┌──────┬──────────────┬────────┬──────┐
│ CUSIP│ EFFECTIVEDATE│ price  │ salt │
├──────┼──────────────┼────────┼──────┤
│ ABC  │ 2024-01-01   │ 50.00  │  0   │  ← Same row...
│ ABC  │ 2024-01-01   │ 50.00  │  1   │  ← replicated...
│ ABC  │ 2024-01-01   │ 50.00  │  2   │  ← 100 times...
│ ...  │ ...          │ ...    │ ...  │
│ ABC  │ 2024-01-01   │ 50.00  │ 99   │  ← with different salt
│ ABC  │ 2024-01-01   │ 50.10  │  0   │  ← Next row...
│ ABC  │ 2024-01-01   │ 50.10  │  1   │  ← also replicated...
│ ...  │ ...          │ ...    │ ...  │
└──────┴──────────────┴────────┴──────┘

Original 10K rows → Now 1M rows (10K × 100)
```

### The Join Now Works

```
JOIN on (CUSIP, EFFECTIVEDATE, salt)

Partition 0 (salt=0):
  Table1: ~5.7M rows with salt=0
  Table2: 10K rows with salt=0
  → 5.7M × 10K = 57B comparisons ✓

Partition 37 (salt=37):
  Table1: ~5.7M rows with salt=37
  Table2: 10K rows with salt=37
  → 5.7M × 10K = 57B comparisons ✓

... (100 partitions total)

All partitions run in PARALLEL! 🚀
```

### Visual Comparison

**Without Salting:**

```
Single Executor doing ALL the work:
[████████████████████████████████████] 100% load
[                                    ] 0% load
[                                    ] 0% load
[                                    ] 0% load
... (99 idle executors)
```

**With Salting (SALT_FACTOR=100):**

```
Work distributed across 100 executors:
[███] 1% of work
[███] 1% of work
[███] 1% of work
[███] 1% of work
... (100 executors, all working)
```

### Why the Math Works

Original total work:

- 570M × 10K = **5.7 trillion comparisons**

After salting:

- 570M × (10K × 100) = **57 trillion comparisons**

**Wait, that's 10x more work!?**

Yes, but:

- Original: 1 executor does 5.7T comparisons ⏱️ **40+ minutes**
- Salted: 100 executors each do 57B comparisons ⏱️ **~5 minutes** (if parallel)

The **replication overhead** (10x more comparisons) is offset by **100x parallelism**.

### Alternative: Why Not Salt Table2 Randomly Too?

```python
# What if we do this?
table2_salted = table2.withColumn("salt", (F.rand() * 100).cast("int"))
```

**Problem**: Rows won't match!

```
Table1:
│ ABC  │ 2024-01-01   │ 100    │  37  │

Table2:
│ ABC  │ 2024-01-01   │ 50.00  │  82  │  ← Different salt!

JOIN fails! No match because salt 37 ≠ 82
```

### Summary

**Why replicate Table2?**

1. Table1 gets **random** salt (0-99) to distribute rows
2. Table2 must have copies with **all** salts (0-99) to match any Table1 row
3. This ensures every Table1 row finds its matching Table2 row
4. Trade-off: 100x replication overhead for 100x parallelism gain

**SALT_FACTOR=1000 means:**

- Table2 replicated 1000 times (instead of 100)
- Better parallelism (1000 partitions instead of 100)
- But 10x more memory/shuffle overhead
