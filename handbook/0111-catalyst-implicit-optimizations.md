# 0111 - Catalyst Implicit Optimizations

## Overview

Catalyst is Spark's query optimizer that automatically applies sophisticated optimizations without you writing extra code. Understanding these helps you trust DataFrame performance and write better queries.

---

## Table of Contents

1. [IsNotNull Checks](#isnotnull-checks)
2. [Constant Folding](#constant-folding)
3. [Dead Code Elimination](#dead-code-elimination)
4. [Join Reordering](#join-reordering)
5. [Boolean Simplification](#boolean-simplification)
6. [Null Propagation](#null-propagation)
7. [Type Coercion](#type-coercion)
8. [Common Sub-Expression Elimination](#common-sub-expression-elimination)
9. [Partition Pruning](#partition-pruning)

---

## IsNotNull Checks

### What It Does

Catalyst automatically adds null checks for comparison operations because `null > 25` evaluates to `null` (not `true` or `false`), which would return incorrect results.

### Example

```python
# Your code:
df.filter(df.age > 25)

# Catalyst implicitly becomes:
df.filter((df.age.isNotNull()) & (df.age > 25))

# Why: null > 25 = null, not false
# So Catalyst adds the null check to avoid including nulls
```

### Real Impact

```python
# Proof: Check execution plan
result = df.filter(df.age > 25)
result.explain()

# Shows: PushedFilters: [IsNotNull(age), GreaterThan(age, 25)]
#        ↑ IsNotNull added implicitly!
```

### When It Matters

- Filtering on numeric comparisons
- String length comparisons
- Any operation that shouldn't include nulls

---

## Constant Folding

### What It Does

Catalyst evaluates constant expressions at optimization time (not runtime), so `5 + 3` is computed once to `8` instead of during execution.

### Example

```python
# Your code:
df.select((F.lit(5) + F.lit(3)).alias("result"))

# Catalyst optimizes to:
df.select(F.lit(8).alias("result"))  # Computed once!

# Result: No arithmetic during execution
```

### Real Impact

```python
# Without constant folding (slow):
for row in 1000000 rows:
    result = 5 + 3  # Computed 1M times!

# With constant folding (fast):
result = 8  # Computed once at plan time
for row in 1000000 rows:
    use(result)  # Just use the precomputed value
```

### Real-World Scenario

```python
# Complex constants in filters
df.filter(
    (F.col("amount") * 1.10) > (100 * 12)  # 100 * 12 = 1200
).filter(
    (F.col("count") + 5) < (10 * 3)  # 10 * 3 = 30
)

# Catalyst reduces to:
df.filter(
    (F.col("amount") * 1.10) > 1200
).filter(
    (F.col("count") + 5) < 30
)
```

---

## Dead Code Elimination

### What It Does

Catalyst removes columns that are never used, so only needed columns are read from disk (Column Pruning).

### Example

```python
# Your code (reads all 100 columns):
df = spark.read.parquet("huge_file.parquet")  # 100 columns
result = df.select("name", "age")

# Catalyst reads ONLY:
# - name
# - age
# (99 other columns never touched!)

# Real impact on 500GB file:
# Without pruning: Read 500GB
# With pruning: Read ~10GB (name + age only)
```

### Real-World Scenario

```python
# Join, then select only 2 columns
transactions = spark.read.parquet("transactions/")  # 50 columns
customers = spark.read.parquet("customers/")  # 30 columns

result = transactions.join(
    customers, "customer_id"
).select("transaction_id", "amount")

# Catalyst reads ONLY:
# From transactions: customer_id, transaction_id, amount
# From customers: customer_id
# (77 columns never touched!)
```

---

## Join Reordering

### What It Does

Catalyst reorders joins to minimize shuffled data. Puts smaller tables first (or broadcasts them), reducing intermediate results.

### Example

```python
# Your code (3-table join):
large.join(medium).join(small)
# large: 100GB, medium: 50GB, small: 1GB

# Catalyst reorders to:
large.join(small).join(medium)

# Why: Reduces intermediate shuffle
# Stage 1: 100GB ⊕ 1GB → 50GB intermediate (not 150GB)
# Stage 2: 50GB ⊕ 50GB → final result
```

### With Broadcast (< 10MB)

```python
# Your code:
df_large.join(df_tiny, "key")  # df_tiny < 10MB

# Catalyst becomes:
df_large.join(broadcast(df_tiny), "key")

# Effect: df_tiny sent to ALL executors (no shuffle!)
# Only df_large shuffled by join key
# Speed: 10-100x faster!
```

### Real Impact

```python
# Scenario: 4-table join without broadcast
orders(100GB) ⊕ customers(30GB) ⊕ products(5GB) ⊕ stores(1GB)

# Without reordering: Orders ⊕ Customers ⊕ Products ⊕ Stores
# Shuffles: 100GB + 130GB + 135GB + 140GB = 500GB+ data movement

# With Catalyst reordering + broadcast:
# orders ⊕ broadcast(stores) → 100GB
# intermediate ⊕ broadcast(products) → 100GB
# final ⊕ broadcast(customers) → 100GB
# Shuffles: ~100GB (5x less!)
```

---

## Boolean Simplification

### What It Does

Catalyst simplifies boolean expressions: `true AND x` becomes `x`, `false OR x` becomes `x`, etc.

### Example

```python
# Your code:
df.filter((True) & (df.age > 25))

# Catalyst simplifies to:
df.filter(df.age > 25)

# Other simplifications:
# (False) | (df.amount > 0) → df.amount > 0
# (True) | (anything) → always true (filter removed)
# (False) & (anything) → always false (no rows)
```

### Real Scenario

```python
# Dynamic filter building (common in APIs):
def get_filtered_data(age_filter=None, amount_filter=None):
    result = df

    if age_filter:
        result = result.filter(df.age > age_filter)
    else:
        result = result.filter(True)  # "No filter"

    if amount_filter:
        result = result.filter(df.amount > amount_filter)
    else:
        result = result.filter(True)

    return result

# Catalyst removes the `True` filters automatically!
```

---

## Null Propagation

### What It Does

Catalyst eliminates expressions that always produce nulls. Example: `null * anything = null`, so these entire expressions can be optimized away.

### Example

```python
# Your code:
df.select(
    F.when(df.status == "inactive", F.lit(None)).otherwise(df.salary)
)

# If status is always "inactive", Catalyst knows:
# result = None (for all rows)
# Can eliminate the entire column computation!

# Another example:
df.select(F.col("nullable_col") * 100)

# If nullable_col is ALL nulls:
# Catalyst skips the multiplication (null * 100 = null anyway)
```

### Null in Comparisons

```python
# Your code:
df.filter((df.col_a == df.col_b) & (df.col_a.isNull()))

# Catalyst knows: NULL == NULL = NULL (not true)
# So this filter can never be true!
# Entire filter optimized away → returns 0 rows

# Without optimization: Scans entire table, finds nothing
# With optimization: Returns 0 rows immediately
```

---

## Type Coercion

### What It Does

Catalyst applies implicit type conversions intelligently, choosing the best type for operations.

### Example

```python
# Your code (mixing int and string):
df.filter(df.amount > "100")

# Catalyst coerces to:
df.filter(df.amount > 100)  # int comparison, not string

# Another example:
df.select(df.price * 1.5)  # int * float = float
# Catalyst handles: int → float conversion at right time
```

### Smart Coercion

```python
# Your code:
df.select(
    (df.quantity * 1.1).alias("adjusted_qty")  # int * double
)

# Catalyst determines:
# 1. Quantity (int) needs to be double for precise multiplication
# 2. Conversion happens once per row: int → double → multiply
# 3. Optimal type chosen for result (double)

# Not: Convert to string, then back, then multiply (wrong!)
```

---

## Common Sub-Expression Elimination

### What It Does

If the same expression appears multiple times, Catalyst computes it once and reuses the result.

### Example

```python
# Your code:
df.select(
    (df.salary * 1.1).alias("salary_with_bonus"),
    (df.salary * 1.1 + df.bonus).alias("total_comp"),
    F.col("salary").alias("base_salary")
)

# WITHOUT CSE (bad): df.salary * 1.1 computed twice
# Salary * 1.1 → salary_with_bonus (compute)
# Salary * 1.1 + bonus → total_comp (compute AGAIN)
# Inefficient!

# WITH CSE (Catalyst): df.salary * 1.1 computed once
temp = df.salary * 1.1
# temp → salary_with_bonus
# temp + bonus → total_comp
# Efficient!
```

### Real Impact

```python
# Complex expression used 3 times:
df.select(
    (F.sqrt(F.col("x") * F.col("x") + F.col("y") * F.col("y"))).alias("magnitude1"),
    (F.sqrt(F.col("x") * F.col("x") + F.col("y") * F.col("y")) * 2).alias("magnitude2"),
    (F.sqrt(F.col("x") * F.col("x") + F.col("y") * F.col("y")) + 10).alias("magnitude3")
)

# WITHOUT CSE: sqrt computed 3 times per row
# 1M rows × 3 computations = 3M sqrt() calls

# WITH CSE: sqrt computed once per row, result reused
# 1M rows × 1 computation = 1M sqrt() calls
# 3x faster!
```

---

## Partition Pruning

### What It Does

Catalyst skips entire partitions that can't possibly match your filter, dramatically reducing data read.

### Example

```python
# Table partitioned by year:
# /year=2020/
# /year=2021/
# /year=2022/
# /year=2023/

# Your code:
df.filter(df.year == 2023)

# Catalyst: "year is partition key, only 2023 matches"
# Result: Read ONLY /year=2023/ partition
# Skip: 2020, 2021, 2022 (not touched!)

# Real impact:
# Without partition pruning: Read 4 years × 100GB = 400GB
# With partition pruning: Read 1 year = 100GB
# 4x faster!
```

### Date Partitioning (Common)

```python
# Sales table partitioned by load_date:
sales_df = spark.read.parquet("s3://data/sales/")
# /load_date=2024-01-01/
# /load_date=2024-01-02/
# ... 365 partitions

# Your code:
result = sales_df.filter(df.load_date >= "2024-06-01")

# Catalyst: "load_date is partition key"
# Result: Read ONLY 184 partitions (June-Dec)
# Skip: 152 partitions (Jan-May)

# Real impact:
# Without: Read 365GB (all days)
# With: Read ~184GB (half the year)
# 2x faster!
```

### How to Enable

```python
# Write with partition column
df.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("s3://data/sales/")

# Now all year-based filters benefit from partition pruning!
```

---

## Key Takeaways

✅ **Trust Catalyst** - It does most of the hard optimization work automatically

✅ **IsNotNull is implicit** - Comparisons automatically null-safe

✅ **Constant Folding** - `5 + 3` computed once at plan time

✅ **Dead Code Elimination** - Only needed columns read from disk

✅ **Join Reordering** - Smaller tables processed first

✅ **Null Propagation** - Impossible conditions eliminated

✅ **Partition Pruning** - Only matching partitions read (huge savings!)

✅ **CSE** - Duplicate expressions computed once

---

## How to Inspect Catalyst's Work

```python
# See all optimizations applied:
result.explain(extended=True)

# Shows:
# 1. Parsed plan (your code)
# 2. Analyzed plan (with types)
# 3. Optimized plan (Catalyst's optimizations)
# 4. Physical plan (how it executes)

# Key to look for:
# - PushedFilters: What's filtered at source
# - SelectedFields: What columns are kept
# - Broadcast: What tables are broadcast
```

---

## Next Steps

1. **Check explain() output** on your queries to see optimizations
2. **Write partitioned tables** to enable partition pruning
3. **Trust Catalyst** - Focus on clean query logic, not micro-optimizations
4. **Move to File 02** - Understand transformation options that Catalyst optimizes

---

**Remember:** Catalyst is one of Spark's superpowers. Understanding its optimizations helps you write better queries and trust DataFrame performance!
