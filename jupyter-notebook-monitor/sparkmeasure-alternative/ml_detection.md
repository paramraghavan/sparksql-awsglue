# ML Library Detection & Caching Guide

SparkMonitor automatically detects when you're using ML libraries and recommends caching strategies.

---

## Supported Libraries

SparkMonitor detects usage of:

- **scikit-learn** (sklearn) — RandomForest, SVM, LogisticRegression, etc.
- **XGBoost** — XGBClassifier, XGBRegressor, xgb.train()
- **LightGBM** — LGBMClassifier, LGBMRegressor, lgb.train()
- **TensorFlow** — model.fit(), Sequential, etc.
- **PyTorch** — torch operations, optimizer.step()
- **Pandas conversion** — `.toPandas()` calls

When detected, sparkmonitor shows a yellow warning with caching recommendations.

---

## Why Cache Before ML Operations?

### Problem: DataFrame Reused Without Caching

```python
%%measure
from sklearn.ensemble import RandomForestClassifier

df = spark.read.parquet("s3://data/")  # Read from S3

# ML operation 1: convert to Pandas
pdf = df.toPandas()                    # ⚠️ Reads entire DataFrame from S3
model = RandomForestClassifier()
model.fit(pdf, y)

# Later, if you use df again:
pdf2 = df.toPandas()                   # ⚠️ Reads AGAIN from S3 (not cached!)
predictions = model.predict(pdf2)
```

**Result:**
- DataFrame read twice from S3
- Memory pressure increases
- Job runs slower than necessary

### Solution: Cache After Filtering

```python
%%measure
from sklearn.ensemble import RandomForestClassifier

df = spark.read.parquet("s3://data/")
df = df.filter(df.year == 2024)         # Apply business logic first

# Cache BEFORE ML operations
df = df.cache()                          # Mark for caching
df.count()                               # Force cache to load into memory

# ML operation 1: uses cached data
pdf = df.toPandas()                     # ✓ Reads from memory (fast!)
model = RandomForestClassifier()
model.fit(pdf, y)

# Later, reuse without re-reading S3
pdf2 = df.toPandas()                    # ✓ Still from memory (no S3 access)
predictions = model.predict(pdf2)
```

**Result:**
- DataFrame read once, cached in memory
- All subsequent operations are instant
- Memory usage is controlled

---

## Common Patterns

### Pattern 1: scikit-learn with Pandas Conversion

```python
%%measure
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

# Load and prepare
df = spark.read.parquet("s3://training-data/")
df = df.filter(df.is_valid == True)
df = df.cache()                          # ✓ Cache before ML
df.count()                               # ✓ Force load

# Feature engineering
features = [...prepare features...]
X = df.select(*features).toPandas()     # Uses cached df
y = df.select("label").toPandas()

# Train
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
model = RandomForestClassifier(n_estimators=100)
model.fit(X_scaled, y)

# Evaluate on same data
df_test = spark.read.parquet("s3://test-data/")
df_test = df_test.cache()
df_test.count()
X_test = df_test.select(*features).toPandas()
predictions = model.predict(X_test)
```

**SparkMonitor Output:**
```
🟡 ML library detected — consider caching
   Your cell uses sklearn, to_pandas with a Spark DataFrame.
   If you reuse the same DataFrame across multiple cells or operations...

   Solution: Cache the DataFrame BEFORE ML operations.
```

---

### Pattern 2: Spark ML (MLlib) Pipeline

```python
%%measure
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier

df = spark.read.parquet("s3://data/")
df = df.filter(df.year == 2024)
df = df.cache()                          # ✓ Cache before pipeline
df.count()                               # ✓ Force load

# Build pipeline
assembler = VectorAssembler(
    inputCols=["age", "income", "credit"],
    outputCol="features"
)
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
classifier = RandomForestClassifier(featuresCol="scaled_features", numTrees=100)

pipeline = Pipeline(stages=[assembler, scaler, classifier])

# Fit uses cached df
model = pipeline.fit(df)

# Transform also uses cached df
predictions = model.transform(df)
```

**Advantage:** Spark ML pipelines work directly with Spark DataFrames (no conversion to Pandas needed).

---

### Pattern 3: XGBoost with Spark

```python
%%measure
from xgboost import XGBClassifier
import xgboost as xgb

df = spark.read.parquet("s3://data/")
df = df.cache()                          # ✓ Cache before XGBoost
df.count()

# Convert to DMatrix (XGBoost format)
# Option A: Via Pandas
pdf = df.toPandas()                     # Uses cached data
dtrain = xgb.DMatrix(pdf[features], label=pdf["label"])

# Option B: Using spark-xgboost connector (if available)
# dtrain = xgb.DMatrix.from_spark(df, features, "label")

# Train
model = xgb.train(params={...}, dtrain=dtrain, num_boost_round=100)

# Later reuse: still cached
pdf2 = df.filter(df.split == "test").toPandas()
predictions = model.predict(xgb.DMatrix(pdf2[features]))
```

---

## Performance Impact

### Without Caching

```
Iteration 1: df.toPandas() → read 10GB from S3 → 30 seconds
Iteration 2: df.toPandas() → read 10GB from S3 → 30 seconds
Iteration 3: df.toPandas() → read 10GB from S3 → 30 seconds
Total: 90 seconds (+ memory pressure, spill)
```

### With Caching

```
df.cache() → load 10GB to memory → 5 seconds (one-time)
Iteration 1: df.toPandas() → read from memory → 2 seconds
Iteration 2: df.toPandas() → read from memory → 2 seconds
Iteration 3: df.toPandas() → read from memory → 2 seconds
Total: 11 seconds (7-8x faster!)
```

---

## When NOT to Cache

- **Huge DataFrames** that don't fit in memory
  - Instead: use `df.repartition()` to process in chunks
  - Or: use Spark ML pipelines (native, no conversion)

- **One-off operations** you never reuse
  - No need to cache if you only call `.toPandas()` once

- **Small DataFrames**
  - Caching overhead may not be worth it
  - But doesn't hurt either

---

## Debugging: When Caching Doesn't Help

If you cache but still see slow performance:

```python
%%measure
df = spark.read.parquet("s3://large-data/")
df = df.cache()
df.count()  # This should be ~5-10 seconds

# If this is still slow, check:
print(f"DataFrame size: {df.count()} rows")
print(f"Memory per partition: {df.rdd.getNumPartitions()}")

# If memory pressure, reduce:
df = df.repartition(200)  # More partitions, less per executor
df = df.cache()
df.count()
```

---

## Best Practices

1. **Cache after filtering** — filter first, cache the reduced dataset
2. **Force cache load** — always call `.count()` after `.cache()`
3. **Use Spark ML when possible** — avoids Pandas conversion entirely
4. **Broadcast small tables** — use `broadcast(small_df)` in joins
5. **Monitor with sparkmonitor** — check memory pressure and spill

---

## Example: Complete ML Pipeline

```python
# Cell 1: Load sparkmonitor
%run s3://bucket/sparkmonitor.py

# Cell 2: Prepare data
%%measure
df = spark.read.parquet("s3://raw-data/")

# Business logic: filter, aggregate, enrich
df = df.filter(df.created_date >= "2024-01-01")
df = df.dropna()
df = df.cache()
df.count()

print(f"✓ Loaded {df.count()} training records")

# Cell 3: Feature engineering
%%measure
from pyspark.ml.feature import VectorAssembler

df_features = df.withColumn("age_scaled", df.age / 100)
df_features = df_features.cache()
df_features.count()

# Cell 4: Train model
%%measure
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

assembler = VectorAssembler(inputCols=["age_scaled", "income"], outputCol="features")
rf = RandomForestClassifier(numTrees=100)
pipeline = Pipeline(stages=[assembler, rf])

model = pipeline.fit(df_features)  # Uses cached df
print("✓ Model trained")

# Cell 5: Evaluate
%%measure
predictions = model.transform(df_features)  # Uses cache
predictions.select("prediction", "label").show()
```

Each `%%measure` cell will show:
- Spark metrics
- 🟡 ML library warning (if applicable)
- Advice on caching

---

## Summary

**SparkMonitor automatically detects ML library usage and recommends caching.**

Key takeaway: **Cache DataFrames before passing them to ML libraries to avoid expensive re-reads.**

```python
# Good
df = spark.read.parquet("s3://...")
df = df.cache()
df.count()
model.fit(df.toPandas())  # Uses cached data

# Bad
df = spark.read.parquet("s3://...")
model.fit(df.toPandas())  # S3 read every time!
```
