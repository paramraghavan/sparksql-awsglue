# PYSPARK JOIN TROUBLESHOOTING - QUICK START GUIDE

## Your Specific Problem
- Works with 1M rows, hangs with full dataset
- Resources (CPU/memory) not fully utilized
- Already set shuffle partitions to 4000

## Most Likely Cause: SEVERE DATA SKEW
A few Cusip+EFFECTIVEDATE combinations have millions of records while others have just a few.

---

## STEP 1: Run Diagnostics (5 minutes)

```python
from pyspark.sql.functions import col, desc

# Check for duplicate keys
print("Checking df_tempo duplicates...")
df_tempo_dupes = df_tempo.groupBy('Cusip', 'EFFECTIVEDATE').count() \
    .filter(col('count') > 1) \
    .orderBy(desc('count'))

print(f"Duplicate key combinations: {df_tempo_dupes.count()}")
df_tempo_dupes.show(10, truncate=False)

print("\nChecking arm_loan_history duplicates...")
arm_dupes = arm_loan_history.groupBy('Cusip', 'EFFECTIVEDATE').count() \
    .filter(col('count') > 1) \
    .orderBy(desc('count'))

print(f"Duplicate key combinations: {arm_dupes.count()}")
arm_dupes.show(10, truncate=False)

# Check sizes
print(f"\ndf_tempo count: {df_tempo.count():,}")
print(f"arm_loan_history count: {arm_loan_history.count():,}")
```

---

## STEP 2: Choose Your Fix

### OPTION A: If arm_loan_history < 10M rows → BROADCAST JOIN ✓ FASTEST

```python
from pyspark.sql.functions import broadcast, expr

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "8GB")

df_tempo = df_tempo.join(
    broadcast(arm_loan_history),
    on=['Cusip', 'EFFECTIVEDATE'],
    how='left'
) \
.withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
.withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
.withColumn('CURRENTNOTERATE', 
            expr("case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
.drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new')
```

---

### OPTION B: If You Have Duplicates → DEDUPLICATE FIRST

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Deduplicate arm_loan_history (keep most recent)
window_spec = Window.partitionBy('Cusip', 'EFFECTIVEDATE').orderBy(desc('timestamp_column'))
arm_loan_history = arm_loan_history \
    .withColumn('rn', row_number().over(window_spec)) \
    .filter(col('rn') == 1) \
    .drop('rn')

# Then do regular join
df_tempo = df_tempo.join(
    arm_loan_history,
    on=['Cusip', 'EFFECTIVEDATE'],
    how='left'
) \
.withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
.withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
.withColumn('CURRENTNOTERATE', 
            expr("case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
.drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new')
```

---

### OPTION C: If Resources Underutilized (Severe Skew) → SALTING ✓ BEST FOR YOUR CASE

```python
from pyspark.sql.functions import col, expr, rand, explode, array, lit
from pyspark.sql.types import IntegerType

# ENABLE ADAPTIVE QUERY EXECUTION FIRST
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Add random salt to df_tempo
num_salts = 100  # Increase to 200 if still having issues
df_tempo_salted = df_tempo.withColumn('salt', (rand() * num_salts).cast(IntegerType()))

# Explode arm_loan_history with all salt values
arm_loan_history_exploded = arm_loan_history \
    .withColumn('salt', explode(array([lit(i) for i in range(num_salts)])))

# Repartition on join keys + salt
df_tempo_salted = df_tempo_salted.repartition(4000, 'Cusip', 'EFFECTIVEDATE', 'salt')
arm_loan_history_exploded = arm_loan_history_exploded.repartition(4000, 'Cusip', 'EFFECTIVEDATE', 'salt')

# Join with salt
df_tempo = df_tempo_salted.join(
    arm_loan_history_exploded,
    on=['Cusip', 'EFFECTIVEDATE', 'salt'],
    how='left'
) \
.withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
.withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
.withColumn('CURRENTNOTERATE', 
            expr("case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
.drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new', 'salt')
```

---

### OPTION D: Nuclear Option (If All Else Fails) → PROCESS IN BATCHES

```python
# Get distinct Cusips
cusips = [row.Cusip for row in df_tempo.select('Cusip').distinct().collect()]
batch_size = 100

# Process in batches
for i in range(0, len(cusips), batch_size):
    batch_cusips = cusips[i:i+batch_size]
    print(f"Processing batch {i//batch_size + 1}...")
    
    df_tempo_batch = df_tempo.filter(col('Cusip').isin(batch_cusips))
    arm_batch = arm_loan_history.filter(col('Cusip').isin(batch_cusips))
    
    result = df_tempo_batch.join(
        arm_batch,
        on=['Cusip', 'EFFECTIVEDATE'],
        how='left'
    ) \
    .withColumn('MixWAC', expr("case when MixWAC_new is null then 0 else MixWAC_new end")) \
    .withColumnRenamed('CURRENTNOTERATE', 'CURRENTNOTERATE_old') \
    .withColumn('CURRENTNOTERATE', 
                expr("case when CURRENTNOTERATE_new is null then CURRENTNOTERATE_old else CURRENTNOTERATE_new end")) \
    .drop('MixWAC_new', 'CURRENTNOTERATE_old', 'CURRENTNOTERATE_new')
    
    # Write each batch immediately
    result.write.mode('append').parquet('s3://bucket/output/')
```

---

## STEP 3: Monitor Progress

### In Spark UI (EMR Console or port 4040):
1. Go to **Stages** tab
2. Look at the join stage
3. Check if:
   - Tasks are stuck at 99%
   - Data is skewed (some tasks process GB, others KB)
   - Executors are idle

### If Still Hanging:
- Try increasing `num_salts` to 200 or 300
- Try combining solutions (deduplicate + salt)
- Use the batch processing approach

---

## Common Mistakes to Avoid

1. **Typo in your code**: `MixWAC_new` vs `MiXWAC` - make sure column names match
2. **Missing column**: `expr"case..."` should be `expr("case...")`
3. **Wrong join type**: Using `inner` instead of `left`
4. **Not caching**: If you reuse dataframes, add `.persist()`

---

## Quick Decision Tree

```
Resources underutilized? → Try SALTING (Option C)
       ↓ No
arm_loan_history < 10M rows? → Try BROADCAST (Option A)
       ↓ No
Duplicate keys? → Try DEDUPLICATION (Option B)
       ↓ Still fails
Use BATCH PROCESSING (Option D)
```

---

## Need More Help?

See the comprehensive guide: `pyspark_join_troubleshooting_guide.py`

It includes:
- Detailed diagnostics functions
- 6 different solution strategies
- Monitoring and debugging tools
- Complete code examples
