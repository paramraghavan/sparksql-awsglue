"""
Demo: Spark Instrumentation with Housing Data
==============================================
This script demonstrates the spark instrumentation module
using your sample housing data.

RUN:
    spark-submit demo_spark_job.py

    # Or with env var to auto-enable:
    SPARK_DEBUG=1 spark-submit demo_spark_job.py
"""

# ============================================================================
# SINGLE LINE TO ENABLE INSTRUMENTATION
# ============================================================================
from spark_instrumentation import enable_instrumentation; enable_instrumentation()
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import first, col, concat, lit, avg, sum as spark_sum

# Create Spark session
spark = SparkSession.builder \
    .appName("HousingDataETL") \
    .master("local[*]") \
    .getOrCreate()

# Set log level for Spark itself (less noise)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("DEMO: Spark Instrumentation")
print("=" * 60 + "\n")

# ============================================================================
# CREATE SAMPLE DATA (mimicking your Excel file)
# ============================================================================
data = [
    ("price", 10, 100, 34, 260302, "ashburn"),
    ("square_feet", 15, 95, 45, 260302, "ashburn"),
    ("bedrooms", 25, 150, 67, 260302, "ashburn"),
    ("bathrooms", 8, 85, 32, 260302, "ashburn"),
    ("age_years", 12, 110, 52, 260302, "ashburn"),
    ("lot_size", 5, 70, 28, 260302, "ashburn"),
    ("garage_spaces", 9, 88, 38, 260302, "ashburn"),
    ("property_tax", 18, 105, 55, 260302, "ashburn"),
    ("maintenance_cost", 7, 75, 31, 260302, "ashburn"),
    ("year_built", 20, 120, 71, 260302, "ashburn"),
    # Another place
    ("price", 15, 120, 44, 260302, "reston"),
    ("square_feet", 20, 105, 55, 260302, "reston"),
    ("bedrooms", 30, 160, 77, 260302, "reston"),
    ("bathrooms", 12, 95, 42, 260302, "reston"),
    ("age_years", 18, 120, 62, 260302, "reston"),
    ("lot_size", 8, 80, 38, 260302, "reston"),
    ("garage_spaces", 14, 98, 48, 260302, "reston"),
    ("property_tax", 23, 115, 65, 260302, "reston"),
    ("maintenance_cost", 11, 85, 41, 260302, "reston"),
    ("year_built", 25, 130, 81, 260302, "reston"),
    # Another month
    ("price", 12, 110, 38, 260401, "ashburn"),
    ("square_feet", 18, 100, 50, 260401, "ashburn"),
    ("bedrooms", 28, 155, 70, 260401, "ashburn"),
    ("bathrooms", 10, 90, 35, 260401, "ashburn"),
    ("age_years", 15, 115, 55, 260401, "ashburn"),
    ("lot_size", 7, 75, 32, 260401, "ashburn"),
    ("garage_spaces", 12, 92, 42, 260401, "ashburn"),
    ("property_tax", 20, 110, 58, 260401, "ashburn"),
    ("maintenance_cost", 9, 80, 35, 260401, "ashburn"),
    ("year_built", 22, 125, 75, 260401, "ashburn"),
]

columns = ["field", "min", "max", "average", "yymmdd", "place"]
df_raw = spark.createDataFrame(data, columns)

print("📊 Raw Data Sample:")
df_raw.show(5)

# ============================================================================
# TRANSFORM: PIVOT FROM LONG TO WIDE FORMAT
# ============================================================================
print("\n📊 Step 1: Stack columns to key-value pairs")

df_stacked = df_raw.selectExpr(
    "place",
    "yymmdd",
    "stack(3, "
    "  concat(field, '_min'), min, "
    "  concat(field, '_max'), max, "
    "  concat(field, '_average'), average"
    ") as (attribute, value)"
)

# Use debug_checkpoint to mark progress
df_stacked.debug_checkpoint("After stacking")

print("\n📊 Step 2: Pivot to wide format")
df_wide = df_stacked.groupBy("place", "yymmdd") \
    .pivot("attribute") \
    .agg(first("value"))

# Show debug summary
df_wide.debug_summary()

print("\n📊 Wide Format Result:")
df_wide.show(truncate=False)

# ============================================================================
# ADDITIONAL TRANSFORMATIONS (to show more instrumentation)
# ============================================================================
print("\n📊 Step 3: Additional transformations")

# Filter
df_filtered = df_wide.filter(col("price_average") > 35)

# Add derived column
df_with_ratio = df_filtered.withColumn(
    "price_per_sqft",
    col("price_average") / col("square_feet_average")
)

# Select specific columns
df_final = df_with_ratio.select(
    "place",
    "yymmdd",
    "price_average",
    "square_feet_average",
    "price_per_sqft"
)

print("\n📊 Final Result:")
df_final.show()

# ============================================================================
# WRITE TO PARQUET (to show write instrumentation)
# ============================================================================
print("\n📊 Step 4: Write to Parquet")
output_path = "/tmp/housing_wide_output"

df_final.write \
    .mode("overwrite") \
    .parquet(output_path)

print(f"✓ Written to {output_path}")

# ============================================================================
# PRINT FINAL STATISTICS
# ============================================================================
from spark_instrumentation import print_stats_summary
print("\n")
print_stats_summary()

# Cleanup
spark.stop()

print("\n✓ Demo complete! Check 'spark_debug.log' for full logs.")
