from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, from_json, sha2, concat_ws
from pyspark.sql.types import StringType

"""
If candidate_keys is empty or not specified, all columns except skipped ones are 
concatenated and hashed row-wise for comparison.
If candidate_keys are provided, a join on the keys is performed with column-wise 
comparison for differences.
JSON columns are parsed and normalized to strings before comparison in both cases.
"""

spark = SparkSession.builder.appName("ParquetCompare").getOrCreate()

file1_path = "s3://bucket/path/file1.parquet"
file2_path = "s3://bucket/path/file2.parquet"

skip_columns = ["col_to_skip_1", "col_to_skip_2"]
json_columns = ["json_col_1", "json_col_2"]

# Candidate keys - provide empty list or None to use row hashing instead
candidate_keys = []  # Example: ["id", "timestamp"]

df1 = spark.read.parquet(file1_path)
df2 = spark.read.parquet(file2_path)

# Select columns excluding skipped
compare_columns = [c for c in df1.columns if c not in skip_columns]

# Handle JSON parsing to string normalization for comparison
for jc in json_columns:
    df1 = df1.withColumn(jc, from_json(col(jc), StringType()))
    df2 = df2.withColumn(jc, from_json(col(jc), StringType()))

if candidate_keys and len(candidate_keys) > 0:
    # Key-based join comparison
    cols_to_compare = [c for c in compare_columns if c not in candidate_keys]

    joined_df = df1.alias("df1").join(df2.alias("df2"), candidate_keys, how="full_outer")

    for c in cols_to_compare:
        joined_df = joined_df.withColumn(
            f"{c}_diff",
            when(col(f"df1.{c}") != col(f"df2.{c}"), lit(True)).otherwise(lit(False))
        )

    diff_df = joined_df.filter(" or ".join([f"{c}_diff" for c in cols_to_compare]))

else:
    # Use row hashing for full row comparison when no keys available
    df1 = df1.withColumn("row_hash", sha2(concat_ws("||", *compare_columns), 256))
    df2 = df2.withColumn("row_hash", sha2(concat_ws("||", *compare_columns), 256))

    diff_df = df1.alias("df1").join(df2.alias("df2"), on="row_hash", how="full_outer") \
        .filter(col("df1.row_hash").isNull() | col("df2.row_hash").isNull())

# Save difference report
diff_df.write.mode("overwrite").parquet("s3://bucket/path/diff_report/")

spark.stop()
