# Compares two large Parquet datasets using row hash

We do not know the primary keys for the dataset

- compares two large Parquet datasets by row hash**
- creates an HTML report** using comments for clarity
- writes the HTML report to S3 using DataFrame.write()


```python
# 1. Import modules and initialize Spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, md5

# 2. Start Spark session
spark = SparkSession.builder.appName("DatasetCompare").getOrCreate()

# 3. Read Parquet datasets from S3
df1 = spark.read.parquet("s3://bucket/path/to/dataset1.parquet")
df2 = spark.read.parquet("s3://bucket/path/to/dataset2.parquet")

# 4. Add row_hash columns using MD5 hash of concatenated columns
df1_hashed = df1.withColumn("row_hash", md5(concat_ws("||", *df1.columns)))
df2_hashed = df2.withColumn("row_hash", md5(concat_ws("||", *df2.columns)))

# 5. Find hashes unique to each DataFrame
diff_hashes_1_not_2 = df1_hashed.select("row_hash").subtract(df2_hashed.select("row_hash"))
diff_hashes_2_not_1 = df2_hashed.select("row_hash").subtract(df1_hashed.select("row_hash"))

# 6. Sample differing rows (limit 10 for report)
diff_rows_df1 = df1_hashed.join(diff_hashes_1_not_2, "row_hash", "inner").limit(10)
diff_rows_df2 = df2_hashed.join(diff_hashes_2_not_1, "row_hash", "inner").limit(10)

# 7. Collect sample rows to Pandas for easy HTML export
pd_diff_rows_df1 = diff_rows_df1.drop("row_hash").toPandas()
pd_diff_rows_df2 = diff_rows_df2.drop("row_hash").toPandas()

# 8. Get summary counts
n_1_not_2 = diff_hashes_1_not_2.count()
n_2_not_1 = diff_hashes_2_not_1.count()

# 9. Build HTML report as a string
html_report = f"""
<html>
<head><title>Parquet Dataset Comparison Report</title></head>
<body>
<h2>Summary</h2>
<ul>
    <li>Rows in Dataset 1 but not in Dataset 2: <b>{n_1_not_2}</b></li>
    <li>Rows in Dataset 2 but not in Dataset 1: <b>{n_2_not_1}</b></li>
</ul>
<h2>Sample Differences (Dataset 1 only)</h2>
{pd_diff_rows_df1.to_html(index=False, max_rows=10)}
<h2>Sample Differences (Dataset 2 only)</h2>
{pd_diff_rows_df2.to_html(index=False, max_rows=10)}
</body>
</html>
"""

# 10. Convert the HTML string to Spark DataFrame for writing to S3
from pyspark.sql import Row

html_df = spark.createDataFrame([Row(report=html_report)])

# 11. Write as text to S3 bucket
# IMPORTANT: This writes one file, each containing the HTML report string
html_df.write.mode("overwrite").text("s3://your-bucket/comparison-report/compare_report.html")

# -- End of script
```

