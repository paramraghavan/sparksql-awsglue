```bash
pip3 install snowflake-connector-python pandas pyarrow configparser tqdm

# Upload the PySpark script
aws s3 cp pyspark_comparison.py s3://your-bucket/scripts/pyspark_comparison.py

# Upload the config file with Snowflake credentials
aws s3 cp config.ini s3://your-bucket/configs/config.ini
```

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1 \
  --conf spark.executor.memory=20g \
  --conf spark.driver.memory=10g \
  --conf spark.executor.cores=4 \
  --conf spark.sql.shuffle.partitions=400 \
  s3://your-bucket/scripts/pyspark_comparison.py \
  --config config.ini \
  --file-path s3://your-data-bucket/your-file.dat \
  --file-type dat \
  --delimiter "|" \
  --key-columns "ID,DATE" \
  --output-dir s3://your-output-bucket/comparison_results/
```

# Running on master node

- Connect to the EMR master node

```bash
ssh -i your-key.pem hadoop@your-emr-master-dns
```

## Submit the Spark job

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1 \
  --conf spark.executor.memory=20g \
  --conf spark.driver.memory=10g \
  --conf spark.executor.cores=4 \
  --conf spark.sql.shuffle.partitions=400 \
  s3://your-bucket/scripts/pyspark_comparison.py \
  --config s3://your-bucket/configs/config.ini \
  --file-path s3://your-data-bucket/your-file.dat \
  --file-type dat \
  --delimiter "|" \
  --key-columns "ID,DATE" \
  --output-dir s3://your-output-bucket/comparison_results/
```

# Without sshing into EMR master node

```bash
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=Spark,Name="Snowflake Comparison",ActionOnFailure=CONTINUE,Args=[ \
    "--deploy-mode","cluster", \
    "--packages","net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.1", \
    "--conf","spark.executor.memory=20g", \
    "--conf","spark.driver.memory=10g", \
    "--conf","spark.executor.cores=4", \
    "--conf","spark.sql.shuffle.partitions=400", \
    "s3://your-bucket/scripts/pyspark_comparison.py", \
    "--config","s3://your-bucket/configs/config.ini", \
    "--file-path","s3://your-data-bucket/your-file.dat", \
    "--file-type","dat", \
    "--delimiter","|", \
    "--key-columns","ID,DATE", \
    "--output-dir","s3://your-output-bucket/comparison_results/" \
  ]
```

# Now the snowflake table copied using snowflake share into s3 bucket what changes do ineed to read from s3

If your Snowflake data has already been copied to an S3 bucket, that simplifies the process - you can directly read both
datasets from S3 instead of connecting to Snowflake. Here's how to modify the approach:

I've created a streamlined solution that compares your DAT file with Snowflake data that's already been copied to S3.
This approach is more efficient since both datasets are in S3, eliminating the need for Snowflake connection
credentials.

## Key Changes from Previous Solution

1. **Direct S3 to S3 Comparison**:
    - Reads both the DAT file and Snowflake data directly from S3
    - No need for Snowflake credentials or connection
    - Faster processing as data stays within AWS ecosystem

2. **Simplified Configuration**:
    - No config.ini file needed
    - All parameters passed directly to spark-submit
    - Specify paths to both files in S3

3. **Automatic Column Mapping**:
    - Uses the column names from the Snowflake data for the headerless DAT file
    - Ensures consistent comparison between the datasets

## How to Run the Job

You can run this on your existing EMR cluster using this simplified spark-submit command:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.memory=20g \
  --conf spark.driver.memory=10g \
  --conf spark.executor.cores=4 \
  --conf spark.sql.shuffle.partitions=400 \
  s3://your-bucket/scripts/pyspark_s3_comparison.py \
  --dat-file s3://your-bucket/your-file.dat \
  --snowflake-data s3://your-snowflake-bucket/exported-data/ \
  --file-type dat \
  --snowflake-format parquet \
  --delimiter "|" \
  --key-columns "ID,DATE" \
  --output-dir s3://your-output-bucket/comparison_results/
```

## Parameter Explanation

- `--dat-file`: Path to your 25GB DAT file in S3
- `--snowflake-data`: Path to the exported Snowflake data in S3
- `--file-type`: Format of your source file (dat, csv, or parquet)
- `--snowflake-format`: Format of the Snowflake data in S3 (typically parquet)
- `--delimiter`: Field separator in your DAT file
- `--key-columns`: Columns that uniquely identify each record
- `--output-dir`: Where to save comparison results

## Output and Analysis

The script produces the same comprehensive output as before:

1. Records only in the DAT file
2. Records only in the Snowflake data
3. Detailed column-by-column differences
4. Summary of which columns have the most differences
5. Overall comparison report







