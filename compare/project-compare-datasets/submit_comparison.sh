#!/bin/bash

# EMR Parquet Comparison Job Submission Script
# Usage: ./submit_comparison.sh

# Configuration
SCRIPT_PATH="s3://your-bucket/scripts/parquet_comparison.py"
PATH1="s3://your-bucket/data/file1.parquet"
PATH2="s3://your-bucket/data/file2.parquet"
KEY_COLUMNS="id,date,customer_id"  # Comma-separated key columns
SKIP_COLUMNS="timestamp,updated_at"  # Comma-separated columns to skip (optional)
OUTPUT_PATH="s3://your-bucket/reports/comparison_report.html"
DATASET1_NAME="Production"
DATASET2_NAME="Staging"

# EMR Configuration
CLUSTER_ID="j-XXXXXXXXXXXXX"  # Your EMR cluster ID
NUM_EXECUTORS=20
EXECUTOR_CORES=4
EXECUTOR_MEMORY="8g"
DRIVER_MEMORY="8g"
SHUFFLE_PARTITIONS=400  # Adjust based on data size

echo "=================================="
echo "EMR Parquet Comparison Job"
echo "=================================="
echo "Cluster ID: $CLUSTER_ID"
echo "Dataset 1: $PATH1"
echo "Dataset 2: $PATH2"
echo "Key Columns: $KEY_COLUMNS"
echo "Output: $OUTPUT_PATH"
echo "=================================="

# Submit Spark job to EMR
aws emr add-steps \
  --cluster-id $CLUSTER_ID \
  --steps Type=Spark,Name="Parquet-Comparison",ActionOnFailure=CONTINUE,Args=[
--deploy-mode,cluster,
--master,yarn,
--conf,spark.executor.instances=$NUM_EXECUTORS,
--conf,spark.executor.cores=$EXECUTOR_CORES,
--conf,spark.executor.memory=$EXECUTOR_MEMORY,
--conf,spark.driver.memory=$DRIVER_MEMORY,
--conf,spark.sql.shuffle.partitions=$SHUFFLE_PARTITIONS,
--conf,spark.sql.adaptive.enabled=true,
--conf,spark.sql.adaptive.coalescePartitions.enabled=true,
--conf,spark.sql.adaptive.skewJoin.enabled=true,
--conf,spark.dynamicAllocation.enabled=false,
--conf,spark.network.timeout=800s,
--conf,spark.executor.heartbeatInterval=60s,
$SCRIPT_PATH,
--path1,$PATH1,
--path2,$PATH2,
--keys,$KEY_COLUMNS,
--skip,$SKIP_COLUMNS,
--name1,$DATASET1_NAME,
--name2,$DATASET2_NAME,
--output,$OUTPUT_PATH,
--partitions,$SHUFFLE_PARTITIONS
]

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Job submitted successfully!"
    echo ""
    echo "Monitor your job:"
    echo "  aws emr describe-step --cluster-id $CLUSTER_ID --step-id <step-id>"
    echo ""
    echo "View logs:"
    echo "  aws emr ssh --cluster-id $CLUSTER_ID --key-pair-file your-key.pem"
    echo "  yarn logs -applicationId <app-id>"
    echo ""
    echo "Once complete, download report:"
    echo "  aws s3 cp $OUTPUT_PATH ./comparison_report.html"
else
    echo "❌ Job submission failed!"
    exit 1
fi
