#!/bin/bash
# Script to submit customer segmentation pipeline to EMR
# Usage: ./submit_to_emr.sh <EMR_CLUSTER_ID>

# Check if cluster ID is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <EMR_CLUSTER_ID>"
    exit 1
fi

EMR_CLUSTER_ID=$1
SCRIPT_PATH="s3://my-bucket/scripts/customer_segmentation_pipeline.py"
DRIVER_MEMORY="4g"
EXECUTOR_MEMORY="8g"
NUM_EXECUTORS=10
EXECUTOR_CORES=4

# Upload the Python script to S3
echo "Uploading script to S3..."
aws s3 cp customer_segmentation_pipeline.py $SCRIPT_PATH

# Submit the job to EMR
echo "Submitting job to EMR cluster $EMR_CLUSTER_ID..."
aws emr add-steps \
    --cluster-id $EMR_CLUSTER_ID \
    --steps Type=Spark,Name="Customer Segmentation Pipeline",ActionOnFailure=CONTINUE,Args=[\
    --deploy-mode,cluster,\
    --driver-memory,$DRIVER_MEMORY,\
    --executor-memory,$EXECUTOR_MEMORY,\
    --num-executors,$NUM_EXECUTORS,\
    --executor-cores,$EXECUTOR_CORES,\
    --conf,spark.yarn.submit.waitAppCompletion=true,\
    --conf,spark.dynamicAllocation.enabled=false,\
    --py-files,s3://my-bucket/dependencies/src.zip,\
    $SCRIPT_PATH\
    ]

echo "Job submitted. Check EMR console for status."