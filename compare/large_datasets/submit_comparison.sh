#!/bin/bash

################################################################################
# EMR Spark Job Submission Script for Dataset Comparison
# Usage: ./submit_comparison.sh
################################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}Dataset Comparison - EMR Submit${NC}"
echo -e "${GREEN}================================${NC}\n"

# ============================================================================
# CONFIGURATION - Edit these values
# ============================================================================

# EMR Cluster ID (or 'create' to create new cluster)
CLUSTER_ID="j-XXXXXXXXXXXXX"  # Replace with your cluster ID or use 'create'

# S3 paths
SCRIPT_S3_PATH="s3://your-bucket/scripts/parquet_dataset_compare.py"
OUTPUT_S3_PATH="s3://your-bucket/comparison-output/"

# Spark configuration
DRIVER_MEMORY="8g"
EXECUTOR_MEMORY="16g"
EXECUTOR_CORES="4"
NUM_EXECUTORS="10"

# Optional: Dynamic allocation settings
USE_DYNAMIC_ALLOCATION="true"
MIN_EXECUTORS="5"
MAX_EXECUTORS="50"

# ============================================================================

# Function to upload script to S3
upload_script() {
    echo -e "${YELLOW}Uploading script to S3...${NC}"
    aws s3 cp parquet_dataset_compare.py "$SCRIPT_S3_PATH"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Script uploaded successfully${NC}\n"
    else
        echo -e "${RED}✗ Failed to upload script${NC}"
        exit 1
    fi
}

# Function to create EMR cluster
create_cluster() {
    echo -e "${YELLOW}Creating new EMR cluster...${NC}"
    
    CLUSTER_ID=$(aws emr create-cluster \
        --name "Dataset-Comparison-$(date +%Y%m%d-%H%M%S)" \
        --release-label emr-6.15.0 \
        --applications Name=Spark Name=Hadoop \
        --instance-type r5.4xlarge \
        --instance-count 11 \
        --use-default-roles \
        --ec2-attributes KeyName=your-keypair \
        --configurations '[
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.dynamicAllocation.minExecutors": "5",
                    "spark.dynamicAllocation.maxExecutors": "50",
                    "spark.sql.shuffle.partitions": "200",
                    "spark.network.timeout": "800s"
                }
            }
        ]' \
        --query 'ClusterId' \
        --output text)
    
    if [ -z "$CLUSTER_ID" ]; then
        echo -e "${RED}✗ Failed to create cluster${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Cluster created: $CLUSTER_ID${NC}"
    echo -e "${YELLOW}Waiting for cluster to be ready...${NC}"
    
    aws emr wait cluster-running --cluster-id "$CLUSTER_ID"
    echo -e "${GREEN}✓ Cluster is ready${NC}\n"
}

# Function to submit Spark job
submit_job() {
    echo -e "${YELLOW}Submitting Spark job to cluster: $CLUSTER_ID${NC}"
    
    # Build Spark configuration
    SPARK_ARGS="--master yarn --deploy-mode cluster"
    SPARK_ARGS="$SPARK_ARGS --driver-memory $DRIVER_MEMORY"
    SPARK_ARGS="$SPARK_ARGS --executor-memory $EXECUTOR_MEMORY"
    SPARK_ARGS="$SPARK_ARGS --executor-cores $EXECUTOR_CORES"
    
    if [ "$USE_DYNAMIC_ALLOCATION" = "true" ]; then
        SPARK_ARGS="$SPARK_ARGS --conf spark.dynamicAllocation.enabled=true"
        SPARK_ARGS="$SPARK_ARGS --conf spark.dynamicAllocation.minExecutors=$MIN_EXECUTORS"
        SPARK_ARGS="$SPARK_ARGS --conf spark.dynamicAllocation.maxExecutors=$MAX_EXECUTORS"
    else
        SPARK_ARGS="$SPARK_ARGS --num-executors $NUM_EXECUTORS"
    fi
    
    # Additional Spark configurations
    SPARK_ARGS="$SPARK_ARGS --conf spark.sql.adaptive.enabled=true"
    SPARK_ARGS="$SPARK_ARGS --conf spark.sql.adaptive.coalescePartitions.enabled=true"
    SPARK_ARGS="$SPARK_ARGS --conf spark.sql.shuffle.partitions=200"
    SPARK_ARGS="$SPARK_ARGS --conf spark.sql.autoBroadcastJoinThreshold=-1"
    SPARK_ARGS="$SPARK_ARGS --conf spark.sql.join.preferSortMergeJoin=true"
    SPARK_ARGS="$SPARK_ARGS --conf spark.network.timeout=800s"
    SPARK_ARGS="$SPARK_ARGS --conf spark.executor.heartbeatInterval=60s"
    
    # Submit the step
    STEP_ID=$(aws emr add-steps \
        --cluster-id "$CLUSTER_ID" \
        --steps Type=Spark,Name="Dataset-Comparison",ActionOnFailure=CONTINUE,Args=[$SPARK_ARGS,$SCRIPT_S3_PATH] \
        --query 'StepIds[0]' \
        --output text)
    
    if [ -z "$STEP_ID" ]; then
        echo -e "${RED}✗ Failed to submit job${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Job submitted successfully${NC}"
    echo -e "${GREEN}Step ID: $STEP_ID${NC}\n"
    
    # Monitor step
    monitor_step "$STEP_ID"
}

# Function to monitor step progress
monitor_step() {
    local step_id=$1
    echo -e "${YELLOW}Monitoring job progress...${NC}"
    echo -e "You can also check progress in EMR Console:\n"
    echo -e "${GREEN}https://console.aws.amazon.com/elasticmapreduce/home#cluster-details:$CLUSTER_ID${NC}\n"
    
    while true; do
        STATUS=$(aws emr describe-step \
            --cluster-id "$CLUSTER_ID" \
            --step-id "$step_id" \
            --query 'Step.Status.State' \
            --output text)
        
        case "$STATUS" in
            PENDING)
                echo -e "${YELLOW}⏳ Status: PENDING${NC}"
                ;;
            RUNNING)
                echo -e "${YELLOW}▶️  Status: RUNNING${NC}"
                ;;
            COMPLETED)
                echo -e "${GREEN}✓ Job completed successfully!${NC}"
                echo -e "\n${GREEN}Results available at: $OUTPUT_S3_PATH${NC}"
                break
                ;;
            FAILED|CANCELLED)
                echo -e "${RED}✗ Job failed or was cancelled${NC}"
                echo -e "${YELLOW}Fetching logs...${NC}"
                get_logs "$step_id"
                exit 1
                ;;
        esac
        
        sleep 30
    done
}

# Function to get logs on failure
get_logs() {
    local step_id=$1
    echo -e "\n${YELLOW}Error details:${NC}"
    aws emr describe-step \
        --cluster-id "$CLUSTER_ID" \
        --step-id "$step_id" \
        --query 'Step.Status.FailureDetails' \
        --output text
}

# Function to download results
download_results() {
    echo -e "\n${YELLOW}Would you like to download the HTML report? (y/n)${NC}"
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Downloading report...${NC}"
        aws s3 cp "${OUTPUT_S3_PATH}comparison_report.html" ./comparison_report.html
        echo -e "${GREEN}✓ Report downloaded: ./comparison_report.html${NC}"
        
        # Try to open in browser (macOS/Linux)
        if [[ "$OSTYPE" == "darwin"* ]]; then
            open ./comparison_report.html
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            xdg-open ./comparison_report.html 2>/dev/null || echo "Open ./comparison_report.html in your browser"
        fi
    fi
}

# Main execution
main() {
    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        echo -e "${RED}✗ AWS CLI not found. Please install it first.${NC}"
        exit 1
    fi
    
    # Check if script exists
    if [ ! -f "parquet_dataset_compare.py" ]; then
        echo -e "${RED}✗ Script file not found: parquet_dataset_compare.py${NC}"
        exit 1
    fi
    
    # Upload script
    upload_script
    
    # Create cluster if needed
    if [ "$CLUSTER_ID" = "create" ]; then
        create_cluster
    else
        echo -e "${GREEN}Using existing cluster: $CLUSTER_ID${NC}\n"
    fi
    
    # Submit job
    submit_job
    
    # Download results
    download_results
    
    echo -e "\n${GREEN}================================${NC}"
    echo -e "${GREEN}Process Complete!${NC}"
    echo -e "${GREEN}================================${NC}"
}

# Run main function
main
