#!/bin/bash

################################################################################
# Dataset Comparison Runner with YAML Configuration
# Usage: ./run_comparison.sh [config_file] [options]
################################################################################

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Default values
CONFIG_FILE="compare_config.yaml"
CLUSTER_ID=""
UPLOAD_SCRIPT=true
LOCAL_MODE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --cluster)
            CLUSTER_ID="$2"
            shift 2
            ;;
        --local)
            LOCAL_MODE=true
            shift
            ;;
        --no-upload)
            UPLOAD_SCRIPT=false
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -c, --config FILE     Config file (default: compare_config.yaml)"
            echo "  --cluster ID          EMR cluster ID (required for cluster mode)"
            echo "  --local               Run in local mode"
            echo "  --no-upload           Don't upload script (assume already uploaded)"
            echo "  -h, --help            Show this help"
            echo ""
            echo "Examples:"
            echo "  # Run locally"
            echo "  $0 --local --config my_comparison.yaml"
            echo ""
            echo "  # Run on EMR cluster"
            echo "  $0 --config production.yaml --cluster j-XXXXX"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Validate config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}❌ Config file not found: $CONFIG_FILE${NC}"
    exit 1
fi

echo -e "${GREEN}================================${NC}"
echo -e "${GREEN}Dataset Comparison Runner${NC}"
echo -e "${GREEN}================================${NC}"
echo -e "Config file: ${YELLOW}$CONFIG_FILE${NC}"

# Parse output path from YAML (for S3 uploads)
OUTPUT_PATH=$(grep "output_path:" "$CONFIG_FILE" | head -1 | sed 's/.*: *"\(.*\)".*/\1/')
SCRIPT_S3_PATH="${OUTPUT_PATH%/}/scripts/optimized_compare.py"
CONFIG_S3_PATH="${OUTPUT_PATH%/}/config/$(basename $CONFIG_FILE)"

echo -e "Output path: ${YELLOW}$OUTPUT_PATH${NC}"

# Function to run locally
run_local() {
    echo -e "\n${YELLOW}Running in local mode...${NC}"
    
    # Check if Spark is available
    if ! command -v spark-submit &> /dev/null; then
        echo -e "${RED}❌ spark-submit not found. Please install PySpark.${NC}"
        exit 1
    fi
    
    spark-submit \
        --master local[*] \
        --driver-memory 4g \
        optimized_compare.py \
        --config "$CONFIG_FILE"
}

# Function to upload files to S3
upload_files() {
    echo -e "\n${YELLOW}Uploading files to S3...${NC}"
    
    # Upload script
    echo -e "  Uploading script..."
    aws s3 cp optimized_compare.py "$SCRIPT_S3_PATH"
    
    # Upload config
    echo -e "  Uploading config..."
    aws s3 cp "$CONFIG_FILE" "$CONFIG_S3_PATH"
    
    echo -e "${GREEN}✓ Files uploaded${NC}"
}

# Function to run on EMR
run_emr() {
    if [ -z "$CLUSTER_ID" ]; then
        echo -e "${RED}❌ Cluster ID required for EMR mode${NC}"
        echo -e "   Use: $0 --cluster j-XXXXX --config $CONFIG_FILE"
        exit 1
    fi
    
    echo -e "\n${YELLOW}Running on EMR cluster: $CLUSTER_ID${NC}"
    
    # Upload files if needed
    if [ "$UPLOAD_SCRIPT" = true ]; then
        upload_files
    fi
    
    # Submit Spark step
    echo -e "\n${YELLOW}Submitting Spark job...${NC}"
    
    STEP_ID=$(aws emr add-steps \
        --cluster-id "$CLUSTER_ID" \
        --steps Type=Spark,Name="DatasetComparison-$(date +%Y%m%d-%H%M%S)",ActionOnFailure=CONTINUE,Args=[\
--master,yarn,\
--deploy-mode,cluster,\
--driver-memory,8g,\
--executor-memory,16g,\
--executor-cores,4,\
--num-executors,10,\
--conf,spark.sql.adaptive.enabled=true,\
--conf,spark.sql.shuffle.partitions=200,\
$SCRIPT_S3_PATH,\
--config,$CONFIG_S3_PATH\
] \
        --query 'StepIds[0]' \
        --output text)
    
    if [ -z "$STEP_ID" ]; then
        echo -e "${RED}✗ Failed to submit job${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Job submitted successfully${NC}"
    echo -e "${GREEN}Step ID: $STEP_ID${NC}"
    echo -e "\n${YELLOW}Monitor progress:${NC}"
    echo -e "https://console.aws.amazon.com/elasticmapreduce/home#cluster-details:$CLUSTER_ID"
    
    # Optional: Monitor step
    echo -e "\n${YELLOW}Monitoring job (Ctrl+C to stop monitoring)...${NC}"
    monitor_step "$STEP_ID"
}

# Function to monitor step
monitor_step() {
    local step_id=$1
    
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
                echo -e "\n${GREEN}Results available at: $OUTPUT_PATH${NC}"
                break
                ;;
            FAILED|CANCELLED)
                echo -e "${RED}✗ Job failed or was cancelled${NC}"
                echo -e "\n${YELLOW}Error details:${NC}"
                aws emr describe-step \
                    --cluster-id "$CLUSTER_ID" \
                    --step-id "$step_id" \
                    --query 'Step.Status.FailureDetails' \
                    --output text
                exit 1
                ;;
        esac
        
        sleep 30
    done
}

# Main execution
if [ "$LOCAL_MODE" = true ]; then
    run_local
else
    run_emr
fi

echo -e "\n${GREEN}================================${NC}"
echo -e "${GREEN}Complete!${NC}"
echo -e "${GREEN}================================${NC}"
