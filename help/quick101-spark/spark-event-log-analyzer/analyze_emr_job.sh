#!/bin/bash
###############################################################################
# EMR Spark Job Analyzer - Automated Script
# This script fetches Spark event logs from EMR and runs the optimizer
###############################################################################

set -e

# Configuration
EMR_MASTER_DNS="${EMR_MASTER_DNS:-}"
S3_BUCKET="${S3_BUCKET:-}"
OUTPUT_DIR="./spark-analysis"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function usage() {
    cat << EOF
Usage: $0 [OPTIONS] APPLICATION_ID

Analyze Spark application on AWS EMR and generate optimization recommendations.

OPTIONS:
    -m, --master DNS        EMR master node DNS (optional if on master node)
    -s, --s3-bucket BUCKET  S3 bucket for storing results
    -k, --key-file PATH     SSH key file for EMR access
    -l, --local-log PATH    Use local event log file instead of fetching
    -h, --help             Show this help message

EXAMPLES:
    # Run on EMR master node
    $0 application_1234567890_0001

    # Run from local machine
    $0 -m ec2-xx-xx-xx-xx.compute.amazonaws.com -k ~/my-key.pem application_1234567890_0001

    # Analyze local log file
    $0 -l /path/to/event-log application_1234567890_0001

    # Save results to S3
    $0 -s my-results-bucket application_1234567890_0001

EOF
    exit 1
}

# Parse arguments
APP_ID=""
KEY_FILE=""
LOCAL_LOG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--master)
            EMR_MASTER_DNS="$2"
            shift 2
            ;;
        -s|--s3-bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        -k|--key-file)
            KEY_FILE="$2"
            shift 2
            ;;
        -l|--local-log)
            LOCAL_LOG="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            APP_ID="$1"
            shift
            ;;
    esac
done

if [ -z "$APP_ID" ]; then
    echo -e "${RED}Error: APPLICATION_ID is required${NC}"
    usage
fi

echo -e "${GREEN}=== EMR Spark Job Analyzer ===${NC}"
echo "Application ID: $APP_ID"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

###############################################################################
# Function: Fetch event log
###############################################################################
function fetch_event_log() {
    local app_id=$1
    local output_path="$OUTPUT_DIR/$app_id"
    
    if [ -n "$LOCAL_LOG" ]; then
        echo -e "${YELLOW}Using local log file: $LOCAL_LOG${NC}"
        cp "$LOCAL_LOG" "$output_path"
        return 0
    fi
    
    echo -e "${YELLOW}Fetching event log from HDFS...${NC}"
    
    # Check if we're on EMR master node
    if command -v hdfs &> /dev/null && [ -z "$EMR_MASTER_DNS" ]; then
        echo "Running on EMR master node"
        hdfs dfs -get "/var/log/spark/apps/$app_id" "$output_path" 2>/dev/null || {
            echo -e "${RED}Failed to fetch from HDFS. Trying alternative location...${NC}"
            hdfs dfs -get "/tmp/spark-events/$app_id" "$output_path" || {
                echo -e "${RED}Error: Could not find event log in HDFS${NC}"
                return 1
            }
        }
    elif [ -n "$EMR_MASTER_DNS" ] && [ -n "$KEY_FILE" ]; then
        echo "Fetching from remote EMR cluster: $EMR_MASTER_DNS"
        ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no hadoop@"$EMR_MASTER_DNS" \
            "hdfs dfs -get /var/log/spark/apps/$app_id /tmp/$app_id 2>/dev/null || hdfs dfs -get /tmp/spark-events/$app_id /tmp/$app_id" || {
            echo -e "${RED}Error: Could not fetch event log from EMR${NC}"
            return 1
        }
        scp -i "$KEY_FILE" -o StrictHostKeyChecking=no \
            hadoop@"$EMR_MASTER_DNS":/tmp/"$app_id" "$output_path"
        ssh -i "$KEY_FILE" hadoop@"$EMR_MASTER_DNS" "rm /tmp/$app_id"
    else
        echo -e "${RED}Error: Either run on EMR master node or provide -m and -k options${NC}"
        return 1
    fi
    
    echo -e "${GREEN}✓ Event log fetched successfully${NC}"
    return 0
}

###############################################################################
# Function: Run optimizer
###############################################################################
function run_optimizer() {
    local app_id=$1
    local log_file="$OUTPUT_DIR/$app_id"
    local output_json="$OUTPUT_DIR/${app_id}_recommendations.json"
    
    echo -e "${YELLOW}Running Spark optimizer...${NC}"
    
    if [ ! -f "spark_optimizer.py" ]; then
        echo -e "${RED}Error: spark_optimizer.py not found${NC}"
        return 1
    fi
    
    python3 spark_optimizer.py "$log_file" \
        --app-id "$app_id" \
        --output "$output_json" || {
        echo -e "${RED}Error: Optimizer failed${NC}"
        return 1
    }
    
    echo -e "${GREEN}✓ Analysis complete${NC}"
    echo "Results saved to: $output_json"
    return 0
}

###############################################################################
# Function: Upload results to S3
###############################################################################
function upload_to_s3() {
    local app_id=$1
    
    if [ -z "$S3_BUCKET" ]; then
        return 0
    fi
    
    echo -e "${YELLOW}Uploading results to S3...${NC}"
    
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local s3_path="s3://$S3_BUCKET/spark-analysis/$app_id/$timestamp/"
    
    aws s3 cp "$OUTPUT_DIR/${app_id}_recommendations.json" "$s3_path" || {
        echo -e "${RED}Warning: Failed to upload to S3${NC}"
        return 1
    }
    
    echo -e "${GREEN}✓ Results uploaded to: $s3_path${NC}"
    return 0
}

###############################################################################
# Function: Generate summary report
###############################################################################
function generate_summary() {
    local app_id=$1
    local json_file="$OUTPUT_DIR/${app_id}_recommendations.json"
    
    if [ ! -f "$json_file" ]; then
        return 1
    fi
    
    echo ""
    echo -e "${GREEN}=== Quick Summary ===${NC}"
    
    # Extract key metrics using jq if available
    if command -v jq &> /dev/null; then
        echo "Job Duration: $(jq -r '.metrics.job_duration' "$json_file") seconds"
        echo "Total Tasks: $(jq -r '.metrics.total_tasks' "$json_file")"
        echo "Failed Tasks: $(jq -r '.metrics.failed_tasks' "$json_file")"
        echo "Memory Spilled: $(jq -r '.metrics.memory_spilled_gb' "$json_file") GB"
        echo "Disk Spilled: $(jq -r '.metrics.disk_spilled_gb' "$json_file") GB"
        echo "Severity: $(jq -r '.recommendations.severity' "$json_file")"
        
        echo ""
        echo "Top Recommendations:"
        jq -r '.recommendations.code_suggestions[] | "  • " + .' "$json_file" 2>/dev/null | head -5
    else
        echo "Install 'jq' for formatted summary output"
        echo "Raw JSON available at: $json_file"
    fi
}

###############################################################################
# Main execution
###############################################################################
echo "Step 1: Fetching event log..."
fetch_event_log "$APP_ID" || exit 1

echo ""
echo "Step 2: Running analysis..."
run_optimizer "$APP_ID" || exit 1

echo ""
echo "Step 3: Generating summary..."
generate_summary "$APP_ID"

if [ -n "$S3_BUCKET" ]; then
    echo ""
    echo "Step 4: Uploading to S3..."
    upload_to_s3 "$APP_ID"
fi

echo ""
echo -e "${GREEN}=== Analysis Complete ===${NC}"
echo "Full report: $OUTPUT_DIR/${APP_ID}_recommendations.json"
echo ""

###############################################################################
# Additional: Generate Spark config file
###############################################################################
cat > "$OUTPUT_DIR/${APP_ID}_spark_configs.conf" << 'EOF'
# Recommended Spark Configurations
# Generated by EMR Spark Optimizer
# Application ID: 
EOF

echo "# Application ID: $APP_ID" >> "$OUTPUT_DIR/${APP_ID}_spark_configs.conf"
echo "# Generated: $(date)" >> "$OUTPUT_DIR/${APP_ID}_spark_configs.conf"
echo "" >> "$OUTPUT_DIR/${APP_ID}_spark_configs.conf"

if command -v jq &> /dev/null; then
    jq -r '.recommendations.spark_configs | to_entries[] | "\(.key)=\(.value)"' \
        "$OUTPUT_DIR/${APP_ID}_recommendations.json" >> "$OUTPUT_DIR/${APP_ID}_spark_configs.conf"
    
    echo -e "${GREEN}Spark config file created: $OUTPUT_DIR/${APP_ID}_spark_configs.conf${NC}"
    echo ""
    echo "To apply these configs, add to your spark-submit:"
    echo "  spark-submit --properties-file $OUTPUT_DIR/${APP_ID}_spark_configs.conf ..."
fi

echo ""
echo -e "${GREEN}Done!${NC}"
