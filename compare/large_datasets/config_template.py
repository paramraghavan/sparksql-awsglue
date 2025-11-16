"""
Configuration file for Dataset Comparison
Edit this file instead of modifying the main script
"""

# ============================================================================
# BASIC CONFIGURATION
# ============================================================================

# S3 paths to your Parquet datasets
DS1_PATH = "s3://your-bucket/path/to/dataset1/"
DS2_PATH = "s3://your-bucket/path/to/dataset2/"

# Output path for comparison results
OUTPUT_PATH = "s3://your-bucket/comparison-output/"

# Key columns for joining (must exist in both datasets)
# Can be a single column or multiple columns
KEY_COLUMNS = ["id"]  
# Examples:
# KEY_COLUMNS = ["customer_id"]
# KEY_COLUMNS = ["customer_id", "transaction_date"]
# KEY_COLUMNS = ["id", "region", "date"]

# Columns to skip from comparison (optional)
# Use this for columns that are expected to differ (timestamps, ETL metadata, etc.)
SKIP_COLUMNS = []
# Examples:
# SKIP_COLUMNS = ["updated_at", "processed_timestamp", "etl_run_id"]
# SKIP_COLUMNS = ["last_modified", "version", "load_date"]

# ============================================================================
# ADVANCED CONFIGURATION
# ============================================================================

# Date filtering (optional) - compare only data within date range
# Set to None to compare all data
USE_DATE_FILTER = False
DATE_COLUMN = "date"  # Column name for date filtering
START_DATE = "2025-01-01"
END_DATE = "2025-01-31"

# Column filtering (optional) - compare only specific columns
# Set to None to compare all columns (except those in SKIP_COLUMNS)
COMPARE_SPECIFIC_COLUMNS_ONLY = False
COLUMNS_TO_COMPARE = ["amount", "status", "quantity"]

# Sampling (optional) - use for quick validation
# Set to 1.0 for full dataset, 0.1 for 10% sample
USE_SAMPLING = False
SAMPLE_FRACTION = 0.1  # 10% sample

# Numeric tolerance (optional) - allow small differences in numeric columns
# Useful for comparing financial data with rounding differences
USE_NUMERIC_TOLERANCE = False
NUMERIC_COLUMNS = ["amount", "price", "total"]
TOLERANCE = 0.01  # Allow differences up to 0.01

# ============================================================================
# SPARK CONFIGURATION
# ============================================================================

# Basic Spark settings
SPARK_APP_NAME = "DatasetComparison"
DRIVER_MEMORY = "8g"
EXECUTOR_MEMORY = "16g"
EXECUTOR_CORES = 4

# Shuffle partitions - increase for larger datasets
# 200 is good for 50GB, use 400+ for 100GB+
SHUFFLE_PARTITIONS = 200

# Dynamic allocation
USE_DYNAMIC_ALLOCATION = True
MIN_EXECUTORS = 5
MAX_EXECUTORS = 50

# For very large datasets (100GB+)
# SHUFFLE_PARTITIONS = 400
# EXECUTOR_MEMORY = "32g"
# MAX_EXECUTORS = 100

# ============================================================================
# OUTPUT CONFIGURATION
# ============================================================================

# HTML report settings
MAX_SAMPLE_DIFFS_IN_HTML = 10000  # Max differences to show in HTML report

# Export formats
EXPORT_TO_CSV = True
EXPORT_TO_JSON = False
EXPORT_TO_PARQUET = True  # Always recommended

# Coalesce output files (reduce number of files)
COALESCE_OUTPUT = True
OUTPUT_FILE_COUNT = 10

# ============================================================================
# PERFORMANCE TUNING
# ============================================================================

# Use approximate counts for very large datasets (faster but less accurate)
USE_APPROXIMATE_COUNTS = False

# Persist DataFrames during comparison (uses more memory but faster)
PERSIST_INTERMEDIATE_RESULTS = True

# Cache strategy
STORAGE_LEVEL = "MEMORY_AND_DISK"  # or "DISK_ONLY" for large data

# S3 optimization (for AWS)
S3_MAX_CONNECTIONS = 100
S3_MAX_THREADS = 50

# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

# Run data quality checks before comparison
RUN_QUALITY_CHECKS = True

# Check for duplicate keys
CHECK_DUPLICATES = True

# Check for NULL values in key columns (fails if found)
FAIL_ON_NULL_KEYS = True

# Check for NULL values in comparison columns (report but continue)
REPORT_NULL_VALUES = True

# ============================================================================
# NOTIFICATIONS (Optional)
# ============================================================================

# Send notification on completion
SEND_NOTIFICATION = False
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:123456789012:dataset-comparison"

# Email notification (requires SES setup)
SEND_EMAIL = False
EMAIL_TO = ["user@example.com"]
EMAIL_FROM = "noreply@example.com"

# ============================================================================
# LOGGING
# ============================================================================

# Log level: DEBUG, INFO, WARN, ERROR
LOG_LEVEL = "INFO"

# Save detailed logs to S3
SAVE_LOGS_TO_S3 = True
LOG_PATH = "s3://your-bucket/comparison-logs/"

# ============================================================================
# VALIDATION
# ============================================================================

def validate_config():
    """
    Validate configuration before running comparison
    """
    errors = []
    warnings = []
    
    # Check required fields
    if not DS1_PATH or DS1_PATH == "s3://your-bucket/path/to/dataset1/":
        errors.append("DS1_PATH not configured")
    
    if not DS2_PATH or DS2_PATH == "s3://your-bucket/path/to/dataset2/":
        errors.append("DS2_PATH not configured")
    
    if not KEY_COLUMNS:
        errors.append("KEY_COLUMNS not configured")
    
    # Check memory settings
    driver_mem_gb = int(DRIVER_MEMORY.replace('g', ''))
    executor_mem_gb = int(EXECUTOR_MEMORY.replace('g', ''))
    
    if driver_mem_gb < 4:
        warnings.append("DRIVER_MEMORY less than 4g may cause issues")
    
    if executor_mem_gb < 8:
        warnings.append("EXECUTOR_MEMORY less than 8g may cause issues")
    
    # Check shuffle partitions
    if SHUFFLE_PARTITIONS < 100:
        warnings.append("SHUFFLE_PARTITIONS less than 100 may be too low for 50GB datasets")
    
    # Print validation results
    if errors:
        print("❌ Configuration Errors:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    if warnings:
        print("⚠️  Configuration Warnings:")
        for warning in warnings:
            print(f"  - {warning}")
    
    print("✅ Configuration validation passed")
    return True


# ============================================================================
# EXAMPLE CONFIGURATIONS FOR COMMON SCENARIOS
# ============================================================================

"""
SCENARIO 1: Daily comparison of transaction data
-----------------------------------------------
KEY_COLUMNS = ["transaction_id"]
SKIP_COLUMNS = ["processed_at", "etl_timestamp"]
USE_DATE_FILTER = True
DATE_COLUMN = "transaction_date"
START_DATE = "2025-01-15"
END_DATE = "2025-01-15"

SCENARIO 2: Weekly comparison with tolerance for financial data
---------------------------------------------------------------
KEY_COLUMNS = ["account_id", "date"]
USE_NUMERIC_TOLERANCE = True
NUMERIC_COLUMNS = ["balance", "interest", "fees"]
TOLERANCE = 0.01
SKIP_COLUMNS = ["updated_timestamp"]

SCENARIO 3: Monthly reconciliation - specific columns only
---------------------------------------------------------
KEY_COLUMNS = ["customer_id", "month"]
COMPARE_SPECIFIC_COLUMNS_ONLY = True
COLUMNS_TO_COMPARE = ["total_revenue", "total_orders", "status"]
USE_DATE_FILTER = True
START_DATE = "2025-01-01"
END_DATE = "2025-01-31"

SCENARIO 4: Quick validation - sample comparison
------------------------------------------------
USE_SAMPLING = True
SAMPLE_FRACTION = 0.1  # 10% sample
COMPARE_SPECIFIC_COLUMNS_ONLY = True
COLUMNS_TO_COMPARE = ["amount", "status"]

SCENARIO 5: Large dataset (100GB+) with optimization
----------------------------------------------------
SHUFFLE_PARTITIONS = 400
EXECUTOR_MEMORY = "32g"
MAX_EXECUTORS = 100
USE_APPROXIMATE_COUNTS = True
PERSIST_INTERMEDIATE_RESULTS = False
STORAGE_LEVEL = "DISK_ONLY"
"""

# ============================================================================
# USAGE INSTRUCTIONS
# ============================================================================

"""
To use this configuration:

1. Copy this file to 'config.py'
2. Edit the values above to match your datasets
3. Run: python parquet_dataset_compare.py --config config.py

Or import in your custom script:
    from config import *
    comparator = DatasetComparator(
        spark=spark,
        ds1_path=DS1_PATH,
        ds2_path=DS2_PATH,
        key_columns=KEY_COLUMNS,
        skip_columns=SKIP_COLUMNS,
        output_path=OUTPUT_PATH
    )
"""
