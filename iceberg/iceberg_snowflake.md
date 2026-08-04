# Iceberg + Snowflake: Complete Implementation Roadmap

## PART 1: UNDERSTAND YOUR CURRENT STATE

### Your Current Architecture
```
S3 (Raw Data)
  ↓ (EMR/Python reads)
  ↓
Snowflake (Silver Layer)
  ↓ (Snowflake transforms)
  ↓
Snowflake (Gold Layer)
  ↓
Business Users (Query Snowflake)
```

### The Cost Problem
- **Large MERGE operations** in Snowflake consume credits rapidly
- **Storage costs** inflated by Snowflake's markup
- **Unnecessary compute** - Snowflake doing work that's cheaper elsewhere

---

## PART 2: YOUR THREE OPTIONS (SIMPLIFIED)

### Option 1: Keep Everything in Snowflake (Minimal Change)
```
S3 → EMR/Spark (read, minimal transform)
       ↓
   Snowflake Iceberg Table (managed by Snowflake)
       ↓
   Business Users (Query Snowflake)
```
**Cost Savings:** 10-20% (storage markup reduction only)
**Effort:** Low
**Risk:** Low

---

### Option 2: Hybrid Approach (RECOMMENDED FOR YOU) ⭐⭐⭐
```
S3 (Raw Data)
  ↓ (EMR/Spark reads and transforms - CHEAP COMPUTE)
  ↓
S3 Iceberg (Silver & Gold - AWS Glue Catalog)
  ↓ (Snowflake reads only - MODERATE ANALYTICS COST)
  ↓
Business Users (Query Snowflake)
```
**Cost Savings:** 60-70% reduction overall (move expensive ETL to Spark)
**Snowflake Cost:** Reduced by 70-80% but NOT eliminated (analytics queries still cost)
**Effort:** Medium (need to rewrite pipelines from SQL to Spark)
**Risk:** Medium (new architecture, but proven pattern)
**Key Insight:** Savings come from eliminating expensive MERGE/INSERT/transformation operations in Snowflake, not from eliminating all Snowflake costs

---

### Option 3: Full Decoupling (Replace Snowflake Entirely)
```
S3 (Raw Data)
  ↓ (EMR/Spark transforms)
  ↓
S3 Iceberg (Silver & Gold)
  ↓ (Query with Trino/Athena/Spark - NO Snowflake)
  ↓
Business Users (Query Trino/Athena)
```
**Cost Savings:** 95%+ (eliminate Snowflake entirely)
**Effort:** High (rewrite all BI, dashboards, user tools)
**Risk:** High (users need new tools)

---

## PART 3: COMPARISON & RECOMMENDATION

### Cost Comparison - HONEST BREAKDOWN (Example: 100 TB Data, $50K/month Snowflake)

**Current Snowflake Spend Breakdown (Estimated):**
- ETL (MERGE, INSERT, transformations): $40,000/month (80%)
- Analytics (SELECT queries, dashboards): $10,000/month (20%)

| Option | Snowflake ETL | Snowflake Analytics | EMR Compute | Storage | Total | Savings |
|--------|---|---|---|---|---|---|
| **Current (No Change)** | $40,000 | $10,000 | $0 | $2,300 | **$52,300** | **$0** |
| **Option 1** | $38,000 | $10,000 | $0 | $2,300 | **$50,300** | **$2,000** (4%) |
| **Option 2** (Recommended) | $0 | $10,000 | $5,000 | $2,300 | **$17,300** | **$35,000** (67%) ⭐ |
| **Option 3** | $0 | $0 | $15,000 | $2,300 | **$17,300** | **$35,000** (67%) |

**Key Insight for Option 2:**
- ✅ You ELIMINATE expensive ETL operations ($40K → $0)
- ⚠️ Analytics queries still cost in Snowflake (~$10K stays)
- ✅ Add cheaper EMR for ETL (~$5K)
- **Net Savings: $35K/month (67%)**

**Recommendation: Option 2** - Best balance of savings (67%) vs effort and risk. Highest ROI for your situation.

---

## PART 3.5: HONEST COST BREAKDOWN FOR OPTION 2

### What Gets Cheaper (Moved to Spark/EMR)

| Operation | Old Cost in Snowflake | New Cost in Spark | Savings |
|-----------|---|---|---|
| **MERGE operations** (upserts) | $5-10K/month | $500-1K/month | 80-90% ✅ |
| **INSERT operations** (bulk loads) | $5-10K/month | $500-1K/month | 80-90% ✅ |
| **DELETE/UPDATE** | $3-5K/month | $200-500/month | 80-90% ✅ |
| **Data deduplication** | $5-10K/month | $500-1K/month | 80-90% ✅ |
| **Complex transformations** | $10-15K/month | $2-3K/month | 70-85% ✅ |
| **Full table reloads** | $5-10K/month | $500-1K/month | 80-90% ✅ |
| **Total ETL Operations** | **~$40K/month** | **~$5K/month** | **87% savings** ✅ |

### What STAYS THE SAME COST (Still in Snowflake)

| Operation | Cost Reason | Monthly Cost | Stays? |
|-----------|---|---|---|
| **SELECT queries** | Analysts query dashboards, reports | ~$3K/month | ⚠️ Yes |
| **Concurrent users** | Multiple users querying simultaneously | ~$4K/month | ⚠️ Yes |
| **Warehouse runtime** | Keeping warehouse running for queries | ~$3K/month | ⚠️ Yes |
| **Total Analytics Cost** | **Queries that produce business value** | **~$10K/month** | **⚠️ Yes (necessary)** |

### THE BOTTOM LINE

```
OLD ARCHITECTURE (All in Snowflake):
  ETL Operations (expensive):      $40K/month ← PROBLEM
  Analytics Queries (necessary):   $10K/month ← OK
  Storage (Snowflake):              $2.3K/month
  ─────────────────────────────────────
  TOTAL:                           $52.3K/month

NEW ARCHITECTURE (Option 2):
  ETL Operations (cheap Spark):     $5K/month  ← SOLVED!
  Analytics Queries (Snowflake):   $10K/month ← Still needed
  Storage (S3):                     $2.3K/month
  ─────────────────────────────────────
  TOTAL:                           $17.3K/month

  SAVINGS: $35K/month (67%) ✅
```

### What This Means in Practice

**You're NOT making Snowflake free.** You're making it do what it's good at:
- ✅ Keep Snowflake for what it excels at: **Interactive analytics queries**
- ✅ Move Snowflake away from: **Heavy ETL operations**
- ✅ Use Spark for what it's good at: **Bulk transformations, MERGE, deduplication**

**The $10K Snowflake cost is worth it because:**
- Business users get fast, responsive queries
- Snowflake's caching and query optimization is excellent for analytics
- Users don't need to learn new tools (still use SQL)
- BI/dashboarding tools connect natively to Snowflake

---

## PART 4: DETAILED OPTION 2 IMPLEMENTATION

### Architecture Diagram
```
┌─────────────────────────────────────────────────────────────────┐
│                    YOUR NEW ARCHITECTURE                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  S3 Buckets:                                                      │
│  ├─ Raw (s3://my-bucket/raw/)                                   │
│  ├─ Bronze (s3://my-bucket/iceberg/bronze/)                    │
│  ├─ Silver (s3://my-bucket/iceberg/silver/)                    │
│  └─ Gold (s3://my-bucket/iceberg/gold/)                        │
│                                                                   │
│  AWS Glue Catalog (Metadata Registry)                           │
│  ├─ Tracks all Iceberg tables                                  │
│  ├─ Manages versions and snapshots                             │
│  └─ Accessible to EMR and Snowflake                            │
│                                                                   │
│  EMR Cluster (Heavy Lifting)                                   │
│  ├─ Spark jobs read from S3 Raw                               │
│  ├─ Transform and deduplicate                                 │
│  ├─ Write to Iceberg (Bronze → Silver → Gold)                │
│  └─ Runs on schedule (daily/hourly)                           │
│                                                                   │
│  Snowflake (Analytics Only)                                   │
│  ├─ Connected to AWS Glue Catalog                             │
│  ├─ Queries Iceberg tables in S3 (costs ~$10K/month)         │
│  ├─ ZERO compute for ETL (expensive ops moved to Spark)      │
│  ├─ Warehouse sized smaller (less ETL overhead)              │
│  └─ Users query as normal                                      │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## PART 5: STEP-BY-STEP IMPLEMENTATION

### Phase 1: Setup (Week 1-2)

#### Step 1a: Create S3 Bucket for Iceberg Warehouse
```bash
# Create bucket
aws s3 mb s3://my-data-lake-iceberg/ --region us-east-1

# Enable versioning for data retention
aws s3api put-bucket-versioning \
  --bucket my-data-lake-iceberg \
  --versioning-configuration Status=Enabled

# Enable encryption
aws s3api put-bucket-encryption \
  --bucket my-data-lake-iceberg \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "aws:kms"
      }
    }]
  }'

# Create folder structure
aws s3api put-object --bucket my-data-lake-iceberg --key bronze/
aws s3api put-object --bucket my-data-lake-iceberg --key silver/
aws s3api put-object --bucket my-data-lake-iceberg --key gold/
aws s3api put-object --bucket my-data-lake-iceberg --key metadata/
```

#### Step 1b: Create IAM Role for EMR to Access S3
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "IcebergS3Access",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketVersioning"
      ],
      "Resource": [
        "arn:aws:s3:::my-data-lake-iceberg/*",
        "arn:aws:s3:::my-data-lake-iceberg"
      ]
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetPartitions",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition"
      ],
      "Resource": "*"
    }
  ]
}
```

#### Step 1c: Create IAM Role for Snowflake to Access S3
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::YOUR_SNOWFLAKE_ACCOUNT_ID:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "YOUR_SNOWFLAKE_EXTERNAL_ID"
        }
      }
    }
  ]
}
```

Add permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketVersioning"
      ],
      "Resource": [
        "arn:aws:s3:::my-data-lake-iceberg/*",
        "arn:aws:s3:::my-data-lake-iceberg"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    }
  ]
}
```

#### Step 1d: Create Glue Database
```bash
aws glue create-database \
  --database-input Name=iceberg_db,Description="Iceberg medallion layers"
```

---

### Phase 2: Configure Snowflake Connection (Week 2)

#### Step 2a: Create External Volume in Snowflake
```sql
-- In Snowflake as ACCOUNTADMIN
CREATE OR REPLACE EXTERNAL VOLUME iceberg_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 's3-iceberg-storage'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://my-data-lake-iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/SnowflakeIcebergRole'
            ENCRYPTION=(TYPE='AWS_SSE_S3')
         )
      );

-- Get the external ID (copy this to AWS trust policy)
DESC EXTERNAL VOLUME iceberg_volume;
-- Note: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
```

#### Step 2b: Create Catalog Integration in Snowflake
```sql
CREATE OR REPLACE CATALOG INTEGRATION iceberg_glue_catalog
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'arn:aws:glue:us-east-1:ACCOUNT_ID:catalog'
  ENABLED = TRUE;

-- Grant to data engineers
GRANT USAGE ON INTEGRATION iceberg_glue_catalog TO ROLE DATA_ENGINEER;
```

#### Step 2c: Create Schemas in Snowflake
```sql
CREATE DATABASE IF NOT EXISTS data_lake;
CREATE SCHEMA IF NOT EXISTS data_lake.bronze;
CREATE SCHEMA IF NOT EXISTS data_lake.silver;
CREATE SCHEMA IF NOT EXISTS data_lake.gold;
```

---

### Phase 3: Launch EMR Cluster (Week 2-3)

#### Step 3a: Create EMR Bootstrap Script
Save as `s3://my-data-lake-iceberg/scripts/bootstrap-iceberg.sh`:

```bash
#!/bin/bash
set -e

echo "Installing Iceberg dependencies..."

# Versions
ICEBERG_VERSION="1.5.0"
SPARK_VERSION="3.5"
GLUE_VERSION="4.0"

# Download to Spark jars
cd /usr/lib/spark/jars || cd /opt/spark/jars

# Iceberg Spark Runtime
sudo wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar

# AWS Bundle
sudo wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Glue Catalog
sudo wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-glue/${ICEBERG_VERSION}/iceberg-glue-${ICEBERG_VERSION}.jar

echo "✅ Iceberg dependencies installed"
```

#### Step 3b: Launch EMR Cluster
```bash
aws emr create-cluster \
  --name "Iceberg-DataLake" \
  --release-label emr-7.1.0 \
  --applications Name=Spark Name=Hadoop Name=Hive \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 \
    InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=2 \
    InstanceGroupType=TASK,InstanceType=m5.xlarge,InstanceCount=0 \
  --bootstrap-actions Path=s3://my-data-lake-iceberg/scripts/bootstrap-iceberg.sh \
  --service-role EMR_DefaultRole \
  --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,KeyName=my-key-pair \
  --configurations '[
    {
      "Classification": "spark-defaults",
      "Properties": {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.warehouse": "s3://my-data-lake-iceberg",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.defaultCatalog": "glue_catalog"
      }
    }
  ]' \
  --region us-east-1
```

---

### Phase 4: Create Bronze Layer ETL Job (Week 3)

#### Step 4a: Bronze Ingestion Job
Save as `s3://my-data-lake-iceberg/scripts/bronze_ingest.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# Initialize Spark with Iceberg
spark = (
    SparkSession.builder
    .appName("IcebergBronzeIngest")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://my-data-lake-iceberg")
    .getOrCreate()
)

print("✅ Spark initialized with Iceberg")

# Step 1: Read raw data from S3
print("📖 Reading raw data...")
raw_df = spark.read.json("s3://my-data-lake-iceberg/raw/")

# Step 2: Add metadata columns
print("➕ Adding metadata columns...")
bronze_df = (
    raw_df
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_ingest_date", F.to_date("_ingest_ts"))
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_load_id", F.lit(datetime.now().strftime("%Y%m%d%H%M%S")))
)

# Step 3: Create Iceberg table (if not exists)
print("🔧 Creating/updating Iceberg Bronze table...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.iceberg_db.bronze_raw (
        id STRING,
        name STRING,
        value DOUBLE,
        timestamp TIMESTAMP,
        _ingest_ts TIMESTAMP,
        _ingest_date DATE,
        _source_file STRING,
        _load_id STRING
    )
    USING iceberg
    PARTITIONED BY (_ingest_date)
    LOCATION 's3://my-data-lake-iceberg/bronze/raw'
""")

# Step 4: Write to Iceberg (append-only)
print("📝 Writing to Iceberg...")
(
    bronze_df
    .write
    .format("iceberg")
    .mode("append")
    .save("glue_catalog.iceberg_db.bronze_raw")
)

print("✅ Bronze ingestion complete")
print(f"   Rows ingested: {bronze_df.count()}")

spark.stop()
```

#### Step 4b: Submit Job to EMR
```bash
# First, copy script to S3
aws s3 cp bronze_ingest.py s3://my-data-lake-iceberg/scripts/

# Submit to running cluster
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=Spark,Name="Bronze Ingestion",SparkSubmitParameters="--deploy-mode cluster" \
  ActionOnFailure=CONTINUE,HadoopJarStep={Jar=command-runner.jar,Args=[spark-submit,s3://my-data-lake-iceberg/scripts/bronze_ingest.py]}

# Monitor
aws emr describe-step --cluster-id j-XXXXXXXXXXXXX --step-id s-XXXXXXXXXXXXX
```

---

### Phase 5: Create Silver Layer Transformation (Week 3-4)

#### Step 5a: Silver Transformation Job
Save as `s3://my-data-lake-iceberg/scripts/silver_transform.py`:

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("IcebergSilverTransform")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://my-data-lake-iceberg")
    .getOrCreate()
)

print("✅ Spark initialized")

# Step 1: Read from Bronze
print("📖 Reading Bronze layer...")
bronze_df = spark.read.format("iceberg").load("glue_catalog.iceberg_db.bronze_raw")

# Step 2: Validate and clean
print("🧹 Validating and cleaning data...")
cleaned_df = (
    bronze_df
    .filter(F.col("id").isNotNull())  # Remove nulls
    .filter(F.col("value") > 0)        # Business rules
    .withColumn("value", F.round(F.col("value"), 2))  # Precision
    .withColumn("timestamp", F.to_timestamp("timestamp"))  # Type conversion
)

# Step 3: Deduplication (keep latest per ID)
print("🔄 Deduplicating...")
w = Window.partitionBy("id").orderBy(F.col("_ingest_ts").desc())

silver_df = (
    cleaned_df
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("_processed_ts", F.current_timestamp())
)

# Step 4: Create Silver Iceberg table
print("🔧 Creating Silver table...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.iceberg_db.silver_cleaned (
        id STRING,
        name STRING,
        value DOUBLE,
        timestamp TIMESTAMP,
        _ingest_ts TIMESTAMP,
        _processed_ts TIMESTAMP,
        _ingest_date DATE
    )
    USING iceberg
    PARTITIONED BY (_ingest_date)
    LOCATION 's3://my-data-lake-iceberg/silver/cleaned'
""")

# Step 5: Upsert using Iceberg MERGE
print("📝 Merging to Silver...")

# For initial load, just append
if silver_df.count() > 0:
    # Use Iceberg MERGE for upserts
    spark.sql(f"""
        MERGE INTO glue_catalog.iceberg_db.silver_cleaned t
        USING (
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingest_ts DESC) as rn
                FROM {silver_df._jdf.collect()}
            )
            WHERE rn = 1
        ) s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET
            t.name = s.name,
            t.value = s.value,
            t.timestamp = s.timestamp,
            t._processed_ts = s._processed_ts
        WHEN NOT MATCHED THEN INSERT *
    """)

print("✅ Silver transformation complete")
print(f"   Rows in Silver: {spark.read.format('iceberg').load('glue_catalog.iceberg_db.silver_cleaned').count()}")

spark.stop()
```

---

### Phase 6: Create Gold Layer Aggregations (Week 4)

#### Step 6a: Gold Aggregation Job
Save as `s3://my-data-lake-iceberg/scripts/gold_aggregates.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("IcebergGoldAgg")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://my-data-lake-iceberg")
    .getOrCreate()
)

# Read from Silver
print("📖 Reading Silver layer...")
silver_df = spark.read.format("iceberg").load("glue_catalog.iceberg_db.silver_cleaned")

# Aggregate
print("🔢 Aggregating...")
gold_df = (
    silver_df
    .groupBy(F.to_date("timestamp").alias("date"))
    .agg(
        F.count("*").alias("record_count"),
        F.countDistinct("id").alias("unique_ids"),
        F.sum("value").alias("total_value"),
        F.avg("value").alias("avg_value"),
        F.min("value").alias("min_value"),
        F.max("value").alias("max_value")
    )
    .withColumn("_created_ts", F.current_timestamp())
)

# Create Gold table
print("🔧 Creating Gold table...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.iceberg_db.gold_daily_agg (
        date DATE,
        record_count LONG,
        unique_ids LONG,
        total_value DOUBLE,
        avg_value DOUBLE,
        min_value DOUBLE,
        max_value DOUBLE,
        _created_ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (date)
    LOCATION 's3://my-data-lake-iceberg/gold/daily_agg'
""")

# Write
print("📝 Writing to Gold...")
(
    gold_df
    .write
    .format("iceberg")
    .mode("overwrite")
    .save("glue_catalog.iceberg_db.gold_daily_agg")
)

print("✅ Gold aggregation complete")
spark.stop()
```

---

### Phase 7: Query from Snowflake (Week 4)

#### Step 7a: Create Iceberg Tables in Snowflake (Read-Only)
```sql
-- Create external Iceberg tables pointing to Glue Catalog
USE DATABASE data_lake;

-- Bronze table (for reference, usually internal use only)
CREATE OR REPLACE ICEBERG TABLE bronze.raw_data
    EXTERNAL_VOLUME = 'iceberg_volume'
    CATALOG = 'iceberg_glue_catalog'
    CATALOG_NAMESPACE = 'iceberg_db'
    CATALOG_TABLE_NAME = 'bronze_raw';

-- Silver table (clean data for BI/analytics)
CREATE OR REPLACE ICEBERG TABLE silver.cleaned_data
    EXTERNAL_VOLUME = 'iceberg_volume'
    CATALOG = 'iceberg_glue_catalog'
    CATALOG_NAMESPACE = 'iceberg_db'
    CATALOG_TABLE_NAME = 'silver_cleaned';

-- Gold table (ready for dashboards)
CREATE OR REPLACE ICEBERG TABLE gold.daily_aggregates
    EXTERNAL_VOLUME = 'iceberg_volume'
    CATALOG = 'iceberg_glue_catalog'
    CATALOG_NAMESPACE = 'iceberg_db'
    CATALOG_TABLE_NAME = 'gold_daily_agg';
```

#### Step 7b: Query from Snowflake
```sql
-- Analysts can query normally
SELECT *
FROM data_lake.gold.daily_aggregates
WHERE date >= CURRENT_DATE() - 30
ORDER BY date DESC;

-- Check row count
SELECT COUNT(*) as total_records
FROM data_lake.silver.cleaned_data;

-- Join with other Snowflake tables if needed
SELECT
    g.date,
    g.total_value,
    g.record_count,
    c.customer_segment
FROM data_lake.gold.daily_aggregates g
JOIN snowflake_table c ON g.date = c.date
WHERE g.date >= CURRENT_DATE() - 7;
```

#### Step 7c: Snowflake Refresh (Auto-sync Metadata)
```sql
-- Refresh table metadata from Glue
ALTER ICEBERG TABLE data_lake.silver.cleaned_data REFRESH;

-- Check metadata staleness
DESC ICEBERG TABLE data_lake.silver.cleaned_data;
```

---

### Phase 8: Automate with Airflow/MWAA (Week 4-5)

#### Step 8a: Airflow DAG
Save as `emr_iceberg_dag.py`:

```python
from airflow import DAG
from airflow.operators.emr import EmrAddStepsOperator
from airflow.operators.emr import EmrStepSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'iceberg_medallion_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Bronze ingestion
    bronze_step = EmrAddStepsOperator(
        task_id='ingest_bronze',
        job_flow_id='j-XXXXXXXXXXXXX',  # Your cluster ID
        steps=[{
            'Name': 'Bronze Ingestion',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    's3://my-data-lake-iceberg/scripts/bronze_ingest.py',
                ]
            }
        }],
    )

    # Silver transformation
    silver_step = EmrAddStepsOperator(
        task_id='transform_silver',
        job_flow_id='j-XXXXXXXXXXXXX',
        steps=[{
            'Name': 'Silver Transformation',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', 's3://my-data-lake-iceberg/scripts/silver_transform.py']
            }
        }],
    )

    # Gold aggregation
    gold_step = EmrAddStepsOperator(
        task_id='aggregate_gold',
        job_flow_id='j-XXXXXXXXXXXXX',
        steps=[{
            'Name': 'Gold Aggregation',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', 's3://my-data-lake-iceberg/scripts/gold_aggregates.py']
            }
        }],
    )

    # Wait for completion
    bronze_wait = EmrStepSensor(
        task_id='wait_bronze',
        job_flow_id='j-XXXXXXXXXXXXX',
        step_id="{{ task_instance.xcom_pull('ingest_bronze', key='return_value') }}",
    )

    silver_wait = EmrStepSensor(
        task_id='wait_silver',
        job_flow_id='j-XXXXXXXXXXXXX',
        step_id="{{ task_instance.xcom_pull('transform_silver', key='return_value') }}",
    )

    gold_wait = EmrStepSensor(
        task_id='wait_gold',
        job_flow_id='j-XXXXXXXXXXXXX',
        step_id="{{ task_instance.xcom_pull('aggregate_gold', key='return_value') }}",
    )

    # Set dependencies
    bronze_step >> bronze_wait >> silver_step >> silver_wait >> gold_step >> gold_wait
```

---

## PART 6: MIGRATION CHECKLIST

### Pre-Migration (Week 1)
- [ ] Create S3 bucket structure
- [ ] Create IAM roles (EMR + Snowflake)
- [ ] Create Glue database
- [ ] Test S3 access from EMR
- [ ] Test Snowflake Iceberg connectivity

### Migration (Week 2-3)
- [ ] Export existing Snowflake Silver data to S3 as Parquet
- [ ] Convert Parquet to Iceberg format in EMR
- [ ] Verify data integrity and counts
- [ ] Connect Snowflake to Glue Catalog
- [ ] Create external Iceberg tables in Snowflake
- [ ] Test queries from Snowflake

### Testing (Week 3-4)
- [ ] Run parallel: Old pipeline + New pipeline
- [ ] Compare results
- [ ] Performance test queries
- [ ] Load test concurrent queries
- [ ] Test edge cases (nulls, duplicates, etc.)

### Cutover (Week 4-5)
- [ ] Update documentation
- [ ] Train team on new architecture
- [ ] Redirect queries to new tables
- [ ] Monitor Snowflake costs
- [ ] Archive old tables (keep 30 days)

### Post-Migration (Week 5+)
- [ ] Monitor performance metrics
- [ ] Optimize based on actual workload
- [ ] Schedule maintenance (VACUUM, OPTIMIZE)
- [ ] Document lessons learned

---

## PART 7: COST MONITORING & OPTIMIZATION

### Monitor EMR Costs
```python
import boto3

emr = boto3.client('emr')
ec2 = boto3.client('ec2')

# Get cluster details
clusters = emr.list_clusters(ClusterStates=['RUNNING'])

for cluster in clusters['Clusters']:
    print(f"Cluster: {cluster['Name']}")
    print(f"  State: {cluster['Status']['State']}")
    print(f"  Steps: {emr.list_steps(ClusterId=cluster['Id'])}")
```

### Optimize Iceberg Performance
```python
# Run weekly OPTIMIZE
spark.sql("OPTIMIZE glue_catalog.iceberg_db.silver_cleaned ZORDER BY (id, date)")

# Run monthly CLEANUP
spark.sql("CALL glue_catalog.system.remove_orphan_files('glue_catalog.iceberg_db.silver_cleaned')")

# Check stats
spark.sql("""
    SELECT
        file_count,
        total_size_bytes,
        avg_file_size
    FROM glue_catalog.system.iceberg_metadata('glue_catalog.iceberg_db.silver_cleaned')
""").show()
```

---

## PART 8: QUICK REFERENCE COMMANDS

### Test Iceberg Connectivity
```bash
# SSH into EMR master
aws emr ssh --cluster-id j-XXXXX --key-pair-file ~/mykey.pem

# Start Spark shell
spark-shell

# Create test table
spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.iceberg_db.test (
        id STRING,
        value INT
    )
    USING iceberg
""")

# Write data
spark.sql("""
    INSERT INTO glue_catalog.iceberg_db.test VALUES ('1', 100), ('2', 200)
""")

# Read data
spark.sql("SELECT * FROM glue_catalog.iceberg_db.test").show()
```

### Troubleshoot Snowflake Connection
```sql
-- Check external volume
DESC EXTERNAL VOLUME iceberg_volume;

-- Check catalog integration
DESC CATALOG INTEGRATION iceberg_glue_catalog;

-- Test query with detailed error
SELECT * FROM data_lake.silver.cleaned_data LIMIT 10;
```

---

## PART 9: COMMON PITFALLS & SOLUTIONS

### Pitfall 1: Snowflake Can't Find Iceberg Tables
**Solution:**
```sql
-- Refresh metadata
ALTER ICEBERG TABLE data_lake.silver.cleaned_data REFRESH;

-- Verify Glue has the table
-- Use AWS Glue console to check catalog
```

### Pitfall 2: Slow Queries from Snowflake
**Solution:**
- Iceberg tables need OPTIMIZE to compact files
- Too many small files slow down queries
- Run `spark.sql("OPTIMIZE glue_catalog.iceberg_db.silver_cleaned")`

### Pitfall 3: EMR Job Fails with Permission Error
**Solution:**
- Check EC2 instance profile has correct IAM role
- Verify role has s3:*, glue:* permissions
- Check S3 bucket encryption (may need KMS permissions)

### Pitfall 4: Cost Higher Than Expected
**Solution:**
- EMR costs with Spark on m5.xlarge are ~$0.192/hour
- Use Spot instances for task nodes (save 70%)
- Scale down cluster when not running jobs
- Use smaller instance types if possible

---

## PART 10: SUCCESS METRICS

### Track These KPIs (HONEST EXPECTATIONS)

| Metric | Expectation | Why |
|--------|---|---|
| **Total Monthly Cost** | Decrease from $52K → $17K (67%) | ETL moved to cheap Spark |
| **Snowflake Spend** | Decrease from $50K → $10K (80%) | No more expensive ETL ops |
| **Snowflake ETL Cost** | Decrease from $40K → $0 (100%) | All ETL now in Spark |
| **Snowflake Analytics Cost** | STAYS ~$10K (0% decrease) | Necessary for query performance |
| **EMR Cost** | New cost ~$5K/month | Replacement for Snowflake ETL |
| **Query Performance** | Same or better | Snowflake optimized for analytics |
| **ETL Runtime** | Faster (Spark better for CRUD) | More efficient dedup/merge |
| **Data Freshness** | Maintain or improve | Scheduled Spark jobs run reliably |
| **User Satisfaction** | No changes to their experience | Still query Snowflake as before |

### 30-Day Goal (Realistic)
- [ ] Bronze, Silver, Gold layers working in Iceberg
- [ ] Snowflake querying Iceberg successfully
- [ ] Automated pipeline running daily
- [ ] Total costs reduced by 50%+ (ETL operations eliminated)
- [ ] Snowflake costs reduced by 75%+ (ETL eliminated, analytics remain)
- [ ] Team trained on Spark/EMR (new requirement)
- [ ] Monitoring dashboard showing cost breakdown

---

## PART 11: CRITICAL REQUIREMENT - CRUD OPERATIONS

### ⚠️ IF YOU DO FREQUENT UPDATE/DELETE OPERATIONS

**If your workload includes:**
- ✅ Frequent MERGE statements
- ✅ Row-level UPDATE operations
- ✅ DELETE for data corrections
- ✅ Ad-hoc Snowflake SQL modifications

**THEN Option 2 is NOT suitable.**

### The CRUD Problem with Option 2

In Option 2 (Hybrid), Snowflake Iceberg tables are **read-only**:

```sql
-- These WORK:
SELECT * FROM data_lake.silver.cleaned_data;  ✅

-- These FAIL:
UPDATE data_lake.silver.cleaned_data SET value = 100 WHERE id = '1';  ❌
DELETE FROM data_lake.silver.cleaned_data WHERE id = '2';  ❌
MERGE INTO data_lake.silver.cleaned_data USING source...;  ❌
```

### Solutions for CRUD Workloads

**See CRUD_OPERATIONS_GUIDE.md for complete details. Quick summary:**

| Strategy | CRUD Support | Cost Savings | Effort | Risk |
|----------|---|---|---|---|
| **Strategy 1** (No change) | ✅ Full | ❌ 4% | ✅ Low | ✅ Low |
| **Strategy 2** (Recommended) | ⚠️ Hybrid (Spark bulk + SF ad-hoc) | ✅ 44% | ⚠️ Medium | ✅ Low |
| **Strategy 3** (Trino) | ✅ Full | ✅ 65% | ❌ High | ⚠️ Medium |

### Recommended for CRUD-Heavy Workload: Strategy 2

- Use **Spark for bulk MERGE/INSERT** (cheap)
- Use **Snowflake for ad-hoc UPDATE/DELETE** (reduced volume)
- Keep **Snowflake for analytics** (what it's good at)
- Result: **44% savings ($21.7K/month) with full CRUD capability**

---

## PART 11: COMMON MISCONCEPTIONS (Set Realistic Expectations)

### ❌ MYTH: "Iceberg will make Snowflake free"
**Reality:** Iceberg doesn't make Snowflake free. It makes ETL cheap. You still need Snowflake for analytics queries, and that will cost money. But it's a worthwhile cost because Snowflake is excellent at analytics.

### ❌ MYTH: "All Snowflake costs go away"
**Reality:** Only the expensive ETL operations go away (~80% of costs). Analytics queries stay (~20% of costs remain), which is actually good.

### ❌ MYTH: "Snowflake reads from Iceberg are free"
**Reality:** Every query Snowflake executes costs credits. Reading from Iceberg tables in S3 still requires compute. But because Spark already processed the data, queries are simpler and faster, using fewer credits.

### ❌ MYTH: "You need to rewrite everything"
**Reality:** You need to rewrite ETL pipelines (SQL → Spark/Python). Analytics queries in Snowflake stay mostly the same.

### ✅ TRUTH: "Expensive operations move to cheaper engines"
This is the actual benefit. MERGE, INSERT, and complex transformations are expensive in Snowflake but cheap in Spark.

### ✅ TRUTH: "You save 60-70% total cost"
By moving expensive operations out and keeping analytics in Snowflake, you get real savings while maintaining performance.

### ✅ TRUTH: "Option 2 is the sweet spot"
Hybrid approach gives you:
- Maximum savings (67% overall)
- Minimal risk (proven pattern)
- No user disruption (analytics layer unchanged)
- Reasonable effort (ETL rewrite needed)

---

## NEXT STEPS

1. **Start with Phase 1-2** (Week 1-2): Setup infrastructure
2. **Run Phase 3-5** (Week 3-4): Build ETL pipelines
3. **Test Phase 6-7** (Week 4): Query from Snowflake
4. **Automate Phase 8** (Week 5): Schedule with Airflow
5. **Migrate** (Week 5-6): Move real data, verify, cutover

**Estimated Total Effort:** 5-6 weeks for full implementation
**Cost Savings Realized:** Day 1 (no more Snowflake CRUD charges)
**ROI:** Typically 2-3 months

---

## QUESTIONS?

Refer to:
- Original docs in `/iceberg/` folder
- AWS Glue Iceberg documentation
- Snowflake Iceberg documentation
- Apache Iceberg best practices
