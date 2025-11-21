# Complete Setup Guide for Iceberg + Snowflake Catalog

## Part 1: Snowflake Catalog Setup

### Step 1: Create Snowflake External Volume (One-time Setup)

Snowflake needs to know about your S3 location. You need to create an **External Volume** in Snowflake:

```sql
-- 1. Create IAM role in AWS for Snowflake access (see AWS section below)
-- 2. Create external volume in Snowflake

CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'iceberg-s3-storage'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://your-bucket/iceberg-warehouse/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeIcebergRole'
            -- Optional: encryption
            ENCRYPTION=(TYPE='AWS_SSE_S3')
         )
      );

-- 3. Get the IAM user ARN and External ID from Snowflake (for AWS trust policy)
DESC EXTERNAL VOLUME iceberg_external_volume;
-- Note down: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
```

### Step 2: Configure AWS IAM for Snowflake

Create an IAM role with this trust policy and permissions:

```json
// Trust Policy
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123456789012:user/abc123-s"
        // From DESC EXTERNAL VOLUME
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "ABC123_SFCRole=..."
          // From DESC EXTERNAL VOLUME
        }
      }
    }
  ]
}

// Permissions Policy
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion"
      ],
      "Resource": "arn:aws:s3:::your-bucket/iceberg-warehouse/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::your-bucket"
    }
  ]
}
```

### Step 3: Create Iceberg Catalog in Snowflake

```sql
-- Create a catalog object that references the external volume
CREATE OR REPLACE CATALOG INTEGRATION iceberg_catalog
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;

-- Grant usage
GRANT USAGE ON INTEGRATION iceberg_catalog TO ROLE DATA_ENGINEER;

-- Create database and schemas for organizing tables
CREATE DATABASE IF NOT EXISTS emissions_db;
CREATE SCHEMA IF NOT EXISTS emissions_db.raw;
CREATE SCHEMA IF NOT EXISTS emissions_db.transformed;
CREATE SCHEMA IF NOT EXISTS emissions_db.metadata;
```

### Step 4: Create Iceberg Tables in Snowflake (Two Options)

**Option A: Create from Snowflake SQL** (Direct approach)

```sql
-- Create Iceberg table managed by Snowflake
CREATE OR REPLACE ICEBERG TABLE emissions_db.raw.emissions_tests (
    test_id STRING,
    vehicle_id STRING,
    test_date TIMESTAMP,
    station_id STRING,
    odometer_reading NUMBER(38,0),
    test_result STRING,
    co_emissions FLOAT,
    hc_emissions FLOAT,
    nox_emissions FLOAT,
    particulate_matter FLOAT,
    inspector_id STRING,
    created_timestamp TIMESTAMP,
    updated_timestamp TIMESTAMP,
    source_file STRING,
    ingestion_timestamp TIMESTAMP
)
CATALOG = 'iceberg_catalog'
EXTERNAL_VOLUME = 'iceberg_external_volume'
BASE_LOCATION = 'emissions_tests' -- subfolder under base path
PARTITION BY (DATE(test_date));

-- Verify table creation
SHOW ICEBERG TABLES IN SCHEMA emissions_db.raw;
```

**Option B: Create from PySpark and Register in Snowflake** (Your approach)

The PySpark code I provided earlier works, but you need to configure the catalog correctly:

```python
# Corrected Spark configuration for Snowflake catalog
spark = SparkSession.builder
.appName("VehicleEmissions")
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.snowflake_catalog", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.snowflake_catalog.catalog-impl", "org.apache.iceberg.snowflake.SnowflakeCatalog")
.config("spark.sql.catalog.snowflake_catalog.uri", "jdbc:snowflake://your-account.snowflakecomputing.com")
.config("spark.sql.catalog.snowflake_catalog.jdbc.user", "your_user")
.config("spark.sql.catalog.snowflake_catalog.jdbc.password", "your_password")
.config("spark.sql.catalog.snowflake_catalog.jdbc.role", "DATA_ENGINEER")
.config("spark.sql.catalog.snowflake_catalog.jdbc.warehouse", "COMPUTE_WH")
.config("spark.sql.catalog.snowflake_catalog.warehouse", "s3://your-bucket/iceberg-warehouse/")
.getOrCreate()
```

---

## Part 2: EMR Setup for Apache Iceberg

### Step 1: EMR Cluster Configuration

Create EMR cluster with Iceberg support:

```json
{
  "Name": "Iceberg-EMR-Cluster",
  "ReleaseLabel": "emr-7.0.0",
  // or latest
  "Applications": [
    {
      "Name": "Spark"
    },
    {
      "Name": "Hadoop"
    },
    {
      "Name": "Hive"
    }
  ],
  "Configurations": [
    {
      "Classification": "spark-defaults",
      "Properties": {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.snowflake_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.snowflake_catalog.catalog-impl": "org.apache.iceberg.snowflake.SnowflakeCatalog",
        "spark.sql.catalog.snowflake_catalog.uri": "jdbc:snowflake://your-account.snowflakecomputing.com",
        "spark.sql.catalog.snowflake_catalog.warehouse": "s3://your-bucket/iceberg-warehouse/",
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.catalog.glue_catalog.warehouse": "s3://your-bucket/iceberg-warehouse/",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": "s3://your-bucket/iceberg-warehouse/",
        "spark.sql.defaultCatalog": "snowflake_catalog"
      }
    },
    {
      "Classification": "spark",
      "Properties": {
        "maximizeResourceAllocation": "true"
      }
    }
  ],
  "Instances": {
    "InstanceGroups": [
      {
        "Name": "Master",
        "InstanceRole": "MASTER",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 1
      },
      {
        "Name": "Core",
        "InstanceRole": "CORE",
        "InstanceType": "m5.xlarge",
        "InstanceCount": 2
      }
    ],
    "Ec2KeyName": "your-key-pair",
    "KeepJobFlowAliveWhenNoSteps": true,
    "TerminationProtected": false
  },
  "ServiceRole": "EMR_DefaultRole",
  "JobFlowRole": "EMR_EC2_DefaultRole",
  "BootstrapActions": [
    {
      "Name": "Install Iceberg Dependencies",
      "ScriptBootstrapAction": {
        "Path": "s3://your-bucket/scripts/bootstrap-iceberg.sh"
      }
    }
  ]
}
```

### Step 2: Bootstrap Script for Dependencies

Create `bootstrap-iceberg.sh`:

```bash
#!/bin/bash

# Set versions
ICEBERG_VERSION="1.5.0"
SPARK_VERSION="3.5"
SNOWFLAKE_JDBC_VERSION="3.14.5"

# Download Iceberg runtime JARs
sudo mkdir -p /usr/lib/spark/jars/iceberg
cd /usr/lib/spark/jars/iceberg

# Iceberg Spark Runtime (includes core Iceberg)
sudo wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar

# AWS Bundle (for S3 operations)
sudo wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Snowflake JDBC driver
sudo wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/${SNOWFLAKE_JDBC_VERSION}/snowflake-jdbc-${SNOWFLAKE_JDBC_VERSION}.jar

# Snowflake Iceberg Catalog (if using Snowflake as catalog)
sudo wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-snowflake/${ICEBERG_VERSION}/iceberg-snowflake-${ICEBERG_VERSION}.jar

# Copy to Spark classpath
sudo cp /usr/lib/spark/jars/iceberg/*.jar /usr/lib/spark/jars/

echo "Iceberg dependencies installed successfully"
```

### Step 3: Alternative - Use EMR with Step Configuration

If you prefer submitting individual jobs instead of long-running cluster:

```python
# In your Airflow DAG or script
SPARK_SUBMIT_STEP = {
    'Name': 'Iceberg-Emissions-Load',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': [
            'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'cluster',
            '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            '--conf', 'spark.sql.catalog.snowflake_catalog=org.apache.iceberg.spark.SparkCatalog',
            '--conf', 'spark.sql.catalog.snowflake_catalog.catalog-impl=org.apache.iceberg.snowflake.SnowflakeCatalog',
            '--conf', 'spark.sql.catalog.snowflake_catalog.uri=jdbc:snowflake://your-account.snowflakecomputing.com',
            '--conf', 'spark.sql.catalog.snowflake_catalog.warehouse=s3://your-bucket/iceberg-warehouse/',
            '--packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,net.snowflake:snowflake-jdbc:3.14.5',
            '--jars', 's3://your-bucket/jars/iceberg-snowflake-1.5.0.jar',
            's3://your-bucket/scripts/emissions_incremental_load.py',
            '--checkpoint-table', 'emissions_checkpoints',
            '--target-table', 'emissions_tests'
        ]
    }
}
```

---

## Part 3: Alternative Approach - AWS Glue Catalog (Simpler)

If Snowflake catalog is too complex, use AWS Glue as intermediate catalog:

### Setup AWS Glue Catalog

```python
# Create table using Glue catalog (simpler than Snowflake catalog)
spark = SparkSession.builder
.appName("VehicleEmissions")
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
.config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-bucket/iceberg-warehouse/")
.config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
.getOrCreate()

# Create table
spark.sql("""
    CREATE TABLE IF NOT EXISTS glue_catalog.emissions_db.emissions_tests (
        test_id STRING,
        vehicle_id STRING,
        test_date TIMESTAMP,
        -- other fields...
    )
    USING iceberg
    PARTITIONED BY (days(test_date))
""")
```

### Query from Snowflake using External Tables

```sql
-- In Snowflake, create external table pointing to Glue catalog
CREATE OR REPLACE EXTERNAL TABLE emissions_db.raw.emissions_tests
WITH LOCATION = @iceberg_stage/emissions_tests/
AUTO_REFRESH = TRUE
FILE_FORMAT = (TYPE = PARQUET)
AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:123456789:iceberg-updates';

-- Or use Snowflake's Iceberg external table (simpler)
CREATE OR REPLACE ICEBERG TABLE emissions_db.raw.emissions_tests_external
  CATALOG='GLUE'
  CATALOG_NAMESPACE='emissions_db'
  CATALOG_TABLE_NAME='emissions_tests'
  CATALOG_SYNC='AWS_GLUE';
```

---

## Part 4: Complete Working Example with Minimal Setup

Here's the **simplest production-ready approach**:

### 1. Use Hadoop Catalog (No external dependencies)

```python
from pyspark.sql import SparkSession

# Simplest Iceberg setup - no external catalog needed
spark = SparkSession.builder
.appName("VehicleEmissions")
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.local.type", "hadoop")
.config("spark.sql.catalog.local.warehouse", "s3://your-bucket/iceberg-warehouse/")
.getOrCreate()

# Create table
spark.sql("""
    CREATE TABLE IF NOT EXISTS local.emissions_db.emissions_tests (
        test_id STRING,
        vehicle_id STRING,
        test_date TIMESTAMP,
        station_id STRING,
        test_result STRING,
        co_emissions DOUBLE,
        nox_emissions DOUBLE,
        ingestion_timestamp TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(test_date))
    LOCATION 's3://your-bucket/iceberg-warehouse/emissions_db/emissions_tests'
""")
```

### 2. Query from Snowflake using Catalog Sync

```sql
-- In Snowflake, sync the Iceberg table
CREATE OR REPLACE ICEBERG TABLE emissions_db.raw.emissions_tests
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  CATALOG = 'SNOWFLAKE'
  METADATA_FILE_PATH = 's3://your-bucket/iceberg-warehouse/emissions_db/emissions_tests/metadata/v1.metadata.json';

-- Auto-refresh metadata
ALTER ICEBERG TABLE emissions_db.raw.emissions_tests 
  REFRESH;

-- Query normally
SELECT * FROM emissions_db.raw.emissions_tests 
WHERE test_date >= CURRENT_DATE() - 7;
```

---

## Required Dependencies Summary

### Maven/POM Dependencies (for reference)

```xml

<dependencies>
    <!-- Iceberg Spark Runtime -->
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-spark-runtime-3.5_2.12</artifactId>
        <version>1.5.0</version>
    </dependency>

    <!-- AWS Bundle (for S3) -->
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-aws-bundle</artifactId>
        <version>1.5.0</version>
    </dependency>

    <!-- Snowflake JDBC (if using Snowflake catalog) -->
    <dependency>
        <groupId>net.snowflake</groupId>
        <artifactId>snowflake-jdbc</artifactId>
        <version>3.14.5</version>
    </dependency>

    <!-- Snowflake Iceberg Catalog -->
    <dependency>
        <groupId>org.apache.iceberg</groupId>
        <artifactId>iceberg-snowflake</artifactId>
        <version>1.5.0</version>
    </dependency>
</dependencies>
```

---

## Quick Start Checklist

- [ ] **AWS S3**: Create bucket for Iceberg warehouse
- [ ] **IAM Role**: Create role with S3 permissions for Snowflake
- [ ] **Snowflake**: Create External Volume pointing to S3
- [ ] **Snowflake**: Create Catalog Integration
- [ ] **EMR**: Launch cluster with Iceberg JARs in bootstrap
- [ ] **Test Connection**: Run simple CREATE TABLE from Spark
- [ ] **Snowflake Query**: Verify table is queryable from Snowflake
