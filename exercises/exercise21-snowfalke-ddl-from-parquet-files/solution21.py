from pyspark.sql import SparkSession
from pyspark.sql.types import *
import boto3
from botocore.exceptions import NoCredentialsError


def create_spark_session():
    """Create Spark session with S3 configuration"""
    spark = SparkSession.builder \
        .appName("S3ParquetToSnowflakeSchema") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    return spark


def pyspark_to_snowflake_type(spark_type):
    """Convert PySpark data types to Snowflake data types"""
    type_mapping = {
        'StringType': 'VARCHAR',
        'IntegerType': 'INTEGER',
        'LongType': 'BIGINT',
        'FloatType': 'FLOAT',
        'DoubleType': 'DOUBLE',
        'BooleanType': 'BOOLEAN',
        'DateType': 'DATE',
        'TimestampType': 'TIMESTAMP',
        'DecimalType': 'NUMBER',
        'BinaryType': 'BINARY',
        'ArrayType': 'ARRAY',
        'MapType': 'OBJECT',
        'StructType': 'OBJECT'
    }

    spark_type_name = type(spark_type).__name__

    # Handle special cases
    if spark_type_name == 'DecimalType':
        precision = spark_type.precision
        scale = spark_type.scale
        return f'NUMBER({precision},{scale})'
    elif spark_type_name == 'StringType':
        return 'VARCHAR(16777216)'  # Snowflake max VARCHAR length
    elif spark_type_name == 'ArrayType':
        return 'ARRAY'
    elif spark_type_name == 'MapType':
        return 'OBJECT'
    elif spark_type_name == 'StructType':
        return 'OBJECT'

    return type_mapping.get(spark_type_name, 'VARCHAR')


def generate_snowflake_ddl(df, table_name, database_name=None, schema_name=None):
    """Generate Snowflake CREATE TABLE DDL from DataFrame schema"""

    # Build table identifier
    table_identifier = table_name
    if database_name and schema_name:
        table_identifier = f"{database_name}.{schema_name}.{table_name}"
    elif schema_name:
        table_identifier = f"{schema_name}.{table_name}"

    # Start DDL
    ddl = f"CREATE OR REPLACE TABLE {table_identifier} (\n"

    # Process each field
    columns = []
    for field in df.schema.fields:
        column_name = field.name
        snowflake_type = pyspark_to_snowflake_type(field.dataType)
        nullable = "NULL" if field.nullable else "NOT NULL"
        columns.append(f"    {column_name} {snowflake_type} {nullable}")

    ddl += ",\n".join(columns)
    ddl += "\n);"

    return ddl


def read_s3_parquet_schema(spark, s3_path):
    """Read Parquet files from S3 and return schema information"""
    try:
        # Read the Parquet files
        df = spark.read.parquet(s3_path)

        print(f"Successfully read Parquet files from: {s3_path}")
        print(f"Number of columns: {len(df.columns)}")
        print(f"Row count: {df.count()}")

        return df

    except Exception as e:
        print(f"Error reading Parquet files: {str(e)}")
        return None


def print_schema_info(df):
    """Print detailed schema information"""
    print("\n" + "=" * 60)
    print("SCHEMA INFORMATION")
    print("=" * 60)

    print(f"\nColumns ({len(df.columns)}):")
    for i, col in enumerate(df.columns, 1):
        print(f"{i:2d}. {col}")

    print(f"\nDetailed Schema:")
    df.printSchema()

    print(f"\nData Types Summary:")
    for field in df.schema.fields:
        spark_type = type(field.dataType).__name__
        snowflake_type = pyspark_to_snowflake_type(field.dataType)
        nullable = "nullable" if field.nullable else "not null"
        print(f"  {field.name}: {spark_type} -> {snowflake_type} ({nullable})")


def main():
    # Configuration
    S3_PATH = "s3a://your-bucket-name/your-prefix/"  # Update this path
    TABLE_NAME = "your_table_name"  # Update table name
    DATABASE_NAME = "your_database"  # Optional: Update database name
    SCHEMA_NAME = "your_schema"  # Optional: Update schema name

    # Create Spark session
    spark = create_spark_session()

    try:
        # Read Parquet files from S3
        df = read_s3_parquet_schema(spark, S3_PATH)

        if df is not None:
            # Print schema information
            print_schema_info(df)

            # Generate Snowflake DDL
            ddl = generate_snowflake_ddl(df, TABLE_NAME, DATABASE_NAME, SCHEMA_NAME)

            print("\n" + "=" * 60)
            print("SNOWFLAKE DDL")
            print("=" * 60)
            print(ddl)

            # Optionally save DDL to file
            with open(f"{TABLE_NAME}_ddl.sql", "w") as f:
                f.write(ddl)
            print(f"\nDDL saved to: {TABLE_NAME}_ddl.sql")

            # Show sample data
            print("\n" + "=" * 60)
            print("SAMPLE DATA (first 5 rows)")
            print("=" * 60)
            df.show(5, truncate=False)

    except Exception as e:
        print(f"Error in main execution: {str(e)}")

    finally:
        spark.stop()


# Additional utility functions
def validate_s3_access(bucket_name, prefix=""):
    """Validate S3 access before running the main process"""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix,
            MaxKeys=1
        )

        if 'Contents' in response:
            print(f"✓ S3 access validated for s3://{bucket_name}/{prefix}")
            return True
        else:
            print(f"✗ No objects found in s3://{bucket_name}/{prefix}")
            return False

    except NoCredentialsError:
        print("✗ AWS credentials not found")
        return False
    except Exception as e:
        print(f"✗ S3 access error: {str(e)}")
        return False


def analyze_parquet_files(spark, s3_path):
    """Analyze Parquet files for additional insights"""
    try:
        df = spark.read.parquet(s3_path)

        print("\n" + "=" * 60)
        print("PARQUET FILE ANALYSIS")
        print("=" * 60)

        # Basic statistics
        print(f"Total rows: {df.count():,}")
        print(f"Total columns: {len(df.columns)}")

        # Null count analysis
        print(f"\nNull counts by column:")
        total_rows = df.count()
        for col in df.columns:
            null_count = df.filter(df[col].isNull()).count()
            null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
            print(f"  {col}: {null_count:,} ({null_percentage:.2f}%)")

        # Data types distribution
        type_counts = {}
        for field in df.schema.fields:
            type_name = type(field.dataType).__name__
            type_counts[type_name] = type_counts.get(type_name, 0) + 1

        print(f"\nData type distribution:")
        for dtype, count in sorted(type_counts.items()):
            print(f"  {dtype}: {count} columns")

    except Exception as e:
        print(f"Error in analysis: {str(e)}")


if __name__ == "__main__":
    # Example usage with validation
    BUCKET_NAME = "your-bucket-name"  # Update this
    PREFIX = "your-prefix/"  # Update this

    # Validate S3 access first
    if validate_s3_access(BUCKET_NAME, PREFIX):
        main()
    else:
        print("Please check your S3 configuration and try again.")