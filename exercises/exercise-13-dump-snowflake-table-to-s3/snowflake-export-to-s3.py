import snowflake.connector
import boto3
import os
import pandas as pd
from io import StringIO, BytesIO
import time

# Snowflake connection parameters
snowflake_params = {
    'user': os.environ.get('SNOWFLAKE_USER'),
    'password': os.environ.get('SNOWFLAKE_PASSWORD'),
    'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
    'database': os.environ.get('SNOWFLAKE_DATABASE'),
    'schema': os.environ.get('SNOWFLAKE_SCHEMA')
}

# S3 parameters
s3_bucket = 'your-s3-bucket-name'
s3_prefix = 'your/prefix/path/'  # Include trailing slash
country_code = 'US'  # Replace with your country code filter
output_format = 'parquet'  # Options: 'parquet', 'csv', 'csv-pipe'

# Configure AWS session with profile
boto3.setup_default_session(profile_name='your-aws-profile-name')
s3 = boto3.client('s3')

# Table details
table_name = 'your_table_name'

# Batch size - adjust based on memory constraints
BATCH_SIZE = 500000  # Number of rows per batch


def export_to_s3():
    """Export Snowflake table data to S3 in batches"""
    print(f"Starting export of {table_name} filtered by country_code={country_code}")

    # Connect to Snowflake
    conn = snowflake.connector.connect(**snowflake_params)
    cursor = conn.cursor()

    try:
        # Get total row count for progress tracking
        count_query = f"""
        SELECT COUNT(*) FROM {table_name} 
        WHERE country_code = '{country_code}'
        """
        cursor.execute(count_query)
        total_rows = cursor.fetchone()[0]
        print(f"Total rows to export: {total_rows}")

        # Determine number of batches
        num_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE

        # Use LIMIT and OFFSET for batching
        for batch_num in range(num_batches):
            offset = batch_num * BATCH_SIZE

            print(f"Processing batch {batch_num + 1}/{num_batches} (offset {offset})")

            # Query to get batch of data
            query = f"""
            SELECT * FROM {table_name} 
            WHERE country_code = '{country_code}'
            ORDER BY some_id_column
            LIMIT {BATCH_SIZE} OFFSET {offset}
            """

            start_time = time.time()
            # Execute query and fetch results
            cursor.execute(query)

            # Get column names
            columns = [col[0] for col in cursor.description]

            # Fetch data
            data = cursor.fetchall()

            # Convert to DataFrame
            df = pd.DataFrame(data, columns=columns)

            # Skip if batch is empty
            if df.empty:
                print("Empty batch, skipping")
                continue

            # Generate filename with batch number
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            if output_format == 'parquet':
                filename = f"{table_name}_country-{country_code}_batch-{batch_num}_{timestamp}.parquet"
                buffer = BytesIO()
                df.to_parquet(buffer, engine='pyarrow', compression='snappy')
                content_type = 'application/octet-stream'
            elif output_format == 'csv-pipe':
                filename = f"{table_name}_country-{country_code}_batch-{batch_num}_{timestamp}.csv"
                buffer = StringIO()
                df.to_csv(buffer, sep='|', index=False)
                content_type = 'text/csv'
            else:  # Default to regular CSV
                filename = f"{table_name}_country-{country_code}_batch-{batch_num}_{timestamp}.csv"
                buffer = StringIO()
                df.to_csv(buffer, index=False)
                content_type = 'text/csv'

            # Upload to S3
            s3_key = s3_prefix + filename

            if isinstance(buffer, StringIO):
                s3.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=buffer.getvalue(),
                    ContentType=content_type
                )
            else:  # BytesIO for parquet
                buffer.seek(0)
                s3.put_object(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    Body=buffer.getvalue(),
                    ContentType=content_type
                )

            end_time = time.time()
            print(f"Batch {batch_num + 1} processed in {end_time - start_time:.2f} seconds")
            print(f"Uploaded to s3://{s3_bucket}/{s3_key}")

    finally:
        cursor.close()
        conn.close()

    print("Export completed successfully!")


if __name__ == "__main__":
    export_to_s3()
