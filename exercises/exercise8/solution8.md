Using boto3 query AWS Athena

Here we are querying S3 access cloudtrail logs.

```python
import boto3
import time
from datetime import datetime


def query_s3_logs(database, table, request_param_value):
    athena_client = boto3.client('athena')

    # Query to filter S3 logs for Get and Put operations
    query = f"""
    SELECT 
        eventtime,
        eventname,
        awsregion,
        sourceipaddress,
        useragent,
        requestparameters
    FROM {database}.{table}
    WHERE eventname IN ('GetObject', 'PutObject')
    AND requestparameters LIKE '%{request_param_value}%'
    LIMIT 100
    """

    # Start the query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://your-query-results-bucket/athena-results/'  # Replace with your bucket
        }
    )

    query_execution_id = response['QueryExecutionId']

    # Wait for query to complete
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response['QueryExecution']['Status']['State']

        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

        time.sleep(1)

    if state != 'SUCCEEDED':
        raise Exception(f"Query failed with state: {state}")

    # Get query results
    results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Process and format results
    formatted_rows = []
    headers = [col['Label'] for col in results['ResultSet']['ResultMetadata']]

    # Skip the header row and process data rows
    for row in results['ResultSet']['Rows'][1:]:
        values = [field.get('VarCharValue', '') for field in row['Data']]
        formatted_rows.append(','.join(values))

    return formatted_rows


def main():
    # Configuration
    DATABASE = 'your_database_name'  # Replace with your Athena database name
    TABLE = 'your_table_name'  # Replace with your table name
    REQUEST_PARAM_VALUE = 'your-bucket-name'  # Replace with value to filter requestParameters

    try:
        results = query_s3_logs(DATABASE, TABLE, REQUEST_PARAM_VALUE)

        # Print or process results
        for row in results:
            print(row)

    except Exception as e:
        print(f"Error occurred: {str(e)}")


if __name__ == '__main__':
    main()

```

To use this script, you'll need to:

1. Replace the placeholders:
    - `your-query-results-bucket` with your S3 bucket for Athena query results
    - `your_database_name` with your Athena database name
    - `your_table_name` with your table name
    - `your-bucket-name` with the value you want to filter in requestParameters

2. Ensure you have:
    - AWS credentials configured
    - Required IAM permissions for Athena and S3
    - boto3 installed (`pip install boto3`)

The script will:

1. Connect to AWS Athena
2. Execute a query filtering for GetObject and PutObject events
3. Wait for the query to complete
4. Return results as a list of comma-separated strings

Each row will contain:

- eventtime
- eventname
- awsregion
- sourceipaddress
- useragent
- requestparameters
