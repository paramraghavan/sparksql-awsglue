import pandas as pd
import snowflake.connector

# pip install snowflake-connector-python pandas
# Snowflake connection parameters
conn_params = {
    "user": 'YOUR_USERNAME',
    "password": 'YOUR_PASSWORD',
    "account": 'YOUR_ACCOUNT_IDENTIFIER',
    "warehouse": 'YOUR_WAREHOUSE',
    "database": 'YOUR_DATABASE',
    "schema": 'YOUR_SCHEMA'
}

# List of tables to process
tables = ['table1', 'table2', 'table3']

# Create a connection to Snowflake
conn = snowflake.connector.connect(**conn_params)
cursor = conn.cursor()

try:
    for table_name in tables:
        # Query 100 records from each table
        query = f"SELECT * FROM {table_name} LIMIT 100;"
        cursor.execute(query)

        # Fetch the result into DataFrame
        # df = cursor.fetch_pandas_all()
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[x[0] for x in cursor.description])

        # Write DataFrame to a pipe-delimited text file
        output_file = f"{table_name}.dat"
        df.to_csv(output_file, sep='|', header=False, index=False)
        print(f"Data for {table_name} written to {output_file}")
finally:
    cursor.close()
    conn.close()
