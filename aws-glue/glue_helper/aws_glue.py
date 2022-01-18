import boto3

boto3.__version__msck
import os, time


def get_boto3_client(service='glue'):
    # os.environ['HTTPS_PROXY'] = 'proxy.xyz.com:12345'
    # os.environ['HTTP_PROXY'] = 'proxy.xyz.com:12345'
    return boto3.client(service)


import awswrangler as wr


def write_parquet_to_s3(dataframe, path, partKey):
    dataframe.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy(partKey) \
        .save(compression="gzip", path=path)


'''    
 'sample_db', 'flights', 's3://pp-database/tables/flights',
     columns_types= {'origin': 'string', 'destination': 'string', 
                   'departure': 'string', 'departure_delay': 'string',
                   'arrival': 'string', 'arrival_delay': 'string',
                   'air_time': 'string', 'distance': 'string'
                   }
    partitions_types={'airlines': 'string', 'date': 'int', 'flight_number': 'string'}
'''
'''
Creates table metadata in glue catalog
'''


def create_glue_table(database, table_name, path, columns_types, partitions_types):
    wr.catalog.create_parquet_table(
        database=database,
        table=table_name,
        path=path,
        columns_types=columns_types,
        partitions_types=partitions_types,
        compression='gzip',  # snappy
        description=table_name + '!'
    )


def get_databases():
    """
    Returns the databases available in the Glue data catalog

    :return: list of databases
    """
    return [dat["Name"] for dat in get_boto3_client('glue').get_databases()["DatabaseList"]]


def get_tables_for_database(database):
    """
    Returns a list of tables in a Glue database catalog

    :param database: Glue database
    :return: list of tables
    """
    starting_token = None
    next_page = True
    tables = []
    while next_page:
        paginator = get_boto3_client('glue').get_paginator(operation_name="get_tables")
        response_iterator = paginator.paginate(
            DatabaseName=database,
            PaginationConfig={"PageSize": 100, "StartingToken": starting_token},
        )
        for elem in response_iterator:
            tables += [
                {
                    "name": table["Name"],
                }
                for table in elem["TableList"]
            ]
            try:
                starting_token = elem["NextToken"]
            except:
                next_page = False
    return tables


def create_table_helper():
    columns = ['origin', 'destination', 'departure', 'departure_delay', 'arrival',
               'arrival_delay', 'air_time', 'distance']
    partition_key = ['airlines', 'date', 'flight_number']

    columns_array = []
    for item in columns:
        col = {"Name": item, "Type": "string"}
        columns_array.append(col)

    partition_keys_array = []
    for item in partition_key:
        if item == 'date':
            col = {"Name": item, "Type": "int"}
        else:
            # col = {"Name": item, "Type": "int"}
            col = {"Name": item, "Type": "string"}

        partition_keys_array.append(col)

    create_table('sample_db', 'airlines', 'parquet airline table', "s3://pp-database/sampledb/flights",
                 columns_array, partition_keys_array)


# https://docs.aws.amazon.com/glue/latest/webapi/API_CreateTable.html
# https://boto3.amazonaws.com/v1/documentation/api/1.9.185/reference/services/glue.html#Glue.Client.create_table
# "TableType" : "EXTERNAL_TABLE"
def create_table(database_name, table_name, comment, location, columns, partition_keys):
    params = {
        # "CatalogId": "232456781",
        "DatabaseName": database_name,
        "TableInput": {
            "Name": table_name,
            "Description": comment,
            "StorageDescriptor": {
                "Columns": columns,
                "Location": location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "name": "my-stream",
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {
                        "serialization.format": "1"
                    }
                }
            },
            "PartitionKeys": partition_keys,
            "TableType": "EXTERNAL_TABLE"
        }
    }
    # params['TableInput'].update({'Name': table_name})
    # params['DatabaseName'].update({'Name': database_name})

    glue_client = get_boto3_client('glue')
    resp = glue_client.create_table(**params)
    print(resp)


def get_current_schema_partition(database_name):
    resp = get_tables_for_database(database_name)
    # print(resp)
    for dict in resp:
        # print(dict['name'])
        table_name = dict['name']
        # if table_name.startswith('hao_')
        if True:
            response = get_boto3_client('glue').get_table(
                DatabaseName=database_name,
                Name=table_name)
            # print( 'Tablename/Partition/TableType {}/{}/{}'.format(table_name,response['Table']['PartitionKeys'],response['Table']['TableType']))
            # print('{} {}'.format(table_name, response['Table']['PartitionKeys'][0]['Name']))
            col_val = ''
            for col in response['Table']['PartitionKeys']:
                col_val += col['Name'] + ' '
            print('{} {}'.format(table_name.upper(), col_val.lower()))


def get_current_schema(database_name, table_name):
    response = get_boto3_client('glue').get_table(
        DatabaseName=database_name,
        Name=table_name)

    table_data = {}
    table_data['input_format'] = response['Table']['StorageDescriptor']['InputFormat']
    table_data['output_format'] = response['Table']['StorageDescriptor']['OutputFormat']
    table_data['table_location'] = response['Table']['StorageDescriptor']['Location']
    table_data['serde_info'] = response['Table']['StorageDescriptor']['SerdeInfo']
    table_data['partition_keys'] = response['Table']['PartitionKeys']

    return table_data


def create_partition(database_name, table_name, location_partition, key_values, input_format, output_format,
                     serde_info):
    params = [{
        "Values": key_values,
        "StorageDescriptor": {
            "InputFormat": input_format,
            "Location": location_partition,
            "OutputFormat": output_format,
            "Compressed": False,
            "SerdeInfo": serde_info
        },
    }]

    # params['TableInput'].update({'Name': table_name})
    # params['DatabaseName'].update({'Name': database_name})

    glue_client = get_boto3_client('glue')

    create_partition_response = glue_client.batch_create_partition(
        # CatalogId="23456678999",
        DatabaseName=database_name,
        TableName=table_name,
        PartitionInputList=params
    )

    # resp =glue_client.create_partition(**params)
    # print(create_partition_response)
    return create_partition_response


def add_partition(database_name, table_name, location, part_key_values_in_order):
    # create partition, first get table schema
    table_info = get_current_schema(database_name, table_name)
    input_format = table_info["input_format"]
    output_format = table_info["output_format"]
    serde_info_ser_library = table_info["serde_info"]

    print("input_format: " + input_format)
    print("output_format: " + output_format)
    print("SerializationLibrary: " + str(serde_info_ser_library))

    location_partition = location
    # key_values_input = "20200630"
    key_values_input = part_key_values_in_order
    key_values = key_values_input.split(',')

    # create partition
    resp = create_partition(database_name, table_name, location_partition, key_values, input_format, output_format,
                            serde_info_ser_library)
    print('partition creation successful')
    print(resp)
    # print('---')
    return resp


'''
WIP 
'''


def apply_msck_repair(database, table):
    # Athena query part
    client = get_boto3_client('athena')
    # client = boto3.client('athena', region_name='us-east-2')
    data_catalog_table = table
    db = database  # glue data_catalog db, not Postgres DB
    # this supposed to update all partitions for data_catalog_table, so glue job
    # can upload new file data partition added to S3 into DB
    q = "MSCK REPAIR TABLE " + data_catalog_table
    # output of the query goes to s3 file normally
    output = "s3://pp-query-results/results/"
    response = client.start_query_execution(
        QueryString=q,
        QueryExecutionContext={
            'Database': db
        },
        ResultConfiguration={
            'OutputLocation': output,
        }
    )

    return response


if __name__ == '__main__':
    # resp = get_databases()
    database = 'sample_db'
    table_name = 'flights'

    # resp = get_current_schema(database, table_name )
    # print(resp)

    # create_table_partition(database, table_name)

    # resp = get_tables_for_database(database)
    # print(resp)
    # for dict in resp:
    #     print(dict['name'])

    get_current_schema_partition(database)
