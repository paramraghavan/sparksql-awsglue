import awswrangler as wr


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

df is the spark dataframe

'''
def create_glue_table_parquet(df, database, table_name, path, partitions_types):

    columns_types = {}
    for item in df.dtypes:
        print(f'name: {item[0]} type: {item[1]}')
        columns_types[item[0]] = item[1]

    keys = partitions_types.keys()
    for key in keys:
        columns_types.pop(key)    

    wr.catalog.create_parquet_table(
        database=database,
        table=table_name,
        path=path,
        columns_types=columns_types,
        partitions_types=partitions_types,
        compression='gzip',  # snappy
        description=table_name + '!'
    )


# .mode("overwrite") \
def write_parquet_to_s3(dataframe, path, partKey):
    dataframe.write \
        .mode("append") \
        .format("parquet") \
        .partitionBy(partKey) \
        .save(compression="gzip", path=path)

import boto3
boto3.__version__
import os, time


def get_boto3_client(service='glue'):
    # os.environ['HTTPS_PROXY'] = 'proxy.xyz.com:12345'
    # os.environ['HTTP_PROXY'] = 'proxy.xyz.com:12345'
    return boto3.client(service)


'''
Partition can be added via spark sql and glue api
'''

'''
Add partition using spark sql
'''

import pyspark

def add_partition_sql(spark_handle, database_name, table_name, partition_keys_in_order):
    #spark.sql('ALTER TABLE sample_db.flights_001 ADD PARTITION ( airlines = "19930", date = 20140401)')
    sql = f'ALTER TABLE {database_name}.{table_name} ADD PARTITION ( {partition_keys_in_order})'
    print(f'sql : {sql}')

    try:
        spark_handle.sql(sql)
    except pyspark.sql.utils.AnalysisException as ex:
        value = str(ex)
        if 'AlreadyExistsException' in value:
            print("Partition Already exists!!, ignoring" )
        else:    
            print("Error adding partition!!" + str(ex))
            raise
    except Exception as x:
        print("Unable to process your query" + str(x))
        raise



'''
    Add partion using glue aPI
    database_name
    table_name
    location, "s3://pp-database/tables/" + table_name
    part_key_values_in_order, "20140401,19690,1"  ## #date=20140401, airlines=19690, flight_number=1
'''
def add_partition(database_name, table_name, location, part_key_values_in_order):
    #create partition, first get table schema
    table_info = get_current_schema(database_name, table_name)
    input_format =  table_info["input_format"]
    output_format = table_info["output_format"]
    serde_info_ser_library = table_info["serde_info"]

    print("input_format: " +  input_format )
    print("output_format: " + output_format)
    print("SerializationLibrary: " + str(serde_info_ser_library))

    location_partition = "s3://pp-database/tables/" + table_name + "/date=20140401"
    location_partition = location
    key_values_input = part_key_values_in_order
    key_values = key_values_input.split(',')

    #create partition
    resp = create_partition(database_name, table_name, location_partition, key_values, input_format, output_format, serde_info_ser_library)
    print('partition creation successful')
    print(resp)
    #print('---')
    return resp


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

def create_partition(database_name, table_name, location_partition, key_values, input_format, output_format, serde_info):
    params = [{
        "Values":key_values,
        "StorageDescriptor": {
                "InputFormat": input_format,
                "Location": location_partition,
                "OutputFormat": output_format,
                "Compressed": False,
                "SerdeInfo": serde_info
            },
        }]

    glue_client = get_boto3_client('glue')

    partition = glue_client.batch_create_partition(
        # CatalogId="12345671",
        DatabaseName=database_name,
        TableName=table_name,
        PartitionInputList=params
    )
    create_partition_response = partition

    return create_partition_response

    