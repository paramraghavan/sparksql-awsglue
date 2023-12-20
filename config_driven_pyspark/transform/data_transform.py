from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import json


def read_config(config_path):
    with open(config_path, 'r') as file:
        return json.load(file)

# Define a sample configuration

# config = {
#     "transformations": [
#         {"type": "filter", "condition": "age > 30"},
#         {"type": "drop_columns", "columns": ["unnecessary_column"]},
#         {"type": "rename_column", "old_name": "oldName", "new_name": "newName"},
#         {"type": "add_column", "column_name": "new_column", "default_value": "value"},
#         {"type": "change_data_type", "column_name": "age", "new_data_type": "string"},
#         {"type": "custom_transformation", "udf": "your_custom_function"}
#     ]
# }

def apply_transformations(df, config):
    # Apply transformations
    for transformation in config['transformations']:
        if transformation['type'] == "filter":
            df = df.filter(transformation['condition'])
        elif transformation['type'] == "drop_columns":
            df = df.drop(*transformation['columns'])
        elif transformation['type'] == "rename_column":
            df = df.withColumnRenamed(transformation['old_name'], transformation['new_name'])
        elif transformation['type'] == "add_column":
            df = df.withColumn(transformation['column_name'], lit(transformation['default_value']))
        elif transformation['type'] == "change_data_type":
            df = df.withColumn(transformation['column_name'],
                               col(transformation['column_name']).cast(transformation['new_data_type']))
        elif transformation['type'] == "custom_transformation":
            df = df.withColumn("new_column", your_custom_function(col("some_column")))

    return df


def apply_join_transform(spark, config):
    # Load datasets from the specified paths
    left_df = spark.read.format("parquet").load(config['join']['left_dataset'])
    right_df = spark.read.format("parquet").load(config['join']['right_dataset'])

    # Perform the join
    joined_df = left_df.join(
        right_df,
        left_df[config['left_keys'][0]] == right_df[config['right_keys'][0]],
        config['type']
    )
    # Selecting specific columns if required
    if 'columns' in config['join]']:
        joined_df = joined_df.select(config['join']['columns'])

    return joined_df


def main():
    config = read_config('../config/config.json')

    spark = SparkSession.builder.appName("DataTransformer").getOrCreate()

    # Read data from S3 Source
    # df = spark.read.csv(f"s3://{config['source']['bucket_name']}/{config['source']['file_path']}")
    # Example DataFrame - replace this with your DataFrame loading code
    df = spark.createDataFrame([(25, 'John', 30000), (35, 'Jane', 40000), (30, 'Doe', 50000)],
                               ["age", "oldName", "salary"])

    # Register UDF
    spark.udf.register("your_custom_function", your_custom_function)

    # Apply transformations
    transformed_df = apply_transformations(df, config)

    # Write data to S3 Target
    transformed_df.write.csv(f"s3://{config['target']['bucket_name']}/{config['target']['file_path']}")

    joined_df = apply_join_transform(config);

    # Close the Spark session
    spark.stop()


# Define a sample UDF - replace with your actual function
def your_custom_function(col):
    return col * 2

if __name__ == "__main__":
    main()
