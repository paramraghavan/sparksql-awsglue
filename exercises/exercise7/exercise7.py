from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
import json
import yaml


def create_schema(field_config):
    fields = []
    for field in field_config:
        if field['type'] == 'string':
            data_type = StringType()
        elif field['type'] == 'double':
            data_type = DoubleType()
        elif field['type'] == 'integer':
            data_type = IntegerType()
        elif field['type'] == 'array':
            data_type = ArrayType(create_schema(field['fields']))
        else:
            raise ValueError(f"Unsupported type: {field['type']}")

        fields.append(StructField(field['name'], data_type, True))

    return StructType(fields)


def parse_json(spark, json_data, config):
    # Create a DataFrame from the JSON data
    df = spark.read.json(spark.sparkContext.parallelize([json_data]))

    # Create the schema based on the configuration
    schema = create_schema(config['fields'])

    # Parse the nested JSON structure
    for field in config['fields']:
        if field['type'] == 'array':
            df = df.withColumn(field['name'],
                               explode(from_json(col(field['path']), ArrayType(create_schema(field['fields'])))))
        else:
            df = df.withColumn(field['name'], col(field['path']))

    # Select only the fields defined in the configuration
    selected_columns = [field['name'] for field in config['fields']]
    df = df.select(*selected_columns)

    return df


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("ConfigDrivenJsonParser").getOrCreate()

    # Load configuration from YAML file
    with open('exercise7.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)

    # Load sample JSON data
    with open('exercise6.json', 'r') as data_file:
        json_data = json.load(data_file)

    # Parse the JSON data
    result_df = parse_json(spark, json_data, config)

    # Show the result
    result_df.show(truncate=False)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()