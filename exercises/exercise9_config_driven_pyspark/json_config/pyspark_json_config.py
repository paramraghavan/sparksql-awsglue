from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import json


def create_schema(fields):
    return StructType([
        StructField(field["name"],
                    globals()[field["type"].capitalize() + "Type"](),
                    True)
        for field in fields
    ])


def parse_json_config(config_path):
    with open(config_path, 'r') as config_file:
        return json.load(config_file)


def process_table(df, table_config, parent_keys=None):
    schema = create_schema(table_config["fields"])

    if parent_keys:
        for key in parent_keys:
            df = df.withColumn(key, col(f"$.{key}"))

    parsed_df = df.select(
        *[col(field["path"].split(".")[-1]).alias(field["name"])
          for field in table_config["fields"]]
    )

    if "nested_tables" in table_config:
        for nested_table in table_config["nested_tables"]:
            nested_df = df.select(
                *parent_keys,
                explode(col(nested_table["path"])).alias("nested_data")
            )
            nested_parsed_df = process_table(
                nested_df.select(*parent_keys, "nested_data"),
                nested_table,
                parent_keys + [table_config["fields"][0]["name"]]
            )
            yield (nested_table["name"], nested_parsed_df)

    yield (table_config["name"], parsed_df)


def main():
    spark = SparkSession.builder.appName("ConfigDrivenJsonParser").getOrCreate()

    config = parse_json_config("config.json")

    # Read the JSON file
    json_df = spark.read.json("../customer_order_ordelinitem.json")

    # Process the main table and nested tables
    for table_name, table_df in process_table(json_df, config):
        # Write the DataFrame to a database table
        table_df.write.mode("overwrite").saveAsTable(table_name)

    spark.stop()


if __name__ == "__main__":
    main()