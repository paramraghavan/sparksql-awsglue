# config.yaml
main_table: customers
fields:
  - name: id
    path: $.id
    type: string
  - name: name
    path: $.name
    type: string
  - name: email
    path: $.email
    type: string
  - name: street
    path: $.address.street
    type: string
  - name: city
    path: $.address.city
    type: string
  - name: state
    path: $.address.state
    type: string
  - name: zip
    path: $.address.zip
    type: string

nested_tables:
  - name: orders
    path: $.orders
    fields:
      - name: customer_id
        path: $.id
        type: string
      - name: order_id
        path: $.order_id
        type: string
      - name: date
        path: $.date
        type: date
      - name: total
        path: $.total
        type: double
    nested_tables:
      - name: order_items
        path: $.items
        fields:
          - name: order_id
            path: $.order_id
            type: string
          - name: product_id
            path: $.product_id
            type: string
          - name: name
            path: $.name
            type: string
          - name: quantity
            path: $.quantity
            type: integer
          - name: price
            path: $.price
            type: double

# pyspark_yaml_parser.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, explode, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import yaml

def create_schema(fields):
    return StructType([
        StructField(field["name"], 
                    globals()[field["type"].capitalize() + "Type"](), 
                    True)
        for field in fields
    ])

def parse_yaml_config(config_path):
    with open(config_path, 'r') as config_file:
        return yaml.safe_load(config_file)

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
    spark = SparkSession.builder.appName("ConfigDrivenYamlParser").getOrCreate()
    
    config = parse_yaml_config("config.yaml")
    
    # Read the JSON file
    json_df = spark.read.json("path/to/your/json/file.json")
    
    # Process the main table and nested tables
    for table_name, table_df in process_table(json_df, config):
        # Write the DataFrame to a database table
        table_df.write.mode("overwrite").saveAsTable(table_name)
    
    spark.stop()

if __name__ == "__main__":
    main()