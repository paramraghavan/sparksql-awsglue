'''
Config driven Spark-SQl example
- Temporary SQL View:
    The input DataFrame is registered as a temporary view (inputTable) which is then used in the SQL transformations.
- Transformation Queries:
    SQL queries are read from the configuration file and executed in sequence. These transformations are written in Spark SQL syntax.
- Write to S3
'''

from pyspark.sql import SparkSession
import json


def apply_transformations(df, spark, config):
    # Apply transformations
    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("inputTable")

    # Apply transformations based on the configuration
    for sql_query in config['transformations']:
        print(sql_query)
        df = spark.sql(sql_query)
        df.createOrReplaceTempView("inputTable")

    return df

def read_config(config_path):
    with open(config_path, 'r') as file:
        return json.load(file)

def main():
    config = read_config('../config/config_sql.json')

    spark = SparkSession.builder.appName("DataTransformer").getOrCreate()

    # Read data from S3 Source
    # df = spark.read.format(config['source']['input_format']) \
    #     .load(config['source']['s3_bucket'])
    # Example DataFrame - replace this with your DataFrame loading code
    df = spark.createDataFrame([(1, 'John', 25, 30000, 'abc way', 'male'), (2, 'Jane', 35, 40000, 'goldy lock way', 'female'), (3, 'Doe', 30, 50000, '123 way', 'male'),
                                (4, 'Jack', 24, 30000, 'abc way', 'male'), (5, 'Dunkin', 35, 40000, 'goldy lock way', 'male'), (6, 'Ram', 33, 50000, '123 way', 'male'),
                                (7, 'Leela', 36, 40000, 'leela  way', 'female')],
                               ["id", "name", "age", "salary", "address", "gender"])

    # Apply transformations
    transformed_df = apply_transformations(df, config)

    transformed_df.show()
    # Write data to S3 target
    # df.write.format(config['target']['output_format']) \
    #     .save(config['target']['s3_bucket'])
    # Close the Spark session
    spark.stop()

# Result:
# SELECT * FROM inputTable WHERE age > 30
# SELECT *, salary * 1.1 as updated_salary FROM inputTable
# SELECT *, CONCAT(name, ' - ', address) as name_address FROM inputTable
# SELECT *, CASE WHEN gender = 'male' THEN 'M' WHEN gender = 'female' THEN 'F' ELSE 'Other' END as gender_short FROM inputTable
# SELECT date_format(current_date(), 'yyyy-MM-dd') as processing_date, * FROM inputTable
# +---------------+---+------+---+------+--------------+------+--------------+--------------------+------------+
# |processing_date| id|  name|age|salary|       address|gender|updated_salary|        name_address|gender_short|
# +---------------+---+------+---+------+--------------+------+--------------+--------------------+------------+
# |     2023-12-20|  2|  Jane| 35| 40000|goldy lock way|female|       44000.0|Jane - goldy lock...|           F|
# |     2023-12-20|  5|Dunkin| 35| 40000|goldy lock way|  male|       44000.0|Dunkin - goldy lo...|           M|
# |     2023-12-20|  6|   Ram| 33| 50000|       123 way|  male|       55000.0|       Ram - 123 way|           M|
# |     2023-12-20|  7| Leela| 36| 40000|    leela  way|female|       44000.0|  Leela - leela  way|           F|
# +---------------+---+------+---+------+--------------+------+--------------+--------------------+------------+
