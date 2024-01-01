"""
Process and analyze the logs using PySpark to extract insights.
Index the processed logs into ElasticSearch for easy querying and visualization.

pip install pyspark
pip install elasticsearch

"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def readLogs(spark, path) :
  # Define a schema for the logs (assuming Apache log format)
  schema = "..."
  
  # Apply schema and create a DataFrame
  logs_df = spark.read.format("csv").schema(schema).load("path_to_log_file.log")
  
  # Example processing: Count requests by response code
  response_code_counts = logs_df.groupBy("response_code").count()
  response_code_counts.show()

  return logs_df

from elasticsearch import Elasticsearch

# Function to index a row to ElasticSearch
def index_to_es(row, index_value):
 """
   Following command takes a row from a PySpark DataFrame, converts it into a dictionary, and then indexes it in Elasticsearch under the 
   specified index with a unique ID. This is typically used in a loop or a DataFrame operation to index multiple rows/documents 
   into Elasticsearch for storage, search, and analysis.
  * es.index(...): This is a method call to the Elasticsearch Python client. The index method is used to add or update a document in an Elasticsearch index.
  * index="log_index": Specifies the name of the Elasticsearch index where the document is to be stored. In this case, "log_index" is the name of the index.
    If this index doesn't exist, Elasticsearch will create it automatically.
  * id=row["id"]: This part sets the unique identifier for the document within the index. Here, row["id"] implies that the document's ID is being taken 
    from the id field of the current row in the DataFrame. This is important for identifying the document uniquely in the Elasticsearch index and can be 
    used for updating or deleting the document at a later stage.
  * body=row.asDict(): The body parameter contains the actual data of the document to be indexed. row.asDict() converts the current row of the DataFrame 
    into a Python dictionary. Each key-value pair in this dictionary corresponds to a column name and its value in the row.
  """
  es.index(index=index_value, id=row["id"], body=row.asDict())

def index_data_into_elasticsearch(es, logs_df):
  """
  Add index into elasticsearch
  """

  # Apply the function to each row in DataFrame
  logs_df.foreach(lambda row: index_to_es(row, "log_index"))  


def index_data_into_elasticsearch_using_elasticsearch_hadoop_conenctor(df):
  # Assuming Elasticsearch is running and accessible
  # Add index into elasticsearch
  
  # Required configuration for Elasticsearch
  es_write_conf = {
      "es.nodes": "localhost", # Elasticsearch server
      "es.port": "9200", # Elasticsearch port
      "es.resource": "log_index", # Elasticsearch index/type
      "es.input.json": "yes"
  }
  
  # Write DataFrame to Elasticsearch
  df.write.format("org.elasticsearch.spark.sql").options(**es_write_conf).save()
  


def query_print_result(col_name, col_value):
  """
  # Define your search query
  query = {
      "query": {
          "match_all": {}  # This is a simple query that matches all documents
      }
  }
  """
  query = {
    "query": {
        "match": {
            col_name: col_value
        }
    }
  }

# Execute the search query against the 'log_index' index
response = es.search(index="log_index", body=query)

# Print the results
for hit in response['hits']['hits']:
    print(hit["_source"])  # This prints the source of each document returned





def main():
  spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()
  df = readLogs("path_to_log_file.log")

  # Initialize Elasticsearch client
  es = Elasticsearch()
  index_data_into_elasticsearch(es, df)

  query_print_result("column_name", "col_value")
  
  # Close the Spark session
  spark.stop()
