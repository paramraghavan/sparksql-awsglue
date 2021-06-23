# python code to create glue catalog without using crawler
- https://acloud.guru/forums/aws-certified-big-data-specialty/discussion/-LkvnxZXAHoMrkFl-_xl/Manually%20create%20glue%20schema%20without%20crawler

# Glue
- https://docs.aws.amazon.com/cli/latest/reference/glue/create-table.html
- https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/glue_catalog_table
- https://medium.com/capital-one-tech/aws-glue-an-etl-solution-with-huge-potential-91a04a2a0712
- *https://stackoverflow.com/questions/58329935/how-to-create-a-data-catalog-in-amazon-glue-externally*
- ~~https://data.solita.fi/aws-glue-tutorial-with-spark-and-python-for-data-developers/ ~~

# Using the AWS Glue Data Catalog as the metastore for Spark SQL *****
- https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-glue.html

# parquet and pyspark
- https://www.upsolver.com/blog/apache-parquet-why-use **
- https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
  
- https://towardsdatascience.com/the-most-complete-guide-to-pyspark-dataframes-2702c343b2e8 - installing pyspark
- http://pytolearn.csd.auth.gr/b4-pandas/40/moddfcols.html
- https://64lines.medium.com/12-useful-pyspark-functions-for-dataframe-transformations-78504e902906 *
# cast
- https://www.datasciencemadesimple.com/typecast-integer-to-string-and-string-to-integer-in-pyspark/ **
- https://sparkbyexamples.com/pyspark/pyspark-sql-types-datatype-with-examples/
- https://sparkbyexamples.com/spark/spark-cast-string-type-to-integer-type-int/

# Create table over an existing parquet file in glue ***
- https://stackoverflow.com/questions/61056274/create-table-over-an-existing-parquet-file-in-glue
- https://sparkbyexamples.com/spark/spark-read-write-parquet-file-from-amazon-s3/#:~:text=Spark%20Read%20Parquet%20file%20from,file%20we%20have%20written%20before.
- Copy hadoop.dll in folder C:\Users\padma\spark\spark-sql\tools\spark-3.1.2-bin-hadoop2.7 to C:\Windows\System32 directory path
- awswrangler, pip install awswrangler


# glue and jupyter-notebook
- https://towardsai.net/p/programming/pyspark-aws-s3-read-write-operations ***
- https://aws-data-wrangler.readthedocs.io/en/stable/stubs/awswrangler.catalog.create_parquet_table.html *
- https://aws-data-wrangler.readthedocs.io/en/stable/ **
- https://dheerajinampudi.medium.com/getting-started-on-aws-data-wrangler-and-athena-7b446c834076 **

# Spark Performance Tuning – Best Guidelines & Practices
Use DataFrame/Dataset over RDD.
Use coalesce() over repartition()
Use mapPartitions() over map()
Use Serialized data format's.
Avoid UDF's (User Defined Functions)
Caching data in memory.
Reduce expensive Shuffle operations.
Disable DEBUG & INFO Logging.

# Spark Performance Tuning & Best Practices 
https://sparkbyexamples.com/spark/spark-performance-tuning/


# Read csv file and convert it to parquet
spark = SparkSession \
    .builder \
    .appName("Protob Conversion to Parquet") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# read csv
df = spark.read.csv("/temp/proto_temp.csv")

# Displays the content of the DataFrame to stdout
df.show()
df.write.parquet("output/proto.parquet")

# how-spark-read-a-large-file-petabyte-when-file-can-not-be-fit-in-sparks-main
- https://stackoverflow.com/questions/46638901/how-spark-read-a-large-file-petabyte-when-file-can-not-be-fit-in-sparks-main