- [python code to create glue catalog without using crawler](https://acloud.guru/forums/aws-certified-big-data-specialty/discussion/-LkvnxZXAHoMrkFl-_xl/Manually%20create%20glue%20schema%20without%20crawler)

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
- https://luminousmen.com/post/schema-on-read-vs-schema-on-write **

# glue DynamicFrame, DataFrame. RDD
- https://aws-blog.de/2021/06/what-i-wish-somebody-had-explained-to-me-before-i-started-to-use-aws-glue.html


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

# spark caching
- https://medium.com/swlh/caching-spark-dataframe-how-when-79a8c13254c0
- https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-caching-and-persistence.html
- https://stackoverflow.com/questions/26870537/what-is-the-difference-between-cache-and-persist

Use Case 1
-------------

I have a spark data frame in a AWS glue job with 4 million records
I need to write it as a SINGLE parquet file in AWS s3
- Current code

file_spark_df.write.parquet("s3://"+target_bucket_name)
Issue the above code creates 100+ files each 17.8 to 18.1 MB in size , guess its some default break down size

- Question : How do I create just one file ? for one spark data frame ?

Use coalesce(1) to write into one file : file_spark_df.coalesce(1).write.parquet("s3_path")

Coalesce vs repartition
-----------------------------
<pre>
Difference between coalesce and repartition

coalesce uses existing partitions to minimize the amount of data that's shuffled.
repartition creates new partitions and does a full shuffle. coalesce results in 
partitions with different amounts of data (sometimes partitions that have much different sizes) 
and repartition results in roughly equal sized partitions.

Is coalesce or repartition faster?

coalesce may run faster than repartition, but unequal sized partitions are
generally slower to work with than equal sized partitions. You'll usually need to repartition datasets
after filtering a large data set. I've found repartition to be faster overall because Spark is 
built to work with equal sized partitions.
ref: https://stackoverflow.com/questions/31610971/spark-repartition-vs-coalesce
</pre>
