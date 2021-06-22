# pyspark sparksql-awsglue
Working wiith pyspark/spark-sql and aws glue using jupyter notebook/windows

I happened to attend one of the data conferences few years back when I came to know the power of spark-sql. I have been working with relational databases and sql
for a long time,and I sure there are many developers like me out there who missed the hadoop boat while working on rdbms based projects. I have spend some time scouring the web, reading up on pluralsight, googled into various websites and  cobbled up a spark/sql on jupyter notebook running on windows, so thought it will be useful for folks like me  who want to get into big data via spark-sql.

Spark runs on top of Hadoop, then why do we need Spark? Spark abstracts the hadoop  distributed processing. We work on data as though it were on a single node, but under the hood, the data processing occurs on multiple nodes on your spark cluster. The most interesting thing for me is it allows me to use sql with columnnar storage - parquet, with csv, etc.. and allows me to harness the power of hadoop without going into the details of hadoop/map-reducce.

# AWS Glue Catalog:
WE have tons of data stored in S3, the glue helps us to add a metadata on top our S3 data. You can use glue crawler to perform ETL, to pull the data and create metadata in glue catalog. I like to use boto3 api to create glue metadata in glue catalog to map data in S3. This S3 data can be accessed via sql - using redshift spectrum, accesed via AWS EMR/pyspark/spark-sql, or via  Aws Athena/Mesos(the sql engine), etc.  

It's easy to create table in Glue catalog via boto3, I will try and see if we can access this table mapped on S3 using sql via jupyter notebook running on a local pc - not so sure on this


# Interactive Jupyter Notebook
Under aws-glue there are 2 ipynb files. 
- plain spark-sql 
- aws-glue, this is a work in progress
