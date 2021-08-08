from pyspark import SparkContext, SparkConf
# ref: https://towardsai.net/p/programming/pyspark-aws-s3-read-write-operations
#spark configuration
conf = SparkConf().set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true'). \
 set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true'). \
 setAppName('pyspark_aw_glue').setMaster('local[*]')

sc=SparkContext(conf=conf)
sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

# read aws credentials
import configparser
config = configparser.ConfigParser()
config.read_file(open(r'/root/.aws/credentials'))

accessKeyId= config['default']['AWS_ACCESS_KEY_ID']
secretAccessKey= config['default']['AWS_SECRET_ACCESS_KEY']

hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('fs.s3a.endpoint', 's3.amazonaws.com')
hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

print(sc)
from pyspark.sql import SparkSession
spark=SparkSession(sc)
spark.sql("use sample_db")
#spark.sql("select * from  flights where airlines < 20000").show()

# STEP 1
# Read from flighta table airlines > 20000 and convert date to int data type
#####################################################
from pyspark.sql.functions import col
flight_df = spark.read.csv('s3a://bucket/flights.csv',header=True,inferSchema=True).filter(col("airlines") > 20000)
flight_df.printSchema
flight_df.schema
flight_df.dtypes

# example of user defined functions, udf
# from pyspark.sql.functions import udf
# remove_dashes = udf(  lambda  x:  x.replace('-', ''))
# flight_df= flight_df.withColumn("date1", remove_dashes(flight_df.date))

import pyspark.sql.functions as F
flight_df= flight_df.withColumn("date1", F.expr("replace(date, '-', '')"))
flight_df= flight_df.withColumn("date1", F.expr("replace(date1, ' 00:00:00', '')"))

from pyspark.sql.types import IntegerType
flight_df = flight_df.withColumn("date1",flight_df["date1"].cast(IntegerType()))
flight_df.select("date1").dtypes
flight_df = flight_df.drop("date")
flight_df = flight_df.withColumnRenamed("date1","date") 
flight_df = flight_df.limit(1500)

######################################################
# this read to add partitions
smry_flight_df = flight_df.groupBy('airlines', 'date').count()
#################################################################


#Create flight_001 table, only once
#create table in glue catalog

# partitions_types = { 'airlines': 'string',\
#                     'date': 'int',\
#                     'flight_number': 'string' }

partitions_types = { 'airlines': 'string',\
                    'date': 'int'}

glue_helper.create_glue_table_parquet(flight_df, 'sample_db', 'flights_001', 's3://bucket/tables/flights_001', partitions_types)
######################################################################################


import importlib
importlib.reload(glue_helper)

# write as parquet file to s3 airlines > 20000, using default mode, append
from glue import helper as glue_helper
glue_helper.write_parquet_to_s3(flight_df, 's3a://bucket/tables/flights_001', ['airlines', 'date'])

# Add partitions partitions, 
data_collect = smry_flight_df.collect()
for row in data_collect:
    # while looping through each
    # row printing the data of airlines, date
    print(row["airlines"],row["date"])
    airlines = row["airlines"]
    date = row["date"]
    partition_keys_in_order = f' airlines = "{airlines}", date={date} '  
    glue_helper.add_partition_sql(spark,'sample_db', 'flights_001', partition_keys_in_order)

#partition_keys_in_order = f' airlines = "19930", date=20140401 '                 
#glue_helper.add_partition_sql(spark, 'sample_db', 'flights_001', partition_keys_in_order)


# add partitons - via alter table or glue api
#spark.sql('ALTER TABLE sample_db.flights_001 ADD PARTITION ( airlines = "20409", date = 20140401)')
#spark.sql('ALTER TABLE sample_db.flights_001 ADD PARTITION ( airlines = "19805", date = 20140401)')
#spark.sql('ALTER TABLE sample_db.flights_001 ADD PARTITION ( airlines = "19930", date = 20140401)')


# read this flight_001 table
df1 = spark.sql('select distinct airlines from  sample_db.flights_001')
df1.show()


# STEP2

# write as parquet file to s3 airlines < 20000
# add partitions
# read this flight_001 table
# u shoudl see 19000 and 20000






# smry_flight_df.foreach(f)
# smry_flight_df.count()



# AlreadyExistsException

spark.sql('Refresh table sample_db.flights_001')
df1 = spark.sql('select * from  sample_db.flights_001')

df1 = spark.sql('select distinct airlines from  sample_db.flights_001')





