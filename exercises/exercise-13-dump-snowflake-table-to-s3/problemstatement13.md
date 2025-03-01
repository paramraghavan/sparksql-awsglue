i have large snowflake table about 25 to 30 gb, i will like the dump/write the entire table to a give s3 bucket and
prefix location, dump as parquet, comma separated or pipe separated file. the query filter by column country_code. Later
 use pyspark to process this dump from s3 location. Will use aws profile