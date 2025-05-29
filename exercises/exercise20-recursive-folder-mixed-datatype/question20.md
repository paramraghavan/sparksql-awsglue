**Recursive folder read with mixed datatype**

I have folder which has subfolders with parquet files written using emr/pyspark model_out root folder | |—iter_1 -
subfolder with parquet files | |—iten n . . |—iter 15 - subfolder with parquet files

Typically data reads from iter_1 is empty and other sub folders are not empty

So used spark.read option recursive - for sub folders and mergeschema to overcome empty dataset for iter_1 df =
spark.read.option("recursiveFileLookup", "true")
.option("mergeSchema", "true").parquet(“model_out/")
When I run the above command I get error compatible data types. Double to Long or Double to INT All the parquet files
have same number of columns and column names. Looks like there is a mismatch in data type> Is it possible to convert all
the columns on read to String data type or some sort of common denominator such that it works for all sub folders

Note that iter_1 folder is most of the time empty dataset so do I have ro use merge scheme or any other way around.
typically Iter_1 sub folder has parquet file but it has not rows in it 