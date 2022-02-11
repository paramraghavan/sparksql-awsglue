# Schema-merge

Schema merge(evolution) is supported by many frameworks or data serialization systems such as Avro, Orc,
Protocol Buffer and Parquet. With schema evolution, one set of data can be stored in multiple files
with different but compatible schema. In Spark, Parquet data source can detect and merge schema of
those files automatically. Without automatic schema merging, the typical way of handling schema
evolution is through historical data reload that requires lot's of work.

> Note
Schema merge is turned off by default starting from Spark 1.5.0 as it is a relatively expensive operation. To enable it,
we can set mergeSchema option to true or set global SQL option spark.sql.parquet.mergeSchema to true.

- DataFrame 1
<pre>
A dataframe df1 is created with the following attributes:

Schema version 0

1) id bigint
2) attr0 string

df1 is saved as parquet format in data/partition-date=2020-01-01.
</pre>

- Dataframe 2, a new dataframe df2 is added attr1
<pre>
Schema version 1
1) id bigint
2) attr0 string
3) attr1 string

Compared with schema version 0, one new attribute attr1 is added. df2 is saved as parquet format in data/partition-date=2020-01-02.
</pre>

- Dataframe 3,  a new dataframe df3 is created with attr0 removed:
<pre>
Schema version 2
1) id bigint
2) attr1 string

The data is saved as parquet format in data/partition-date=2020-01-03.
</pre>

- The Spark application will need to read data from these three folders with schema merging

- [How to handle the schema change with no data type conflict](https://medium.com/@11amitvishwas/how-to-handle-the-schema-change-1e3965e9bcbe)
- [Spark merge dataframe with mismatching schemas](https://stackoverflow.com/questions/39869084/spark-merge-dataframe-with-mismatching-schemas-without-extra-disk-io)
- Scenario merge table 1 and table2, where table2 has new column added:

  - table1, for example Emp1
    ![img.png](img.png)

  - table2, for example Emp2
    ![img_4.png](img_4.png)

  - write table, Emp1 to table anmed DyanmicTable
    ![img_8.png](img_8.png)

  - merge Emp1 and  Emp2
    ![img_7.png](img_7.png)

  - View merged table
    ![img_3.png](img_3.png)

  If the merge can't take place because the two dataframes share a column with conflicting type or nullability, then the right thing is to
  raise a TypeError (because that's a conflict you probably want to know about).
  - https://sparkbyexamples.com/spark/spark-merge-two-dataframes-with-different-columns/
     PySpark examples of how to merge two DataFrames with different columns can be done by adding
    missing columns to the DataFrame’s and finally union them using unionByName()
  - [merge dataframe with differnt columns](https://stackoverflow.com/questions/68844904/merge-two-spark-dataframes-with-different-columns-to-get-all-columns)


ref: https://kontext.tech/column/spark/381/schema-merging-evolution-with-parquet-in-spark-and-hive


# Drop Duplicates
- [Drop Duplicates](https://sparkbyexamples.com/pyspark/pyspark-distinct-to-drop-duplicates/)

- Problem
I have a large dataframe which I created with 800 partitions.
df.rdd.getNumPartitions() --> returns 800
When we use dropDuplicates on the dataframe, it changes the partitions to default 200

df = df.dropDuplicates()
df.rdd.getNumPartitions() --> returns 200

This behaviour causes problem for me, as it will lead to out of memory as the number of partition goes down from 800 to 200

- Fix:
This happens because dropDuplicates requires a shuffle. If you want to get a specific number of partitions you
should set spark.sql.shuffle.partitions (its default value is 200)
<pre>
    df = sc.parallelize([("a", 1)]).toDF()
    df.rdd.getNumPartitions()
    ## 8

    df.dropDuplicates().rdd.getNumPartitions()
    ## 200

    sqlContext.setConf("spark.sql.shuffle.partitions", "800")

    df.dropDuplicates().rdd.getNumPartitions()
    ## 800
</pre>


