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


ref: https://kontext.tech/column/spark/381/schema-merging-evolution-with-parquet-in-spark-and-hive
