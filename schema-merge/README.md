# Schema-merge

Schema merge(evolution) is supported by many frameworks or data serialization systems such as Avro, Orc,
Protocol Buffer and Parquet. With schema evolution, one set of data can be stored in multiple files
with different but compatible schema. In Spark, Parquet data source can detect and merge schema of
those files automatically. Without automatic schema merging, the typical way of handling schema
evolution is through historical data reload that requires lot's of work.

> Note
Schema merge is turned off by default starting from Spark 1.5.0 as it is a relatively expensive operation. To enable it,
we can set mergeSchema option to true or set global SQL option spark.sql.parquet.mergeSchema to true.



ref: https://kontext.tech/column/spark/381/schema-merging-evolution-with-parquet-in-spark-and-hive