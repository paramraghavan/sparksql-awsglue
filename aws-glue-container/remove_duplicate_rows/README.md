# Drop Duplicates

## Problem
I have a large dataframe which I created with 800 partitions.
df.rdd.getNumPartitions() --> returns 800
When we use dropDuplicates on the dataframe, it changes the partitions to default 200

df = df.dropDuplicates()
df.rdd.getNumPartitions() --> returns 200

This behaviour causes problem for me, as it will lead to out of memory as the number of partition goes down from
 800 to 200

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

ref: [Drop Duplicates](https://sparkbyexamples.com/pyspark/pyspark-distinct-to-drop-duplicates/)

