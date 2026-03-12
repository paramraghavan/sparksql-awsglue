spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

```python


# # If the process function joins on 'user_id'
# # force a repartition on the column that the function uses for joining or grouping.
# input_df = input_df.repartition(2000, "user_id")
# process(input_df)
# # Force the data to be spread out AND saved in that state
# input_df = input_df.repartition(2000, f.rand()).persist()
# # Now call your function
# process(input_df)


from pyspark import StorageLevel

# 1. Shuffle into 2000 even pieces
# 2. Save it to the local NVMe/SSD disks on your EMR instances
input_df_ready = input_df.repartition(2000, f.rand()).persist(StorageLevel.DISK_ONLY)

# 3. Trigger the persist so it's ready before the function starts
input_df_ready.count() 

# 4. Now pass it to the "black box" function
process(input_df_ready)
```

Configuration,Value,Why?
spark.sql.shuffle.partitions,2000,Handles the 295GB shuffle volume.
spark.sql.adaptive.enabled,true,Allows Spark to fix its own plan at runtime.
spark.memory.offHeap.enabled,true,Reduces Garbage Collection (GC) pauses during long jobs.
spark.memory.offHeap.size,4g,Extra buffer for shuffle operations.
spark.rpc.message.maxSize,1024,"Prevents ""Frame size too large"" errors with 2000+ partitions."


# EMR Setting: Set your Minimum Nodes to 20 so the cluster stops shrinking while it's in the middle of that massive shuffle.