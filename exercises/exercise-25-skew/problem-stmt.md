I am joining 2 tables tables using pyspark on AWS/EMR cluster Joining table1 and table2  and this is taking too long to run and is not using all the EMR resources Following are the metrics I have for the join of table1 and table2. Share code/recommendations to speedup the jobs so it can run to completion using all the resources it needs and runs as fast as it can 

1. SPARK CONFIGURATION
Executors: dynamic
Executoe cores: 5
Driver memory: 48G
Shuffle partitions: 4000
Default parallelism:4000
ADE enabled: true
AQE enabled: true
Broadcast threshold:10485760b
2. DATA SIZES
Table1 : 641,107, 683
Table2 : 72, 823, 883 3 DISTINCT KEY ANALYSIS
Distinct Cusip+EFFECTIVEDATE in table1 : 641,107,670
Distinct Cusip+EFFECTIVEDATE in table2: 72,823,883
Ratio table1 rows/ distinct keys): 1.00 
Ratio table2 rows / distinct keys): 1.00
4. PARTITION DISTRIBUTION
Table1  partitions: 4000
Min rows per partition: 0
Max rows per partition: 570,140,815
Avg rows per partition: 160,277
Std dev: 9,047,970
Skew factor (max/avg) : 3557.22
SEVERE PARTITION SKEN DETECTED!