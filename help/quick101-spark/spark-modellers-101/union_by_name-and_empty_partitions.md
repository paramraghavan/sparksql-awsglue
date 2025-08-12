# union by name and empty partitions
Create a sample code which creates a empty partition on unionByName usage
we have lots of dat frames and union all these dataframes by name and writing the on the resulting parquet, it errors
out saying cannot write empty parttion and apply a fix. 

**Possible Solutions for Empty Partition Error:**

* Filter Empty DataFrames: Remove DataFrames with zero rows before union
* Coalesce Partitions: Use coalesce(1) to combine partitions before writing
* Partition Column Validation: Check if partition columns have valid values
* Default Values: Add default values for null partition columns
* Conditional Partitioning: Write without partitioning if partition columns are problematic
* df.count() on the resulting partition also helps. When you do this spark trigers a full computation(action) on Df
This forces the spark to materialize the DF and resolve any lazy tansformatios and potentially eliemiante empty or problamatic partitions
this way the Df is in a "clean" state for writing.

