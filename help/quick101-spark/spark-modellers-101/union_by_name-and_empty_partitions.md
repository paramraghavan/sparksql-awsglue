# union by name and empty partitions
Create a sample code which creates a empty partition on unionByName usage
we have lots of dat frames and union all these dataframes by name and writing the on the resulting parquet, it errors
out saying cannot erite empty parttion and apply a fix. 

**Possible Solutions for Empty Partition Error:**

* Filter Empty DataFrames: Remove DataFrames with zero rows before union
* Coalesce Partitions: Use coalesce(1) to combine partitions before writing
* Partition Column Validation: Check if partition columns have valid values
* Default Values: Add default values for null partition columns
* Conditional Partitioning: Write without partitioning if partition columns are problematic

