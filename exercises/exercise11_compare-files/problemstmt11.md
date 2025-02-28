Compare the dat(could be csv or parquet) file and snowflake table using pyspark framewrok running on WS EMR
The dat file and the snowflake table have the exact  same schema
Download the snowflake table into a files - DAT (or PArquet or CSV)
The dat file does not have header, use the header information from snowflake table
* compare the table at row level
* compare at column level - values and data type
* create a difference report
The file size is approximately 20+ gb