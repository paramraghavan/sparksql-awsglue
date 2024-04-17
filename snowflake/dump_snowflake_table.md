Using snowflake-connetcor for python connect to snowflake database and pass in a list of tables , reads 100 records into
into a pandas data frame and write out these records into a pipe delimited file by table name

```shell
pip install snowflake-connector-python pandas
```

**Note**
When you are dealing with larger datasets, be mindful of memory usage when fetching data into a pandas DataFrame. Adjust
your approach accordingly, possibly by processing data in chunks.