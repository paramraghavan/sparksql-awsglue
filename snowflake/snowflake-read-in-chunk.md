How to pull ~30 millon rows each from 3-4 snowflake tables using python connector. any special handling and save
as csv file

Some of the important things to keep in mind:

1. Memory constraints: Pulling 30M rows directly into your laptop's memory could cause crashes
2. Disk space: Each CSV file could be several GB depending on the number and type of columns
3. Network bandwidth and timeout risks with large transfers

Here's a robust approach using chunking and efficient file handling:

1. Chunking: Processes data in configurable chunks (default 1MB) to manage memory. Start with a small chunk size (like
   100,000 rows) and check memory usage on you client machine
2. Compression: Saves CSVs with gzip compression to reduce disk space, check disk space as well
3. Resume capability: Can resume interrupted exports by checking for existing chunks
4. Error handling: Comprehensive logging and exception handling
5. Resource management: Proper connection and cursor cleanup
6. Progress tracking: Logs progress for each chunk and table


```shell
pip install snowflake-connector-python pandas
```