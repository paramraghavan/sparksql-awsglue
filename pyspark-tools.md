# tools

## Avro/Parquet Viewer plugin in pycharm

## Apache Drill
- Steps to Install apache Drill
  - Drill can be used to query parquet, json, avro, etc... files
  - [installing-drill-on-linux-and-mac-os-x/](https://drill.apache.org/docs/installing-drill-on-linux-and-mac-os-x/)
  - [starting-drill-on-linux-and-mac-os-x](https://drill.apache.org/docs/starting-drill-on-linux-and-mac-os-x/), bin/drill-embedded
  - [sql query using drill](https://drill.apache.org/docs/query-data/)
  - example query,  SELECT * FROM dfs.`<path-tofile/sample-data/region.parquet`;

## parquet-tools
  - pip install parquet-tools
  - https://pypi.org/project/parquet-tools/
  - ~~brew install parquet-tools~~
    - parquet-tools meta file.parquet
    - ref: https://linuxcommandlibrary.com/man/parquet-tools
    <pre>
    Display the content of a Parquet file
    $ parquet-tools cat [path/to/parquet]
    
    Display the first few lines of a Parquet file
    $ parquet-tools head [path/to/parquet]
    
    Print the schema of a Parquet file
    $ parquet-tools schema [path/to/parquet]
    
    Print the metadata of a Parquet file
    $ parquet-tools meta [path/to/parquet]
    
    Print the content and metadata of a Parquet file
    $ parquet-tools dump [path/to/parquet]
    
    Concatenate several Parquet files into the target one
    $ parquet-tools merge [path/to/parquet1] [path/to/parquet2] [path/to/target_parquet]
    
    
    Print the count of rows in a Parquet file
    $ parquet-tools rowcount [path/to/parquet]
    
    Print the column and offset indexes of a Parquet file
    $ parquet-tools column-index [path/to/parquet]

    </pre>