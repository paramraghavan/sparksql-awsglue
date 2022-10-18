```
-- mapping json files on S3 bucket
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.table_name(
 col1 string,
 col2 string,
 type string)
PARTITIONED BY (dt string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://s3-path/data-src/'

-- to sync the table metadata with existing partitions
msck repair table database_name.table_name;
```

**When using Athena, keep in mind**
- Columnar storage is used, so when Selecting only use the columns you  need, if you have to select * to look have a quick look, 
limit your resultset - select * from database_name.table_name limit 10;
- You pay for the data scanned not the amount of time the query takes to runs.
- $5.00 per TB of data scanned
- The data Scanned can be observed in the Recent Queries tab as well as the where the query result is displayed.
- The above created table can be viewed in AWS Glue as well
- [Create table via athena](https://docs.aws.amazon.com/athena/latest/ug/create-table.html)
