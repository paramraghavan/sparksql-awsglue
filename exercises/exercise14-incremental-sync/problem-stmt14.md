- I have n files in s3 or local file system
- All these files have common key columns 1 or key columns
- Read one file at and apply update if the row exists in data frame, if it does not exist add it to the dataframe
- it should pick the file in the order of date time and the file name is typically agency_filetype_yyyymmdd_hhmmss_abc.DAT 
- use pyspark