-- check is this storage intergation exists
SHOW STORAGE INTEGRATIONS;
SHOW STORAGE INTEGRATIONS LIKE 's3_integration';

USE ROLE ACCOUNTADMIN; -- Or another role with appropriate access
USE DATABASE SNOWFLAKE; -- The system database

SELECT integration_name, integration_type, enabled, comment
FROM SNOWFLAKE.INFORMATION_SCHEMA.INTEGRATIONS
WHERE integration_name = 's3_integration';

-- Get the external ID to configure AWS trust relationship
DESC INTEGRATION s3_integration;

-- Set up Snowflake storage integration with AWS
-- This only needs to be done once by an account admin
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/your-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://your-s3-bucket-name/your/prefix/path/');



-- Create an external stage using the integration
CREATE OR REPLACE STAGE my_s3_stage
  URL = 's3://your-s3-bucket-name/your/prefix/path/'
  STORAGE_INTEGRATION = s3_integration
  FILE_FORMAT = (TYPE = 'PARQUET'); -- or CSV for comma-separated, etc.
-- , delimited
CREATE OR REPLACE STAGE my_s3_stage
  URL = 's3://your-s3-bucket-name/your/prefix/path/'
  STORAGE_INTEGRATION = s3_integration
  FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = ',',
    SKIP_HEADER = 1,
    DATE_FORMAT = 'AUTO',
    TIMESTAMP_FORMAT = 'AUTO',
    NULL_IF = ('NULL', 'null', '')
  );
-- | delimited
CREATE OR REPLACE STAGE my_s3_stage
  URL = 's3://your-s3-bucket-name/your/prefix/path/'
  STORAGE_INTEGRATION = s3_integration
  FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_DELIMITER = '|',
    SKIP_HEADER = 1,
    DATE_FORMAT = 'AUTO',
    TIMESTAMP_FORMAT = 'AUTO',
    NULL_IF = ('NULL', 'null', '')
  );


-- Export data directly to S3 using COPY INTO
COPY INTO @my_s3_stage/your_table_export_
FROM (
  SELECT *
  FROM your_table_name
  WHERE country_code = 'US'
)
FILE_FORMAT = (TYPE = 'PARQUET') -- Choose your format (PARQUET, CSV, etc.)
HEADER = TRUE -- For CSV only
SINGLE = FALSE -- Split into multiple files
ROWS_PER_FILE = 1000000;  -- 1 million rows per file
--MAX_FILE_SIZE = 5368709120; -- 5 GB per file


-- delimiter , or |
COPY INTO @my_s3_stage/your_table_export_
FROM (
  SELECT *
  FROM your_table_name
  WHERE country_code = 'US'
)
FILE_FORMAT = (
  TYPE = 'CSV',
  FIELD_DELIMITER = ',',
  RECORD_DELIMITER = '\n',
  NULL_IF = ('NULL', ''),
  COMPRESSION = 'NONE'
)
HEADER = TRUE
SINGLE = FALSE
ROWS_PER_FILE = 1000000;

COPY INTO @my_s3_stage/your_table_export_
FROM (
  SELECT *
  FROM your_table_name
  WHERE country_code = 'US'
)
FILE_FORMAT = (
  TYPE = 'CSV',
  FIELD_DELIMITER = '|',
  RECORD_DELIMITER = '\n',
  NULL_IF = ('NULL', ''),
  COMPRESSION = 'NONE'
)
HEADER = TRUE
SINGLE = FALSE
ROWS_PER_FILE = 1000000;