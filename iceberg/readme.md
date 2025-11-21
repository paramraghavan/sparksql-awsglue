The user wants to transition from a Snowflake-centric architecture to an Iceberg-based architecture where:
```text
Current state: Data → S3 → Snowflake, PySpark transformations → S3 + Snowflake, scheduled jobs in Snowflake
Target state: Data → S3 (Iceberg format), PySpark transformations → S3 (Iceberg), business logic jobs use Iceberg, users
query via Snowflake (using Snowflake's Iceberg catalog integration)
```
Implement  incremental loads (hourly/daily) for something like vehicle pollution tests.

## Key requirements:
* AWS S3 for storage
* Apache Iceberg table format
* PySpark for ETL
* Incremental and full load patterns
* Iceberg metadata managed in Snowflake
* Business users query via Snowflake SQL
* Example - vehicle pollution test scenario
