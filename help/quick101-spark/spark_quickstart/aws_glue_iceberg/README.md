# AWS Glue and Iceberg: start here

This folder explains how AWS lake tables work and how to use them from Athena, PySpark, and Redshift. It includes a detailed guide and seven diagrams.

## What you will learn

- How S3 files, Glue table definitions, and Iceberg metadata fit together.
- Why ordinary Parquet tables and Iceberg tables handle changes differently.
- How to create and change a table, inspect snapshots, and share it across engines.

Start with familiar SQL; return to the file and commit internals when you need them.

## First reading: concepts and worked examples

| Step | Read | Outcome |
|---|---|---|
| 1 | [Components and layers](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#1-three-layers-of-an-aws-lakehouse) | Understand the roles of storage, catalogs, and engines |
| 2 | [Traditional Parquet](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#3-traditional-parquet-with-glue-data-catalog) | Learn what Glue stores without Iceberg |
| 3 | [Athena walkthrough](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#8-athena-iceberg-crud-and-introspection) | Create, insert, update, delete, and check the result |
| 4 | [Glue PySpark](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#9-glue-pyspark--iceberg) | Append and upsert complete sample records |
| 5 | [Redshift table types](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#10-redshift-distinguish-three-different-table-types) | Distinguish internal, Spectrum, and writable Iceberg tables |

The Athena and PySpark examples build on each other. Traditional Parquet and Redshift internal-table examples use separate tables. Example setup requirements are listed at the top of the guide.

## Table of contents

Full guide: [AWS Glue Iceberg Athena Redshift PySpark Notes](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md).

- [1. Three layers of an AWS lakehouse](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#1-three-layers-of-an-aws-lakehouse)
- [2. CRUD support at a glance](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#2-crud-support-at-a-glance)
- [3. Traditional Parquet with Glue Data Catalog](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#3-traditional-parquet-with-glue-data-catalog)
  - [3.1 What Glue stores without Iceberg](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#31-what-glue-stores-without-iceberg)
  - [3.2 Create and read the example table](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#32-create-and-read-the-example-table)
  - [3.3 Append with PySpark](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#33-append-with-pyspark)
  - [3.4 Correct or delete rows with plain Parquet](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#34-correct-or-delete-rows-with-plain-parquet)
- [4. Iceberg metadata hierarchy — where snapshots live](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#4-iceberg-metadata-hierarchy--where-snapshots-live)
- [5. How an Iceberg write updates the Glue pointer](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#5-how-an-iceberg-write-updates-the-glue-pointer)
  - [Before the UPDATE](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#before-the-update)
  - [Athena UPDATE](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#athena-update)
  - [After the successful commit](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#after-the-successful-commit)
  - [Who does what?](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#who-does-what)
  - [Does a SELECT advance the Glue pointer?](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#does-a-select-advance-the-glue-pointer)
- [6. Iceberg commit flow and concurrency](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#6-iceberg-commit-flow-and-concurrency)
- [7. Iceberg UPDATE strategies: COW and MOR](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#7-iceberg-update-strategies-cow-and-mor)
- [8. Athena Iceberg CRUD and introspection](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#8-athena-iceberg-crud-and-introspection)
- [9. Glue PySpark + Iceberg](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#9-glue-pyspark--iceberg)
  - [9.1 Configure the job](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#91-configure-the-job)
  - [9.2 Append a complete order](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#92-append-a-complete-order)
  - [9.3 Upsert a batch with MERGE](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#93-upsert-a-batch-with-merge)
- [10. Redshift: distinguish three different table types](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#10-redshift-distinguish-three-different-table-types)
  - [10.1 Redshift internal tables](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#101-redshift-internal-tables)
  - [10.2 Redshift Spectrum traditional external tables](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#102-redshift-spectrum-traditional-external-tables)
  - [10.3 Redshift writable Iceberg tables](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#103-redshift-writable-iceberg-tables)
- [11. Where to inspect Glue vs Iceberg metadata](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#11-where-to-inspect-glue-vs-iceberg-metadata)
  - [AWS Glue API / CLI](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#aws-glue-api--cli)
  - [S3 Iceberg metadata file](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#s3-iceberg-metadata-file)
  - [Historical query in Athena](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#historical-query-in-athena)
- [12. Do you need Glue for Spark + Iceberg?](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#12-do-you-need-glue-for-spark--iceberg)
- [13. Athena and PySpark accessing the same table](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#13-athena-and-pyspark-accessing-the-same-table)
- [14. Recommended AWS BI lakehouse architecture](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#14-recommended-aws-bi-lakehouse-architecture)
  - [14.1 Maintenance and troubleshooting](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#141-maintenance-and-troubleshooting)
- [15. Final behavior matrix](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#15-final-behavior-matrix)
  - [Five key takeaways](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#five-key-takeaways)
- [16. Official references](AWS_Glue_Iceberg_Athena_Redshift_PySpark_Notes.md#16-official-references)
