# Master Data Management (MDM) Data Engineering

## What an MDM Data Engineer Does

An MDM (Master Data Management) Data Engineer builds and maintains the
platform that creates a **single, trusted source of truth** for core
business entities such as customers, vendors, products, employees, and
locations. Rather than focusing on transactional data (orders, invoices,
sales), the role centers on ensuring that enterprise master data is
accurate, consistent, and available across the organization.

A typical job description might mention responsibilities such as:

> Designing and implementing scalable, cloud-native data architectures
> across AWS, building data pipelines from ingestion to modeling, and
> exposing data through APIs for analytics, reporting, and business
> intelligence.

In practice, this means designing AWS-based data platforms using
services such as Amazon S3, Glue, EMR, Lambda, Athena, Redshift, API
Gateway, and optionally Snowflake or Databricks.

### Typical Data Flow

``` text
ERP          CRM         Procurement        HR
 |             |               |             |
 +-------------+---------------+-------------+
               |
          Data Ingestion
       (Glue / Spark / EMR)
               |
          Data Quality Rules
               |
      Matching & Deduplication
               |
         MDM Golden Records
               |
    +----------+-----------+
    |          |           |
 APIs       Snowflake    Kafka
    |          |           |
Applications  BI      Other Systems
```

A typical pipeline might:

1.  Read data from source systems.
2.  Validate schemas.
3.  Standardize names and addresses.
4.  Remove duplicates.
5.  Match existing records.
6.  Create or update the golden record.
7.  Publish the mastered data.

Example (PySpark):

``` python
raw = spark.read.csv("s3://landing/suppliers")

clean = (
    raw
      .dropDuplicates(["supplier_id"])
      .filter(col("supplier_id").isNotNull())
)

clean.write.mode("append").saveAsTable("master_supplier")
```

------------------------------------------------------------------------

# What is Master Data Management?

Organizations often have multiple systems storing different versions of
the same entity.

Example:

  System        Vendor Name
  ------------- -----------------
  Finance       Amazon LLC
  Procurement   Amazon.com Inc.
  Shipping      Amazon

Without MDM, every system may maintain its own version, resulting in
inconsistent reporting, payment errors, or shipping mistakes.

The MDM platform creates a **Golden Record**.

  Attribute       Value
  --------------- -----------------
  Vendor_ID       5001
  Legal Name      Amazon.com Inc.
  Address         Seattle, WA
  Tax ID          12-3456789
  Payment Terms   Net 30
  Status          Active

Every downstream application references this single authoritative
record.

------------------------------------------------------------------------

# How Business Systems Consume MDM Data

Business applications generally do not own the master data. Instead,
they consume it using one of four common integration patterns.

## 1. REST APIs

Applications request the latest information directly from the MDM
platform.

Example:

``` http
GET /vendors/5001
```

Response:

``` json
{
  "vendor_id":5001,
  "vendor_name":"ABC Foods",
  "payment_terms":"Net30",
  "status":"Active"
}
```

This approach ensures every application receives the same data.

------------------------------------------------------------------------

## 2. Database Views

If master data resides in Snowflake or another warehouse, different
departments may receive tailored SQL views.

Procurement:

``` sql
SELECT VendorID, Name, Status
FROM MDM_VENDOR;
```

Finance:

``` sql
SELECT VendorID, Name, TaxID, PaymentTerms
FROM MDM_VENDOR;
```

Each department sees only the attributes it requires while sharing the
same underlying master table.

------------------------------------------------------------------------

## 3. Event Streaming

Whenever master data changes, the MDM platform publishes an event
through Kafka, Amazon EventBridge, or similar technology.

``` text
MDM
 |
 +--> VendorUpdated Event
          |
      Kafka Topic
      /     |      \
 Finance Procurement Shipping
```

Each subscribing application updates automatically.

------------------------------------------------------------------------

## 4. Batch Synchronization

Legacy systems that cannot call APIs may receive nightly exports.

``` text
MDM
 |
Export CSV
 |
S3 / FTP
 |
ERP Import
```

------------------------------------------------------------------------

# Typical Master Data Managed by MDM

## Customer Master

-   Customer ID
-   Name
-   Address
-   Email
-   Phone
-   Customer Type
-   Status

Used by CRM, Sales, Marketing, Billing, and Customer Support.

------------------------------------------------------------------------

## Vendor Master

-   Vendor ID
-   Vendor Name
-   Tax ID
-   Payment Terms
-   Address
-   Currency
-   Contact Information
-   Approval Status

Used by Procurement, Finance, and Accounts Payable.

------------------------------------------------------------------------

## Product Master

-   Product ID
-   SKU
-   UPC
-   Product Name
-   Brand
-   Category
-   Manufacturer
-   Unit of Measure
-   Status

Used by Inventory, POS, Ecommerce, and Warehousing.

------------------------------------------------------------------------

## Employee Master

-   Employee ID
-   Name
-   Department
-   Manager
-   Job Title
-   Cost Center
-   Status

------------------------------------------------------------------------

## Location Master

-   Store ID
-   Warehouse ID
-   Address
-   Region
-   Time Zone
-   Manager

------------------------------------------------------------------------

## Other Common Master Data

-   Material Master
-   Asset Master
-   Chart of Accounts
-   Organizational Hierarchies
-   Reference Data (Countries, Currencies, Payment Terms)

------------------------------------------------------------------------

# What MDM Does Not Store

MDM is **not** designed for transactional data such as:

-   Orders
-   Sales transactions
-   Invoices
-   Inventory movements
-   Sensor readings

Those remain in operational systems, while MDM manages the relatively
stable definitions of the business entities involved.

------------------------------------------------------------------------

# Compass Group Example

Compass Group operates food services for hospitals, schools, airports,
corporate offices, and sports venues.

Its MDM platform might manage:

### Vendor Master

-   Sysco
-   US Foods
-   PepsiCo
-   Coca-Cola

### Product Master

-   Chicken Breast
-   Coffee Beans
-   Milk
-   Lettuce
-   Disposable Cups

### Location Master

-   University Cafeteria
-   Hospital Cafeteria
-   Airport Food Court
-   Corporate Dining Center

### Customer Master

-   Universities
-   Hospitals
-   Corporate Clients

### Employee Master

-   Chefs
-   Cashiers
-   Managers
-   Dietitians

Suppose Sysco changes its payment terms from **Net 30** to **Net 45**.

The MDM system updates the golden record once.

-   Procurement uses the updated vendor status when creating purchase
    orders.
-   Finance automatically receives the new payment terms.
-   Shipping uses the updated address.
-   Power BI and Tableau continue to report consistently because every
    department references the same Vendor ID.

------------------------------------------------------------------------

# Responsibilities of an MDM Data Engineer

Typical responsibilities include:

-   Designing AWS-based cloud data architectures.
-   Building ingestion pipelines.
-   Developing Spark and Python ETL jobs.
-   Matching and merging duplicate records.
-   Implementing data quality and validation rules.
-   Maintaining golden records.
-   Publishing master data through APIs, SQL views, and event streams.
-   Building analytics-ready datasets.
-   Monitoring data quality, lineage, and pipeline health.
-   Supporting CI/CD and Infrastructure as Code.

The ultimate objective is to provide a reliable, scalable, and governed
**single source of truth** that every application and business function
can trust.
