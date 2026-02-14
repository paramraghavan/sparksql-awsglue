# PySpark Sample Data Files

This folder contains sample datasets for practicing with the PySpark Cheat Sheet.

## Files Overview

| File | Rows | Description | Use Cases |
|------|------|-------------|-----------|
| `employees.csv/parquet` | 500 | Employee records with salary, department, etc. | Basic operations, joins, window functions, null handling |
| `departments.csv/parquet` | 6 | Department lookup table | Broadcast joins (small table) |
| `sales.csv/parquet` | 10,000 | Sales transactions over 2 years | Aggregations, date functions, partitioning |
| `customers.csv/parquet` | 1,000 | Customer information | String functions, joins |
| `products.csv/parquet` | 8 | Product catalog | Joins, lookups |
| `web_logs.csv/parquet` | 50,000 | Website activity logs | Large dataset operations, partitioning |
| `skewed_data.csv/parquet` | 10,000 | Intentionally skewed data | Data skew troubleshooting |
| `ml_features.csv/parquet` | 2,000 | ML-ready features with labels | Machine learning pipelines |
| `sample_data.json` | 100 | Nested JSON records | JSON parsing, complex schemas |

## Quick Start

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Practice") \
    .master("local[*]") \
    .getOrCreate()

# Load CSV
employees = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Load Parquet (faster)
employees = spark.read.parquet("employees.parquet")

# Load JSON
json_data = spark.read.json("sample_data.json")
```

## Dataset Details

### employees
- Contains NULL values in `age`, `salary`, and `preferred_name` columns
- Has duplicate emails for deduplication practice
- Good for: filtering, groupBy, window functions, null handling

### sales
- Includes `year` and `month` columns for partitioning
- Transaction data with dates, amounts, products
- Good for: date functions, aggregations, time-series analysis

### skewed_data
- Key "A" appears in ~60% of rows (intentionally skewed)
- Use for practicing skew mitigation techniques (salting, repartitioning)

### ml_features
- Binary `label` column for classification
- Contains NULL values for imputation practice
- Categorical and numerical features

## Notes

- Parquet files are recommended for better performance
- All datasets use consistent naming conventions
- Data is randomly generated but realistic for practice purposes
