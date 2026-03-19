# Spark SQL Basics

## Spark SQL Overview

Spark SQL allows you to write **SQL queries on DataFrames**. You can:
1. Create temporary views from DataFrames
2. Write SQL queries on those views
3. Mix SQL and DataFrame API seamlessly

### Why Use SQL?
- Familiar syntax for data professionals
- Catalyst optimizer works on SQL too (same optimization as DataFrame API)
- Can join SQL with DataFrame operations
- Some operations are clearer in SQL (e.g., complex joins)

---

## Temporary Views

### Temp Views (Session-Scoped)
```python
df = spark.read.csv("customers.csv", header=True)

# Create temporary view
df.createOrReplaceTempView("customers")  # Session-scoped

# Now query with SQL
spark.sql("SELECT * FROM customers WHERE age > 30").show()

# View exists until session ends
# Multiple DataFrames can have same view name (will replace)
```

### Global Temp Views (Application-Scoped)
```python
df.createGlobalTempView("global_customers")

# Access with global_temp prefix
spark.sql("SELECT * FROM global_temp.global_customers").show()

# Survives across sessions in same app
```

### Database & Schema

```python
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS my_db")

# Create table in database
spark.sql("""
    CREATE TABLE IF NOT EXISTS my_db.customers (
        id INT,
        name STRING,
        age INT,
        salary DECIMAL(10, 2)
    )
""")

# Query from database
spark.sql("SELECT * FROM my_db.customers").show()

# List all tables
spark.sql("SHOW TABLES").show()

# List databases
spark.sql("SHOW DATABASES").show()
```

---

## SQL Queries on DataFrames

### Basic Queries
```python
df.createOrReplaceTempView("sales")

# SELECT
spark.sql("SELECT name, amount FROM sales").show()

# WHERE
spark.sql("SELECT * FROM sales WHERE amount > 100").show()

# GROUP BY with aggregation
spark.sql("""
    SELECT category, COUNT(*) as total_sales, AVG(amount) as avg_amount
    FROM sales
    GROUP BY category
""").show()

# ORDER BY
spark.sql("SELECT * FROM sales ORDER BY amount DESC").show()

# LIMIT
spark.sql("SELECT * FROM sales LIMIT 10").show()
```

### JOINs in SQL
```python
customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")

# INNER JOIN
spark.sql("""
    SELECT c.name, o.order_id, o.amount
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.amount > 50
""").show()

# LEFT JOIN
spark.sql("""
    SELECT c.name, COUNT(o.order_id) as order_count
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.name
""").show()
```

### Window Functions in SQL
```python
sales.createOrReplaceTempView("sales")

spark.sql("""
    SELECT
        salesman,
        sale_amount,
        RANK() OVER (PARTITION BY salesman ORDER BY sale_amount DESC) as rank
    FROM sales
""").show()
```

---

## SQL vs DataFrame API

### Same Result, Different Syntax
```python
# SQL
spark.sql("""
    SELECT department, AVG(salary) as avg_salary, COUNT(*) as emp_count
    FROM employees
    WHERE age > 30
    GROUP BY department
    HAVING COUNT(*) > 5
    ORDER BY avg_salary DESC
""").show()

# DataFrame API
from pyspark.sql.functions import col, avg, count

df.filter(col("age") > 30) \
    .groupBy("department") \
    .agg(
        avg("salary").alias("avg_salary"),
        count("*").alias("emp_count")
    ) \
    .filter(col("emp_count") > 5) \
    .orderBy(col("avg_salary").desc()) \
    .show()

# Both compile to same optimized plan!
```

---

## Catalyst Optimizer & Predicate Pushdown

Catalyst optimizes SQL queries automatically:

### Example 1: Predicate Pushdown
```python
# Written query
df.filter(df.age > 30).select("name", "age")

# Optimized query (pushes filter down)
# SELECT name, age WHERE age > 30
# Reads less data from source!
```

### Example 2: Column Pruning
```python
# Written query
df.select("age", "name", "salary", "department", "hired_date") \
    .filter(df.age > 30)

# Optimized query reads only 5 columns, even if source has 50
# Catalyst knows only these columns are used
```

### Example 3: Check the Plan
```python
df = spark.read.parquet("large_file.parquet")

query = df.filter(df.age > 30) \
           .select("name", "age", "salary") \
           .groupBy("salary") \
           .count()

# View execution plan
query.explain()  # Logical plan
query.explain(True)  # Extended (physical) plan

# Look for:
# - PushedFilters (predicate pushed to source)
# - Columns listed in scan (column pruning)
# - Join strategy (Broadcast vs SortMerge)
```

---

## Caching SQL Tables

```python
# Cache table
spark.sql("CACHE TABLE my_table")

# Query uses cached version
spark.sql("SELECT * FROM my_table WHERE ...").show()  # Fast!

# Uncache
spark.sql("UNCACHE TABLE my_table")

# Check cached tables
spark.sql("SHOW CACHES").show()
```

---

## Spark Catalog

Access metadata about tables, databases, columns:

```python
# Get current database
spark.catalog.currentDatabase()  # Default: 'default'

# Set database
spark.catalog.setCurrentDatabase("my_db")

# List tables
spark.catalog.listTables()

# List columns in table
spark.catalog.listColumns("my_table")

# Get table metadata
spark.catalog.getTable("my_table")

# List databases
spark.catalog.listDatabases()

# Check if table exists
"my_table" in [t.name for t in spark.catalog.listTables()]

# Get function definitions
spark.catalog.listFunctions()
```

---

## Real-World Example

```python
# Load sales data
sales_df = spark.read.csv("sales.csv", header=True, inferSchema=True)
sales_df.createOrReplaceTempView("sales")

# Load products data
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
products_df.createOrReplaceTempView("products")

# Complex query: Top products by revenue in Q1
result = spark.sql("""
    SELECT
        p.product_name,
        SUM(s.amount) as total_revenue,
        COUNT(s.sale_id) as num_sales,
        AVG(s.amount) as avg_sale,
        ROW_NUMBER() OVER (ORDER BY SUM(s.amount) DESC) as rank
    FROM sales s
    INNER JOIN products p ON s.product_id = p.product_id
    WHERE QUARTER(s.sale_date) = 1
    GROUP BY p.product_name
    HAVING COUNT(s.sale_id) > 10
    ORDER BY total_revenue DESC
    LIMIT 10
""")

result.show()

# Save results
result.write.mode("overwrite").parquet("q1_top_products/")
```

---

## Interview Questions

### Q: Can Catalyst optimize SQL queries the same as DataFrame API?

**Answer**: Yes, Catalyst optimizes both equally. SQL is translated to DataFrame operations and optimized using the same Catalyst rules (predicate pushdown, column pruning, join strategy selection, etc.).

### Q: What's the difference between temp views and global temp views?

**Answer**:
- Temp view: Session-scoped, only visible in current session
- Global temp view: Application-scoped, visible across sessions
- Both are temporary (lost when app ends)
- Use temp views for most cases

### Q: When should you use SQL vs DataFrame API?

**Answer**:
- SQL: Complex joins, familiar syntax, team knows SQL
- DataFrame: Programmatic logic, control flow, UDFs
- Mix them: Use whichever is clearest for each operation

---

## Key Takeaways

1. Create temp views to use SQL on DataFrames
2. SQL queries get optimized by Catalyst (same as DataFrame API)
3. Mix SQL and DataFrame API in same job
4. Predicate pushdown filters at source (read less data)
5. Use Explain plan to understand optimization

---

## See Also
- [DataFrame Operations](../02-data-operations/02-dataframe-operations.md)
- [Catalyst Optimizer](../04-performance-optimization/03-catalyst-optimizer.md)
- [Join Strategies](../03-joins-partitioning/02-join-strategies.md)
