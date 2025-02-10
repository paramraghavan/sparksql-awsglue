# PROBLEM/QUESTION
`JOB1` (using `CREATE OR REPLACE TEMPORARY TABLE temp AS ...`) is taking more time than
`JOB2` (using `CREATE OR REPLACE TABLE temp AS ...`)

Here are some things you can try

### **1. Avoid Recreating Temporary Tables Repeatedly**

Since your job is running the `CREATE OR REPLACE TEMPORARY TABLE temp AS ...` 19 times per execution, try using **common
table expressions (CTEs)** or **inline views** instead of repeatedly creating temporary tables.

Instead of:

```sql
CREATE OR REPLACE TEMPORARY TABLE temp AS
SELECT ... FROM large_table;
```

Use:

```sql
WITH temp AS (
    SELECT ... FROM large_table
)
SELECT ... FROM temp;
```

This avoids unnecessary DDL operations.

---

### **2. Use `WITH` (Common Table Expressions) or Views**

- If the temporary table is being created only for further processing within the same session, **CTEs (`WITH` queries)
  or views** may be a better alternative.
- If you need multiple references to the temporary table, a **temporary view** could also be an option.

Example:

```sql
CREATE OR REPLACE TEMPORARY VIEW temp_view AS
SELECT ... FROM large_table;
```

Then reference `temp_view` instead of creating a new temporary table each time.

---

### **3. Use `TRANSIENT TABLE` Instead of `TEMPORARY TABLE`**

- **`TEMPORARY TABLES` are metadata-heavy** and get cleaned up at the end of the session.
- A **`TRANSIENT TABLE`** persists across queries without transactional overhead, improving performance.

Example:

```sql
CREATE OR REPLACE TRANSIENT TABLE temp AS
SELECT ... FROM large_table;
```

- `TRANSIENT TABLE` avoids logging overhead (unlike standard tables) while persisting across queries.

---

### **4. Increase Warehouse Size Temporarily**

- If the queries are running slow due to computational limits, consider scaling up the warehouse **just for this job**
  and then scaling it back down.

```sql
ALTER WAREHOUSE my_warehouse SET WAREHOUSE_SIZE = 'LARGE';
-- Run Job 1
ALTER WAREHOUSE my_warehouse SET WAREHOUSE_SIZE = 'SMALL';
```

- This ensures that query execution gets more compute power temporarily.

---

### **5. Use Clustering and Pruning Strategies**

- If `SELECT` queries are slow, check **clustering and pruning efficiency**.
- Use **clustering keys** on frequently filtered columns in the source tables to improve read performance.

```sql
ALTER TABLE large_table CLUSTER BY (date_column, category_column);
```

- Run:

```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS());
```

to check table pruning efficiency.

---

### **6. Reduce Data Scanned Using Filters or Indexing**

- If you're querying different tables, ensure that your queries **use proper WHERE conditions** to minimize full-table
  scans.

For example, instead of:

```sql
SELECT * FROM large_table;
```

Use:

```sql
SELECT * FROM large_table WHERE date_column >= '2024-01-01';
```

This improves **query efficiency** and reduces processing time.

---

### **7. Use Result Caching**

- If the same query is run multiple times, **Snowflake caches query results**.
- If your data isn't changing frequently, Snowflake will **automatically reuse cached results**.

To verify:

```sql
SELECT * FROM large_table;
```

- Running the same query again should be **almost instant** since it retrieves results from cache.

---

### **8. Optimize Data Storage Format**

- If the tables being queried are large, **ensure proper file storage format**.
- **Use Snowflake's `AUTOMATIC CLUSTERING`** for large tables to improve read speeds.

```sql
ALTER TABLE large_table SET AUTO_CLUSTERING = TRUE;
```

---

### **9. Parallelize the Job Execution**

- If you are executing `CREATE OR REPLACE TEMPORARY TABLE` statements sequentially, consider **running them in parallel
  ** using **Snowflake tasks or multi-threaded execution**.
- Example: Instead of running all 19 queries in sequence, distribute them across **multiple warehouses or parallel
  streams**.

---

### **10. Check Query Performance Using Query History**

Run:

```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
ORDER BY start_time DESC;
```

- Look at `EXECUTION_TIME` and `BYTES_SCANNED` to identify slow queries.
- If needed, analyze the query profile for bottlenecks.

---

### **Summary:**

| Strategy                                | Action                             |
|-----------------------------------------|------------------------------------|
| **Use CTEs or Views**                   | Avoid temporary tables if possible |
| **Use TRANSIENT TABLE**                 | Less metadata overhead             |
| **Increase Warehouse Size Temporarily** | Faster execution for heavy queries |
| **Use Clustering Keys**                 | Improves read performance          |
| **Use WHERE Filters**                   | Reduce scanned data                |
| **Enable Auto Clustering**              | Optimizes storage                  |
| **Leverage Query Caching**              | Reduce redundant processing        |
| **Parallel Execution**                  | Run queries in parallel for speed  |
