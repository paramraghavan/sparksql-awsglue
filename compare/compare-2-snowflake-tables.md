
## 1. Row count comparison
First, check if both tables have the same number of rows:

```sql
SELECT 
  (SELECT COUNT(*) FROM table1) AS table1_count,
  (SELECT COUNT(*) FROM table2) AS table2_count,
  ((SELECT COUNT(*) FROM table1) - (SELECT COUNT(*) FROM table2)) AS difference;
```

## 2. Hash-based full table comparison
For a full comparison that's efficient with large tables:

```sql
SELECT 
  COUNT(*) AS mismatch_count
FROM (
  SELECT HASH(*) AS row_hash FROM table1
  EXCEPT
  SELECT HASH(*) AS row_hash FROM table2
);
```

## 3. Column-by-column comparison
To identify which columns have differences:

```sql
SELECT 
  'column1' AS column_name,
  COUNT(*) AS mismatch_count
FROM (
  SELECT column1 FROM table1
  EXCEPT
  SELECT column1 FROM table2
)
UNION ALL
-- Repeat for each column
SELECT 
  'column2' AS column_name,
  COUNT(*) AS mismatch_count
FROM (
  SELECT column2 FROM table1
  EXCEPT
  SELECT column2 FROM table2
);
```

## 4. Identify specific differing rows
To find exactly which rows differ:

```sql
WITH table1_hash AS (
  SELECT *, HASH(*) AS row_hash FROM table1
),
table2_hash AS (
  SELECT *, HASH(*) AS row_hash FROM table2
)
SELECT t1.* 
FROM table1_hash t1
LEFT JOIN table2_hash t2 ON t1.row_hash = t2.row_hash
WHERE t2.row_hash IS NULL;
```

## 5. Using a primary key
If you have a primary key or unique identifier:

```sql
SELECT 
  a.*,
  b.*
FROM table1 a
FULL OUTER JOIN table2 b ON a.primary_key = b.primary_key
WHERE a.primary_key IS NULL OR b.primary_key IS NULL
OR a.column1 <> b.column1 
OR a.column2 <> b.column2;
-- Add more column comparisons as needed
```

For very large tables, you may want to perform these operations in batches or use Snowflake tasks to handle the comparison in the background to avoid timeout issues.