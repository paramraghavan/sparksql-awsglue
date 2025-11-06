Here is a concise explanation of all the common Spark SQL join types with a simple grid showing how rows and columns from two datasets (Left and Right) are affected:

| Join Type     | Description                                      | Rows Included from Left | Rows Included from Right | Notes on Columns                                                  |
|---------------|-------------------------------------------------|------------------------|-------------------------|------------------------------------------------------------------|
| Inner Join    | Rows with matching keys in both datasets        | Only matched            | Only matched             | Columns from both sides for matched rows                        |
| Left Outer    | All rows from left, matched from right          | All left rows           | Only matched             | Right columns are NULL if no match                              |
| Right Outer   | All rows from right, matched from left          | Only matched            | All right rows           | Left columns are NULL if no match                               |
| Full Outer    | All rows from both, matched where possible      | All left rows           | All right rows           | Columns NULL where no match exists                              |
| Left Semi     | Rows from left with at least one match in right | Only matched left rows  | No columns from right    | Only left columns returned; unlike inner join no right columns |
| Left Anti     | Rows from left with NO match in right            | Only unmatched left rows| No columns from right    | Only left columns, filtered by no match in right               |
| Cross Join    | Cartesian product, every combination             | All rows                | All rows                 | Every left row combined with every right row                   |

### Simple Example to Visualize Rows:

Suppose:

Left Table (L):

| id | name   |
|----|--------|
| 1  | Alice  |
| 2  | Bob    |
| 3  | Charlie|

Right Table (R):

| id | city     |
|----|----------|
| 2  | London   |
| 3  | Paris    |
| 4  | New York |

| Join Type   | Result Rows (combined L + R columns)                      |
|-------------|------------------------------------------------------------|
| Inner Join  | (2, Bob, London), (3, Charlie, Paris)                      |
| Left Outer  | (1, Alice, NULL), (2, Bob, London), (3, Charlie, Paris)    |
| Right Outer | (2, Bob, London), (3, Charlie, Paris), (NULL, NULL, New York)|
| Full Outer  | (1, Alice, NULL), (2, Bob, London), (3, Charlie, Paris), (NULL, NULL, New York)|
| Left Semi   | (2, Bob), (3, Charlie)                                     |
| Left Anti   | (1, Alice)                                                 |
| Cross Join  | (1, Alice, 2, London), (1, Alice, 3, Paris), (1, Alice,4, New York), (2, Bob, 2, London), (2, Bob, 3, Paris), (2, Bob, 4, New York), (3, Charlie, 2, London), (3, Charlie, 3, Paris), (3, Charlie, 4, New York)|

### Summary:
- Inner join only keeps rows with matches on both sides.
- Outer joins keep non-matching rows with NULLs accordingly.
- Left semi and anti joins filter the left dataset based on match existence, returning only left columns.
- Cross join pairs all rows.

This grid and explanation clarify how Spark joins affect included rows and columns in SQL queries.[1][5][7]

[1](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-join.html)
[2](https://dzone.com/articles/spark-join-types-explained)
[3](https://www.linkedin.com/pulse/spark-join-strategies-mastering-joins-apache-venkatesh-nandikolla-mk4qc)
[4](https://www.java-success.com/apache-spark-sql-join-types-interview-qas/)
[5](https://www.waitingforcode.com/apache-spark-sql/join-types-spark-sql/read)
[6](https://stackoverflow.com/questions/45990633/what-are-the-various-join-types-in-spark)
[7](https://iomete.com/resources/reference/pyspark/pyspark-join)