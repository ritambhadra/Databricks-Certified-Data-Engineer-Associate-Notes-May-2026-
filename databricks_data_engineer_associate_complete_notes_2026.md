# Databricks Certified Data Engineer Associate — Complete Detailed Notes (2026)

Based on the official exam outline. fileciteturn1file0L1-L76

---

# 1. Exam Overview

## Certification Focus
The Databricks Certified Data Engineer Associate exam tests:

- Databricks platform fundamentals
- Delta Lake
- Unity Catalog
- Data ingestion
- PySpark transformations
- SQL transformations
- Streaming concepts
- Lakeflow Jobs
- CI/CD basics
- Governance and security
- Monitoring and troubleshooting

The exam is practical and scenario-based.
You must understand:

- WHEN to use a feature
- WHY to use a feature
- HOW it works internally
- LIMITATIONS of features
- COST implications

---

# 2. Databricks Lakehouse Architecture

## What is a Lakehouse?

Lakehouse = Combination of:

| Data Warehouse | Data Lake |
|---|---|
| Structured data | Structured + semi-structured + unstructured |
| ACID | Cheap storage |
| BI optimized | ML + AI friendly |
| Expensive | Scalable |

Databricks combines both.

---

# 3. Core Components of Databricks

## 3.1 Workspace

Main UI where you:

- Create notebooks
- Run jobs
- Create dashboards
- Manage clusters
- Create pipelines

---

## 3.2 Compute

Compute executes workloads.

Types:

| Compute Type | Use Case |
|---|---|
| All-purpose cluster | Development, notebooks |
| Job cluster | Scheduled jobs |
| SQL Warehouse | BI dashboards, SQL analytics |
| Serverless | Fully managed compute |

---

## 3.3 DBU (Databricks Unit)

Billing metric.

Cost depends on:

- Cluster type
- Node size
- Runtime
- Serverless usage

### Important

Serverless:
- Easy management
- Faster startup
- More expensive sometimes

Job cluster:
- Cheaper for scheduled pipelines
- Terminates automatically

All-purpose:
- Best for development
- Expensive if always running

Exam tip:

If question says:

- "ad hoc analysis" → All-purpose
- "scheduled ETL" → Job cluster
- "BI dashboard" → SQL Warehouse
- "minimal administration" → Serverless

---

# 4. Delta Lake

MOST IMPORTANT TOPIC.

## What is Delta Lake?

Storage layer providing:

- ACID transactions
- Schema enforcement
- Schema evolution
- Time travel
- Versioning
- Efficient updates/deletes/merges

Delta Lake solves problems of raw Parquet.

---

## 4.1 ACID Transactions

ACID:

| Property | Meaning |
|---|---|
| Atomicity | All or nothing |
| Consistency | Valid state |
| Isolation | Concurrent safety |
| Durability | Permanent commit |

Delta transaction log:

`_delta_log`

Contains:

- Metadata
- Commit history
- Schema
- File tracking

---

## 4.2 Delta Table Creation

### SQL

```sql
CREATE TABLE sales (
  id INT,
  amount DOUBLE
)
USING DELTA;
```

### PySpark

```python
(df.write
   .format("delta")
   .save("/mnt/sales"))
```

---

# 5. Managed vs External Tables

VERY IMPORTANT FOR EXAM.

## Managed Table

Databricks manages:

- Metadata
- Data files

Dropping table deletes data.

```sql
CREATE TABLE sales;
```

Storage managed internally.

---

## External Table

You manage storage location.

Dropping table removes only metadata.
Data remains.

```sql
CREATE TABLE sales
LOCATION 'abfss://container@storage/path';
```

---

## Managed vs External Summary

| Feature | Managed | External |
|---|---|---|
| Metadata managed | Yes | Yes |
| Data managed | Yes | No |
| DROP deletes files | Yes | No |
| Better governance | Yes | Partial |

Exam favorite question.

---

# 6. Unity Catalog

Extremely important.

## What is Unity Catalog?

Centralized governance solution.

Provides:

- Access control
- Data lineage
- Auditing
- Central metadata
- Row-level security
- Column masking

---

## 6.1 UC Hierarchy

```text
Metastore
   Catalog
      Schema
         Table/View
```

Example:

```sql
catalog.schema.table
```

---

## 6.2 Common Privileges

| Privilege | Meaning |
|---|---|
| SELECT | Read |
| MODIFY | Update/Delete |
| USAGE | Access catalog/schema |
| CREATE | Create objects |
| ALL PRIVILEGES | Full access |

---

## 6.3 GRANT Example

```sql
GRANT SELECT ON TABLE sales TO analysts;
```

---

## 6.4 REVOKE Example

```sql
REVOKE MODIFY ON TABLE sales FROM analysts;
```

---

## 6.5 DENY

Explicitly blocks access.

DENY overrides GRANT.

---

# 7. Row-Level Security & Column Masking

## Row-Level Security

Restricts rows.

Example:

India users see only India records.

---

## Column Masking

Masks sensitive columns.

Example:

Show:

```text
XXXX-XXXX-1234
```

instead of full card number.

---

# 8. ABAC (Attribute Based Access Control)

Policy-based security.

Rules based on:

- User role
- Department
- Region
- Tags

Centralized governance.

---

# 9. Bronze, Silver, Gold Architecture

Extremely important.

## Bronze Layer

Raw ingestion.

Characteristics:

- Minimal transformations
- Append only
- Keeps raw history

---

## Silver Layer

Cleaned and validated.

Operations:

- Deduplication
- Null handling
- Type casting
- Joins
- Validation

---

## Gold Layer

Business-ready data.

Used for:

- BI dashboards
- ML features
- Analytics
- KPIs

---

## Medallion Architecture Flow

```text
Source → Bronze → Silver → Gold
```

---

# 10. Data Ingestion

Very important.

## Ingestion Types

| Type | Description |
|---|---|
| Batch | Periodic loading |
| Streaming | Continuous loading |
| Incremental | Only new data |

---

# 11. COPY INTO

Important exam topic.

## Purpose

Incrementally load files from cloud storage.

Supports:

- ADLS
- S3
- GCS

---

## Example

```sql
COPY INTO bronze.sales
FROM 'abfss://container/path/'
FILEFORMAT = CSV;
```

---

## Important Features

- Tracks loaded files
- Avoids duplicates
- Incremental by default
- Easy batch ingestion

---

## COPY INTO vs Auto Loader

| Feature | COPY INTO | Auto Loader |
|---|---|---|
| Streaming | No | Yes |
| Incremental | Yes | Yes |
| Large scale | Medium | Excellent |
| Schema evolution | Limited | Strong |
| Recommended for real-time | No | Yes |

Exam tip:

Huge streaming ingestion → Auto Loader.

Simple incremental file load → COPY INTO.

---

# 12. Auto Loader

VERY IMPORTANT.

## Purpose

Efficient cloud file ingestion.

Optimized for:

- Millions of files
- Incremental processing
- Streaming ingestion

---

## Auto Loader Modes

### Directory Listing Mode

Scans directories.

Simpler.

Higher cloud API cost.

---

### File Notification Mode

Uses cloud notifications.

More scalable.

Lower cost.

Preferred for production.

---

## Example

```python
(df.writeStream
   .format("delta")
   .option("checkpointLocation", "/chk")
   .start("/output"))
```

---

## Schema Evolution

Allows new columns.

```python
.option("mergeSchema", "true")
```

---

## Schema Enforcement

Rejects invalid schema.

Improves data quality.

---

# 13. Structured Streaming Basics

## Key Concepts

| Term | Meaning |
|---|---|
| Trigger | Controls execution |
| Checkpoint | Stores progress |
| Watermark | Handles late data |
| Output mode | Append/update/complete |

---

## Output Modes

| Mode | Description |
|---|---|
| Append | New rows only |
| Update | Updated rows |
| Complete | Entire table rewritten |

---

# 14. Checkpointing

Stores:

- Offsets
- Metadata
- Progress state

Required for fault tolerance.

Without checkpoint:

- Duplicate processing possible
- Recovery impossible

---

# 15. DataFrame Transformations

MOST EXAM QUESTIONS COME FROM HERE.

---

# 16. Reading Data

## CSV

```python
spark.read.csv("/path", header=True)
```

## JSON

```python
spark.read.json("/path")
```

## Delta

```python
spark.read.table("catalog.schema.table")
```

---

# 17. Common Transformations

## Select Columns

```python
df.select("id", "name")
```

---

## Filter Rows

```python
df.filter(col("age") > 18)
```

---

## Add Column

```python
df.withColumn("year", year(col("date")))
```

---

## Rename Column

```python
df.withColumnRenamed("old", "new")
```

---

## Drop Column

```python
df.drop("temp")
```

---

# 18. Null Handling

## Drop Nulls

```python
df.na.drop()
```

---

## Fill Nulls

```python
df.na.fill(0)
```

---

## Replace Specific Nulls

```python
df.na.fill({"salary": 0})
```

---

# 19. explode()

Used for arrays.

Example:

```python
from pyspark.sql.functions import explode

df.select(explode("items"))
```

Converts:

```text
[1,2,3]
```

into multiple rows.

---

# 20. Joins

VERY IMPORTANT.

## Inner Join

```python
df1.join(df2, "id", "inner")
```

Returns matching rows only.

---

## Left Join

```python
df1.join(df2, "id", "left")
```

Keeps all left rows.

---

## Right Join

Keeps all right rows.

---

## Full Join

Keeps all rows.

---

## Cross Join

Cartesian product.

Very expensive.

Avoid unless necessary.

---

## Broadcast Join

Important optimization.

Used when one table is small.

```python
from pyspark.sql.functions import broadcast

large.join(broadcast(small), "id")
```

Reduces shuffle.

---

# 21. Union vs Union All

## union()

Combines rows.

Does NOT remove duplicates in Spark.

Important:

Spark union behaves like SQL UNION ALL.

Many exam questions trick here.

---

# 22. Deduplication

## dropDuplicates()

```python
df.dropDuplicates(["id"])
```

---

# 23. Aggregations

## Count

```python
df.count()
```

---

## GroupBy

```python
df.groupBy("dept").count()
```

---

## Mean

```python
df.groupBy("dept").mean("salary")
```

---

## Approx Count Distinct

Fast estimation.

```python
approx_count_distinct("id")
```

Less accurate.
Faster on large datasets.

---

# 24. Spark Execution Basics

## Lazy Evaluation

Transformations are lazy.

Execution occurs only after actions.

---

## Transformations

Examples:

- select
- filter
- join

Do NOT execute immediately.

---

## Actions

Examples:

- show()
- count()
- collect()

Trigger execution.

---

# 25. Partitioning

Important optimization topic.

## Repartition

Increases/decreases partitions.

Causes full shuffle.

```python
df.repartition(10)
```

---

## Coalesce

Reduces partitions.

Avoids full shuffle.

```python
df.coalesce(2)
```

---

# 26. Shuffle

One of biggest bottlenecks.

Occurs during:

- joins
- groupBy
- distinct
- repartition

Shuffle causes:

- network I/O
- disk spill
- slowness

---

# 27. Data Skew

Occurs when:

One partition has huge data.

Symptoms:

- One task slow
- Executors idle
- Long stage completion

Solutions:

- Broadcast join
- Salting
- Repartitioning

---

# 28. Disk Spill

Occurs when memory insufficient.

Spark writes temporary data to disk.

Very slow.

Causes:

- Large shuffle
- Low executor memory

---

# 29. Spark UI

Important exam topic.

Used to analyze:

- Jobs
- Stages
- Tasks
- Shuffle
- Skew
- Spill

---

## Stage Metrics

Look for:

| Metric | Meaning |
|---|---|
| Shuffle Read | Incoming shuffle |
| Shuffle Write | Outgoing shuffle |
| Spill | Memory overflow |
| Task Time | Slow tasks |

---

# 30. Spark Configurations

VERY IMPORTANT.

## spark.sql.shuffle.partitions

Default partitions for shuffle.

Too high:
- Too many small tasks

Too low:
- Large partitions

---

## spark.default.parallelism

Controls task parallelism.

---

## spark.executor.memory

Executor memory.

Low memory:
- Spill
- OOM

---

## spark.driver.memory

Driver memory.

---

## spark.sql.autoBroadcastJoinThreshold

Threshold for automatic broadcast joins.

---

# 31. Caching

## cache()

Stores DataFrame in memory.

Useful for repeated access.

```python
df.cache()
```

---

## persist()

Allows storage levels.

More flexible.

---

# 32. Delta Lake Advanced Features

## MERGE INTO

Very important.

Used for:

- UPSERT
- CDC
- Incremental loads

Example:

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

---

## DELETE

```sql
DELETE FROM sales WHERE amount < 0;
```

---

## UPDATE

```sql
UPDATE sales SET status='DONE';
```

---

# 33. Time Travel

Read old versions.

```sql
SELECT * FROM sales VERSION AS OF 5;
```

or

```sql
SELECT * FROM sales TIMESTAMP AS OF '2025-01-01';
```

---

# 34. VACUUM

Deletes old files.

Frees storage.

```sql
VACUUM sales;
```

Important:

Can remove time travel history.

---

# 35. OPTIMIZE

Compacts small files.

Improves performance.

```sql
OPTIMIZE sales;
```

---

# 36. ZORDER

Co-locates related data.

Improves filtering.

```sql
OPTIMIZE sales
ZORDER BY (customer_id);
```

---

# 37. Liquid Clustering

New important topic.

Automatically optimizes data layout.

Advantages:

- Easier maintenance
- Better query performance
- Adaptive clustering

Better than static partitioning in many cases.

---

# 38. Predictive Optimization

Databricks automatically:

- Runs optimize
- Manages maintenance
- Improves performance

Reduces manual tuning.

---

# 39. Views

## Standard View

Virtual query.

No data stored.

---

## Materialized View

Stores results physically.

Faster reads.

Requires refresh.

---

## Streaming Table

Continuously updated.

Used with streaming pipelines.

---

# 40. Data Quality

Important topic.

Checks include:

- Null validation
- Duplicate validation
- Range checks
- Type checks
- Business rules

---

## Example

```python
df.filter(col("salary").isNull())
```

---

# 41. Lakeflow Jobs

Very important.

Used for orchestration.

---

# 42. DAG

Directed Acyclic Graph.

Defines task dependencies.

Example:

```text
Task A → Task B → Task C
```

---

# 43. Task Types

| Task | Use |
|---|---|
| Notebook | Run notebook |
| SQL | Execute SQL |
| Pipeline | Run pipeline |
| Dashboard | Refresh dashboard |

---

# 44. Retries

Automatically reruns failed tasks.

Useful for transient failures.

---

# 45. Conditional Tasks

Supports:

- If conditions
- Branching
- Looping

---

# 46. Triggers

## Scheduled Trigger

Time-based.

Example:

Daily ETL.

---

## File Arrival Trigger

Runs when file arrives.

Event-driven.

---

## Table Update Trigger

Runs after upstream table updates.

---

# 47. CI/CD

Increasingly important in exam.

---

# 48. Git Folders (Repos)

Integrates Git.

Supports:

- Clone repo
- Branching
- Commit
- Push
- Pull request workflow

---

# 49. Automation Bundles (Asset Bundles)

Used for deployment.

Deploys:

- Jobs
- Pipelines
- Dashboards
- Workflows

Across:

- Dev
- Test
- Prod

---

# 50. Environment Variables

Different configs per environment.

Example:

```yaml
variables:
  catalog: dev_catalog
```

---

# 51. Databricks CLI

Command-line interface.

Used for:

- Deployment
- Validation
- Automation

---

## Common Commands

```bash
databricks bundle validate
```

```bash
databricks bundle deploy
```

---

# 52. Monitoring Jobs

Lakeflow Jobs UI shows:

- Success/failure
- Runtime
- Logs
- Dependencies
- Historical trends

---

# 53. Common Failures

## Cluster Startup Failure

Possible causes:

- Capacity unavailable
- Wrong configuration
- Policy restriction

---

## Library Conflict

Different package versions conflict.

---

## Out Of Memory (OOM)

Causes:

- Large collect()
- Large shuffle
- Skew
- Insufficient memory

---

# 54. Best Practices

## Avoid collect()

collect() brings data to driver.

Dangerous for large datasets.

---

## Prefer broadcast joins for small tables

Reduces shuffle.

---

## Use Delta instead of Parquet

Better reliability.

---

## Use checkpointing for streaming

Improves fault tolerance.

---

## Optimize small files

Too many small files hurt performance.

---

# 55. Common Exam Trap Questions

## Trap 1

Question:

Need minimal admin + auto scaling.

Correct answer:

Serverless.

---

## Trap 2

Question:

Need incremental scalable ingestion from millions of files.

Correct:

Auto Loader.

---

## Trap 3

Question:

Need update + insert handling.

Correct:

MERGE INTO.

---

## Trap 4

Question:

Need repeated queries faster.

Correct:

Cache.

---

## Trap 5

Question:

One table very small.

Correct:

Broadcast join.

---

## Trap 6

Question:

Need historical version.

Correct:

Time travel.

---

# 56. SQL Essentials

Very important.

## Create Table

```sql
CREATE TABLE employees (
 id INT,
 name STRING
);
```

---

## Insert

```sql
INSERT INTO employees VALUES (1, 'John');
```

---

## Select

```sql
SELECT * FROM employees;
```

---

## Group By

```sql
SELECT dept, COUNT(*)
FROM employees
GROUP BY dept;
```

---

## Having

```sql
SELECT dept, COUNT(*)
FROM employees
GROUP BY dept
HAVING COUNT(*) > 5;
```

---

# 57. Window Functions

Possible exam topic.

## Row Number

```sql
ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
```

---

## Rank

```sql
RANK() OVER (PARTITION BY dept ORDER BY salary DESC)
```

---

# 58. CDC (Change Data Capture)

Tracks changes.

Operations:

- INSERT
- UPDATE
- DELETE

Usually implemented with:

- MERGE
- Watermarks
- Incremental loads

---

# 59. Watermarks

Streaming concept.

Used to:

- Handle late data
- Limit state growth

---

# 60. File Formats

## Parquet

- Columnar
- Efficient
- Compressed

---

## Delta

Parquet + transaction log.

---

## JSON

Semi-structured.

Nested support.

---

## CSV

Simple text format.

No schema support.

---

# 61. Semi-Structured Data

Examples:

- JSON
- XML
- Nested arrays

Common operations:

- explode
- select nested fields
- flattening

---

# 62. Streaming vs Batch

| Batch | Streaming |
|---|---|
| Finite data | Infinite data |
| Scheduled | Continuous |
| Lower complexity | Higher complexity |

---

# 63. Important PySpark Functions

## col()

```python
from pyspark.sql.functions import col
```

---

## lit()

```python
lit("ACTIVE")
```

---

## when()

```python
when(col("salary") > 5000, "HIGH")
```

---

## concat()

```python
concat(col("first"), col("last"))
```

---

## split()

```python
split(col("name"), " ")
```

---

# 64. Performance Optimization Summary

| Problem | Solution |
|---|---|
| Small table join | Broadcast join |
| Too many small files | OPTIMIZE |
| Repeated computation | Cache |
| Large shuffle | Repartition wisely |
| Slow query filters | ZORDER |
| Streaming recovery | Checkpoint |

---

# 65. Most Important Topics to Focus On

HIGH PRIORITY:

1. Delta Lake
2. Unity Catalog
3. Auto Loader
4. PySpark joins
5. DataFrame transformations
6. Lakeflow Jobs
7. Medallion architecture
8. Spark optimization
9. MERGE INTO
10. Managed vs External tables
11. Structured Streaming basics
12. Governance and security
13. Performance troubleshooting

---

# 66. Last-Minute Revision Sheet

## Remember

- Delta = ACID + Time Travel
- Auto Loader = scalable ingestion
- COPY INTO = incremental batch loading
- Broadcast join = reduce shuffle
- Cache = repeated usage optimization
- Managed table DROP deletes files
- External table DROP keeps files
- Checkpoint = streaming fault tolerance
- MERGE INTO = UPSERT
- OPTIMIZE = compact small files
- ZORDER = improve filtering
- Liquid clustering = adaptive optimization
- Unity Catalog = governance
- Serverless = minimal admin
- Job cluster = scheduled ETL
- SQL warehouse = BI workloads

---

# 67. Exam Strategy

## During Exam

1. Read carefully.
2. Look for keywords:
   - scalable
   - low cost
   - streaming
   - governance
   - minimal administration
   - incremental
3. Eliminate wrong options.
4. Think production best practices.
5. Choose most optimized solution.

---

# 68. Most Common Keyword Mapping

| Keyword | Likely Answer |
|---|---|
| Incremental ingestion | Auto Loader/COPY INTO |
| UPSERT | MERGE INTO |
| Small dimension table | Broadcast join |
| Historical data | Time travel |
| Central governance | Unity Catalog |
| Streaming reliability | Checkpoint |
| Real-time ingestion | Structured Streaming |
| Production ETL | Job cluster |
| BI analytics | SQL Warehouse |
| Minimal admin | Serverless |

---

# 69. Final Advice

To pass the certification:

- Practice PySpark transformations daily.
- Learn Delta Lake commands thoroughly.
- Understand WHY optimization techniques work.
- Focus on practical scenarios.
- Practice joins and streaming concepts.
- Memorize Unity Catalog hierarchy.
- Understand ingestion tool selection.
- Revise performance bottlenecks.

If you can explain:

- why shuffle is expensive,
- when to use Auto Loader,
- why Delta is superior to Parquet,
- how Unity Catalog improves governance,
- and how MERGE INTO works,

you are already close to passing.

