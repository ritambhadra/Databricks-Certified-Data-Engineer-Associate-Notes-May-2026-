# Databricks Certified Data Engineer Associate — Complete Detailed Notes (2026)



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


These are exactly the concepts that separate someone who only memorized syntax from someone who can actually pass scenario-based Databricks questions.

# 1. Why Shuffle is Expensive

## What is Shuffle?

Shuffle happens when Spark redistributes data across executors.

It occurs during:

* `join`
* `groupBy`
* `distinct`
* `repartition`
* `orderBy`

Example:

```python
df.groupBy("department").count()
```

Spark must move rows with the same department to the same partition.

---

## Why is it expensive?

Because data movement is expensive.

During shuffle Spark performs:

1. Serialization
2. Disk writes
3. Network transfer
4. Disk reads
5. Sorting

This creates:

* Heavy network I/O
* Disk spilling
* Executor imbalance
* Long stage execution

---

## Symptoms in Spark UI

You may see:

* High shuffle read/write
* Long-running tasks
* Spill to disk
* One stage much slower

---

## Example

Suppose:

* Executor 1 has customer_id 1-100
* Executor 2 has customer_id 101-200

Now you do:

```python
df.groupBy("country")
```

Spark must redistribute all rows based on country.

Huge data movement happens.

---

## How to Reduce Shuffle

## Use Broadcast Join

If one table is small:

```python
large.join(broadcast(small), "id")
```

Small table copied to all executors.

No large redistribution needed.

---

## Repartition Carefully

Too many repartitions create unnecessary shuffle.

Bad:

```python
df.repartition(1000)
```

without reason.

---

## Use Proper Partitioning

Partition data using frequently filtered columns.

---

# 2. When to Use Auto Loader

Auto Loader is for scalable cloud ingestion.

Best for:

* Incremental ingestion
* Streaming pipelines
* Millions of files
* Continuous ingestion
* Schema evolution

---

# Auto Loader vs COPY INTO

| Feature             | Auto Loader          | COPY INTO              |
| ------------------- | -------------------- | ---------------------- |
| Real-time ingestion | Yes                  | No                     |
| Streaming support   | Yes                  | No                     |
| Massive scale       | Excellent            | Medium                 |
| Schema evolution    | Strong               | Limited                |
| Cost optimization   | Better at scale      | Simpler                |
| Best use case       | Production streaming | Simple batch ingestion |

---

# Use Auto Loader When

## Scenario 1 — Continuous File Arrival

Files arrive every minute in ADLS/S3.

Correct answer:
Auto Loader.

---

## Scenario 2 — Millions of Small Files

Directory listing manually becomes expensive.

Auto Loader handles this efficiently.

---

## Scenario 3 — Schema Changes Frequently

New columns appear in JSON files.

Use:

```python
.option("mergeSchema", "true")
```

---

## Scenario 4 — Production Streaming Pipeline

Need checkpointing + fault tolerance.

Auto Loader integrates naturally with Structured Streaming.

---

# File Notification Mode vs Directory Listing

## Directory Listing

* Scans folders repeatedly
* Simpler
* Higher cloud API cost

---

## File Notification Mode

Uses cloud event notifications.

Advantages:

* More scalable
* Lower cost
* Faster detection

Preferred in production.

---

# 3. Why Delta Lake is Superior to Parquet

VERY IMPORTANT QUESTION.

---

# Plain Parquet Problems

Parquet is only a file format.

It lacks:

* ACID transactions
* Versioning
* Time travel
* Reliable concurrent writes
* Schema enforcement

---

# Delta Lake Adds Transaction Layer

Delta = Parquet + `_delta_log`

The transaction log stores:

* Commits
* Metadata
* Schema
* File tracking

---

# Advantages of Delta Lake

| Feature            | Parquet | Delta     |
| ------------------ | ------- | --------- |
| ACID transactions  | No      | Yes       |
| Time travel        | No      | Yes       |
| MERGE support      | No      | Yes       |
| UPDATE/DELETE      | Hard    | Easy      |
| Schema enforcement | Weak    | Strong    |
| Streaming support  | Basic   | Excellent |
| Reliability        | Lower   | High      |

---

# Example — UPDATE

Impossible directly in Parquet.

Easy in Delta:

```sql
UPDATE sales
SET amount = 100
WHERE id = 1;
```

---

# Example — MERGE

Delta supports UPSERT.

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Parquet cannot do this efficiently.

---

# Example — Time Travel

```sql
SELECT * FROM sales VERSION AS OF 5;
```

Useful for:

* Auditing
* Recovery
* Debugging

---

# 4. How Unity Catalog Improves Governance

Unity Catalog = centralized governance layer.

Extremely important topic.

---

# Problems Without Unity Catalog

Without UC:

* Permissions scattered
* No central governance
* Hard lineage tracking
* Security inconsistency
* Multiple metastores

---

# What Unity Catalog Provides

## Centralized Security

Single governance model across:

* Workspaces
* Tables
* Files
* Models

---

## Fine-Grained Access Control

Supports:

* Table-level security
* Column-level masking
* Row-level filtering

Example:

```sql
GRANT SELECT ON TABLE sales TO analysts;
```

---

# Data Lineage

Tracks:

* Upstream tables
* Downstream dependencies
* Data flow

Very useful for debugging and auditing.

---

# Unity catalog Hierarchy

```text
Metastore
   Catalog
      Schema
         Table
```

Must memorize.

---

# Managed Governance

Unity Catalog manages:

* Access
* Policies
* Auditing
* Metadata

centrally.

---

# ABAC Policies

Attribute-Based Access Control.

Rules based on:

* User role
* Department
* Geography
* Tags

---

# Example

HR users can see salary column.

Others get masked values.

---

# 5. How MERGE INTO Works

One of the MOST IMPORTANT Delta commands.

---

# What is MERGE?

Combines:

* INSERT
* UPDATE
* DELETE

into one operation.

Also called:

UPSERT.

---

# Why Needed?

Suppose source data contains:

* New customers
* Updated customers

Need to:

* Insert new rows
* Update existing rows

MERGE handles both.

---

# Syntax

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
```

---

# How It Works Internally

Step-by-step:

1. Delta compares source and target
2. Matching rows updated
3. Non-matching rows inserted
4. Transaction committed atomically

ACID guarantees consistency.

---

# Common Use Cases

## CDC Pipelines

Handling:

* INSERT
* UPDATE
* DELETE

from source systems.

---

## Incremental Loading

Only process changed records.

---

## SCD Type 1

Overwrite old values.

---

# Why Better Than Traditional Approach

Without MERGE:

You would need:

* Separate UPDATE
* Separate INSERT
* Manual logic
* Multiple operations

MERGE simplifies everything.

---

# MERGE Performance Considerations

MERGE can become expensive if:

* Huge target table
* Poor partitioning
* No clustering

Optimization techniques:

* ZORDER
* Liquid clustering
* Proper partitioning
* Predicate pushdown

---

# Most Important Exam Understanding

The exam usually tests:

| Scenario                       | Correct Concept |
| ------------------------------ | --------------- |
| Large shuffle causing slowness | Broadcast join  |
| Incremental scalable ingestion | Auto Loader     |
| Need ACID + UPDATE             | Delta Lake      |
| Centralized governance         | Unity Catalog   |
| UPSERT logic                   | MERGE INTO      |


“Ad hoc” means:

> **Done for a specific purpose only when needed**, not planned or scheduled regularly.

## In Databricks Context

### Ad hoc analysis

Means:

* A user runs queries manually
* Exploratory analysis
* Temporary investigation
* One-time data checks

Example:

A data engineer opens a notebook and runs:

```sql
SELECT * FROM sales
WHERE amount > 10000;
```

just to investigate an issue.

That is an **ad hoc query**.

---

# Ad Hoc vs Scheduled

| Ad Hoc            | Scheduled              |
| ----------------- | ---------------------- |
| Manual            | Automated              |
| One-time          | Repeated               |
| Exploratory       | Production workflow    |
| Usually notebooks | Usually jobs/pipelines |

---

# In Exam Questions

If the question says:

* “Data scientist wants to explore data interactively”
* “Users run occasional queries”
* “Exploratory analysis”
* “Interactive notebook usage”

Then the answer is often:

* All-purpose cluster
* SQL Warehouse (for analysts)

because these are good for ad hoc workloads.

---

# Example

## Ad hoc workload

```text
An analyst occasionally queries customer data for investigation.
```

Best choice:

* SQL Warehouse

---

## Non-ad hoc workload

```text
A pipeline runs every night at 2 AM.
```

Best choice:

* Job cluster
* Lakeflow Job

because this is scheduled production ETL.


These 13 topics are essentially the **core of the Databricks Data Engineer Associate exam**.
If you master them deeply, your probability of passing becomes very high.

Below is the **high-value conceptual understanding** you should have for each topic.

---

# 1. Delta Lake

MOST IMPORTANT TOPIC.

# What You Must Know

## Core Features

* ACID transactions
* Time travel
* Schema enforcement
* Schema evolution
* MERGE/UPDATE/DELETE
* Transaction log (`_delta_log`)

---

# Why Delta Exists

Parquet alone cannot safely handle:

* concurrent writes
* updates
* deletes
* reliable streaming

Delta solves these problems.

---

# Most Important Commands

## Create Delta Table

```sql id="bocm71"
CREATE TABLE sales
USING DELTA;
```

---

## MERGE

```sql id="jv8u3j"
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

---

## Time Travel

```sql id="d7g5tv"
SELECT * FROM sales VERSION AS OF 5;
```

---

## OPTIMIZE

```sql id="zy46lf"
OPTIMIZE sales;
```

---

## VACUUM

```sql id="f4qz2q"
VACUUM sales;
```

---

# Exam Focus

Understand:

* WHY Delta is superior to Parquet
* WHEN to use MERGE
* HOW ACID works
* WHY time travel matters

---

# 2. Unity Catalog

Governance layer of Databricks.

VERY IMPORTANT.

---

# Hierarchy

Must memorize:

```text id="z89e7w"
Metastore
   Catalog
      Schema
         Table
```

---

# Key Features

* Central governance
* Access control
* Auditing
* Lineage
* Row-level security
* Column masking

---

# Important Commands

## GRANT

```sql id="tzgqv4"
GRANT SELECT ON TABLE sales TO analysts;
```

---

## REVOKE

```sql id="obn7ut"
REVOKE MODIFY ON TABLE sales FROM analysts;
```

---

# Common Exam Scenario

Question says:

* centralized governance
* cross-workspace security
* auditing

Answer:
Unity Catalog.

---

# 3. Auto Loader

VERY IMPORTANT ingestion topic.

---

# Purpose

Efficient incremental ingestion from cloud storage.

Supports:

* S3
* ADLS
* GCS

---

# Best For

* Streaming ingestion
* Millions of files
* Continuous arrival
* Schema evolution

---

# Key Features

## Incremental Processing

Processes only new files.

---

## Checkpointing

Tracks ingestion progress.

---

## Schema Evolution

```python id="wqv91j"
.option("mergeSchema", "true")
```

---

# Modes

## Directory Listing

* scans folders
* easier
* higher cloud API cost

---

## File Notification

* cloud events
* scalable
* cheaper
* preferred in production

---

# Exam Focus

Know difference between:

| Tool        | Best Use                     |
| ----------- | ---------------------------- |
| COPY INTO   | Simple incremental batch     |
| Auto Loader | Scalable streaming ingestion |

---

# 4. PySpark Joins

VERY HIGH probability topic.

---

# Inner Join

```python id="3g3w0r"
df1.join(df2, "id", "inner")
```

Matching rows only.

---

# Left Join

```python id="q0czqx"
df1.join(df2, "id", "left")
```

Keeps all left rows.

---

# Broadcast Join

MOST IMPORTANT optimization join.

```python id="q0z8ic"
large.join(broadcast(small), "id")
```

---

# Why Broadcast Join?

Avoids shuffle.

Huge exam topic.

Use when:

* one table small
* dimension table small

---

# Cross Join

Cartesian product.

Very expensive.

Usually avoid.

---

# Exam Focus

Know:

* which join keeps which rows
* broadcast join optimization
* shuffle reduction

---

# 5. DataFrame Transformations

Core PySpark area.

---

# Common Transformations

## select

```python id="i5uk6q"
df.select("name")
```

---

## filter

```python id="s3c1b7"
df.filter(col("age") > 18)
```

---

## withColumn

```python id="n5wbbe"
df.withColumn("year", year(col("date")))
```

---

## drop

```python id="ms1xew"
df.drop("temp")
```

---

## explode

Very important for arrays.

```python id="lsk5tm"
df.select(explode("items"))
```

---

# Null Handling

```python id="vtq0l9"
df.na.fill(0)
```

```python id="u0s0t4"
df.na.drop()
```

---

# Deduplication

```python id="hmf7wd"
df.dropDuplicates(["id"])
```

---

# Exam Focus

Expect transformation-based MCQs.

---

# 6. Lakeflow Jobs

Workflow orchestration service.

---

# Used For

* ETL scheduling
* dependencies
* retries
* automation

---

# DAG

Directed Acyclic Graph.

```text id="9m8v57"
Task A → Task B → Task C
```

---

# Important Features

## Retries

Automatically reruns failed tasks.

---

## Conditional Logic

Supports:

* branching
* loops
* conditions

---

# Trigger Types

| Trigger      | Use              |
| ------------ | ---------------- |
| Scheduled    | Time-based       |
| File arrival | Event-driven     |
| Table update | Dependency-based |

---

# Exam Focus

Understand:

* DAG dependencies
* retries
* scheduling
* trigger selection

---

# 7. Medallion Architecture

VERY COMMON exam topic.

---

# Bronze

Raw data.

Minimal transformations.

Append-only.

---

# Silver

Cleaned data.

Includes:

* deduplication
* joins
* validation
* standardization

---

# Gold

Business-ready.

Used for:

* dashboards
* KPIs
* BI

---

# Flow

```text id="w0b85w"
Source → Bronze → Silver → Gold
```

---

# Exam Focus

Know what operations belong in each layer.

---

# 8. Spark Optimization

Critical topic.

---

# Shuffle

Most important bottleneck.

Occurs during:

* joins
* groupBy
* distinct
* repartition

---

# Broadcast Join

Reduces shuffle.

---

# Repartition vs Coalesce

## repartition()

Full shuffle.

```python id="k44k0u"
df.repartition(10)
```

---

## coalesce()

Reduce partitions efficiently.

```python id="5l5w6u"
df.coalesce(2)
```

---

# Cache

```python id="61zt0i"
df.cache()
```

Useful for repeated computation.

---

# Important Configurations

## shuffle partitions

```text id="rzmr7m"
spark.sql.shuffle.partitions
```

---

## broadcast threshold

```text id="pvt3wu"
spark.sql.autoBroadcastJoinThreshold
```

---

# Exam Focus

Understand WHY optimizations work.

---

# 9. MERGE INTO

VERY IMPORTANT.

UPSERT operation.

---

# Combines

* INSERT
* UPDATE
* DELETE

---

# Syntax

```sql id="nd0y7k"
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

---

# Used For

* CDC
* incremental loading
* SCD Type 1

---

# Why Important?

Atomic operation.

Reliable production pipelines.

---

# 10. Managed vs External Tables

Extremely common exam question.

---

# Managed Table

Databricks manages:

* metadata
* storage

DROP deletes data.

---

# External Table

You manage storage path.

DROP removes metadata only.

Data remains.

---

# Comparison

| Feature           | Managed | External |
| ----------------- | ------- | -------- |
| Storage managed   | Yes     | No       |
| DROP deletes data | Yes     | No       |
| Better governance | Yes     | Partial  |

---

# Exam Focus

Very commonly tested.

---

# 11. Structured Streaming Basics

Important foundational topic.

---

# Key Concepts

## Trigger

Controls execution timing.

---

## Checkpoint

Stores progress/state.

Required for fault tolerance.

---

## Watermark

Handles late-arriving data.

---

# Output Modes

| Mode     | Meaning                 |
| -------- | ----------------------- |
| Append   | New rows only           |
| Update   | Updated rows            |
| Complete | Entire result rewritten |

---

# Exam Focus

Understand checkpointing deeply.

---

# 12. Governance and Security

Mostly Unity Catalog concepts.

---

# Access Control

Using:

* GRANT
* REVOKE
* DENY

---

# Security Levels

* Catalog
* Schema
* Table
* Column
* Row

---

# Column Masking

Hide sensitive data.

---

# Row Filtering

Restrict rows by user/group.

---

# ABAC

Attribute-based access control.

Policy-driven governance.

---

# 13. Performance Troubleshooting

Very practical exam area.

---

# Common Bottlenecks

## Shuffle

Network-heavy redistribution.

---

## Data Skew

One partition too large.

Symptoms:

* one slow task
* executor imbalance

---

## Disk Spill

Memory insufficient.

Spark writes temporary data to disk.

Very slow.

---

# Spark UI

Used to analyze:

* stages
* tasks
* spill
* shuffle
* skew

---

# Important Metrics

| Metric        | Meaning             |
| ------------- | ------------------- |
| Shuffle Read  | Incoming shuffle    |
| Shuffle Write | Outgoing shuffle    |
| Spill         | Memory overflow     |
| Task Time     | Slow task detection |

---

# Final Priority Ranking for Exam

## Tier 1 (Must Master)

1. Delta Lake
2. Auto Loader
3. PySpark transformations
4. Joins
5. MERGE INTO
6. Unity Catalog
7. Spark optimization

---

## Tier 2 (Strong Understanding)

8. Lakeflow Jobs
9. Structured Streaming
10. Medallion architecture
11. Managed vs External tables

---

## Tier 3 (Support Topics)

12. Governance/security
13. Troubleshooting/monitoring

---

# Most Important Mental Model

The exam repeatedly asks:

## “Which solution is MOST optimized, scalable, and production-ready?”

Usually the correct answer is the one that:

* minimizes shuffle
* supports incremental processing
* improves governance
* reduces operational overhead
* scales automatically
* uses Delta Lake features properly

