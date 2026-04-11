# PySpark L9: Performance Optimization --- ULTIMATE MASTER GUIDE (DEEP INTERNALS + REAL DEBUGGING)

------------------------------------------------------------------------

# 1. INTRODUCTION (WHAT PERFORMANCE REALLY MEANS IN SPARK)

Performance in Spark is NOT just about speed --- it is about:

-   Efficient resource utilization (CPU, memory, network)
-   Minimizing data movement (shuffle)
-   Avoiding skew and bottlenecks
-   Ensuring scalability

A slow Spark job usually fails due to: - Excessive shuffle - Poor
partitioning - Skewed data - Wrong join strategy - Inefficient
transformations

This guide will teach you: - HOW Spark executes jobs - HOW to identify
bottlenecks - HOW to optimize correctly

------------------------------------------------------------------------

# 2. CORE EXECUTION MODEL (FOUNDATION)

Spark execution flow:

1.  Logical Plan (your code)
2.  Optimized Plan (Catalyst)
3.  Physical Plan (execution strategy)
4.  DAG → Stages → Tasks

------------------------------------------------------------------------

## Key Terms

### Job

Triggered by an action

### Stage

Group of tasks separated by shuffle

### Task

Unit of execution per partition

------------------------------------------------------------------------

# 3. LAZY EVALUATION (CRITICAL)

Spark builds execution plan but does not execute until action.

``` python
df = df.filter(...).select(...)
```

No execution yet.

``` python
df.show()
```

Execution triggered.

------------------------------------------------------------------------

# 4. SHUFFLE (BIGGEST PERFORMANCE KILLER)

------------------------------------------------------------------------

## What is Shuffle?

Data movement across partitions.

------------------------------------------------------------------------

## When it happens

-   groupBy
-   join
-   distinct
-   orderBy

------------------------------------------------------------------------

## Why expensive?

-   Network IO
-   Disk spill
-   Serialization

------------------------------------------------------------------------

## Identify in plan

    Exchange hashpartitioning(...)

------------------------------------------------------------------------

# 5. PARTITIONING (VERY IMPORTANT)

------------------------------------------------------------------------

## Concept

Data divided into partitions → parallel execution

------------------------------------------------------------------------

## 5.1 repartition()

``` python
df.repartition(10)
```

-   Full shuffle
-   Redistributes data

------------------------------------------------------------------------

## 5.2 coalesce()

``` python
df.coalesce(2)
```

-   Reduces partitions
-   No shuffle

------------------------------------------------------------------------

## When to use

  Operation     Use
  ------------- -------------------
  repartition   rebalance data
  coalesce      reduce partitions

------------------------------------------------------------------------

## Pitfall

Too many partitions → overhead\
Too few partitions → underutilization

------------------------------------------------------------------------

# 6. DATA SKEW (REAL PROBLEM)

------------------------------------------------------------------------

## What is Skew?

Uneven data distribution

Example: - One key = 90% data

------------------------------------------------------------------------

## Impact

-   One task runs forever
-   Others finish quickly

------------------------------------------------------------------------

## Detection

-   Long-running task
-   Uneven stage time

------------------------------------------------------------------------

## Solutions

### 1. Salting

Add random key

### 2. Broadcast small table

### 3. Skew join hint

``` python
df.hint("skew")
```

------------------------------------------------------------------------

# 7. JOIN OPTIMIZATION

------------------------------------------------------------------------

## Broadcast Join

``` python
df1.join(broadcast(df2), "id")
```

------------------------------------------------------------------------

## Why faster?

-   No shuffle

------------------------------------------------------------------------

## Config

``` python
spark.sql.autoBroadcastJoinThreshold
```

------------------------------------------------------------------------

## Pitfall

Broadcast too large → OOM

------------------------------------------------------------------------

# 8. CACHING & PERSISTENCE

------------------------------------------------------------------------

## cache()

``` python
df.cache()
```

------------------------------------------------------------------------

## persist()

``` python
df.persist(StorageLevel.MEMORY_AND_DISK)
```

------------------------------------------------------------------------

## When to use

-   Reused DataFrames
-   Iterative operations

------------------------------------------------------------------------

## Pitfall

-   Over-caching → memory pressure

------------------------------------------------------------------------

# 9. FILE FORMAT OPTIMIZATION

------------------------------------------------------------------------

## Use Parquet

-   Columnar
-   Compressed
-   Supports predicate pushdown

------------------------------------------------------------------------

## Avoid CSV

-   Slow parsing
-   No schema

------------------------------------------------------------------------

# 10. COLUMN PRUNING & PREDICATE PUSHdown

------------------------------------------------------------------------

## Concept

Read only required columns

``` python
df.select("id")
```

------------------------------------------------------------------------

## Predicate Pushdown

``` python
df.filter("date > '2024-01-01'")
```

------------------------------------------------------------------------

## Benefit

-   Less data read
-   Faster execution

------------------------------------------------------------------------

# 11. UDF vs BUILT-IN FUNCTIONS

------------------------------------------------------------------------

## UDF

-   Black box
-   No optimization

------------------------------------------------------------------------

## Built-in

-   Catalyst optimized
-   Faster

------------------------------------------------------------------------

## Rule

ALWAYS prefer built-in

------------------------------------------------------------------------

# 12. EXPLAIN PLAN ANALYSIS

------------------------------------------------------------------------

## Use

``` python
df.explain(True)
```

------------------------------------------------------------------------

## Check for

-   Exchange (shuffle)
-   Join type
-   Scan type

------------------------------------------------------------------------

# 13. COMMON PRODUCTION ISSUES

------------------------------------------------------------------------

1.  Excessive shuffle\
2.  Skewed joins\
3.  Using UDF unnecessarily\
4.  Wrong partitioning\
5.  Not using broadcast

------------------------------------------------------------------------

# 14. DEBUGGING WORKFLOW (STEP-BY-STEP)

------------------------------------------------------------------------

1.  Run explain(True)
2.  Identify shuffle
3.  Check join type
4.  Check partition size
5.  Optimize

------------------------------------------------------------------------

# 15. BEST PRACTICES

------------------------------------------------------------------------

-   Filter early
-   Select only required columns
-   Avoid wide transformations
-   Use broadcast wisely
-   Monitor skew

------------------------------------------------------------------------

# FINAL UNDERSTANDING

Performance optimization is:

-   Understanding execution
-   Reducing data movement
-   Using correct strategies

Mastering this means: - Faster pipelines - Lower costs - Production
reliability
