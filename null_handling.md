# PySpark L5: NULL Handling --- Ultimate Master Guide

------------------------------------------------------------------------

# 1. INTRODUCTION

NULL handling in PySpark is one of the most critical and misunderstood
areas in data engineering. NULL does not behave like normal values. It
represents **unknown or missing data**, and Spark follows **SQL-based
3-valued logic**, which directly impacts filtering, joins, aggregations,
and transformations.

If NULL is not handled properly: - Joins silently drop records -
Aggregations give incorrect results - Filters behave unexpectedly - Data
pipelines become unreliable

This document provides **complete coverage of NULL operations in
PySpark**, including syntax, behavior, edge cases, and real-world usage.

------------------------------------------------------------------------

# 2. SQL 3-VALUED LOGIC (FOUNDATION)

Spark follows SQL logic with three states: - TRUE - FALSE - NULL
(UNKNOWN)

### Example

``` python
from pyspark.sql.functions import col

df = spark.createDataFrame([(1, None), (2, 10)], ["id", "value"])

df.select((col("value") == 10).alias("comparison")).show()
```

### Output Explanation

-   NULL == 10 → NULL (not False)
-   10 == 10 → TRUE

### Key Understanding

-   NULL comparisons never return TRUE or FALSE
-   They return UNKNOWN (NULL)

### Pitfall

-   Assuming NULL behaves like Python None → incorrect

------------------------------------------------------------------------

# 3. DETECTING NULL VALUES

## 3.1 isNull()

``` python
df.filter(col("value").isNull())
```

### Use Case

-   Identify missing records
-   Data validation checks

------------------------------------------------------------------------

## 3.2 isNotNull()

``` python
df.filter(col("value").isNotNull())
```

------------------------------------------------------------------------

## ❌ WRONG APPROACH

``` python
df.filter(col("value") == None)
```

### Why Wrong

-   `==` does not work with NULL
-   Returns NULL, not True/False

------------------------------------------------------------------------

# 4. REPLACING NULL VALUES

------------------------------------------------------------------------

## 4.1 fillna()

``` python
df.fillna({"value": 0})
```

### Explanation

-   Replaces NULL with given value
-   Works column-wise or globally

### Industry Use Case

-   Filling missing metrics
-   Default values in reports

### Pitfall

-   NULL ≠ 0 (semantic difference)

------------------------------------------------------------------------

## 4.2 na.fill()

``` python
df.na.fill({"value": 0})
```

Same as fillna()

------------------------------------------------------------------------

## 4.3 coalesce() (VERY IMPORTANT)

``` python
from pyspark.sql.functions import coalesce

df.withColumn("final", coalesce("col1", "col2"))
```

### Explanation

-   Returns first non-null value

### Industry Use Case

-   Data merging from multiple sources
-   Fallback logic

------------------------------------------------------------------------

## 4.4 when().otherwise()

``` python
from pyspark.sql.functions import when

df.withColumn(
    "value",
    when(col("value").isNull(), 100).otherwise(col("value"))
)
```

### Use Case

-   Business-rule based replacement

------------------------------------------------------------------------

# 5. DROPPING NULL VALUES

## 5.1 dropna()

``` python
df.dropna()
```

Removes rows with ANY NULL

------------------------------------------------------------------------

## 5.2 dropna(subset)

``` python
df.dropna(subset=["value"])
```

Removes rows where specific column is NULL

------------------------------------------------------------------------

### Pitfall

-   Can lead to data loss
-   Always validate impact before dropping

------------------------------------------------------------------------

# 6. NULL-SAFE COMPARISON

## eqNullSafe()

``` python
col("a").eqNullSafe(col("b"))
```

### Explanation

-   Treats NULL == NULL as TRUE

### Use Case

-   Joins involving NULL keys

------------------------------------------------------------------------

# 7. NULL IN JOINS (CRITICAL)

## Default Behavior

``` python
df1.join(df2, df1.id == df2.id)
```

### Problem

-   NULL values do NOT match
-   Rows get dropped silently

------------------------------------------------------------------------

## Solution

``` python
df1.join(df2, df1.id.eqNullSafe(df2.id))
```

------------------------------------------------------------------------

# 8. NULL IN AGGREGATIONS

``` python
df.select(sum("value"))
```

### Behavior

-   NULLs are ignored

### Example

\[NULL, 10\] → sum = 10

------------------------------------------------------------------------

### Pitfall

-   Assuming NULL contributes as 0

------------------------------------------------------------------------

# 9. NULL IN FILTERS

``` python
df.filter(col("value") > 10)
```

### Behavior

-   NULL rows are excluded

------------------------------------------------------------------------

# 10. COMMON PRODUCTION BUGS

1.  NULL join keys dropping data
2.  fillna incorrectly changing business meaning
3.  casting errors producing NULL silently
4.  filtering ignoring NULL rows
5.  NULL comparison errors

------------------------------------------------------------------------

# 11. BEST PRACTICES

-   Always detect NULLs explicitly
-   Avoid blind fillna
-   Use eqNullSafe for joins when needed
-   Validate data before dropping NULLs
-   Understand NULL semantics before writing conditions

------------------------------------------------------------------------

# FINAL UNDERSTANDING

NULL is: - Not a value - A state of unknown

Mastering NULL handling ensures: - Correct joins - Accurate
aggregations - Reliable pipelines
