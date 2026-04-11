# PySpark L7: Aggregations & GroupBy --- ULTIMATE MASTER GUIDE (DEEP + ZERO CONFUSION)

------------------------------------------------------------------------

# 1. INTRODUCTION

Aggregations are the **core of business logic in data engineering**.
Every reporting layer, dashboard, and metric computation depends on
correct aggregation.

If done incorrectly, aggregations can: - Produce incorrect metrics (most
dangerous) - Double count data - Drop records silently - Cause massive
performance issues (shuffle-heavy)

This guide ensures: - Complete conceptual clarity - Full function
coverage - Internal execution understanding - Real-world patterns - Edge
cases & pitfalls

------------------------------------------------------------------------

# 2. CORE MENTAL MODEL

Aggregation = 1. Group data (groupBy) 2. Apply computation (sum, count,
etc.)

BUT internally: - Data is shuffled by grouping keys - Aggregation
happens per partition → then merged

------------------------------------------------------------------------

# 3. GROUPBY --- FOUNDATION

## Syntax

``` python
df.groupBy("col")
df.groupBy("col1", "col2")
```

## Example

``` python
df = spark.createDataFrame([
    ("A", 10),
    ("A", 20),
    ("B", 5)
], ["category", "value"])

df.groupBy("category").sum("value")
```

------------------------------------------------------------------------

## INTERNAL BEHAVIOR

-   Wide transformation
-   Causes shuffle (Exchange)
-   Expensive

------------------------------------------------------------------------

## PITFALL

High cardinality keys → too many partitions → performance issues

------------------------------------------------------------------------

# 4. AGGREGATION FUNCTIONS (FULL COVERAGE)

------------------------------------------------------------------------

## 4.1 sum()

``` python
df.groupBy("category").sum("value")
```

### Behavior

-   Ignores NULL

------------------------------------------------------------------------

## 4.2 count()

``` python
df.groupBy("category").count()
```

### Behavior

-   Counts rows (including NULL rows)

------------------------------------------------------------------------

## 4.3 count(col)

``` python
df.groupBy("category").count("value")
```

### Behavior

-   Counts non-null values only

------------------------------------------------------------------------

## 4.4 avg / mean

``` python
df.groupBy("category").avg("value")
```

------------------------------------------------------------------------

## 4.5 min / max

``` python
df.groupBy("category").min("value")
```

------------------------------------------------------------------------

## 4.6 approx_count_distinct()

``` python
from pyspark.sql.functions import approx_count_distinct

df.groupBy("category").agg(approx_count_distinct("user_id"))
```

### Use Case

-   Large-scale distinct counting (faster, approximate)

------------------------------------------------------------------------

## 4.7 collect_list / collect_set

``` python
from pyspark.sql.functions import collect_list, collect_set

df.groupBy("category").agg(collect_list("value"))
```

### Difference

  Function       Behavior
  -------------- --------------------
  collect_list   keeps duplicates
  collect_set    removes duplicates

------------------------------------------------------------------------

# 5. agg() --- FLEXIBLE AGGREGATION

## Syntax

``` python
from pyspark.sql.functions import sum, avg

df.groupBy("category").agg(
    sum("value").alias("total"),
    avg("value").alias("avg_val")
)
```

------------------------------------------------------------------------

## Why Use agg()

-   Multiple aggregations
-   Custom naming
-   Required in real pipelines

------------------------------------------------------------------------

# 6. GROUPBY MULTIPLE COLUMNS

``` python
df.groupBy("category", "date").sum("value")
```

------------------------------------------------------------------------

## Pitfall

-   High cardinality → huge shuffle

------------------------------------------------------------------------

# 7. DISTINCT VS GROUPBY

## DISTINCT

``` python
df.select("col").distinct()
```

## GROUPBY

``` python
df.groupBy("col").count()
```

------------------------------------------------------------------------

## Difference

-   distinct → removes duplicates
-   groupBy → aggregates

------------------------------------------------------------------------

# 8. NULL BEHAVIOR IN AGGREGATION

------------------------------------------------------------------------

## Example

``` python
df = spark.createDataFrame([
    ("A", None),
    ("A", 10)
], ["category", "value"])
```

------------------------------------------------------------------------

## sum

→ ignores NULL → result = 10

------------------------------------------------------------------------

## count(value)

→ counts non-null → result = 1

------------------------------------------------------------------------

## count(\*)

→ counts rows → result = 2

------------------------------------------------------------------------

# 9. HAVING FILTER (POST AGGREGATION)

## Pattern

``` python
df.groupBy("category").sum("value").filter("sum(value) > 10")
```

------------------------------------------------------------------------

## Pitfall

-   Filtering before aggregation ≠ after aggregation

------------------------------------------------------------------------

# 10. PERFORMANCE --- CRITICAL

------------------------------------------------------------------------

## 10.1 SHUFFLE

Aggregation causes:

    Exchange hashpartitioning(...)

------------------------------------------------------------------------

## 10.2 PARTIAL AGGREGATION

Spark does: - Local aggregation - Then global aggregation

------------------------------------------------------------------------

## 10.3 OPTIMIZATION

-   Reduce columns before groupBy
-   Filter early
-   Use proper partitioning

------------------------------------------------------------------------

# 11. SKEW IN AGGREGATION

------------------------------------------------------------------------

## Problem

One key has huge data

------------------------------------------------------------------------

## Impact

-   One partition overloaded

------------------------------------------------------------------------

## Solution

-   Salting
-   Pre-aggregation

------------------------------------------------------------------------

# 12. COMMON PRODUCTION BUGS

1.  Double counting due to duplicate rows\
2.  Wrong grouping keys\
3.  NULL misinterpretation\
4.  Aggregation after join explosion\
5.  Using collect_list → memory issues

------------------------------------------------------------------------

# 13. BEST PRACTICES

-   Always validate aggregation result
-   Check duplicates before aggregation
-   Use approx_count_distinct for scale
-   Avoid collect_list on large data
-   Understand NULL behavior

------------------------------------------------------------------------

# FINAL UNDERSTANDING

Aggregation is: - Core of business logic - Performance-heavy -
Error-prone if misunderstood

Mastering this ensures: - Correct metrics - Scalable pipelines -
Interview confidence
