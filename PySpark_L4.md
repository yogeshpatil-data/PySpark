# PySpark L4: Row-Level Transformations --- Ultimate Master Guide

------------------------------------------------------------------------

# 1. INTRODUCTION TO ROW-LEVEL TRANSFORMATIONS

Row-level transformations in PySpark operate on individual rows or
subsets of rows within a DataFrame. Unlike column expressions (L3),
which define computations on columns, row-level transformations
determine **which rows are included, excluded, ordered, or
deduplicated**.

These transformations are critical in: - Filtering datasets - Removing
duplicates - Sorting data - Sampling data for analysis

They directly influence: - Data correctness - Execution performance -
Shuffle behavior

------------------------------------------------------------------------

# 2. FILTER / WHERE (CORE OPERATION)

## Concept

`filter()` and `where()` are identical. They apply a condition to retain
rows.

## Syntax

``` python
df.filter(condition)
df.where(condition)
```

## Example (Standard)

``` python
from pyspark.sql.functions import col

df = spark.createDataFrame([
    (1, 25),
    (2, 17),
    (3, 30)
], ["id", "age"])

df_filtered = df.filter(col("age") > 18)
df_filtered.show()
```

## Industry Use Case

-   Filtering valid records
-   Removing inactive users

## Internal Behavior

-   This is a **narrow transformation**
-   No shuffle
-   Applied at partition level

## Pitfalls

-   Using Python conditions instead of column expressions

``` python
# WRONG
df.filter(df["age"] > 18)
```

-   Not handling NULLs properly

------------------------------------------------------------------------

# 3. COMPLEX FILTER CONDITIONS

## Example

``` python
df.filter((col("age") > 18) & (col("age") < 60))
```

## Key Rules

-   Use `&` instead of `and`
-   Use `|` instead of `or`

## Pitfalls

``` python
# WRONG
df.filter(col("age") > 18 and col("age") < 60)
```

## Special Case: NULL Handling

``` python
df.filter(col("age").isNull())
```

------------------------------------------------------------------------

# 4. DISTINCT

## Concept

Removes duplicate rows across all columns.

## Example

``` python
df.distinct()
```

## Internal Behavior

-   Wide transformation
-   Causes shuffle

## Industry Use Case

-   Removing duplicate records in raw data

## Pitfalls

-   Expensive on large datasets
-   Not suitable for partial deduplication

------------------------------------------------------------------------

# 5. dropDuplicates()

## Concept

Removes duplicates based on specific columns.

## Example

``` python
df.dropDuplicates(["id"])
```

## Industry Use Case

-   Deduplicating records using primary key

## Internal Behavior

-   Wide transformation
-   Requires shuffle

## Pitfalls

-   Keeps arbitrary record (not deterministic)
-   Needs window function for deterministic deduplication

------------------------------------------------------------------------

# 6. DETERMINISTIC DEDUPLICATION (IMPORTANT)

## Example

``` python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window = Window.partitionBy("id").orderBy(col("timestamp").desc())

df = df.withColumn("rn", row_number().over(window))        .filter(col("rn") == 1)        .drop("rn")
```

## Industry Use Case

-   Latest record per user

## Why Important

-   dropDuplicates is non-deterministic
-   window ensures controlled selection

------------------------------------------------------------------------

# 7. SORTING (orderBy / sort)

## Syntax

``` python
df.orderBy(col("age").desc())
```

## Example

``` python
df.orderBy("age")
```

## Internal Behavior

-   Wide transformation
-   Full shuffle

## Industry Use Case

-   Ranking data
-   Preparing reports

## Pitfalls

-   Expensive on large datasets
-   Avoid unnecessary sorting

------------------------------------------------------------------------

# 8. SORT WITH MULTIPLE COLUMNS

``` python
df.orderBy(col("age").desc(), col("salary").asc())
```

------------------------------------------------------------------------

# 9. LIMIT

## Concept

Returns top N rows.

``` python
df.limit(10)
```

## Behavior

-   Stops early execution
-   Efficient compared to full scan

## Pitfalls

-   No guaranteed order unless sorted

------------------------------------------------------------------------

# 10. SAMPLE

## Syntax

``` python
df.sample(fraction=0.1)
```

## Example

``` python
df.sample(fraction=0.2, seed=42)
```

## Industry Use Case

-   Data sampling for testing
-   ML model training

## Pitfalls

-   Not exact number of rows
-   Probabilistic

------------------------------------------------------------------------

# 11. SAMPLE BY (STRATIFIED SAMPLING)

``` python
df.sampleBy("category", fractions={"A": 0.1, "B": 0.2})
```

## Use Case

-   Balanced dataset creation

------------------------------------------------------------------------

# 12. REPARTITION VS COALESCE (ROW DISTRIBUTION)

## repartition

``` python
df.repartition(10)
```

-   Full shuffle
-   Redistributes data

## coalesce

``` python
df.coalesce(2)
```

-   Reduces partitions
-   No shuffle

## Pitfalls

-   Overusing repartition → expensive

------------------------------------------------------------------------

# 13. COMMON MISTAKES

-   Using filter after aggregation incorrectly
-   Not understanding shuffle in distinct/orderBy
-   Using dropDuplicates blindly
-   Ignoring NULL handling
-   Sorting unnecessarily

------------------------------------------------------------------------

# 14. PERFORMANCE INSIGHTS

-   Filter early → reduce data size
-   Avoid wide transformations unless needed
-   Combine transformations

------------------------------------------------------------------------

# FINAL UNDERSTANDING

Row-level transformations control: - Which data flows through pipeline -
How data is structured and ordered

Mastering this ensures: - Correct results - Optimized performance -
Scalable pipelines
