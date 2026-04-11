# PySpark L5: Type Casting & Data Type Handling --- Ultimate Master Guide

------------------------------------------------------------------------

# 1. INTRODUCTION

Type casting and data type handling in PySpark is one of the most
critical aspects of building reliable and performant data pipelines.
Data coming from real-world systems is often inconsistent --- numbers
stored as strings, malformed values, mixed types, or unexpected formats.

Improper type handling leads to: - Silent NULL generation (very
dangerous) - Incorrect joins (string vs int mismatch) - Failed
aggregations - Performance degradation (no predicate pushdown)

This guide provides **complete coverage of type casting and data type
handling**, including: - All casting methods - Safe vs unsafe casting -
Internal behavior - Real-world bugs - Best practices

------------------------------------------------------------------------

# 2. CORE CONCEPT: DATA TYPES IN SPARK

Spark supports:

## Primitive Types

-   IntegerType
-   LongType
-   DoubleType
-   FloatType
-   StringType
-   BooleanType
-   DateType
-   TimestampType
-   DecimalType

## Key Understanding

Data type determines: - How data is stored - How operations behave -
Whether optimizations apply

------------------------------------------------------------------------

# 3. BASIC TYPE CASTING

## Concept

Casting converts a column from one data type to another.

## Syntax

``` python
from pyspark.sql.functions import col

df.withColumn("age", col("age").cast("int"))
```

------------------------------------------------------------------------

## Example (Complete)

``` python
df = spark.createDataFrame([
    ("1",),
    ("2",),
    ("abc",)
], ["age"])

df_casted = df.withColumn("age_int", col("age").cast("int"))
df_casted.show()
```

## Output Behavior

-   "1" → 1
-   "2" → 2
-   "abc" → NULL

------------------------------------------------------------------------

## Pitfall (CRITICAL)

``` text
Invalid casting → NULL silently
```

This is one of the biggest production bugs.

------------------------------------------------------------------------

# 4. SAFE CASTING (INDUSTRY PATTERN)

## Concept

Validate before casting to avoid silent NULLs.

## Syntax

``` python
from pyspark.sql.functions import when

df.withColumn(
    "age_int",
    when(col("age").rlike("^[0-9]+$"), col("age").cast("int"))
)
```

------------------------------------------------------------------------

## Explanation

-   Only numeric strings are cast
-   Invalid values remain NULL or handled separately

------------------------------------------------------------------------

## Industry Use Case

-   Data ingestion pipelines
-   Schema enforcement

------------------------------------------------------------------------

# 5. MULTIPLE COLUMN CASTING

## Syntax

``` python
df.select(
    col("id").cast("int"),
    col("salary").cast("double"),
    col("date").cast("date")
)
```

------------------------------------------------------------------------

## Use Case

-   Schema normalization
-   Preparing data for analytics

------------------------------------------------------------------------

# 6. CAST VS ASTYPE

## Syntax

``` python
df.withColumn("age", col("age").astype("int"))
```

## Note

-   `astype()` is alias of `cast()`
-   Prefer `cast()` for clarity

------------------------------------------------------------------------

# 7. TYPE MISMATCH IN JOINS (CRITICAL BUG)

## Problem

``` python
df1.join(df2, "id")
```

If: - df1.id → string - df2.id → int

→ Join fails silently (no match)

------------------------------------------------------------------------

## Solution

``` python
df1 = df1.withColumn("id", col("id").cast("int"))
```

------------------------------------------------------------------------

# 8. DECIMAL TYPE (FINANCIAL DATA)

## Syntax

``` python
from pyspark.sql.types import DecimalType

df.withColumn("amount", col("amount").cast(DecimalType(10,2)))
```

------------------------------------------------------------------------

## Use Case

-   Financial calculations
-   Avoid floating point errors

------------------------------------------------------------------------

## Pitfall

-   Decimal operations slower than double

------------------------------------------------------------------------

# 9. DATE & TIMESTAMP CASTING

## Date

``` python
df.withColumn("date", col("date_str").cast("date"))
```

## Timestamp

``` python
df.withColumn("ts", col("ts_str").cast("timestamp"))
```

------------------------------------------------------------------------

## Pitfall

-   Wrong format → NULL
-   Prefer to_date / to_timestamp

------------------------------------------------------------------------

# 10. BOOLEAN CASTING

## Example

``` python
df.withColumn("flag", col("flag").cast("boolean"))
```

------------------------------------------------------------------------

## Behavior

-   "true" → True
-   "false" → False
-   invalid → NULL

------------------------------------------------------------------------

# 11. TYPE HANDLING IN AGGREGATIONS

## Example

``` python
df.groupBy().sum("salary")
```

If salary is string: → aggregation fails or gives incorrect result

------------------------------------------------------------------------

## Best Practice

Always cast before aggregation

------------------------------------------------------------------------

# 12. INTERNAL BEHAVIOR

-   Casting is a column expression
-   No immediate execution
-   Optimized by Catalyst
-   Invalid cast handled as NULL (no exception)

------------------------------------------------------------------------

# 13. COMMON PRODUCTION BUGS

1.  Silent NULL due to bad casting\
2.  Join mismatch due to type difference\
3.  Wrong date format causing NULL\
4.  Using string for numeric → slow queries\
5.  Decimal precision issues

------------------------------------------------------------------------

# 14. BEST PRACTICES

-   Always define schema explicitly
-   Validate before casting
-   Avoid string for numeric fields
-   Use DecimalType for finance
-   Normalize types before joins

------------------------------------------------------------------------

# FINAL UNDERSTANDING

Type casting is: - A correctness layer - A schema enforcement
mechanism - A performance optimization tool

Mastering this ensures: - Correct joins - Accurate aggregations - Stable
pipelines
