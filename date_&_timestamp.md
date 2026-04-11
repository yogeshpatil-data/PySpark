# PySpark L5: Date & Timestamp Handling --- COMPLETE MASTER GUIDE (FULL COVERAGE)

------------------------------------------------------------------------

# 1. INTRODUCTION

Date & Timestamp handling in PySpark is one of the most complex and
critical areas in data engineering because it combines: - Parsing
(string → date/timestamp) - Timezone interpretation - Arithmetic
operations - Formatting - Partitioning & performance

Unlike basic transformations, mistakes here **do NOT fail loudly** ---
they produce **wrong data silently**, which is far more dangerous.

------------------------------------------------------------------------

# 2. COMPLETE FUNCTION COVERAGE (WHAT YOU MUST KNOW)

This guide covers ALL commonly used and interview-relevant functions:

## Parsing

-   to_date
-   to_timestamp
-   cast (date/timestamp)

## Formatting

-   date_format

## Extraction

-   year, month, dayofmonth
-   hour, minute, second
-   weekofyear, quarter

## Arithmetic

-   date_add, date_sub
-   datediff
-   add_months, months_between

## Current Time

-   current_date, current_timestamp

## Truncation

-   trunc
-   date_trunc

## Timezone

-   from_utc_timestamp
-   to_utc_timestamp

------------------------------------------------------------------------

# 3. PARSING (STRING → DATE/TIMESTAMP)

## to_date()

### Concept

Explicit parsing using format string

### Syntax

``` python
to_date(col("col"), "yyyy-MM-dd")
```

### Example

``` python
df = spark.createDataFrame([("2024-01-10",)], ["raw"])
df.withColumn("date", to_date(col("raw"), "yyyy-MM-dd"))
```

### Pitfall

Wrong format → NULL (silent failure)

------------------------------------------------------------------------

## to_timestamp()

``` python
to_timestamp(col("col"), "yyyy-MM-dd HH:mm:ss")
```

### Key Difference vs cast

-   cast → implicit parsing (unsafe)
-   to_timestamp → explicit (safe)

------------------------------------------------------------------------

# 4. EXTRACTION FUNCTIONS (FULL)

## Syntax

``` python
from pyspark.sql.functions import year, month, dayofmonth, hour

df.select(
    year("ts"),
    month("ts"),
    dayofmonth("ts"),
    hour("ts")
)
```

## Additional Functions

-   weekofyear
-   quarter
-   dayofweek

------------------------------------------------------------------------

# 5. FORMATTING

## date_format()

``` python
date_format(col("ts"), "yyyy/MM/dd")
```

### Use Case

-   Reporting layer
-   Export formatting

------------------------------------------------------------------------

# 6. DATE ARITHMETIC (FULL COVERAGE)

## date_add / date_sub

``` python
date_add(col("date"), 1)
date_sub(col("date"), 1)
```

------------------------------------------------------------------------

## datediff

``` python
datediff(col("d1"), col("d2"))
```

Returns number of days

------------------------------------------------------------------------

## add_months

``` python
add_months(col("date"), 1)
```

------------------------------------------------------------------------

## months_between

``` python
months_between(col("d1"), col("d2"))
```

------------------------------------------------------------------------

# 7. CURRENT TIME FUNCTIONS

``` python
current_date()
current_timestamp()
```

------------------------------------------------------------------------

# 8. TRUNCATION FUNCTIONS

## trunc (DATE)

``` python
trunc(col("date"), "MM")
```

## date_trunc (TIMESTAMP)

``` python
date_trunc("hour", col("ts"))
```

------------------------------------------------------------------------

# 9. TIMEZONE HANDLING (FULL CONCEPT)

## Internal Truth

Spark stores timestamps in **UTC internally**

------------------------------------------------------------------------

## Problem

If your data is local time but Spark assumes UTC → shifted values

------------------------------------------------------------------------

## Fix

``` python
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

------------------------------------------------------------------------

## Conversion Functions

### from_utc_timestamp

``` python
from_utc_timestamp(col("ts"), "Asia/Kolkata")
```

### to_utc_timestamp

``` python
to_utc_timestamp(col("ts"), "Asia/Kolkata")
```

------------------------------------------------------------------------

# 10. STRING vs DATE COMPARISON (CRITICAL BUG)

## WRONG

``` python
col("date_str") > "2024-01-01"
```

## CORRECT

``` python
to_date(col("date_str"), "yyyy-MM-dd") > "2024-01-01"
```

------------------------------------------------------------------------

# 11. PARTITIONING USE CASE

## Correct Pattern

``` python
df.withColumn("date", to_date(col("ts")))
```

Partition on DateType

------------------------------------------------------------------------

# 12. INTERNAL BEHAVIOR

-   All functions are Catalyst optimized
-   No shuffle (column operations)
-   Enables partition pruning when used correctly

------------------------------------------------------------------------

# 13. REAL-WORLD BUGS (IMPORTANT)

1.  Wrong format → NULL silently\
2.  Timezone mismatch → wrong aggregations\
3.  Using cast instead of to_timestamp\
4.  Comparing strings instead of dates\
5.  Partitioning on string

------------------------------------------------------------------------

# 14. BEST PRACTICES

-   Always parse early in pipeline
-   Always use explicit formats
-   Always standardize timezone (UTC)
-   Validate parsed output (check NULL count)
-   Never treat date as string

------------------------------------------------------------------------

# FINAL UNDERSTANDING

Date handling is: - Parsing problem - Timezone problem - Data type
problem

Mastering all three = production-grade pipelines
