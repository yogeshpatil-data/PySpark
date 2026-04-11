# PySpark L5: String Operations --- Master-Level Structured Guide

------------------------------------------------------------------------

# 1. INTRODUCTION

String operations in PySpark are fundamental to data cleaning,
normalization, joining, and feature engineering. In real-world
pipelines, string data is often inconsistent due to casing, whitespace,
encoding, and formatting issues. Proper handling ensures correctness in
joins, deduplication, and analytics.

------------------------------------------------------------------------

# 2. TRIM FUNCTIONS (trim, ltrim, rtrim)

## Concept

Used to remove unwanted whitespace from strings.

## When to Use

-   Before joins
-   Cleaning raw ingestion data

## Syntax

``` python
from pyspark.sql.functions import trim, ltrim, rtrim, col
df.withColumn("clean_name", trim(col("name")))
```

## Example

``` python
df = spark.createDataFrame([(" John ",)], ["name"])
df.withColumn("clean", trim(col("name"))).show()
```

## Internal Behavior

-   Narrow transformation (no shuffle)
-   Applied per row

## Edge Cases

-   Does NOT remove internal spaces
-   Only trims edges

## Pitfalls

-   Forgetting trim → join mismatches

------------------------------------------------------------------------

# 3. CASE FUNCTIONS (lower, upper, initcap)

## Concept

Standardize text casing.

## When to Use

-   Normalizing join keys
-   Deduplication

## Syntax

``` python
from pyspark.sql.functions import lower, upper, initcap, col
df.withColumn("name", lower(col("name")))
```

## Example

``` python
df = spark.createDataFrame([("JOHN",)], ["name"])
df.withColumn("name", lower(col("name"))).show()
```

## Edge Cases

-   Locale-specific behavior may differ

## Pitfalls

-   Case mismatch causes duplicate records

------------------------------------------------------------------------

# 4. LENGTH & POSITION FUNCTIONS

## Functions

-   length
-   instr
-   locate

## Syntax

``` python
from pyspark.sql.functions import length, instr, col
df.withColumn("len", length(col("name")))
```

## Example

``` python
df = spark.createDataFrame([("john@example.com",)], ["email"])
df.withColumn("pos", instr(col("email"), "@")).show()
```

## Use Case

-   Validation (email, IDs)

## Pitfalls

-   Returns 0 if not found (not NULL)

------------------------------------------------------------------------

# 5. SUBSTRING OPERATIONS

## Concept

Extract part of string.

## Syntax

``` python
from pyspark.sql.functions import substring, col
df.withColumn("prefix", substring(col("id"), 1, 3))
```

## Example

``` python
df = spark.createDataFrame([("ABC123",)], ["id"])
df.withColumn("prefix", substring(col("id"), 1, 3)).show()
```

## Edge Case

-   Index starts at 1

------------------------------------------------------------------------

# 6. CONCAT FUNCTIONS

## Functions

-   concat
-   concat_ws

## Syntax

``` python
from pyspark.sql.functions import concat_ws, col
df.withColumn("full", concat_ws(" ", col("first"), col("last")))
```

## Example

``` python
df = spark.createDataFrame([("John","Doe")], ["first","last"])
df.withColumn("full", concat_ws(" ", col("first"), col("last"))).show()
```

## Pitfalls

-   concat returns NULL if any column is NULL
-   concat_ws ignores NULL values

------------------------------------------------------------------------

# 7. SPLIT & EXPLODE

## Concept

Tokenize strings

## Syntax

``` python
from pyspark.sql.functions import split, explode, col
df.withColumn("word", explode(split(col("text"), " ")))
```

## Example

``` python
df = spark.createDataFrame([("hello world",)], ["text"])
df.withColumn("word", explode(split(col("text"), " "))).show()
```

## Pitfalls

-   explode increases row count

------------------------------------------------------------------------

# 8. REGEX FUNCTIONS

## Functions

-   regexp_replace
-   regexp_extract

## Syntax

``` python
from pyspark.sql.functions import regexp_replace, col
df.withColumn("clean", regexp_replace(col("text"), "[^a-zA-Z]", ""))
```

## Example

``` python
df = spark.createDataFrame([("abc123",)], ["text"])
df.withColumn("clean", regexp_replace(col("text"), "\\d", "")).show()
```

## Pitfalls

-   Incorrect regex can corrupt data

------------------------------------------------------------------------

# 9. REPLACE FUNCTIONS

## Functions

-   DataFrame.replace
-   regexp_replace
-   functions.replace (Spark 3.5+)

## Example

``` python
df.replace({"A": "Alpha"}, subset=["col"])
```

------------------------------------------------------------------------

# 10. TRANSLATE

## Concept

Character-level replacement

## Syntax

``` python
from pyspark.sql.functions import translate, col
df.withColumn("clean", translate(col("text"), "abc", "123"))
```

------------------------------------------------------------------------

# 11. ADVANCED (levenshtein)

## Concept

Measure string similarity

## Syntax

``` python
from pyspark.sql.functions import levenshtein, col
df.withColumn("dist", levenshtein(col("a"), col("b")))
```

------------------------------------------------------------------------

# 12. COMPARISON SUMMARY

  Function         Use Case           Notes
  ---------------- ------------------ --------------------
  trim             clean whitespace   critical for joins
  lower            normalize text     avoid duplicates
  regexp_replace   pattern cleaning   powerful but risky
  concat_ws        combine strings    null-safe
  split            tokenize           creates array

------------------------------------------------------------------------

# 13. COMMON MISTAKES

-   Not trimming before join
-   Ignoring case normalization
-   Using wrong regex
-   Not handling NULL in concat
-   Using UDF instead of built-ins

------------------------------------------------------------------------

# FINAL UNDERSTANDING

String operations are: - Core to data cleaning - Critical for joins and
deduplication - Major source of production bugs

Mastering them ensures correctness and performance.
