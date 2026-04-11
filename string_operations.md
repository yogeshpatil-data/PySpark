# PySpark L5: String Operations --- Ultimate Master Guide (Full Coverage)

------------------------------------------------------------------------

# 1. INTRODUCTION

String handling in PySpark is one of the most frequently used and most
error-prone areas in real-world data pipelines. Almost every dataset
contains textual data --- names, emails, addresses, product
descriptions, IDs, etc.

In production systems, string data is often: - inconsistent (case
differences, spaces) - malformed (special characters, encoding issues) -
duplicated logically but not exactly (e.g., "John" vs " john ")

Improper string handling leads to: - join mismatches - duplicate
records - incorrect aggregations - failed deduplication

This guide covers **ALL important string functions used in industry**,
with: - proper syntax - real examples - edge cases - interview insights

------------------------------------------------------------------------

# 2. BASIC STRING NORMALIZATION (MOST CRITICAL)

## 2.1 trim(), ltrim(), rtrim()

### Syntax

``` python
from pyspark.sql.functions import trim, ltrim, rtrim

df.withColumn("name", trim(col("name")))
```

### Explanation

-   `trim()` removes spaces from both sides
-   `ltrim()` removes leading spaces
-   `rtrim()` removes trailing spaces

### Industry Use Case

-   Cleaning join keys
-   Removing hidden whitespace issues

### Real Bug

"John" != "John" → join fails

### Pitfall

-   Forgetting trim before join → silent mismatch

------------------------------------------------------------------------

## 2.2 lower(), upper(), initcap()

``` python
from pyspark.sql.functions import lower, upper, initcap

df.withColumn("name", lower(col("name")))
```

### Explanation

-   lower → convert to lowercase
-   upper → convert to uppercase
-   initcap → capitalize words

### Industry Use Case

-   Standardizing keys before joins

### Pitfall

"john" != "John" → duplicates

------------------------------------------------------------------------

# 3. STRING LENGTH & POSITION FUNCTIONS

## 3.1 length()

``` python
from pyspark.sql.functions import length

df.withColumn("len", length(col("name")))
```

### Use Case

-   Data validation
-   Detect invalid values

------------------------------------------------------------------------

## 3.2 instr() / locate()

``` python
from pyspark.sql.functions import instr

df.withColumn("pos", instr(col("email"), "@"))
```

### Explanation

-   Finds position of substring

### Use Case

-   Email validation
-   Pattern detection

------------------------------------------------------------------------

# 4. SUBSTRING OPERATIONS

## 4.1 substring()

``` python
from pyspark.sql.functions import substring

df.withColumn("prefix", substring(col("id"), 1, 3))
```

### Explanation

-   Extracts part of string

### Pitfall

-   Index starts from 1 (NOT 0)

------------------------------------------------------------------------

# 5. CONCATENATION FUNCTIONS

## 5.1 concat()

``` python
from pyspark.sql.functions import concat

df.withColumn("full", concat(col("first"), col("last")))
```

------------------------------------------------------------------------

## 5.2 concat_ws()

``` python
from pyspark.sql.functions import concat_ws

df.withColumn("full", concat_ws(" ", col("first"), col("last")))
```

### Difference

-   concat → no separator
-   concat_ws → with separator

------------------------------------------------------------------------

# 6. SPLIT & ARRAY-BASED STRING OPS

## 6.1 split()

``` python
from pyspark.sql.functions import split

df.withColumn("words", split(col("sentence"), " "))
```

### Output

-   Returns array

------------------------------------------------------------------------

## 6.2 explode()

``` python
from pyspark.sql.functions import explode

df.withColumn("word", explode(split(col("sentence"), " ")))
```

### Use Case

-   Tokenization
-   NLP preprocessing

------------------------------------------------------------------------

# 7. REGEX OPERATIONS (VERY IMPORTANT)

## 7.1 regexp_replace()

``` python
from pyspark.sql.functions import regexp_replace

df.withColumn("clean", regexp_replace(col("text"), "[^a-zA-Z0-9]", ""))
```

### Use Case

-   Removing special characters

### Pitfall

-   Removing meaningful characters accidentally

------------------------------------------------------------------------

## 7.2 regexp_extract()

``` python
from pyspark.sql.functions import regexp_extract

df.withColumn("domain", regexp_extract(col("email"), "@(.*)", 1))
```

### Use Case

-   Extracting structured data from strings

------------------------------------------------------------------------

# 8. TRANSLATE FUNCTION

``` python
from pyspark.sql.functions import translate

df.withColumn("clean", translate(col("text"), "abc", "123"))
```

### Explanation

-   Character-by-character replacement

------------------------------------------------------------------------

# 9. FORMAT FUNCTIONS

## format_string()

``` python
from pyspark.sql.functions import format_string

df.withColumn("formatted", format_string("%s-%s", col("id"), col("name")))
```

------------------------------------------------------------------------

# 10. ADVANCED FUNCTIONS (INTERVIEW + RARE)

## 10.1 levenshtein()

``` python
from pyspark.sql.functions import levenshtein

df.withColumn("dist", levenshtein(col("name1"), col("name2")))
```

### Use Case

-   Fuzzy matching

------------------------------------------------------------------------

# 11. STRING COMPARISON & CLEAN JOIN STRATEGY

## Proper Pattern

``` python
df1 = df1.withColumn("key", trim(lower(col("key"))))
df2 = df2.withColumn("key", trim(lower(col("key"))))

df1.join(df2, "key")
```

### Why

-   Avoid mismatch due to formatting

------------------------------------------------------------------------

# 12. UNICODE & ENCODING ISSUES (REAL WORLD)

### Problems

-   Hidden characters
-   Different encodings

### Solution

-   Normalize input
-   Remove non-printable chars

------------------------------------------------------------------------

# 13. COMMON PRODUCTION BUGS

1.  Join failure due to spaces\
2.  Case mismatch causing duplicates\
3.  Regex removing valid data\
4.  Split producing unexpected arrays\
5.  Encoding issues

------------------------------------------------------------------------

# 14. BEST PRACTICES

-   Always trim before join\
-   Normalize case for keys\
-   Validate regex patterns\
-   Avoid unnecessary string transformations\
-   Use built-in functions (avoid UDF)

------------------------------------------------------------------------

# FINAL UNDERSTANDING

String handling is: - Data standardization layer - Critical for joins &
deduplication - Source of most silent bugs

Mastering this ensures: - Correct joins - Clean data - Reliable
pipelines
