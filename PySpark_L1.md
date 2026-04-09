# PySpark L1: Data Ingestion & Output (Master Guide)

## Overview

This document provides a production-grade understanding of PySpark I/O
operations including reading, schema enforcement, bad record handling,
and writing optimized datasets.

------------------------------------------------------------------------

## 1. DataFrameReader (Read Path)

### Standard Pattern

``` python
df = (
    spark.read
    .format("csv")
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .schema(schema)
    .load("s3://bucket/path")
)
```

### Key APIs

-   read
-   format
-   option / options
-   schema
-   load

### Supported Formats

-   csv
-   json
-   parquet
-   orc
-   delta
-   text

------------------------------------------------------------------------

## 2. File Format Deep Dive

### CSV

-   No schema enforcement
-   Slow parsing
-   Requires strict handling

### JSON

-   Handles nested data
-   Schema inconsistency issues

### Parquet (Recommended)

-   Columnar storage
-   Efficient compression
-   Predicate pushdown

### Delta (Best for production)

-   ACID transactions
-   Schema evolution
-   Time travel

------------------------------------------------------------------------

## 3. Schema Management

### Define Schema

``` python
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
```

### Why Schema Matters

-   Prevents data inconsistency
-   Improves performance
-   Enables optimization

------------------------------------------------------------------------

## 4. Bad Record Handling

### Read Options

``` python
.option("mode", "PERMISSIVE")
.option("columnNameOfCorruptRecord", "_corrupt_record")
```

### Modes

-   PERMISSIVE
-   DROPMALFORMED
-   FAILFAST

### Split Data

``` python
good_df = df.filter(col("_corrupt_record").isNull())
bad_df = df.filter(col("_corrupt_record").isNotNull())
```

------------------------------------------------------------------------

## 5. DataFrameWriter (Write Path)

### Standard Pattern

``` python
df.write.format("parquet").mode("overwrite").save(path)
```

### Modes

-   overwrite
-   append
-   ignore
-   errorIfExists

------------------------------------------------------------------------

## 6. Partitioning Strategy

### Example

``` python
df.write.partitionBy("year", "month").parquet(path)
```

### Best Practices

-   Avoid high cardinality columns
-   Use time-based partitioning

------------------------------------------------------------------------

## 7. Bucketing

``` python
df.write.bucketBy(10, "id").saveAsTable("table")
```

Use for optimizing joins.

------------------------------------------------------------------------

## 8. save vs saveAsTable

-   save → writes to storage
-   saveAsTable → registers in metastore

------------------------------------------------------------------------

## 9. Production Pipeline Example

``` python
df_raw = spark.read.format("csv")     .option("header", True)     .option("mode", "PERMISSIVE")     .option("columnNameOfCorruptRecord", "_corrupt_record")     .schema(schema)     .load("s3://raw/")

good_df = df_raw.filter(col("_corrupt_record").isNull())
bad_df = df_raw.filter(col("_corrupt_record").isNotNull())

bad_df.write.mode("append").parquet("s3://quarantine/")

good_df.write.partitionBy("year", "month")     .mode("overwrite")     .parquet("s3://clean/")
```

------------------------------------------------------------------------

## 10. Common Mistakes

-   Using inferSchema in production
-   Ignoring bad records
-   Poor partitioning strategy
-   Writing too many small files

------------------------------------------------------------------------

## Conclusion

Mastering I/O ensures data reliability, performance, and scalability in
production pipelines.
