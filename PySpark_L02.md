# PySpark L2: Schema & Data Representation --- Ultimate Master-Level Guide

------------------------------------------------------------------------

# 1. INTRODUCTION TO SCHEMA (FOUNDATION)

Schema in PySpark defines the structure of data, but more importantly,
it controls how Spark interprets, optimizes, and executes operations on
data. A DataFrame is not just rows of data --- it is a combination of
schema (metadata) and distributed data. Spark relies on schema to build
logical and physical plans using Catalyst Optimizer. Without a proper
schema, Spark cannot apply optimizations like predicate pushdown or
column pruning effectively.

In real-world pipelines, schema acts as a **contract between systems**.
For example, if upstream sends `age` as string instead of integer,
downstream aggregations may break or produce incorrect results. Schema
also impacts performance directly --- wrong types lead to unnecessary
casting and slower execution.

### Example

``` python
df = spark.read.parquet("s3://data")
df.printSchema()
```

### Industry Use Case

-   Data warehouse ingestion layer where schema defines contract between
    ingestion and transformation layers

### Pitfalls

-   Assuming schema is just metadata (it is execution-critical)
-   Ignoring schema consistency across batches

------------------------------------------------------------------------

# 2. SCHEMA DEFINITION (StructType & StructField)

Schema is defined using `StructType` and `StructField`. Each column is
explicitly defined with: - name - data type - nullability - metadata

Explicit schema ensures deterministic behavior.

### Example

``` python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read.schema(schema).csv("data.csv")
```

### Industry Use Case

-   Ingesting CSV from external vendors where schema must be enforced

### Pitfalls

-   Using wrong datatype → causes casting overhead later
-   Marking all fields nullable=True → weak data validation

### Special Case

-   If schema mismatches data, Spark sets invalid values as NULL

------------------------------------------------------------------------

# 3. INTERNAL SCHEMA REPRESENTATION

Schema is internally stored as a tree structure.

    root
     |-- user: struct
     |    |-- name: string
     |    |-- age: integer

This enables Spark to handle nested and semi-structured data
efficiently.

### Example

``` python
df.select("user.name")
```

### Industry Use Case

-   Processing nested JSON from APIs

### Pitfalls

-   Incorrect field access → runtime errors
-   Deep nesting → complex transformations

------------------------------------------------------------------------

# 4. DATA TYPES (CRITICAL FOR PERFORMANCE)

Choosing correct data types is essential.

### Example

``` python
df = df.withColumn("salary", col("salary").cast("double"))
```

### Industry Examples

-   `IntegerType` → IDs, counts
-   `DecimalType` → financial data
-   `TimestampType` → event tracking

### Pitfalls

-   Using StringType for numeric → no predicate pushdown
-   Mixing types across datasets → join issues

### Special Case

-   DecimalType ensures precision but is slower than DoubleType

------------------------------------------------------------------------

# 5. NULLABILITY

Defines whether a column can contain null values.

### Example

``` python
StructField("id", IntegerType(), False)
```

### Important Behavior

-   Spark does not strictly enforce nullability

### Industry Use Case

-   Critical columns like primary keys marked non-null

### Pitfalls

-   Assuming Spark enforces nullability → it does not
-   Null values breaking joins and aggregations

------------------------------------------------------------------------

# 6. SCHEMA INFERENCE VS EXPLICIT SCHEMA

### Inference

``` python
df = spark.read.option("inferSchema", True).csv("data.csv")
```

### Explicit

``` python
df = spark.read.schema(schema).csv("data.csv")
```

### Industry Use Case

-   Always explicit schema in production pipelines

### Pitfalls

-   Inference inconsistency across batches
-   Performance overhead due to scanning

------------------------------------------------------------------------

# 7. SCHEMA & CATALYST OPTIMIZER

Schema enables: - Column pruning - Predicate pushdown - Type
optimization

### Example

``` python
df.select("id").filter(col("id") > 10)
```

### Industry Use Case

-   Large parquet tables where only few columns are needed

### Pitfalls

-   Using UDF blocks optimization
-   Wrong datatype prevents pushdown

------------------------------------------------------------------------

# 8. TUNGSTEN EXECUTION & MEMORY

Spark uses binary format (UnsafeRow).

Schema defines: - memory layout - offsets

### Industry Impact

-   Efficient CPU usage
-   Reduced GC overhead

### Pitfalls

-   Complex nested schema → memory overhead

------------------------------------------------------------------------

# 9. SCHEMA EVOLUTION

Allows adding new columns dynamically.

### Example

``` python
df.write.format("delta").option("mergeSchema", "true").save(path)
```

### Industry Use Case

-   Evolving data sources

### Pitfalls

-   Uncontrolled evolution → inconsistent schema

------------------------------------------------------------------------

# 10. SCHEMA DRIFT HANDLING

Different sources → different schema.

### Example

``` python
from pyspark.sql.functions import lit

df = df.withColumn("new_col", lit(None).cast("string"))
```

### Industry Pattern

1.  Define canonical schema
2.  Align incoming data
3.  Validate

### Pitfalls

-   Not handling drift → pipeline failure

------------------------------------------------------------------------

# 11. NESTED SCHEMA HANDLING

### Example

``` python
df.select("user.name")
df.select("user.*")
```

### Industry Use Case

-   JSON ingestion pipelines

### Pitfalls

-   Deep nesting → complex joins

------------------------------------------------------------------------

# 12. SCHEMA VALIDATION

### Example

``` python
expected = ["id", "name", "age"]

if sorted(df.columns) != sorted(expected):
    raise Exception("Schema mismatch")
```

### Industry Use Case

-   Data quality checks

### Pitfalls

-   Skipping validation → silent errors

------------------------------------------------------------------------

# 13. PERFORMANCE IMPACT

### Example

``` python
df.select("id")
```

### Concepts

-   Column pruning
-   Predicate pushdown

### Pitfalls

-   Wrong schema → full scan

------------------------------------------------------------------------

# FINAL UNDERSTANDING

Schema is: - contract - optimizer input - execution blueprint

Mastering schema ensures: - performance - reliability - scalability
