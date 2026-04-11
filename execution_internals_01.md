# 🚀 PySpark MASTER NOTES (Interview + Deep Understanding)

------------------------------------------------------------------------

# 🔥 1. Spark Architecture (Deep)

## 1.1 Core Truth

Spark is a **distributed execution engine running on JVM (Scala)**.

PySpark is: - NOT Spark - It is a **Python wrapper over JVM Spark**

------------------------------------------------------------------------

## 1.2 Execution Layers

    Python (Driver Code)
        ↓
    Py4J (Bridge)
        ↓
    JVM Driver (SparkContext)
        ↓
    Executors (JVM processes)
        ↓
    Tasks (operate on partitions)

------------------------------------------------------------------------

## 1.3 Critical Understanding

-   Python builds **logical plan**
-   JVM executes **physical plan**
-   Data never lives in Python (unless collect)

------------------------------------------------------------------------

# 🔥 2. RDD (Deep Dive)

## 2.1 Definition

RDD = Immutable, distributed collection of JVM objects

------------------------------------------------------------------------

## 2.2 Structure

    RDD
     ├── Partition 1
     ├── Partition 2
     └── Partition N

Each partition processed independently.

------------------------------------------------------------------------

## 2.3 Lineage (Fault Tolerance)

    RDD1 → map → RDD2 → filter → RDD3

If partition lost → recomputed from lineage

------------------------------------------------------------------------

## 2.4 Transformations

### map

1 input → 1 output

### flatMap

1 input → multiple outputs

### filter

condition-based selection

------------------------------------------------------------------------

## 2.5 Shuffle (VERY IMPORTANT)

Occurs in: - groupByKey - reduceByKey - join

------------------------------------------------------------------------

## reduceByKey vs groupByKey

### reduceByKey

-   Local aggregation
-   Less shuffle
-   Efficient

### groupByKey

-   Full shuffle
-   Memory heavy
-   Avoid

------------------------------------------------------------------------

# 🔥 3. DataFrame (Deep Dive)

## 3.1 Definition

DataFrame = Distributed dataset with schema

Internally:

    Dataset[Row]

------------------------------------------------------------------------

## 3.2 Components

    DataFrame =
        Schema (StructType)
        +
        Logical Plan
        +
        Physical Data (UnsafeRow)

------------------------------------------------------------------------

## 3.3 Schema

    StructType
     ├── StructField(name, type)

Example:

    id → IntegerType
    name → StringType

------------------------------------------------------------------------

## 3.4 Storage (VERY IMPORTANT)

Data stored as:

    Partitions → UnsafeRow (binary)

NOT Python objects.

------------------------------------------------------------------------

# 🔥 4. Execution Pipeline

    Python Code
        ↓
    Logical Plan
        ↓
    Analyzed Plan (schema applied)
        ↓
    Optimized Plan (Catalyst)
        ↓
    Physical Plan
        ↓
    Execution (Tungsten)

------------------------------------------------------------------------

# 🔥 5. Catalyst Optimizer

## What it does

-   Predicate pushdown
-   Column pruning
-   Constant folding

------------------------------------------------------------------------

# 🔥 6. Tungsten Engine

-   Binary memory (UnsafeRow)
-   Cache-efficient
-   CPU optimized

------------------------------------------------------------------------

# 🔥 7. Serialization

## Types

### Pickle

Python ↔ JVM (slow)

### Arrow

Columnar, fast

------------------------------------------------------------------------

## Flow

    Python → serialize → JVM → execute → deserialize

------------------------------------------------------------------------

# 🔥 8. Complex Types (DEEP)

------------------------------------------------------------------------

## STRUCT

### Definition

Nested object with fixed schema

### Internal

Stored as offsets in UnsafeRow

------------------------------------------------------------------------

## ARRAY

### Definition

Ordered list of same type

### Internal

    ArrayData
     ├── size
     ├── null bitset
     └── values

------------------------------------------------------------------------

## MAP

### Definition

Key-value structure

### Internal

    keys array + values array

------------------------------------------------------------------------

# 🔥 9. Flattening

    STRUCT → select
    ARRAY → explode

------------------------------------------------------------------------

## explode()

-   1 row → multiple rows
-   increases data size
-   may cause shuffle

------------------------------------------------------------------------

# 🔥 10. Higher Order Functions (ADVANCED)

    transform()
    filter()
    aggregate()

Used to avoid explode.

------------------------------------------------------------------------

# 🔥 11. from_json / to_json

    from_json → string → struct
    to_json → struct → string

------------------------------------------------------------------------

# 🔥 12. Performance Rules

## Avoid

-   Python UDF
-   groupByKey
-   excessive explode

## Prefer

-   built-in functions
-   reduceByKey
-   column pruning

------------------------------------------------------------------------

# 🔥 13. Final Mental Models

## Spark

JVM execution engine

## PySpark

Control layer

## RDD

Raw distributed objects

## DataFrame

Structured + optimized execution

## Complex Types

Binary nested structures

------------------------------------------------------------------------

# 🚀 FINAL TAKEAWAY

Spark is fast because: - execution is in JVM - data is binary
(UnsafeRow) - optimizer rewrites queries
