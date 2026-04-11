# PySpark L3: Column Expression Engine --- Ultimate Master Guide (with Internal Execution)

------------------------------------------------------------------------

# 1. INTRODUCTION TO COLUMN EXPRESSION ENGINE

In PySpark, every transformation you write using DataFrame APIs is
internally translated into a Column Expression Tree. Spark does not
execute your Python code directly; instead, it builds a declarative
representation of computation. Each expression (e.g., col("a") + 10) is
converted into a logical plan which is later optimized and executed
across the cluster.

A Column object is not data --- it is a symbolic reference to
computation. This abstraction allows Spark to optimize entire pipelines
instead of executing line-by-line operations like pandas.

### Example

``` python
from pyspark.sql.functions import col

df = spark.createDataFrame([(1, 100), (2, 200)], ["id", "salary"])

df2 = df.withColumn("salary_plus_10", col("salary") + 10)
df2.show()
```

### Key Understanding

-   No computation happens at withColumn
-   Expression is stored in DAG

------------------------------------------------------------------------

# 2. INTERNAL EXPRESSION TREE

When you write:

``` python
df.withColumn("x", col("a") + col("b") * 2)
```

Spark builds:

    Add
     ├── Column(a)
     └── Multiply
           ├── Column(b)
           └── Literal(2)

This tree is part of the Logical Plan.

### Why Important

-   Spark can reorder operations
-   Combine expressions
-   Eliminate redundant computations

------------------------------------------------------------------------

# 3. CATALYST OPTIMIZER (DETAILED FLOW)

Spark follows 4 steps:

1.  Parsed Logical Plan
2.  Analyzed Logical Plan
3.  Optimized Logical Plan
4.  Physical Plan

------------------------------------------------------------------------

## 3.1 Parsed Logical Plan

Initial plan from your code.

``` python
df.select(col("salary") * 2)
```

------------------------------------------------------------------------

## 3.2 Analyzed Logical Plan

-   Resolves column names
-   Validates schema

Error example:

``` python
df.select(col("unknown_column"))
```

------------------------------------------------------------------------

## 3.3 Optimized Logical Plan

Catalyst applies rules:

### Constant Folding

``` python
df.select(lit(2) + lit(3))
```

→ Optimized to 5

------------------------------------------------------------------------

### Predicate Pushdown

``` python
df.filter(col("age") > 30)
```

→ pushed to data source

------------------------------------------------------------------------

### Column Pruning

``` python
df.select("id")
```

→ only id column read

------------------------------------------------------------------------

## 3.4 Physical Plan

Actual execution strategy:

-   HashAggregate
-   SortMergeJoin
-   BroadcastHashJoin

------------------------------------------------------------------------

# 4. FULL EXECUTION EXAMPLE (END-TO-END)

``` python
from pyspark.sql.functions import col

df = spark.read.parquet("data")

df_transformed = (
    df
    .filter(col("age") > 25)
    .withColumn("bonus", col("salary") * 0.1)
    .select("id", "bonus")
)

df_transformed.explain(True)
```

### What Happens

-   Builds expression tree
-   Optimizes filter + projection
-   Generates physical plan
-   Executes

------------------------------------------------------------------------

# 5. WHOLESTAGE CODEGEN (IMPORTANT)

Spark generates Java code for execution.

### Example

``` python
df.select((col("salary") * 2) + 10).explain(True)
```

You will see:

-   Generated Java code
-   Combined operations into single stage

### Benefit

-   Faster execution
-   Less overhead

------------------------------------------------------------------------

# 6. EXPRESSION OPTIMIZATION RULES

------------------------------------------------------------------------

## 6.1 Constant Folding

``` python
df.select(lit(10) + lit(20))
```

→ optimized to 30

------------------------------------------------------------------------

## 6.2 Predicate Simplification

``` python
df.filter((col("age") > 10) & (col("age") > 5))
```

→ simplified to age \> 10

------------------------------------------------------------------------

## 6.3 Null Propagation

``` python
col("a") + None → NULL
```

------------------------------------------------------------------------

# 7. ADVANCED COLUMN OPERATIONS

------------------------------------------------------------------------

## 7.1 Complex Expression

``` python
df.withColumn(
    "final_salary",
    (col("salary") + col("bonus") * 2) / 3
)
```

------------------------------------------------------------------------

## 7.2 Nested Conditions

``` python
from pyspark.sql.functions import when

df.withColumn(
    "grade",
    when(col("marks") > 90, "A")
    .when(col("marks") > 75, "B")
    .otherwise("C")
)
```

------------------------------------------------------------------------

# 8. COMMON MISTAKES (CRITICAL)

------------------------------------------------------------------------

## Mistake 1: Using Python operators

``` python
# WRONG
if col("age") > 18:
    ...
```

------------------------------------------------------------------------

## Mistake 2: Using and/or instead of & / \|

``` python
# WRONG
col("a") > 1 and col("b") > 2
```

------------------------------------------------------------------------

## Mistake 3: Using UDF unnecessarily

``` python
# BAD
df.withColumn("x", my_udf(col("a")))
```

------------------------------------------------------------------------

## Correct Approach

``` python
df.withColumn("x", col("a") + 1)
```

------------------------------------------------------------------------

# 9. NULL HANDLING IN EXPRESSIONS

------------------------------------------------------------------------

## Important Behavior

-   NULL != NULL
-   NULL comparisons return NULL

------------------------------------------------------------------------

## Example

``` python
df.filter(col("a") == None)  # WRONG
```

Correct:

``` python
df.filter(col("a").isNull())
```

------------------------------------------------------------------------

## Null-safe equality

``` python
col("a").eqNullSafe(col("b"))
```

------------------------------------------------------------------------

# 10. PERFORMANCE BEST PRACTICES

------------------------------------------------------------------------

## 10.1 Prefer built-in functions

-   Optimized
-   Catalyst-friendly

------------------------------------------------------------------------

## 10.2 Avoid multiple withColumn

``` python
# BAD
df.withColumn("a", ...).withColumn("b", ...)
```

Better:

``` python
df.select(
    col("a"),
    (col("salary") * 2).alias("new_salary"),
    (col("salary") + 10).alias("bonus")
)
```

------------------------------------------------------------------------

## 10.3 Avoid UDFs unless necessary

------------------------------------------------------------------------

# FINAL UNDERSTANDING

Column Expression Engine is:

-   Declarative computation system
-   Backbone of Catalyst optimizer
-   Foundation of Spark performance

If you master this: - You can optimize any transformation - You can
debug execution plans - You can outperform most engineers in interviews
