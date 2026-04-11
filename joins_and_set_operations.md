# PySpark L6: Joins & Set Operations --- ULTIMATE MASTER GUIDE (FULL + NO GAPS + ZERO CONFUSION)

------------------------------------------------------------------------

# 1. INTRODUCTION

Joins and Set Operations are the **most critical transformations in
PySpark**. They directly impact:

-   Data correctness (missing / duplicate rows)
-   Performance (shuffle-heavy operations)
-   Scalability (cluster utilization)
-   Debuggability (silent failures are common)

This guide is built with: - FULL function coverage (no missing
variants) - Deep conceptual clarity - Real-world production scenarios -
Edge cases + failure modes - Internal execution understanding

------------------------------------------------------------------------

# 2. CORE JOIN MENTAL MODEL (VERY IMPORTANT)

In Spark:

``` text
Join = Align Data + Match Keys + Combine Rows
```

BUT internally:

1.  Data is distributed across partitions
2.  Spark must bring matching keys together
3.  This requires:
    -   Shuffle OR
    -   Broadcast

👉 If you don't understand this → you cannot optimize joins

------------------------------------------------------------------------

# 3. STANDARD JOIN SYNTAX (ALL FORMS)

## 3.1 USING COLUMN NAME

``` python
df1.join(df2, on="id", how="inner")
```

## 3.2 USING CONDITION

``` python
df1.join(df2, df1.id == df2.id, "inner")
```

## 3.3 MULTI-COLUMN JOIN

``` python
df1.join(df2, ["id", "date"], "inner")
```

## 3.4 COMPLEX CONDITION

``` python
df1.join(
    df2,
    (df1.id == df2.id) & (df1.date == df2.date),
    "inner"
)
```

------------------------------------------------------------------------

# 4. JOIN TYPES (FULL COVERAGE + EDGE CASES)

------------------------------------------------------------------------

## 4.1 INNER JOIN

### Concept

Only matching rows

### Example (REALISTIC)

``` python
orders = spark.createDataFrame([
    (1, "A"), (2, "B"), (3, "C"), (3, "D")
], ["id", "product"])

payments = spark.createDataFrame([
    (1, 100), (2, 200), (3, 300)
], ["id", "amount"])

orders.join(payments, "id", "inner")
```

### Edge Case --- DUPLICATES

id=3 appears twice in orders → result duplicates rows

👉 Row explosion problem

------------------------------------------------------------------------

### NULL Behavior

-   NULL keys are NOT matched

------------------------------------------------------------------------

### Pitfall

-   Silent data loss (unmatched rows dropped)

------------------------------------------------------------------------

## 4.2 LEFT JOIN

### Concept

All rows from left + matched from right

### Behavior

-   Missing → NULL
-   Duplicates still cause explosion

------------------------------------------------------------------------

### Real Use Case

-   Enrichment (add attributes)

------------------------------------------------------------------------

### Pitfall

-   Assuming 1:1 mapping → wrong

------------------------------------------------------------------------

## 4.3 RIGHT JOIN

Mirror of left join

------------------------------------------------------------------------

## 4.4 FULL OUTER JOIN

### Concept

All rows from both sides

### Behavior

-   Missing → NULL on either side

------------------------------------------------------------------------

### Pitfall

-   Can increase dataset size significantly

------------------------------------------------------------------------

## 4.5 LEFT SEMI JOIN

### Concept

Return ONLY left rows where match exists

``` python
df1.join(df2, "id", "left_semi")
```

------------------------------------------------------------------------

### Internal Behavior

-   Exists check (optimized)
-   Does NOT bring right columns

------------------------------------------------------------------------

### Use Case

-   Filtering records

------------------------------------------------------------------------

## 4.6 LEFT ANTI JOIN

### Concept

Return rows in left with NO match

``` python
df1.join(df2, "id", "left_anti")
```

------------------------------------------------------------------------

### Use Case

-   Find missing records

------------------------------------------------------------------------

# 5. NULL HANDLING IN JOINS (CRITICAL)

------------------------------------------------------------------------

## DEFAULT BEHAVIOR

``` python
df1.join(df2, df1.id == df2.id)
```

-   NULL != NULL → NO MATCH

------------------------------------------------------------------------

## NULL-SAFE JOIN

``` python
df1.join(df2, df1.id.eqNullSafe(df2.id))
```

------------------------------------------------------------------------

### When Needed

-   When NULL should be treated as valid key

------------------------------------------------------------------------

### Pitfall

-   Can introduce unexpected matches

------------------------------------------------------------------------

# 6. DUPLICATE KEY PROBLEM (ROW EXPLOSION)

------------------------------------------------------------------------

## Example

df1: id=1 (3 rows)

df2: id=1 (4 rows)

👉 Result = 12 rows

------------------------------------------------------------------------

## Solutions

### 1. Deduplicate

``` python
df1.dropDuplicates(["id"])
```

### 2. Window function

### 3. Aggregate before join

------------------------------------------------------------------------

# 7. JOIN EXECUTION STRATEGIES (VERY IMPORTANT)

------------------------------------------------------------------------

## 7.1 BROADCAST HASH JOIN

### Concept

Small table → broadcast to all executors

``` python
from pyspark.sql.functions import broadcast

df1.join(broadcast(df2), "id")
```

------------------------------------------------------------------------

### When Used

-   One side small (\~10MB or configurable)

------------------------------------------------------------------------

### Benefit

-   No shuffle

------------------------------------------------------------------------

## 7.2 SORT MERGE JOIN

### Default for large joins

Steps: 1. Shuffle both sides 2. Sort 3. Merge

------------------------------------------------------------------------

### Cost

-   Expensive but scalable

------------------------------------------------------------------------

## 7.3 SHUFFLE HASH JOIN

-   Used when hash join feasible
-   Depends on config

------------------------------------------------------------------------

## 7.4 CARTESIAN JOIN

``` python
df1.crossJoin(df2)
```

------------------------------------------------------------------------

### Danger

-   Explodes rows massively

------------------------------------------------------------------------

# 8. HOW SPARK CHOOSES JOIN STRATEGY

------------------------------------------------------------------------

Based on: - Data size - Statistics - Config
(`spark.sql.autoBroadcastJoinThreshold`)

------------------------------------------------------------------------

# 9. DATA SKEW (REAL-WORLD PROBLEM)

------------------------------------------------------------------------

## Problem

One key has huge data

------------------------------------------------------------------------

## Impact

-   One partition overloaded
-   Job slow / fails

------------------------------------------------------------------------

## Solutions

### 1. Salting

Add random key

### 2. Skew hint

``` python
df.hint("skew")
```

### 3. Broadcast small side

------------------------------------------------------------------------

# 10. COLUMN AMBIGUITY

------------------------------------------------------------------------

## Problem

Same column names

------------------------------------------------------------------------

## Solution

``` python
df1.alias("a").join(df2.alias("b"), col("a.id") == col("b.id"))
```

------------------------------------------------------------------------

# 11. SET OPERATIONS (FULL COVERAGE)

------------------------------------------------------------------------

## 11.1 UNION (POSITION BASED)

``` python
df1.union(df2)
```

### Pitfall

-   Column order mismatch → wrong data

------------------------------------------------------------------------

## 11.2 UNION BY NAME

``` python
df1.unionByName(df2)
```

------------------------------------------------------------------------

## 11.3 UNION BY NAME (ALLOW MISSING)

``` python
df1.unionByName(df2, allowMissingColumns=True)
```

------------------------------------------------------------------------

### Behavior

-   Missing columns filled with NULL

------------------------------------------------------------------------

### Critical Use Case

-   Schema evolution pipelines

------------------------------------------------------------------------

## 11.4 INTERSECT

``` python
df1.intersect(df2)
```

-   Removes duplicates

------------------------------------------------------------------------

## 11.5 INTERSECT ALL

``` python
df1.intersectAll(df2)
```

-   Keeps duplicates

------------------------------------------------------------------------

## 11.6 EXCEPT / SUBTRACT

``` python
df1.subtract(df2)
```

------------------------------------------------------------------------

## 11.7 EXCEPT ALL

``` python
df1.exceptAll(df2)
```

------------------------------------------------------------------------

# 12. SET OPS EDGE CASES

-   Schema mismatch → error
-   Type mismatch → failure
-   Order irrelevant
-   NULL treated as value

------------------------------------------------------------------------

# 13. INTERNAL BEHAVIOR

-   Joins = wide transformation
-   Shuffle required (except broadcast)
-   Expensive operation

------------------------------------------------------------------------

# 14. COMMON PRODUCTION BUGS

1.  Inner join dropping data\
2.  Duplicate key explosion\
3.  NULL mismatch\
4.  Schema mismatch in union\
5.  Wrong column order in union

------------------------------------------------------------------------

# 15. BEST PRACTICES

-   Normalize keys (trim, lower)
-   Align data types before join
-   Use broadcast wisely
-   Handle NULL explicitly
-   Validate join output size

------------------------------------------------------------------------

# FINAL UNDERSTANDING

Joins & Set Ops are:

-   Most powerful
-   Most expensive
-   Most error-prone

Mastering them = real Data Engineering maturity
