# PySpark L8: Window Functions --- MASTER DOCUMENT (FULL DEPTH, NO SHORTCUTS)

------------------------------------------------------------------------

# 1. INTRODUCTION (WHAT WINDOW FUNCTIONS ACTUALLY SOLVE)

Window functions allow you to perform calculations **across a set of
related rows** while **retaining each original row** in the output.

This is fundamentally different from aggregation:

-   groupBy → collapses rows (many → one)
-   window → preserves rows (many → many)

This makes window functions essential for: - deduplication with
control - ranking - running totals - time-series comparisons - event
sequencing

------------------------------------------------------------------------

# 2. CORE MENTAL MODEL (REMOVE CONFUSION COMPLETELY)

A window operation consists of **3 components**:

1.  PARTITION → how data is grouped
2.  ORDER → how rows are sequenced within group
3.  FRAME → which rows are considered for calculation

Think of it as:

For each row: look at a defined group (partition) ordered in a specific
way and compute based on a subset (frame)

------------------------------------------------------------------------

# 3. WINDOW SPEC DEFINITION (FOUNDATION)

## Syntax

``` python
from pyspark.sql.window import Window

window_spec = Window.partitionBy("user_id").orderBy("timestamp")
```

------------------------------------------------------------------------

## Deep Understanding

### partitionBy

-   Defines logical groups (like groupBy)
-   BUT does NOT reduce rows
-   Only defines scope of computation

### orderBy

-   Defines sequence inside each partition
-   REQUIRED for deterministic results in most cases

------------------------------------------------------------------------

## Example

``` python
data = [
    (1, "2024-01-01", 100),
    (1, "2024-01-02", 200),
    (1, "2024-01-03", 300)
]

df = spark.createDataFrame(data, ["user_id", "date", "amount"])
```

Partition → user_id\
Order → date

------------------------------------------------------------------------

# 4. RANKING FUNCTIONS (FULL COVERAGE)

------------------------------------------------------------------------

## 4.1 row_number()

### Concept

Assigns a unique sequential number per partition.

### Syntax

``` python
from pyspark.sql.functions import row_number

df.withColumn("rn", row_number().over(window_spec))
```

------------------------------------------------------------------------

### Example

``` python
data = [
    (1, "A", 100),
    (1, "B", 200),
    (1, "C", 200)
]

df = spark.createDataFrame(data, ["id", "item", "value"])

window = Window.partitionBy("id").orderBy("value")

df.withColumn("rn", row_number().over(window)).show()
```

------------------------------------------------------------------------

### Step-by-step Explanation

Order by value → 100, 200, 200\
row_number assigns: - 100 → 1 - 200 → 2 - 200 → 3

------------------------------------------------------------------------

### Important Property

-   ALWAYS unique
-   Even if values are equal

------------------------------------------------------------------------

### Use Case (CRITICAL)

Deduplication:

``` python
window = Window.partitionBy("id").orderBy(col("timestamp").desc())

df.withColumn("rn", row_number().over(window))   .filter("rn = 1")
```

------------------------------------------------------------------------

### Pitfall

Without orderBy → non-deterministic results

------------------------------------------------------------------------

## 4.2 rank()

### Concept

Assigns rank with gaps.

### Example

Values: 100, 200, 200

rank: - 100 → 1 - 200 → 2 - 200 → 2 - next rank → 4

------------------------------------------------------------------------

### Why gap occurs

Because rank reflects position in sorted list.

------------------------------------------------------------------------

## 4.3 dense_rank()

### Concept

Same as rank BUT no gaps.

------------------------------------------------------------------------

### Example

Values: 100, 200, 200

dense_rank: - 100 → 1 - 200 → 2 - 200 → 2 - next rank → 3

------------------------------------------------------------------------

## Comparison

  Function     Duplicates   Gaps
  ------------ ------------ ------
  row_number   no           no
  rank         yes          yes
  dense_rank   yes          no

------------------------------------------------------------------------

## 4.4 percent_rank()

### Concept

Gives relative rank between 0 and 1.

------------------------------------------------------------------------

## 4.5 ntile(n)

### Concept

Divides rows into n buckets.

``` python
from pyspark.sql.functions import ntile

df.withColumn("bucket", ntile(4).over(window))
```

------------------------------------------------------------------------

# 5. NAVIGATION FUNCTIONS (lag / lead)

------------------------------------------------------------------------

## 5.1 lag()

### Concept

Access previous row value.

------------------------------------------------------------------------

### Syntax

``` python
from pyspark.sql.functions import lag

df.withColumn("prev_value", lag("value", 1).over(window))
```

------------------------------------------------------------------------

### Example

Values: 100, 200, 300

lag: - 100 → NULL - 200 → 100 - 300 → 200

------------------------------------------------------------------------

### Use Cases

-   time series comparison
-   change detection

------------------------------------------------------------------------

## 5.2 lead()

Opposite of lag.

------------------------------------------------------------------------

# 6. AGGREGATION OVER WINDOW

------------------------------------------------------------------------

## Concept

Apply aggregation WITHOUT collapsing rows.

------------------------------------------------------------------------

## Example

``` python
from pyspark.sql.functions import sum

df.withColumn("running_total", sum("amount").over(window))
```

------------------------------------------------------------------------

### Behavior

Each row gets cumulative result.

------------------------------------------------------------------------

# 7. WINDOW FRAME (MOST CONFUSING PART --- FULL CLARITY)

------------------------------------------------------------------------

## Default Frame

``` text
UNBOUNDED PRECEDING → CURRENT ROW
```

------------------------------------------------------------------------

## rowsBetween()

### Concept

Defines frame based on number of rows.

------------------------------------------------------------------------

### Example

``` python
window = Window.partitionBy("id").orderBy("date")     .rowsBetween(-1, 1)
```

Meaning: - previous row - current row - next row

------------------------------------------------------------------------

## rangeBetween()

### Concept

Defines frame based on value range.

------------------------------------------------------------------------

### Key Difference

-   rowsBetween → position-based
-   rangeBetween → value-based

------------------------------------------------------------------------

### Pitfall

rangeBetween behaves differently with duplicate values

------------------------------------------------------------------------

# 8. VALUE FUNCTIONS (OFTEN MISSED)

------------------------------------------------------------------------

## first()

Returns first value in window.

------------------------------------------------------------------------

## last()

Returns last value.

------------------------------------------------------------------------

## nth_value()

Returns nth row value.

------------------------------------------------------------------------

# 9. INTERNAL EXECUTION (IMPORTANT)

------------------------------------------------------------------------

Window operations involve:

1.  Shuffle (if partitionBy used)
2.  Sort within partition
3.  Compute per row

------------------------------------------------------------------------

## Why Expensive

-   Sorting is costly
-   Large partitions → memory pressure

------------------------------------------------------------------------

# 10. PERFORMANCE CONSIDERATIONS

------------------------------------------------------------------------

## Expensive Cases

-   Large partitions
-   Multiple window functions with different specs

------------------------------------------------------------------------

## Optimization

-   Use same window_spec where possible
-   Reduce dataset before window
-   Avoid unnecessary ordering

------------------------------------------------------------------------

# 11. COMMON PRODUCTION BUGS

------------------------------------------------------------------------

1.  Missing orderBy → inconsistent results\
2.  Using dropDuplicates instead of window dedup\
3.  Wrong frame definition\
4.  Large partition causing OOM\
5.  Incorrect ranking logic

------------------------------------------------------------------------

# 12. BEST PRACTICES

------------------------------------------------------------------------

-   Always define orderBy for deterministic behavior\
-   Use row_number for deduplication\
-   Validate window output\
-   Avoid huge partitions\
-   Understand frame before using

------------------------------------------------------------------------

# FINAL UNDERSTANDING

Window functions are:

-   Row-preserving transformations\
-   Order-sensitive computations\
-   Performance-heavy operations

Mastering them allows you to: - solve complex data problems\
- write production-grade pipelines\
- clear advanced interviews
