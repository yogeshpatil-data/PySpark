# Spark Structured Streaming --- Ultimate Mastery Guide (With Examples + Storage Internals)

------------------------------------------------------------------------

# 1. CORE IDEA (VERY IMPORTANT)

Think of Structured Streaming as:

→ A table that keeps updating forever.

Every few seconds: - New data comes in - Spark updates a table - Outputs
changes

------------------------------------------------------------------------

# 2. SIMPLE EXAMPLE (START HERE)

### Input Stream (Kafka)

  user   city   amount   time
  ------ ------ -------- -------
  A      Pune   100      10:01
  B      Pune   200      10:02

------------------------------------------------------------------------

### Query

Group by city and sum amount

------------------------------------------------------------------------

### Result Table (IN MEMORY STATE)

  city   total
  ------ -------
  Pune   300

------------------------------------------------------------------------

This table is NOT static → it keeps updating.

------------------------------------------------------------------------

# 3. WHAT HAPPENS INTERNALLY (STEP BY STEP)

Micro-batch 1:

Input: \| Pune 100 \| \| Pune 200 \|

State: \| Pune 300 \|

------------------------------------------------------------------------

Micro-batch 2:

Input: \| Pune 50 \|

State update: \| Pune 350 \|

------------------------------------------------------------------------

# 4. STATE STORE (WHAT IT REALLY LOOKS LIKE)

State is stored as:

Key → Value

Example:

"Pune" → 350

------------------------------------------------------------------------

### On Disk (Checkpoint)

checkpoint/state/0/0/1/

Inside files:

  key    value
  ------ -------
  Pune   350

------------------------------------------------------------------------

Each batch creates new version:

Version 1 → Pune = 300\
Version 2 → Pune = 350

------------------------------------------------------------------------

# 5. STATEFUL vs STATELESS

### Stateless

Input: \| 100 \| \| 200 \|

Output: \| 200 \| \| 400 \|

No memory.

------------------------------------------------------------------------

### Stateful

Input: \| Pune 100 \| \| Pune 200 \|

Output depends on past:

| Pune 300 \|

------------------------------------------------------------------------

# 6. WINDOWING (WITH EXAMPLE)

### Tumbling Window (5 min)

Window: 10:00--10:05

Input:

  time    amount
  ------- --------
  10:01   100
  10:03   200

Result:

  window        total
  ------------- -------
  10:00-10:05   300

------------------------------------------------------------------------

# 7. WATERMARK (WHY NEEDED)

Late data can arrive.

Example:

Late event: \| 10:02 \| 50 \|

Correct total should be 350.

------------------------------------------------------------------------

Watermark:

max_time = 10:10\
delay = 5 min\
watermark = 10:05

If event \< watermark → DROP

------------------------------------------------------------------------

# 8. OUTPUT MODES (WITH REAL DATA)

------------------------------------------------------------------------

## Append Mode

Output only FINAL result

Output:

  window        total
  ------------- -------
  10:00-10:05   300

------------------------------------------------------------------------

## Update Mode

Output every change

  window        total
  ------------- -------
  10:00-10:05   100
  10:00-10:05   300
  10:00-10:05   350

------------------------------------------------------------------------

## Complete Mode

Every batch outputs full table

------------------------------------------------------------------------

# 9. HOW DATA IS WRITTEN (S3 / HDFS)

Each batch writes NEW files.

Batch 1:

file1: \| Pune 300 \|

Batch 2:

file2: \| Pune 350 \|

------------------------------------------------------------------------

### Physical Storage:

file1: Pune 300

file2: Pune 350

------------------------------------------------------------------------

### Logical View (Delta):

Pune 350

------------------------------------------------------------------------

# 10. STORAGE TYPES BEHAVIOR

------------------------------------------------------------------------

## Kafka

-   Append-only
-   Cannot update
-   Only append mode supported

------------------------------------------------------------------------

## S3 / ADLS / HDFS

-   Immutable
-   New files per batch
-   Causes small file problem

------------------------------------------------------------------------

## Delta Lake

-   Transaction log
-   Tracks latest version
-   Supports updates logically

------------------------------------------------------------------------

# 11. WHAT USER QUERY SEES

User queries NOT raw data.

User sees:

| Pune \| 350 \|

From: - Delta table OR - Database

------------------------------------------------------------------------

# 12. EXACTLY-ONCE (SIMPLIFIED)

Steps:

1.  Read offsets
2.  Process
3.  Write output
4.  Save checkpoint

If failure: → Restart from last checkpoint

------------------------------------------------------------------------

# 13. END-TO-END FLOW

Kafka → Spark → State → Output → Storage → Dashboard

------------------------------------------------------------------------

# FINAL TRUTH

Streaming = continuously updating stored state

NOT: - Not raw event reading - Not direct querying Kafka

------------------------------------------------------------------------

# MASTER INSIGHT

If you understand: - State - Windows - Output modes - Storage behavior

You understand Structured Streaming.
