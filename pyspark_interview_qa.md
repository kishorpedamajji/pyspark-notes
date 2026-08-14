# PySpark Interview Questions & Answers
### Beginner → Advanced | Concept-Complete Guide

Aligned with the [25-Day Learning Schedule](25_day_pyspark_learning_schedule.md).  
Format: **Q** → short strong answer → optional deeper notes for follow-ups.

---

## Table of Contents

1. [Spark Architecture & Core Concepts](#1-spark-architecture--core-concepts)
2. [RDD vs DataFrame vs Dataset](#2-rdd-vs-dataframe-vs-dataset)
3. [SparkSession, Jobs, Stages, Tasks](#3-sparksession-jobs-stages-tasks)
4. [Transformations & Actions](#4-transformations--actions)
5. [Reading / Writing Data](#5-reading--writing-data)
6. [DataFrame API & Spark SQL](#6-dataframe-api--spark-sql)
7. [Aggregations](#7-aggregations)
8. [Joins](#8-joins)
9. [Window Functions](#9-window-functions)
10. [Complex / Nested Types](#10-complex--nested-types)
11. [Built-in Functions vs UDFs](#11-built-in-functions-vs-udfs)
12. [Partitioning & Parallelism](#12-partitioning--parallelism)
13. [Caching & Persistence](#13-caching--persistence)
14. [Performance Tuning & Skew](#14-performance-tuning--skew)
15. [Catalyst, Tungsten & AQE](#15-catalyst-tungsten--aqe)
16. [Spark UI & Debugging](#16-spark-ui--debugging)
17. [Memory, Configs & Failures](#17-memory-configs--failures)
18. [Structured Streaming](#18-structured-streaming)
19. [Delta Lake / Lakehouse](#19-delta-lake--lakehouse)
20. [Production, DQ & Databricks](#20-production-dq--databricks)
21. [Scenario-Based / Coding Questions](#21-scenario-based--coding-questions)
22. [Quick Fire Round](#22-quick-fire-round)

---

## 1. Spark Architecture & Core Concepts

### Q1. What is Apache Spark?
**A:** Spark is a distributed data processing engine for large-scale batch and streaming workloads. It processes data in memory across a cluster for speed, with lineage-based fault tolerance and APIs for SQL, streaming, and ML.

### Q2. What is PySpark?
**A:** PySpark is Spark’s Python API. Your Python code builds a logical plan; execution largely happens on the JVM executors. That’s why Python UDFs can be slower than built-in Spark functions.

### Q3. Explain driver, executor, and cluster manager.
**A:**
- **Driver:** runs your main program, builds DAG, schedules tasks  
- **Executors:** worker processes that run tasks and store data/cache  
- **Cluster manager:** allocates resources (YARN, Kubernetes, Databricks, Standalone)

### Q4. What is lazy evaluation?
**A:** Transformations are not executed immediately. Spark builds a plan and runs it only when an **action** (like `count`, `show`, `write`) is called. This lets Catalyst optimize the full plan.

### Q5. What is lineage?
**A:** Lineage is the record of how a DataFrame/RDD was derived. If a partition is lost, Spark recomputes it from lineage instead of needing full data replication.

### Q6. Narrow vs wide transformation?
**A:**
- **Narrow:** each input partition contributes to at most one output partition (`map`, `filter`, `withColumn`) — no shuffle  
- **Wide:** data must be redistributed across partitions (`groupBy`, `join`, `repartition`) — causes shuffle

### Q7. What is a shuffle?
**A:** Shuffle is redistribution of data across executors (disk + network). It’s expensive and often the root of performance issues.

---

## 2. RDD vs DataFrame vs Dataset

### Q8. Difference between RDD, DataFrame, and Dataset?
**A:**
| Feature | RDD | DataFrame | Dataset |
|---|---|---|---|
| Abstraction | Low-level | Tabular with schema | Typed (JVM) |
| Optimization | Limited | Catalyst + Tungsten | Catalyst + Tungsten |
| Python support | Yes | Yes (primary) | Mostly Scala/Java |
| Use today | Rare / special cases | Default choice | JVM typed apps |

### Q9. Why prefer DataFrames over RDDs in PySpark?
**A:** DataFrames give schema awareness, Catalyst optimization, better memory layout (Tungsten), and SQL interoperability. RDD code is harder to optimize and usually slower.

### Q10. Can you convert RDD ↔ DataFrame?
**A:** Yes. `rdd.toDF(...)` / `spark.createDataFrame(rdd, schema)` and `df.rdd`.

---

## 3. SparkSession, Jobs, Stages, Tasks

### Q11. What is SparkSession?
**A:** Entry point to Spark functionality (DF API, SQL, config, catalog). Replaced older separate contexts for most app code (`SQLContext`, etc.).

### Q12. Job vs Stage vs Task?
**A:**
- **Job:** triggered by one action  
- **Stage:** set of tasks that can run without shuffle boundaries  
- **Task:** unit of work on one partition  

Example: `filter → map → groupBy → count` usually becomes multiple stages because `groupBy` introduces a shuffle.

### Q13. How does Spark achieve fault tolerance?
**A:** Through lineage recomputation, speculative execution options, retrying failed tasks, and for streaming/storage layers: checkpoints / write-ahead mechanisms (and Delta transaction logs for table reliability).

---

## 4. Transformations & Actions

### Q14. Give examples of transformations and actions.
**A:**
- **Transformations:** `select`, `filter`, `withColumn`, `join`, `groupBy`, `distinct`  
- **Actions:** `show`, `count`, `collect`, `take`, `write`, `foreach`

### Q15. Why is `collect()` dangerous?
**A:** It pulls all data to the driver. Large results can cause driver OOM. Prefer `take`, `limit`, writes, or aggregations.

### Q16. What does `show()` do?
**A:** Action that prints N rows (default 20). Useful for debugging; not for large exports.

### Q17. Difference between `select` and `withColumn`?
**A:**
- `select`: projects columns (can rebuild whole schema)  
- `withColumn`: adds/replaces one column  

Overusing chained `withColumn` can create deep plans; sometimes a single `select` is cleaner/faster.

---

## 5. Reading / Writing Data

### Q18. Infer schema vs explicit schema?
**A:**
- **Infer:** convenient, slower (extra pass), less safe in production  
- **Explicit:** faster, enforceable contracts, preferred for production pipelines

### Q19. Why is Parquet preferred over CSV?
**A:** Columnar format, compression, predicate/projection pushdown, typed schema — much better for analytics at scale.

### Q20. What are write modes?
**A:** `overwrite`, `append`, `ignore`, `errorifexists` (default). Choose based on idempotency needs.

### Q21. What is `partitionBy` when writing?
**A:** Writes data into directory partitions by column values (e.g., `date=2026-08-06/`). Helps prune reads, but too many partition values create tiny-file problems.

### Q22. Predicate pushdown / projection pushdown?
**A:**
- **Predicate:** filter applied at data-source level when possible  
- **Projection:** only required columns are read  

Both reduce I/O and are major Parquet benefits.

---

## 6. DataFrame API & Spark SQL

### Q23. DataFrame API vs Spark SQL — which is better?
**A:** Both compile to similar Catalyst plans. Choose readability/team preference. Many teams mix: DF for ETL plumbing, SQL for analytics.

### Q24. Temp view vs global temp view vs table?
**A:**
- **Temp view:** session-scoped  
- **Global temp view:** cross-session in `global_temp`, still temporary  
- **Table:** persisted metadata in catalog (location depends on managed/external)

### Q25. How do you handle nulls?
**A:** `na.drop`, `na.fill`, `coalesce`, `when(...).otherwise(...)`, null-safe joins (`eqNullSafe`), and explicit DQ checks.

### Q26. `dropDuplicates` vs `distinct`?
**A:**
- `distinct`: unique across all columns  
- `dropDuplicates(["col1","col2"])`: unique on subset (business key dedupe)

---

## 7. Aggregations

### Q27. How do aggregations work internally at a high level?
**A:** Often partial aggregates locally, then shuffle by group key, then final aggregate. Shuffle cost depends on key cardinality and skew.

### Q28. What is `countDistinct` caveat?
**A:** Exact distinct counts can be expensive. For approximations use `approx_count_distinct` (HyperLogLog).

### Q29. What is pivot?
**A:** Turns unique values of a column into multiple columns with aggregated measures (status × amount matrix, etc.).

---

## 8. Joins

### Q30. Types of joins in Spark?
**A:** Inner, left/right/full outer, left semi, left anti, cross. Semi/anti are great for existence checks without duplicate blow-up.

### Q31. What join strategies does Spark use?
**A:** Common ones:
- **Broadcast Hash Join (BHJ):** small side broadcast to all executors  
- **Sort-Merge Join (SMJ):** large-large joins  
- **Shuffled Hash Join:** sometimes used depending on version/config/AQE  

### Q32. When do you broadcast?
**A:** When one side is small enough to fit in executor memory (dimension tables). Use `broadcast(df)` or auto threshold config.

### Q33. What is join explosion?
**A:** One-to-many/many-to-many keys multiply rows unexpectedly. Always validate counts before/after joins.

### Q34. How do you find non-matching keys?
**A:** Left anti join, or left join + filter nulls on right keys.

---

## 9. Window Functions

### Q35. What is a window function?
**A:** Computes values across a related row set without collapsing rows (`rank`, `row_number`, `lag`, `sum` over frame).

### Q36. `row_number` vs `rank` vs `dense_rank`?
**A:**
- `row_number`: unique sequential (ties broken arbitrarily unless fully ordered)  
- `rank`: gaps after ties (1,1,3)  
- `dense_rank`: no gaps (1,1,2)

### Q37. How to get latest record per key?
**A:** Window `partitionBy(key).orderBy(ts.desc())` + `row_number = 1` filter. Classic SCD/dedupe pattern.

### Q38. What is a window frame?
**A:** Defines which rows in the partition participate (`rowsBetween`, `rangeBetween`). Needed for running totals / moving averages.

---

## 10. Complex / Nested Types

### Q39. How do you flatten arrays?
**A:** `explode` / `explode_outer`. Example: line items array → one row per item.

### Q40. How do you parse JSON strings in a column?
**A:** Define schema + `from_json(col, schema)`, then select nested fields (`col.field`).

### Q41. What are higher-order functions?
**A:** Functions over arrays/maps without UDFs: `transform`, `filter`, `exists`, `aggregate`, etc.

---

## 11. Built-in Functions vs UDFs

### Q42. Why are Python UDFs slow?
**A:** Data moves between JVM and Python workers, serialization overhead, and Catalyst can’t optimize inside UDF logic.

### Q43. When use Pandas UDF (vectorized UDF)?
**A:** When logic can’t be expressed with built-ins and you need better performance than row-at-a-time Python UDFs via Arrow batches. Still not first choice.

### Q44. Interview gold answer: “How do you avoid UDFs?”
**A:** Re-express with SQL/`when`/regex/`date` functions/higher-order functions first. Use UDF only as last resort with tests and performance comparison.

---

## 12. Partitioning & Parallelism

### Q45. What is a partition in Spark?
**A:** A chunk of data processed by one task. More partitions → more parallelism (to a point).

### Q46. `repartition` vs `coalesce`?
**A:**
- `repartition(n)` / `repartition(cols)`: full shuffle, can increase or decrease, better balancing  
- `coalesce(n)`: usually reduces partitions with less shuffle (may stay unbalanced)

### Q47. What is `spark.sql.shuffle.partitions`?
**A:** Default number of partitions after wide transformations (often 200). Too high → tiny tasks; too low → huge partitions / less parallelism.

### Q48. How do you choose partition count?
**A:** Rule-of-thumb starting point: aim for partition size roughly ~100–200MB post-shuffle, consider cluster cores, measure with UI, avoid tiny files on write.

### Q49. DataFrame partition vs table partition?
**A:**
- **Spark partitions:** runtime parallelism units  
- **Hive/Delta partitions:** storage layout by column values  

Related, but not the same thing.

---

## 13. Caching & Persistence

### Q50. When should you cache?
**A:** When a DataFrame is reused multiple times and is expensive to recompute. Don’t cache single-use pipelines by default.

### Q51. `cache` vs `persist`?
**A:** `cache()` = persist with default storage level (usually MEMORY_AND_DISK for DataFrames). `persist(level)` lets you choose storage level.

### Q52. Does `cache()` execute immediately?
**A:** No. Cache is lazy until an action materializes it. Common pattern: `df.cache(); df.count()`.

### Q53. Why call `unpersist()`?
**A:** Free memory/disk for other jobs/stages and avoid cluster pressure.

---

## 14. Performance Tuning & Skew

### Q54. What is data skew?
**A:** Uneven key distribution — few keys dominate shuffle partitions, causing straggler tasks and long stages.

### Q55. How do you detect skew?
**A:** Spark UI task duration variance, shuffle read sizes uneven, one task much longer; also pre-agg key frequency analysis (`groupBy(key).count().orderBy(desc)`).

### Q56. How do you fix skew?
**A:** Options:
1. Filter early / pre-aggregate  
2. Broadcast small side  
3. Salting skewed keys  
4. AQE skew join  
5. Isolate hot keys and handle separately  
6. Increase parallelism carefully (not a complete cure)

### Q57. What is salting?
**A:** Add random salt to hot keys on one side and explode salts on the other so one huge key is split across partitions, then aggregate/desalt.

### Q58. Top PySpark performance techniques?
**A:**
1. Filter early, select only needed columns  
2. Prefer built-ins over UDFs  
3. Broadcast small dims  
4. Manage partition counts / avoid tiny files  
5. Handle skew  
6. Use Parquet/Delta  
7. Enable AQE  
8. Cache only reused expensive DFs  
9. Avoid `collect`  
10. Validate with Spark UI, not guesses  

---

## 15. Catalyst, Tungsten & AQE

### Q59. What is Catalyst optimizer?
**A:** Spark SQL’s query optimizer: analyzes logical plan, applies rule-based/cost optimizations, produces optimized physical plan.

### Q60. What is Tungsten?
**A:** Execution/engine project for efficient CPU/memory usage: whole-stage codegen, off-heap/binary memory management improvements, cache-aware layouts.

### Q61. What is AQE (Adaptive Query Execution)?
**A:** Runtime optimization that changes plan based on intermediate stats (e.g., coalesce shuffle partitions, optimize skew joins, change join strategy).

### Q62. Important AQE settings?
**A:**
- `spark.sql.adaptive.enabled`  
- `spark.sql.adaptive.coalescePartitions.enabled`  
- `spark.sql.adaptive.skewJoin.enabled`  

### Q63. How do you inspect a plan?
**A:** `df.explain()` / `explain("formatted")` / `explain(True)` and SQL tab in Spark UI.

---

## 16. Spark UI & Debugging

### Q64. Key Spark UI tabs?
**A:** Jobs, Stages, Tasks, Storage, Executors, SQL/DataFrame.

### Q65. How do you find a bottleneck in UI?
**A:** Longest stage → inspect shuffle read/write, spill, GC time, task skew (max vs median task time), failed tasks.

### Q66. What is spill?
**A:** When data doesn’t fit in memory during aggregation/sort/join, Spark writes temporary data to disk. High spill ⇒ memory pressure / poor partitioning / too-large tasks.

### Q67. Why are there many small tasks?
**A:** Too many partitions / tiny files. Overhead dominates; coalesce/compaction and better write sizing help.

---

## 17. Memory, Configs & Failures

### Q68. Driver OOM vs Executor OOM?
**A:**
- **Driver OOM:** often `collect`, broadcasting too-large DF, large result to driver  
- **Executor OOM:** large partitions, heavy aggregations/joins, big UDFs, insufficient executor memory

### Q69. What is broadcast timeout / broadcast OOM risk?
**A:** Broadcasting datasets that are too large for executors/driver collection step. Keep broadcast tables small; validate size.

### Q70. Common production failure: tiny files — why bad?
**A:** Job planning/listing overhead, many tiny tasks, slow reads. Compact with optimize/coalesce/controlled repartition before write.

### Q71. What does `spark.sql.autoBroadcastJoinThreshold` do?
**A:** Max size under which Spark auto-broadcasts a join side (default often 10MB; environment-dependent). Can raise carefully.

---

## 18. Structured Streaming

### Q72. What is Structured Streaming?
**A:** Stream processing engine built on Spark SQL; treats streams as unbounded tables, usually micro-batch execution.

### Q73. Output modes?
**A:** `append`, `complete`, `update` — validity depends on query type (e.g., aggregations without watermark restrictions).

### Q74. What is a watermark?
**A:** Threshold for how late data is still accepted for windowed aggregations, enabling state cleanup.

### Q75. Why is checkpointing required?
**A:** Stores offsets/progress/state so the stream can recover and maintain consistency after failure/restart.

### Q76. Exactly-once in streaming?
**A:** Achievable with replayable sources + idempotent/transactional sinks (e.g., Delta) and checkpointing end-to-end. Not automatic for every sink.

### Q77. Batch vs streaming API similarity?
**A:** Big interview point: almost the same DataFrame transformations; differences are in readStream/writeStream, triggers, watermarks, state.

---

## 19. Delta Lake / Lakehouse

### Q78. What is Delta Lake?
**A:** Storage layer on Parquet with transaction log enabling ACID, time travel, upserts/merges, schema enforcement/evolution.

### Q79. Bronze / Silver / Gold?
**A:**
- **Bronze:** raw ingestion  
- **Silver:** cleaned, validated, conformed  
- **Gold:** business aggregates / feature tables for consumers  

### Q80. What does MERGE do?
**A:** Upsert: update matched rows, insert unmatched (and optionally delete). Core for SCD/dedupe incremental loads.

### Q81. Time travel?
**A:** Query older table versions by version number or timestamp using Delta history/log.

### Q82. OPTIMIZE / ZORDER (Databricks)?
**A:**
- **OPTIMIZE:** compacts small files  
- **ZORDER:** colocates related data for better data skipping on filter columns  

### Q83. Managed vs external table (high level)?
**A:** Managed: catalog controls metadata + data lifecycle. External: catalog metadata points to external location; dropping table may keep data.

---

## 20. Production, DQ & Databricks

### Q84. How do you make pipelines idempotent?
**A:** Partition overwrite by date, MERGE on keys, deterministic dedupe, checkpointing for streams, avoid blind append duplicates.

### Q85. What data quality checks do you add?
**A:** Row counts, null thresholds, PK uniqueness, referential integrity (anti-joins), range checks, schema checks, freshness SLAs.

### Q86. How do you design a rerunnable daily job?
**A:** Parameterize `run_date`, read incremental window, write to partitioned tables idempotently, log metrics, fail on DQ gates.

### Q87. Databricks-specific concepts interviewers expect?
**A:** Clusters/jobs/workflows, notebooks vs jobs, Delta, Unity Catalog basics, DBFS/Volumes, Photon (vectorized execution engine), AQE defaults, compute sizing.

### Q88. Photon in one sentence?
**A:** Databricks vectorized query engine that can accelerate many SQL/DataFrame workloads under compatible operators.

---

## 21. Scenario-Based / Coding Questions

### Q89. “Job suddenly became slow after data growth. What do you do?”
**A:**  
1. Compare Spark UI stages to baseline  
2. Check skew / spill / shuffle size  
3. Check tiny files and partition explosion  
4. Validate join strategy still correct  
5. Revisit shuffle partitions & broadcast eligibility  
6. Fix biggest bottleneck first, measure again  

### Q90. “How do you deduplicate events with late arrivals?”
**A:** Define business key + ordering (event_ts, ingest_ts). Use window `row_number` or Delta MERGE with newer-wins logic. For streams, watermark + state or merge into silver.

### Q91. Write PySpark to get top N products per category by revenue.
```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("category").orderBy(F.col("revenue").desc())

top_n = (
    sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= N)
)
```

### Q92. Find customers who never placed an order.
```python
customers.join(orders, "customer_id", "left_anti")
```

### Q93. Compute running total per customer by date.
```python
w = (
    Window.partitionBy("customer_id")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, 0)
)
df.withColumn("running_total", F.sum("amount").over(w))
```

### Q94. “Explain this plan: BroadcastHashJoin vs SortMergeJoin”
**A:** BHJ means small side broadcasted (usually good). SMJ means both sides large/sorted/shuffled. If BMH expected but SMJ appears, small side may exceed broadcast threshold or stats prevent broadcast.

### Q95. Design bronze→silver→gold for orders.
**A:**  
- **Bronze:** raw JSON/CSV append with ingest timestamp  
- **Silver:** typed schema, null/range checks, dedupe by order_id, quarantine invalids  
- **Gold:** daily GMV fact, customer LTV, product ranking tables for BI  

### Q96. How would you handle a skewed join on `customer_id`?
**A:** Confirm with key histogram + UI; broadcast if possible; else salt hot customers; or separate hot-key pipeline; enable AQE skew join and re-measure.

### Q97. What’s wrong with this code?
```python
result = df.collect()
for row in result:
    ...
```
**A:** Driver-side iteration on full data. Use Spark transformations distributedly; `collect` only samples/aggregates.

### Q98. Optimize: multiple actions on same cleaned DF.
**A:** Cache/persist cleaned DF after first materialization, perform actions, then `unpersist`. Or better: write cleaned intermediate table once and reuse.

---

## 22. Quick Fire Round

| Question | Short answer |
|---|---|
| Transformation lazy? | Yes |
| Action lazy? | No |
| Default API choice? | DataFrame / SQL |
| Most expensive operation often? | Shuffle / large joins |
| Fast join for small dim? | Broadcast hash join |
| Latest row per key? | `row_number` window |
| Existence check without duplicates? | Left semi |
| Missing keys? | Left anti |
| Avoid Python UDF? | Prefer built-ins |
| Stream recovery? | Checkpoint location |
| ACID on lake files? | Delta Lake |
| Upserts? | MERGE |
| Inspect plan? | `explain` + SQL UI |
| Reduce partitions lightly? | `coalesce` |
| Redistribute evenly? | `repartition` |
| Tiny files fix? | Compact / OPTIMIZE |
| Count distinct expensive? | Yes; approx option exists |
| Cache always? | No, only if reused |
| AQE helps? | Runtime plan adaptation |
| `show` is action? | Yes |

---

## 30-Minute Mock Interview Script

Use this to self-test:

1. Explain Spark architecture end-to-end (3 min)  
2. Narrow vs wide + shuffle example (2 min)  
3. Why DataFrames over RDDs (2 min)  
4. Join strategies + when to broadcast (4 min)  
5. Window: latest record per user code + explanation (4 min)  
6. Skew detection and remediation (4 min)  
7. Spark UI debugging walkthrough (4 min)  
8. Delta bronze/silver/gold + MERGE (4 min)  
9. Streaming watermark + checkpoint (3 min)  

Score yourself: if you can answer without notes cleanly, you’re interview-ready for most PySpark/DE roles.

---

## Concept Coverage Checklist

- [x] Architecture, DAG, lineage, shuffle  
- [x] RDD / DataFrame / Dataset  
- [x] Jobs, stages, tasks  
- [x] Transformations & actions  
- [x] IO formats, schema, partitions on write  
- [x] DF API + Spark SQL  
- [x] Aggregations & pivot  
- [x] All major join types & strategies  
- [x] Windows & frames  
- [x] Nested/JSON/complex types  
- [x] UDFs vs built-ins / Pandas UDFs  
- [x] Partitioning, repartition/coalesce  
- [x] Cache/persist  
- [x] Skew, salting, broadcast tuning  
- [x] Catalyst, Tungsten, AQE  
- [x] Spark UI / spill / debugging  
- [x] Memory & common failures  
- [x] Structured Streaming  
- [x] Delta / lakehouse  
- [x] Production DQ + Databricks topics  
- [x] Scenario + coding questions  

---

## How to Practice Answers

1. Speak answers out loud in 45–90 seconds.  
2. Always add one example from your project/capstone.  
3. For performance questions, structure: **symptom → evidence (UI) → fix → re-measure**.  
4. Keep a “story bank”: one skew fix, one broadcast win, one tiny-files incident, one MERGE design.

Good luck — master the **why**, not just definitions. Interviewers hire people who can debug plans and shuffles, not just recite APIs.
