# PySpark 25-Day Learning Schedule
### Beginner → Advanced | Theory + Hands-On Path

**Audience:** Learners with Python, SQL, and Databricks familiarity (true beginners can follow too — spend extra time on Days 1–5).  
**Daily commitment:** 4–6 hours  
**Goal:** Write, debug, and optimize real PySpark pipelines with confidence  

---

## How to Use This Schedule

| Item | Guidance |
|---|---|
| Daily structure | Theory (30–40%) → Examples (20%) → Hands-on (40–50%) |
| Environment | Databricks Community / personal workspace, or local Spark via `pyspark` |
| Practice rule | Prefer datasets ≥ 100MB–few GB for Days 9+ (tiny CSVs hide performance lessons) |
| Daily output | Notes + one runnable notebook + short “what I learned” summary |
| Checkpoint days | Days 7, 14, 21, 25 — review + mini-project demos |

### Progress Markers

- **Days 1–7:** Foundations & DataFrame fluency  
- **Days 8–14:** Analytics, joins, windows, data quality  
- **Days 15–21:** Performance, Spark UI, tuning  
- **Days 22–25:** Streaming, advanced patterns, capstone  

### Suggested Global Dataset Themes

Use one recurring business domain so skills compound:

1. **Retail orders** (customers, products, orders, payments)  
2. **Clickstream / events** (user_id, event_type, ts, page)  
3. **IoT sensor readings** (device_id, metric, ts, value)  

---

## Week 1 — Spark Foundations & Core DataFrame API

---

### Day 1 — What Is Spark / PySpark?

**Learning objectives**
- Explain Spark vs traditional single-machine processing  
- Describe driver, cluster manager, executors, and jobs  
- Create a Spark session and run a first transformation  

**Concepts**
- Big data problem: scale-out vs scale-up  
- Spark ecosystem: Spark Core, SQL, Streaming, MLlib  
- PySpark = Python API over JVM Spark engine  
- Lazy evaluation (transform vs action)  

**Practical examples**
```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("day1-intro")
    .getOrCreate()
)

data = [("Alice", 34), ("Bob", 28), ("Carla", 41)]
df = spark.createDataFrame(data, ["name", "age"])

# transformation (lazy)
adults = df.filter("age >= 30").select("name")

# action (triggers job)
adults.show()
adults.count()
```

**Hands-on exercises**
1. Start a Spark session and print Spark / Python versions.  
2. Create a DataFrame from a Python list of tuples.  
3. Apply `filter`, `select`, then trigger with `show()` and `count()`.  
4. In Databricks: inspect the job in Spark UI (Jobs tab).  

**Mini-project**
- Load a small CSV of people (or generate one). Count rows, show schema, filter adults, write names to a new table/path.

**End-of-day checklist**
- [ ] Can explain driver vs executor  
- [ ] Know what makes something an action  

---

### Day 2 — Spark Architecture Deep Dive

**Learning objectives**
- Map application → jobs → stages → tasks  
- Distinguish narrow vs wide transformations  
- Explain lineage and fault tolerance  

**Concepts**
- Cluster modes (local, standalone, YARN, Kubernetes, Databricks)  
- Partition = unit of parallelism  
- Shuffle = data redistribution across executors  
- Lineage graph / DAG  

**Practical examples**
```python
df = spark.range(0, 1_000_000).withColumnRenamed("id", "order_id")

# narrow: map-like ops stay in partition
df2 = df.withColumn("bucket", (df.order_id % 10))

# wide (can cause shuffle): groupBy / join / repartition
df3 = df2.groupBy("bucket").count()
df3.explain(True)
```

**Hands-on exercises**
1. Run `spark.range(...).repartition(8)` and compare partition counts.  
2. Use `.explain("formatted")` before/after a `groupBy`.  
3. Intentionally create a shuffle (`repartition` by key) and find the Exchange in the plan.  

**Mini-project**
- Process 1M synthetic rows: add columns, filter, aggregate by key. Draw the DAG stages on paper from Spark UI.

**End-of-day checklist**
- [ ] Can define job / stage / task  
- [ ] Can spot a shuffle in explain output  

---

### Day 3 — RDDs vs DataFrames vs Datasets

**Learning objectives**
- Choose DataFrames for almost all modern work  
- Convert between RDD and DataFrame when needed  
- Understand Catalyst optimizer benefits  

**Concepts**
- RDD: low-level, typed loosely in Python, fewer optimizations  
- DataFrame: schema + Catalyst + Tungsten  
- Dataset: JVM-centric typed API (less relevant in pure PySpark)  

**Practical examples**
```python
rdd = spark.sparkContext.parallelize([(1, "a"), (2, "b")])
df = rdd.toDF(["id", "val"])

# DataFrame API
df.select("id").where("id > 1").show()

# Prefer built-in SQL expressions over Python UDFs
from pyspark.sql import functions as F
df.withColumn("id2", F.col("id") * 10).show()
```

**Hands-on exercises**
1. Convert list → RDD → DataFrame → temp view → SQL query.  
2. Compare readability of RDD `map` vs DataFrame `withColumn`.  
3. Note why DataFrames are preferred in Databricks notebooks.  

**Mini-project**
- Rebuild Day 1 pipeline once with RDD ops and once with DataFrames. Document which is clearer/faster.

---

### Day 4 — Reading & Writing Data

**Learning objectives**
- Read/write CSV, JSON, Parquet, Delta (Databricks)  
- Control schema (infer vs explicit)  
- Use modes: `overwrite`, `append`, `ignore`, `error`  

**Concepts**
- Schema enforcement & evolution basics  
- Parquet columnar benefits  
- Partitioned writes (`partitionBy`)  

**Practical examples**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
])

orders = (
    spark.read
    .option("header", True)
    .schema(schema)
    .csv("/path/orders.csv")
)

(
    orders.write
    .mode("overwrite")
    .partitionBy("status")
    .parquet("/path/orders_parquet")
)
```

**Hands-on exercises**
1. Read a CSV with inferred schema, then again with explicit schema — compare.  
2. Write Parquet and re-read; compare file sizes vs CSV.  
3. Write partitioned by date/status; inspect folder layout.  

**Mini-project**
- Ingest messy CSV → cast types → write clean Parquet/Delta table with partitions.

---

### Day 5 — DataFrame Transformations Core

**Learning objectives**
- Master `select`, `withColumn`, `drop`, `filter`, `where`, `distinct`  
- Use `when`/`otherwise` for conditional columns  
- Keep transformations expressive and chainable  

**Concepts**
- Column expressions  
- Immutable DataFrames (each transform returns a new DF)  
- Projection pushdown awareness  

**Practical examples**
```python
from pyspark.sql import functions as F

clean = (
    orders
    .withColumn("amount_tax", F.col("amount") * 1.18)
    .withColumn(
        "tier",
        F.when(F.col("amount") >= 500, "high")
         .when(F.col("amount") >= 100, "mid")
         .otherwise("low")
    )
    .filter(F.col("status").isin("PAID", "SHIPPED"))
    .dropDuplicates(["order_id"])
)
```

**Hands-on exercises**
1. Create 5 derived columns using only built-in functions.  
2. Deduplicate on business key; count removed rows.  
3. Replace nulls with defaults using `coalesce` / `fillna`.  

**Mini-project**
- Build a “order enrichment” notebook: cleanup + derived metrics + quality counts.

---

### Day 6 — Spark SQL & Temp Views

**Learning objectives**
- Interchange fluently between DataFrame API and Spark SQL  
- Create temp / global temp views  
- Write analytical SQL in Spark  

**Concepts**
- Catalyst treats SQL and DataFrame similarly  
- Views vs tables  
- CTEs for readability  

**Practical examples**
```python
orders.createOrReplaceTempView("orders")

spark.sql("""
WITH paid AS (
  SELECT customer_id, amount
  FROM orders
  WHERE status = 'PAID'
)
SELECT customer_id, ROUND(SUM(amount), 2) AS revenue
FROM paid
GROUP BY customer_id
ORDER BY revenue DESC
LIMIT 10
""").show()
```

**Hands-on exercises**
1. Recreate a DataFrame pipeline entirely in SQL.  
2. Mix: filter in DF API, aggregate in SQL.  
3. Compare plans with `.explain()` for both versions.  

**Mini-project**
- KPI dashboard queries: daily GMV, top customers, cancel rate — SQL only.

---

### Day 7 — Week 1 Checkpoint Mini-Project

**Learning objectives**
- Combine week-1 skills into one coherent pipeline  
- Practice documentation and reproducibility  

**Project: Retail Ingestion & Basic Reporting**
1. Generate/load customers + orders CSVs  
2. Enforce schemas  
3. Clean nulls / invalid amounts  
4. Join-free metrics first (counts by status, amount buckets)  
5. Write curated Parquet/Delta  
6. Answer 5 SQL questions from curated tables  

**Deliverables**
- Notebook + output tables + 1-page notes: architecture terms + lessons learned  

---

## Week 2 — Analytics: Joins, Aggregations, Windows, Quality

---

### Day 8 — Aggregations & Grouping

**Learning objectives**
- Use `groupBy`, `agg`, `pivot`  
- Apply multiple aggregations cleanly  
- Understand partial aggregation & shuffle cost  

**Practical examples**
```python
from pyspark.sql import functions as F

metrics = (
    orders.groupBy("status")
    .agg(
        F.count("*").alias("orders"),
        F.sum("amount").alias("gmv"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("customer_id").alias("customers")
    )
)

pivot_df = (
    orders.groupBy("customer_id")
    .pivot("status")
    .sum("amount")
)
```

**Hands-on exercises**
1. Compute GMV by day and by status.  
2. Build a pivot of status × payment method.  
3. Find customers with > 3 cancelled orders.  

**Mini-project**
- Daily business summary table refreshed from raw orders.

---

### Day 9 — Joins Deep Dive

**Learning objectives**
- Use inner/left/right/full/semi/anti joins  
- Avoid accidental row explosion  
- Broadcast small dimensions intentionally  

**Practical examples**
```python
from pyspark.sql import functions as F

# Prefer explicit join conditions
joined = (
    orders.alias("o")
    .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
    .select("o.*", "c.customer_name", "c.city")
)

# Broadcast hint for small dimension
from pyspark.sql.functions import broadcast
enriched = orders.join(broadcast(countries), "country_code", "left")
```

**Hands-on exercises**
1. Implement semi/anti joins to find customers with/without orders.  
2. Create a one-to-many join and measure row count blow-up.  
3. Compare broadcast vs shuffle join plans.  

**Mini-project**
- Build a wide order fact table from orders + customers + products + payments.

---

### Day 10 — Window Functions

**Learning objectives**
- Rank, lag/lead, running totals, session-like patterns  
- Control `partitionBy` + `orderBy` + frame clauses  

**Practical examples**
```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("customer_id").orderBy("order_ts")

customer_timeline = (
    orders
    .withColumn("rn", F.row_number().over(w))
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn(
        "running_gmv",
        F.sum("amount").over(w.rowsBetween(Window.unboundedPreceding, 0))
    )
)
```

**Hands-on exercises**
1. Latest order per customer (`row_number` + filter `rn=1`).  
2. Days between consecutive orders (`lag` + date diff).  
3. Top 3 products per category by revenue.  

**Mini-project**
- Customer 360 slice: first/last order, lifetime value, order rank.

---

### Day 11 — Complex Types & Nested Data

**Learning objectives**
- Work with arrays, maps, structs  
- Use `explode`, `from_json`, `to_json`, higher-order functions  

**Practical examples**
```python
from pyspark.sql import functions as F

items = spark.createDataFrame(
    [(1, ["milk", "bread"]), (2, ["tea"])],
    ["order_id", "items"]
)

items.select("order_id", F.explode("items").alias("item")).show()

# Higher-order function
items.select(
    F.transform("items", lambda x: F.upper(x)).alias("items_upper")
).show(truncate=False)
```

**Hands-on exercises**
1. Parse a JSON payload column into struct fields.  
2. Flatten nested order line-items with `explode`.  
3. Rebuild nested struct from flat columns.  

**Mini-project**
- Parse semi-structured event JSON into analytical flat tables.

---

### Day 12 — Date/Time, Strings & Built-in Functions

**Learning objectives**
- Handle timestamps/timezones cleanly  
- Prefer Spark built-ins over Python UDFs  
- Use regex / string helpers effectively  

**Practical examples**
```python
from pyspark.sql import functions as F

df = (
    events
    .withColumn("event_ts", F.to_timestamp("event_time"))
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn("hour", F.hour("event_ts"))
    .withColumn("device_clean", F.lower(F.trim("device")))
    .withColumn("is_mobile", F.col("device_clean").rlike("android|iphone|ios"))
)
```

**Hands-on exercises**
1. Bucket events into hour-of-day traffic chart data.  
2. Normalize messy category strings.  
3. Extract domains from URLs via regex.  

**Mini-project**
- Build a daily traffic summary with hour heatmaps (table form).

---

### Day 13 — UDFs, Pandas UDFs & When to Avoid Them

**Learning objectives**
- Know UDF overhead and serialization costs  
- Use Pandas UDFs only when built-ins can’t express logic  
- Default to SQL expressions  

**Practical examples**
```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

@udf(StringType())
def tier_udf(amount):
    if amount is None:
        return "unknown"
    return "high" if amount >= 500 else "low"

# Better: native expression
from pyspark.sql import functions as F
F.when(F.col("amount") >= 500, "high").otherwise("low")
```

**Hands-on exercises**
1. Implement same logic as UDF and as built-in; compare runtimes.  
2. Write one Pandas UDF for a row-wise text normalization.  
3. Refactor an existing notebook to remove unnecessary UDFs.  

**Mini-project**
- Performance bake-off write-up: native vs Python UDF on same dataset.

---

### Day 14 — Week 2 Checkpoint Mini-Project

**Project: Customer Analytics Mart**
1. Multi-table joins (orders/customers/products)  
2. Window metrics (LTV, recency, frequency)  
3. Nested JSON parsing for marketing attributes  
4. Data-quality checks (null%, orphan keys, duplicate business keys)  
5. Publish star-schema style tables (fact + dims)  

**Acceptance criteria**
- Re-runnable notebook  
- Documented assumptions  
- At least 8 analytical queries answered  

---

## Week 3 — Performance, Partitioning, Spark UI & Tuning

---

### Day 15 — Partitioning & Parallelism

**Learning objectives**
- Inspect and change partition counts  
- Use `repartition` vs `coalesce` correctly  
- Link partitions to core counts and file sizes  

**Practical examples**
```python
print(df.rdd.getNumPartitions())

df_even = df.repartition(64, "customer_id")   # shuffle
df_fewer = df.coalesce(8)                     # narrow reduce (usually)
```

**Hands-on exercises**
1. Measure runtime for same job at 4 / 16 / 64 partitions.  
2. Write too-many tiny files; then compact with coalesce.  
3. Repartition by high-cardinality vs low-cardinality key and compare skew symptoms.  

**Mini-project**
- File-size health check script for a curated table path.

---

### Day 16 — Caching, Persistence & Checkpointing

**Learning objectives**
- Cache only reused DataFrames with heavy lineage  
- Choose storage levels thoughtfully  
- Unpersist to free memory  

**Practical examples**
```python
base = spark.read.parquet("/path/orders").filter("status = 'PAID'").cache()
print(base.count())  # materialize cache

# multiple actions reuse cache
base.groupBy("customer_id").sum("amount").show()
base.select("order_id").distinct().count()

base.unpersist()
```

**Hands-on exercises**
1. Time 3 actions with and without cache.  
2. Over-cache intentionally; watch memory pressure.  
3. Compare `cache()` vs `persist(StorageLevel.MEMORY_AND_DISK)`.  

**Mini-project**
- Optimize a branching analytics notebook by caching the shared cleaned base.

---

### Day 17 — Join Strategy & Skew Handling

**Learning objectives**
- Read join strategy from explain/UI  
- Apply broadcast joins safely  
- Mitigate data skew (salting / AQE skew join awareness)  

**Practical examples**
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)

# Salting sketch for skewed key
from pyspark.sql import functions as F
skewed = orders.withColumn("salt", (F.rand() * 10).cast("int"))
dim_salted = (
    customers
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(10)])))
)
```

**Hands-on exercises**
1. Force a sorted-merge join and a broadcast join; capture plans.  
2. Create a synthetic skewed key dataset and observe stage imbalance.  
3. Apply a salt join and compare stage task times.  

**Mini-project**
- “Skew clinic”: diagnose and fix one intentionally skewed pipeline.

---

### Day 18 — Spark UI Mastery

**Learning objectives**
- Navigate Jobs / Stages / Tasks / Storage / SQL / Executors  
- Identify spill, skew, GC pressure, shuffle read/write  
- Convert UI evidence into code changes  

**Study workflow**
1. Run a slow job  
2. Open SQL tab → longest query  
3. Drill into stage with high shuffle  
4. Inspect task duration distribution (skew?)  
5. Check spill / GC time  
6. Change one thing and re-measure  

**Hands-on exercises**
1. Screenshot/annotate one healthy and one unhealthy stage.  
2. Find a job with disk spill; reduce it via partitioning or fewer wide ops.  
3. Correlate explain Exchange operators with UI shuffle stages.  

**Mini-project**
- Write a one-page “Spark UI playbook” with your own examples.

---

### Day 19 — AQE, Catalyst & Query Plans

**Learning objectives**
- Enable/interpret Adaptive Query Execution effects  
- Read physical plans confidently  
- Use `explain` modes effectively  

**Practical examples**
```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)

df.join(dim, "id").groupBy("city").count().explain("formatted")
```

**Hands-on exercises**
1. Run same query with AQE on/off; compare final plans.  
2. Identify broadcast hash join / sort merge join operators.  
3. Find where dynamic partition coalescing changed partition count.  

**Mini-project**
- Before/after AQE report on a join-heavy pipeline.

---

### Day 20 — Memory, Configs & Common Failures

**Learning objectives**
- Interpret executor memory regions at a high level  
- Tune common configs without random guessing  
- Debug OOM, shuffle failures, driver collect mistakes  

**Configs to know**
- `spark.sql.shuffle.partitions`  
- `spark.sql.autoBroadcastJoinThreshold`  
- `spark.executor.memory` / `spark.driver.memory` (cluster sizing awareness)  
- `spark.sql.files.maxPartitionBytes`  

**Anti-patterns**
- `collect()` on large data  
- Python UDFs on huge rows  
- Too many tiny files  
- Joining without filter pushdown  

**Hands-on exercises**
1. Cause a driver OOM with careless `collect`; fix with sampling/limits.  
2. Change shuffle partitions and measure.  
3. Build a “failure → likely cause → fix” cheat sheet (10 rows).  

**Mini-project**
- Hardening checklist applied to Week 2 project.

---

### Day 21 — Week 3 Checkpoint Mini-Project

**Project: Optimize a Slow Pipeline**
1. Take Customer Analytics Mart (Day 14)  
2. Profile with Spark UI (baseline runtime + bottlenecks)  
3. Apply: broadcast, repartitioning, cache (if needed), AQE, file compaction  
4. Produce before/after metrics  

**Acceptance criteria**
- ≥1 measurable speedup  
- Documented plan changes with evidence  
- No correctness regressions (row counts / checksums)  

---

## Week 4 — Streaming, Advanced Patterns & Capstone

---

### Day 22 — Structured Streaming Fundamentals

**Learning objectives**
- Explain micro-batch streaming model  
- Build a simple streaming source → transform → sink pipeline  
- Use checkpoints and output modes  

**Practical examples**
```python
stream_df = (
    spark.readStream
    .format("cloudFiles")  # or json/csv folder stream locally
    .option("cloudFiles.format", "json")
    .load("/path/inbox")
)

clean = stream_df.filter("event_type IS NOT NULL")

query = (
    clean.writeStream
    .format("delta")
    .option("checkpointLocation", "/path/checkpoints/events")
    .outputMode("append")
    .start("/path/tables/events")
)
```

*(Local fallback: `readStream` on a growing file folder with `json`/`csv`.)*

**Hands-on exercises**
1. Stream files dropped into a folder every minute.  
2. Add watermark + aggregation (tumbling window).  
3. Restart job and verify exactly-once-ish behavior via checkpoint.  

**Mini-project**
- Near-real-time event counter by type and minute.

---

### Day 23 — Delta Lake / Lakehouse Patterns (Databricks-friendly)

**Learning objectives**
- Understand bronze/silver/gold architecture  
- Use MERGE (upsert), time travel, optimize/ZORDER basics  
- Design idempotent pipelines  

**Practical examples**
```python
# Pseudocode style for MERGE
spark.sql("""
MERGE INTO silver_customers t
USING updates s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
```

**Hands-on exercises**
1. Build bronze → silver → gold for orders.  
2. Upsert late-arriving customer dimension rows.  
3. Query table `@vN` / timestamp travel and compare.  

**Mini-project**
- Idempotent daily load with quarantine table for bad records.

---

### Day 24 — Production Patterns & Testing Mindset

**Learning objectives**
- Structure notebooks/jobs for production readiness  
- Add data contracts / expectations  
- Parameterize paths and run dates  

**Topics**
- Job orchestration basics (Databricks Jobs / workflows)  
- Replayability and late data  
- Unit-testable transform functions  
- Logging metrics (input count, reject count, runtime)  

**Practical examples**
```python
def enrich_orders(orders_df, customers_df):
    return (
        orders_df.join(customers_df, "customer_id", "left")
        .withColumn("is_orphan", F.col("customer_name").isNull())
    )

assert enrich_orders(sample_orders, sample_customers).count() == expected
```

**Hands-on exercises**
1. Refactor a notebook into functions + parameters.  
2. Add quality gates that fail the job on high null rates.  
3. Create a rerunnable backfill for 7 days.  

**Mini-project**
- Productionize one pipeline with config-driven dates and DQ checks.

---

### Day 25 — Capstone Project (Full Advanced Demo)

**Capstone: End-to-End Commerce Data Platform (Mini)**

**Scope**
1. **Bronze:** ingest raw orders/events (batch + optional stream)  
2. **Silver:** cleanse, dedupe, enforce schemas, quarantine bad rows  
3. **Gold:** customer LTV, product performance, daily KPI fact tables  
4. **Optimize:** partitions, broadcast dims, AQE, file hygiene  
5. **Prove:** Spark UI before/after + data quality report  

**Suggested success metrics**
- Pipeline rerunnable with a date parameter  
- Clear mediate tables and final consumer tables  
- At least 2 performance optimizations with measured impact  
- Notes explaining 5 advanced concepts used  

**Demo checklist**
- [ ] Architecture diagram (brief)  
- [ ] Code walkthrough  
- [ ] UI evidence of improvements  
- [ ] Known limitations & next steps  

---

## Appendix A — Quick Reference Mind Map

```text
PySpark Mastery
├── Architecture (driver, executors, DAG, shuffle)
├── Data APIs (DF / SQL; RDD awareness)
├── Transformations (select, filter, agg, join, window)
├── Storage (Parquet/Delta, partitions, files)
├── Performance (broadcast, skew, cache, AQE, UI)
├── Streaming (readStream/writeStream, watermark, checkpoint)
└── Production (DQ, idempotency, jobs, lakehouse layers)
```

---

## Appendix B — Daily Study Template (Copy Each Day)

```markdown
### Day __ — Topic
Date:
Hours studied:

#### Objectives completed
- 

#### Key concepts (my words)
- 

#### Code snippets worth keeping
- 

#### Spark UI observations
- 

#### Mistakes / blockers
- 

#### Tomorrow’s focus
- 
```

---

## Appendix C — Recommended Practice Cadence

| Block | Time | Activity |
|---|---|---|
| Warm-up | 20–30 min | Review yesterday notes + flash recall (terms) |
| Concept sprint | 45–60 min | Read/watch + annotate |
| Guided coding | 60–90 min | Reproduce examples from scratch |
| Challenge | 60–90 min | Exercises without looking |
| Project slice | 45–75 min | Mini-project progress |
| Recap | 15 min | Fill daily template |

---

## Appendix D — Stretch Goals (If Ahead)

- Spark Connect awareness  
- Photon basics on Databricks (what it accelerates)  
- Cost awareness: DBU / cluster sizing intuition  
- Integration with Airflow / Workflows orchestration  
- Intro to MLlib pipelines (optional, not required for DE path)  

---

## Appendix E — Definition of “Advanced” After 25 Days

By Day 25, you should be able to:

1. Build multi-stage PySpark ETL with SQL + DataFrame APIs  
2. Diagnose slow jobs using explain + Spark UI  
3. Choose partitioning/join strategies with reasoning  
4. Handle nested data, windows, and quality checks  
5. Apply lakehouse layering and idempotent merges  
6. Discuss streaming fundamentals and checkpointing  
7. Present a capstone with measurable optimizations  

This is **strong intermediate → early advanced**. Continued growth comes from production incidents, larger datasets, and repeated optimization cycles.

---

## Final Tips for Hitting the 25-Day Target

1. **Don’t restart notebooks endlessly** — deep-dive failures.  
2. **Always verify with counts + plans + UI**, not just `show()`.  
3. **Prefer built-in functions** over UDFs by default.  
4. **Keep one domain dataset** across weeks for continuity.  
5. **Ship the capstone imperfectly** — finishing teaches more than polishing Day 1 notes.  

You’ve got Python, SQL, and Databricks already — use Days 1–5 as a fast runway, and invest your hardest effort in **Days 15–25** (performance + production). That’s where advanced PySpark is earned.

Good luck — stay consistent, measure everything, and build in public (even if it’s just your notes folder).
