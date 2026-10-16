# 🔥 PySpark 80/20 Guide for College Students

> **Learn the 20% of PySpark that you will use 80% of the time.**
> Eat it (read), sweat it (practice), enjoy it (build things). 🍽️💪😄

---

## 📌 How to use this guide

1. Read one section (10 minutes).
2. **Type the code yourself**. Do not copy-paste.
3. Do the "Try it" at the end of each section.
4. Finish with the **Interview Questions** and **Practice Set**.

**Tip for teachers:** each section = one class of ~30-40 minutes.

---

## 🗺️ The 80/20 Map (what actually matters)

| # | Topic | Why it matters | Time to learn |
|---|-------|----------------|---------------|
| 1 | Spark mental model (lazy, transformations vs actions) | Explains *everything* else | 20 min |
| 2 | Read / write data | Every job starts and ends here | 15 min |
| 3 | `select`, `filter`, `withColumn`, `cast` | 60% of daily code | 30 min |
| 4 | Nulls and duplicates | Real data is dirty | 15 min |
| 5 | `groupBy` + `agg` | Reports and summaries | 20 min |
| 6 | Joins | Combining tables | 30 min |
| 7 | Window functions | Ranking, running totals, "top N per group" | 30 min |
| 8 | Built-in functions vs UDF | Speed and correctness | 15 min |
| 9 | Performance basics (partitions, cache, broadcast) | Interview favourite | 30 min |

**Skip for now:** RDD low-level APIs, MLlib, GraphX, custom partitioners, Structured Streaming internals. Learn them later *when needed*.

---

# 1️⃣ The Mental Model: Why Spark Exists

## 🍕 The pizza-shop analogy

- One chef (Pandas) makes 10 pizzas fine. But 10 million pizzas? One chef dies.
- Spark = **a kitchen with many chefs** (executors) and **one manager** (driver) who splits the work.

```
        ┌───────────────┐
        │    DRIVER     │  ← plans the work (your Python code runs here)
        └───────┬───────┘
                │
     ┌──────────┼──────────┐
     ▼          ▼          ▼
 ┌────────┐ ┌────────┐ ┌────────┐
 │Executor│ │Executor│ │Executor│  ← do the actual work on pieces of data
 └────────┘ └────────┘ └────────┘
```

## 🔑 The 3 ideas that explain 80% of Spark

### Idea 1: Data is split into **partitions**
Your big table is cut into pieces. Each piece is processed in parallel.

### Idea 2: **Transformations are lazy**, **actions are eager**

| Type | Meaning | Examples |
|------|---------|----------|
| **Transformation** (lazy) | "Here is my *plan*." Nothing runs yet. | `select`, `filter`, `withColumn`, `join`, `groupBy` |
| **Action** (eager) | "Now actually *do it* and give me a result." | `show`, `count`, `collect`, `write`, `take` |

Like ordering at a restaurant: writing the order = transformation. The waiter shouting it to the kitchen = action. 🧾➡️👨‍🍳

### Idea 3: **Shuffle is expensive**
Moving data between machines (for `groupBy`, `join`, `orderBy`, `distinct`) is the slowest part of Spark. Good Spark = **fewer shuffles**.

## 🧪 See laziness yourself

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("learn").master("local[*]").getOrCreate()

df = spark.range(1_000_000)               # nothing runs
df2 = df.filter(F.col("id") % 2 == 0)     # still nothing runs
df3 = df2.withColumn("sq", F.col("id")**2)  # still nothing!

df3.count()    # ← ACTION. Now Spark runs everything.
```

> 💡 Spark also **optimizes the whole plan** before running (Catalyst optimizer). That is the benefit of laziness.

**Try it:** Add `df3.explain()` before `count()` and read the plan.

---

# 2️⃣ Setup, DataFrames, Reading & Writing

## Install and start

```bash
pip install pyspark
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder
         .appName("college-demo")
         .master("local[*]")      # use all CPU cores of your laptop
         .getOrCreate())
```

> ⭐ **Always** write `from pyspark.sql import functions as F` and use `F.col`, `F.sum`, etc. It avoids clashing with Python's `sum`, `max`, `min`.

## Our practice dataset (use this through the whole guide)

```python
employees = [
    (1, "Asha",  "IT",      60000, "2021-03-01", None),
    (2, "Ravi",  "IT",      75000, "2020-06-15", 1),
    (3, "Meena", "HR",      50000, "2019-01-10", None),
    (4, "John",  "HR",      52000, "2022-09-20", 3),
    (5, "Sara",  "Finance", 80000, "2018-11-05", None),
    (6, "Kiran", "Finance", 80000, "2021-07-30", 5),
    (7, "Tom",   "IT",      None,  "2023-01-12", 1),
    (2, "Ravi",  "IT",      75000, "2020-06-15", 1),   # duplicate row!
]

cols = ["emp_id", "name", "dept", "salary", "join_date", "manager_id"]
df = spark.createDataFrame(employees, cols)

df.show()
df.printSchema()
```

## Reading data (the 4 you will use 95% of the time)

```python
# CSV
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# JSON
df = spark.read.json("data.json")

# Parquet (the best format for big data)
df = spark.read.parquet("data.parquet")

# Table
df = spark.table("my_table")
```

### ⚠️ `inferSchema=True` — good for learning, bad for production
It reads the data twice and can guess wrong. In real projects, give a schema:

```python
schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", IntegerType(), True),
])
df = spark.read.csv("data.csv", header=True, schema=schema)
```

## Writing data

```python
df.write.mode("overwrite").parquet("out/employees")

# partition by a column (creates folders dept=IT/, dept=HR/ ...)
df.write.mode("overwrite").partitionBy("dept").parquet("out/by_dept")
```

| Mode | What it does |
|------|--------------|
| `overwrite` | Replace existing data |
| `append` | Add to existing data |
| `ignore` | Do nothing if data exists |
| `error` (default) | Fail if data exists |

## Why Parquet?
- **Columnar**: reads only the columns you need.
- **Compressed**: small files.
- **Stores schema**: no guessing.

> CSV = a handwritten notebook 📓. Parquet = a well-organized library 📚.

---

# 3️⃣ The Daily Toolkit: `select`, `filter`, `withColumn`

## select — pick columns

```python
df.select("name", "salary").show()
df.select(F.col("name"), (F.col("salary") * 1.1).alias("salary_after_hike")).show()
```

## filter / where — pick rows (they are identical)

```python
df.filter(F.col("salary") > 60000).show()

# multiple conditions: use & | ~  and ALWAYS use parentheses
df.filter((F.col("dept") == "IT") & (F.col("salary") > 60000)).show()

# in-list and pattern
df.filter(F.col("dept").isin("IT", "HR")).show()
df.filter(F.col("name").like("A%")).show()
```

> ❌ **Common mistake:** using `and` / `or` instead of `&` / `|`. Python's `and` does not work on columns.

## withColumn — add or change a column

```python
df = (df
      .withColumn("bonus", F.col("salary") * 0.10)
      .withColumn("join_date", F.to_date("join_date", "yyyy-MM-dd"))
      .withColumn("join_year", F.year("join_date")))
```

## when / otherwise — Spark's if-else

```python
df = df.withColumn(
    "level",
    F.when(F.col("salary") >= 75000, "Senior")
     .when(F.col("salary") >= 55000, "Mid")
     .otherwise("Junior")
)
df.select("name", "salary", "level").show()
```

## cast, rename, drop

```python
df = df.withColumn("salary", F.col("salary").cast("double"))
df = df.withColumnRenamed("dept", "department")
df = df.drop("manager_id")
```

## Sorting

```python
df.orderBy(F.col("salary").desc()).show()
df.orderBy("dept", F.col("salary").desc()).show()
```

## Handy string and date functions

```python
F.upper("name"), F.lower("name"), F.trim("name")
F.concat_ws(" - ", "name", "dept")
F.substring("name", 1, 3)
F.regexp_replace("name", "[aeiou]", "*")

F.current_date()
F.datediff(F.current_date(), "join_date")
F.date_add("join_date", 30)
F.months_between(F.current_date(), "join_date")
```

**Try it:** Create a column `tenure_years` = years since `join_date`, rounded to 1 decimal.

---

# 4️⃣ Dirty Data: Nulls and Duplicates

Real data is never clean. This is a **top interview topic**.

## Nulls

```python
# find nulls
df.filter(F.col("salary").isNull()).show()

# count nulls per column
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

# drop rows with nulls
df.dropna().show()                      # any column null
df.dropna(subset=["salary"]).show()     # only salary

# fill nulls
df.fillna({"salary": 0}).show()

# fill with the average
avg_sal = df.select(F.avg("salary")).first()[0]
df.fillna({"salary": avg_sal}).show()

# first non-null value
df.withColumn("sal", F.coalesce("salary", F.lit(0))).show()
```

> ⚠️ **Null is not equal to anything**, not even another null.
> `F.col("x") == None` does **not** work. Use `.isNull()`.

## Duplicates

```python
df.distinct().count()                     # whole row duplicates removed
df.dropDuplicates(["emp_id"]).count()     # based on chosen columns
```

> `dropDuplicates(["emp_id"])` keeps an **arbitrary** row per key. If you need "keep the latest", use a **window function** (Section 7).

---

# 5️⃣ Summaries: `groupBy` + `agg`

The "GROUP BY" of SQL.

```python
df.groupBy("dept").agg(
    F.count("*").alias("headcount"),
    F.round(F.avg("salary"), 0).alias("avg_salary"),
    F.max("salary").alias("max_salary"),
    F.sum("salary").alias("total_salary"),
    F.countDistinct("emp_id").alias("unique_emps")
).orderBy(F.col("avg_salary").desc()).show()
```

### Aggregating without groups

```python
df.agg(F.avg("salary"), F.max("salary")).show()
```

### Collect values into a list

```python
df.groupBy("dept").agg(F.collect_list("name").alias("people")).show(truncate=False)
```

### Filtering after aggregation (SQL's HAVING)

```python
(df.groupBy("dept")
   .agg(F.avg("salary").alias("avg_sal"))
   .filter(F.col("avg_sal") > 60000)
   .show())
```

> Remember: `groupBy` causes a **shuffle**.

**Try it:** Find the department with the highest average salary.

---

# 6️⃣ Joins

## Create a second table

```python
depts = spark.createDataFrame(
    [("IT", "Bangalore"), ("HR", "Hyderabad"), ("Finance", "Mumbai"), ("Legal", "Delhi")],
    ["dept", "city"]
)
```

## The join types you need

```python
df.join(depts, on="dept", how="inner").show()   # only matching
df.join(depts, on="dept", how="left").show()    # all from left
df.join(depts, on="dept", how="right").show()   # all from right
df.join(depts, on="dept", how="full").show()    # everything
df.join(depts, on="dept", how="left_anti").show()  # left rows with NO match
df.join(depts, on="dept", how="left_semi").show()  # left rows that HAVE a match (only left columns)
```

### 🍛 Easy memory trick

| Join | Think of it as |
|------|----------------|
| inner | "Only guests who are on **both** lists" |
| left | "All my guests, plus extra info if available" |
| left_anti | "Guests who are on my list but **not** on yours" |
| left_semi | "My guests who also appear on your list (filter only)" |

## Join on different column names

```python
e = df.alias("e")
d = depts.withColumnRenamed("dept", "dept_name").alias("d")
e.join(d, F.col("e.dept") == F.col("d.dept_name"), "left").show()
```

## Self join (employee → manager)

```python
e = df.alias("e")
m = df.alias("m")

(e.join(m, F.col("e.manager_id") == F.col("m.emp_id"), "left")
  .select(F.col("e.name").alias("employee"), F.col("m.name").alias("manager"))
  .show())
```

## ⚠️ Duplicate column trap

If both tables have the same non-key column name, you get ambiguity errors. Fix by using `alias`, renaming, or joining with `on="col_name"` (string), which keeps one copy of the key.

## 🚀 Broadcast join (the biggest join optimization)

If one table is **small** (fits in memory), send a copy to every executor. No shuffle of the big table.

```python
from pyspark.sql.functions import broadcast
big.join(broadcast(small), "dept")
```

Spark auto-broadcasts tables smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10 MB).

**Try it:** Find employees whose department does **not** exist in `depts`. (Hint: `left_anti`.)

---

# 7️⃣ Window Functions (Interview Gold 🏆)

A window function calculates **per group but keeps every row** (unlike `groupBy`, which collapses rows).

```python
from pyspark.sql.window import Window
```

## 1. Rank within a group

```python
w = Window.partitionBy("dept").orderBy(F.col("salary").desc())

df.withColumn("rank", F.rank().over(w)) \
  .withColumn("dense_rank", F.dense_rank().over(w)) \
  .withColumn("row_num", F.row_number().over(w)) \
  .show()
```

### 🏅 rank vs dense_rank vs row_number

Salaries: 80, 80, 70

| Salary | row_number | rank | dense_rank |
|--------|-----------|------|-----------|
| 80 | 1 | 1 | 1 |
| 80 | 2 | 1 | 1 |
| 70 | 3 | **3** (skips 2) | **2** (no gap) |

## 2. Top N per group (very common!)

```python
w = Window.partitionBy("dept").orderBy(F.col("salary").desc())

(df.filter(F.col("salary").isNotNull())
   .withColumn("rn", F.row_number().over(w))
   .filter(F.col("rn") <= 2)
   .drop("rn")
   .show())
```

## 3. Keep the latest record per key (dedupe the smart way)

```python
w = Window.partitionBy("emp_id").orderBy(F.col("join_date").desc())
latest = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
```

## 4. lag / lead (previous / next row)

```python
w = Window.partitionBy("dept").orderBy("join_date")
df.withColumn("prev_salary", F.lag("salary", 1).over(w)) \
  .withColumn("next_salary", F.lead("salary", 1).over(w)) \
  .show()
```

## 5. Running total

```python
w = (Window.partitionBy("dept")
           .orderBy("join_date")
           .rowsBetween(Window.unboundedPreceding, Window.currentRow))

df.withColumn("running_salary", F.sum("salary").over(w)).show()
```

## 6. Compare to the group average without collapsing

```python
w = Window.partitionBy("dept")
df.withColumn("dept_avg", F.avg("salary").over(w)) \
  .withColumn("diff", F.col("salary") - F.col("dept_avg")) \
  .show()
```

**Try it:** Find the **2nd highest salary** in each department.

---

# 8️⃣ Spark SQL and UDFs

## Use SQL when it feels natural

```python
df.createOrReplaceTempView("emp")

spark.sql("""
    SELECT dept, ROUND(AVG(salary), 0) AS avg_sal
    FROM emp
    GROUP BY dept
    HAVING AVG(salary) > 55000
    ORDER BY avg_sal DESC
""").show()
```

> DataFrame API and SQL compile to the **same plan**. Use whichever is clearer.

## UDF (User Defined Function) — use as a last resort

```python
from pyspark.sql.functions import udf

@udf(returnType=StringType())
def grade(s):
    if s is None: return "NA"
    return "A" if s >= 70000 else "B"

df.withColumn("grade", grade("salary")).show()
```

### Why UDFs are slow ⚠️
- Spark can't optimize them (black box).
- Data moves between the JVM and Python for every row.

### ✅ Better: do the same with built-ins

```python
df.withColumn(
    "grade",
    F.when(F.col("salary").isNull(), "NA")
     .when(F.col("salary") >= 70000, "A")
     .otherwise("B")
)
```

> **Golden rule:** *Built-in function > pandas UDF > regular Python UDF.*

---

# 9️⃣ Performance Basics (what interviewers love)

## Narrow vs wide transformations

| Narrow (fast, no shuffle) | Wide (slow, shuffle) |
|---|---|
| `select`, `filter`, `withColumn`, `map` | `groupBy`, `join`, `distinct`, `orderBy`, `repartition` |

## Partitions

```python
df.rdd.getNumPartitions()      # how many partitions now?

df.repartition(8)              # increase/decrease WITH shuffle (even distribution)
df.repartition(8, "dept")      # by column
df.coalesce(2)                 # only DEcrease, NO full shuffle (cheaper)
```

| | `repartition` | `coalesce` |
|---|---|---|
| Can increase partitions | ✅ | ❌ |
| Full shuffle | ✅ | ❌ (mostly merges) |
| Use when | Need even spread, or more partitions | Reduce files before writing |

> Thumb rule: aim for partitions of about **100-200 MB** each.

`spark.sql.shuffle.partitions` (default **200**) controls partitions after shuffles. For small data, lower it (e.g., 8) to speed things up.

## Cache / persist

If you reuse a DataFrame many times, store it in memory.

```python
df.cache()        # or df.persist()
df.count()        # first action fills the cache
# ... use df many times ...
df.unpersist()    # free memory when done
```

> Cache only what you reuse. Caching everything wastes memory.

## Data skew 🐘

One key (e.g., `country = "India"`) has far more rows than others, so one task takes forever.
Fixes: **salting** the key, **broadcast** the small side, or enable **AQE** (`spark.sql.adaptive.enabled=true`, on by default in Spark 3.2+).

## Other quick wins

1. Filter **early**, select only needed columns.
2. Prefer **Parquet** over CSV.
3. Prefer built-in functions over UDFs.
4. Avoid `collect()` on big data (it pulls everything to the driver and can crash it).
5. Use `explain()` to see the plan.

---

# 🎯 Top 25 Interview Questions (with crisp answers)

### Basics

**Q1. What is Spark and why is it faster than Hadoop MapReduce?**
Spark is a distributed data processing engine. It is faster because it keeps intermediate data **in memory** instead of writing to disk after every step, and it optimizes the whole plan (DAG).

**Q2. What is PySpark?**
The Python API for Apache Spark. Your Python code builds a plan; the heavy work runs on the JVM executors.

**Q3. RDD vs DataFrame vs Dataset?**
- **RDD**: low-level, no schema, no optimizer.
- **DataFrame**: table-like with schema, optimized by Catalyst. **Use this.**
- **Dataset**: typed DataFrame, only in Scala/Java (not in Python).

**Q4. What are transformations and actions?**
Transformations build a plan lazily (`filter`, `select`); actions trigger execution (`count`, `show`, `write`).

**Q5. What is lazy evaluation and why is it useful?**
Spark waits until an action to run, so it can see the whole plan and optimize it (e.g., push filters down, skip unused columns).

**Q6. What is a DAG?**
Directed Acyclic Graph: the plan of steps (stages) Spark builds from your transformations.

**Q7. What is a partition?**
A chunk of data processed by one task. More partitions = more parallelism (up to the number of cores).

**Q8. Driver vs Executor?**
Driver: runs your main program, builds the plan, coordinates. Executors: run tasks on data partitions and store cached data.

### Transformations

**Q9. Narrow vs wide transformation?**
Narrow: each output partition depends on one input partition (no shuffle). Wide: data must move between partitions (shuffle).

**Q10. What is a shuffle and why is it expensive?**
Redistributing data across the network between executors. Costs disk I/O, network I/O, and serialization.

**Q11. `repartition` vs `coalesce`?**
`repartition` can increase or decrease with a full shuffle. `coalesce` only decreases and avoids a full shuffle.

**Q12. `cache` vs `persist`?**
`cache()` = `persist()` with the default storage level. `persist` lets you choose levels (memory, disk, both).

**Q13. `groupBy` vs window function?**
`groupBy` collapses rows to one per group. A window function keeps all rows and adds a computed value.

**Q14. `rank` vs `dense_rank` vs `row_number`?**
`row_number`: unique sequence. `rank`: ties share a rank, then gaps. `dense_rank`: ties share a rank, no gaps.

**Q15. How do you find the Nth highest salary per department?**
`dense_rank()` over `Window.partitionBy("dept").orderBy(desc("salary"))`, then filter `rank == N`.

**Q16. How do you remove duplicates but keep the latest?**
`row_number()` over `partitionBy(key).orderBy(desc(timestamp))`, then filter `rn == 1`.

### Joins and performance

**Q17. What is a broadcast join?**
The small table is copied to all executors so the big table need not be shuffled. Use `broadcast(small_df)`.

**Q18. How do you handle data skew?**
Broadcast the small side, salt the skewed key, or use Adaptive Query Execution (AQE).

**Q19. Why avoid Python UDFs?**
They are opaque to the optimizer and need JVM↔Python serialization for each row. Use built-ins or pandas UDFs.

**Q20. Why is `collect()` dangerous?**
It brings all data to the driver's memory. On big data this causes out-of-memory errors. Use `take(n)`, `limit(n)`, or write to storage.

**Q21. Why Parquet over CSV?**
Columnar, compressed, schema included, supports column pruning and predicate pushdown.

**Q22. What is `spark.sql.shuffle.partitions`?**
Number of partitions created after a shuffle (default 200). Tune it based on data size.

**Q23. What is AQE (Adaptive Query Execution)?**
Spark adjusts the plan at runtime: coalesces small shuffle partitions, switches join strategies, and handles skew. Enabled by default in Spark 3.2+.

### Practical

**Q24. How do you handle null values?**
Detect with `isNull()`, drop with `dropna()`, fill with `fillna()` or `coalesce()`, and remember nulls never equal anything.

**Q25. How would you debug a slow Spark job?**
Open the **Spark UI**: check stages with long tasks (skew), large shuffles, spills to disk; then use `explain()`, reduce shuffles, broadcast small tables, filter early, and tune partitions.

---

# 🏋️ Practice Set (with solutions)

Use the `df` (employees) and `depts` tables from above. Make sure you first clean `df` as: `df = df.dropDuplicates()`.

### P1. Show employees earning more than the company average.

```python
avg_sal = df.select(F.avg("salary")).first()[0]
df.filter(F.col("salary") > avg_sal).show()
```

### P2. Department-wise headcount and average salary, highest first.

```python
(df.groupBy("dept")
   .agg(F.count("*").alias("headcount"), F.round(F.avg("salary"), 0).alias("avg_sal"))
   .orderBy(F.col("avg_sal").desc())
   .show())
```

### P3. Top 1 earner per department.

```python
w = Window.partitionBy("dept").orderBy(F.col("salary").desc_nulls_last())
df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").show()
```

### P4. Employees with their manager's name.

```python
e, m = df.alias("e"), df.alias("m")
(e.join(m, F.col("e.manager_id") == F.col("m.emp_id"), "left")
  .select(F.col("e.name").alias("employee"), F.col("m.name").alias("manager"))
  .show())
```

### P5. Departments that have no employees.

```python
depts.join(df, "dept", "left_anti").show()     # → Legal
```

### P6. Replace null salaries with the department average.

```python
w = Window.partitionBy("dept")
df.withColumn("salary", F.coalesce("salary", F.avg("salary").over(w))).show()
```

### P7. Salary difference vs the previous joiner in the same department.

```python
w = Window.partitionBy("dept").orderBy("join_date")
df.withColumn("diff", F.col("salary") - F.lag("salary").over(w)).show()
```

### P8. Word count (classic!).

```python
lines = spark.createDataFrame([("spark is fun",), ("spark is fast",)], ["line"])

(lines
 .select(F.explode(F.split("line", " ")).alias("word"))
 .groupBy("word").count()
 .orderBy(F.col("count").desc())
 .show())
```

---

# 🧾 One-Page Cheat Sheet

```python
# Start
spark = SparkSession.builder.appName("x").getOrCreate()
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read / Write
spark.read.csv(path, header=True, inferSchema=True)
spark.read.parquet(path)
df.write.mode("overwrite").parquet(path)

# Inspect
df.show(5); df.printSchema(); df.count(); df.columns; df.describe().show()

# Columns & rows
df.select("a", F.col("b").alias("c"))
df.filter((F.col("a") > 1) & (F.col("b") == "x"))
df.withColumn("new", F.col("a") * 2)
df.withColumn("flag", F.when(cond, "Y").otherwise("N"))
df.drop("a"); df.withColumnRenamed("a", "b")
df.orderBy(F.col("a").desc())

# Clean
df.dropna(subset=["a"]); df.fillna({"a": 0}); df.dropDuplicates(["id"])

# Aggregate
df.groupBy("k").agg(F.count("*"), F.sum("v"), F.avg("v"))

# Join
df1.join(df2, "key", "left"); df1.join(broadcast(small), "key")

# Window
w = Window.partitionBy("k").orderBy(F.col("v").desc())
F.row_number().over(w); F.rank().over(w); F.lag("v").over(w)

# Performance
df.cache(); df.repartition(8); df.coalesce(2); df.explain()
```

---

# ⚠️ Top 10 Beginner Mistakes

1. Using `and` / `or` instead of `&` / `|` in filters.
2. Forgetting parentheses around each condition.
3. Comparing with `== None` instead of `.isNull()`.
4. Calling `collect()` on large data.
5. Using Python UDFs when a built-in exists.
6. Using `inferSchema` in production.
7. Not importing `functions as F` (and clashing with Python's `sum`/`max`).
8. Caching everything.
9. Ignoring partitions and writing thousands of tiny files.
10. Expecting `show()` to be instant: it **is** an action, so it triggers the full work.

---

# 🚀 Next Steps (after mastering this)

1. Build a mini project: **clean + analyze a Kaggle CSV** (e.g., Netflix, IPL, or NYC Taxi) and write results to Parquet.
2. Learn **Delta Lake** (ACID tables on top of Parquet).
3. Learn **Structured Streaming** basics.
4. Practice on **Databricks Community Edition** (free).
5. Read the **Spark UI** for every job you run.

---

> ⭐ **If this helped you, star the repo and share it with a friend.**
> *Consistency beats intensity. 30 minutes a day for 2 weeks = you are job-ready for PySpark basics.* 💪🔥

*Happy Sparking! ⚡*
