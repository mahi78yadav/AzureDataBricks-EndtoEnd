#  PySpark Essentials   
### The Sweet 80/20 Guide (Most Used Concepts + Perfect Explanations)

> **Goal**: Learn the 20% of PySpark that you will use 80% of the time.  


---

## 1. What is PySpark? (And Why Should You Care?)

**PySpark** is the Python API for **Apache Spark**.

- Spark is a distributed computing engine. It can process huge amounts of data across many computers.
- PySpark lets you write Spark code using Python (the language you already love).

### When to use PySpark instead of Pandas?
| Situation                    | Use Pandas | Use PySpark |
|-----------------------------|------------|-------------|
| Data fits in your laptop RAM | ✅         | ❌          |
| Data is too big for one machine | ❌       | ✅          |
| Need parallel processing    | ❌         | ✅          |
| Working with big data pipelines | ❌      | ✅          |

**Simple rule**: If your data is small → Pandas. If data is big → PySpark.

---

## 2. The Starting Point: SparkSession

Every PySpark program starts with a **SparkSession**.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstPySparkApp") \
    .master("local[*]") \
    .getOrCreate()
```

### Explanation:
- `SparkSession` is the entry point to all Spark functionality.
- It replaces the old `SparkContext` (you still use it behind the scenes).
- Always create it once at the beginning of your program.

---

## 3. RDD vs DataFrame vs Dataset (Very Important!)

| Feature              | RDD                          | DataFrame                          | Dataset (Scala/Java only) |
|----------------------|------------------------------|------------------------------------|---------------------------|
| Structure            | No schema (just objects)     | Has schema (columns + types)       | Typed DataFrame           |
| Optimization         | Almost none                  | Catalyst Optimizer + Tungsten      | Same as DataFrame         |
| Ease of use          | Hard (low-level)             | Easy (SQL-like)                    | Easy                      |
| Performance          | Slower                       | Much faster                        | Fast                      |
| Recommended?         | Rarely                       | **Yes (use this 95% of time)**     | Yes in Scala              |

**Golden Rule for students**:  
Always prefer **DataFrame**. Use RDD only when you have no other choice.

---

## 4. Lazy Evaluation, Transformations & Actions

This is the most important concept in Spark.

### Transformations (Lazy)
They **do not execute** immediately. They just build a plan.

Common ones:
- `select()`, `filter()`, `withColumn()`, `groupBy()`, `join()`, `orderBy()`

### Actions (Eager)
They **trigger** the execution.

Common ones:
- `show()`, `count()`, `collect()`, `take()`, `write()`, `first()`

```python
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

# This does NOTHING yet (transformation)
filtered = df.filter(df.id > 1)

# This actually runs the job (action)
filtered.show()
```

**Why lazy?** Spark can optimize the whole plan before running it (using Catalyst Optimizer).

---

## 5. Creating DataFrames (Most Common Ways)

### Method 1: From a Python list
```python
data = [
    (1, "Alice", 25, "Engineering"),
    (2, "Bob", 30, "Sales"),
    (3, "Cathy", 28, "Engineering"),
    (4, "David", 35, "HR")
]

columns = ["id", "name", "age", "department"]

df = spark.createDataFrame(data, columns)
df.show()
```

### Method 2: From CSV (Very common)
```python
df = spark.read.csv("employees.csv", header=True, inferSchema=True)
```

### Method 3: From Parquet (Best format for big data)
```python
df = spark.read.parquet("data/employees.parquet")
```

---

## 6. The Most Used DataFrame Operations (80% of daily work)

### 6.1 Select columns
```python
df.select("name", "age").show()
df.select(df.name, df.age + 1).show()   # You can do calculations
```

### 6.2 Filter rows
```python
from pyspark.sql.functions import col

df.filter(col("age") > 28).show()
df.where(df.department == "Engineering").show()   # where is alias of filter
```

### 6.3 Add or modify a column (`withColumn`)
```python
from pyspark.sql.functions import col, lit, upper

df = df.withColumn("age_plus_5", col("age") + 5)
df = df.withColumn("country", lit("India"))
df = df.withColumn("name_upper", upper(col("name")))
```

### 6.4 GroupBy + Aggregation (Extremely Important)
```python
from pyspark.sql.functions import avg, sum, count, max, min

df.groupBy("department") \
  .agg(
      avg("age").alias("avg_age"),
      count("*").alias("num_employees"),
      max("age").alias("max_age")
  ).show()
```

### 6.5 Sorting
```python
df.orderBy(col("age").desc()).show()
df.sort(col("department"), col("age").desc()).show()
```

### 6.6 Drop columns / Rename
```python
df = df.drop("age_plus_5")
df = df.withColumnRenamed("name", "employee_name")
```

---

## 7. Joins (One of the most asked topics)

```python
employees = spark.createDataFrame([
    (1, "Alice", 101),
    (2, "Bob", 102),
    (3, "Cathy", 101)
], ["emp_id", "name", "dept_id"])

departments = spark.createDataFrame([
    (101, "Engineering"),
    (102, "Sales"),
    (103, "HR")
], ["dept_id", "dept_name"])

# Inner Join (default)
employees.join(departments, "dept_id").show()

# Left Join
employees.join(departments, "dept_id", "left").show()

# Right Join
employees.join(departments, "dept_id", "right").show()

# Full Outer Join
employees.join(departments, "dept_id", "outer").show()

# Left Semi (only matching rows from left, no columns from right)
employees.join(departments, "dept_id", "left_semi").show()

# Left Anti (rows in left that have NO match in right)
employees.join(departments, "dept_id", "left_anti").show()
```

**Tip**: When one table is small, use **broadcast join** for huge performance boost:
```python
from pyspark.sql.functions import broadcast

employees.join(broadcast(departments), "dept_id").show()
```

---

## 8. Window Functions (Very Powerful & Frequently Asked)

Window functions let you do calculations across a group of rows **without collapsing** the data (unlike groupBy).

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum as _sum

window_spec = Window.partitionBy("department").orderBy(col("age").desc())

df.withColumn("row_num", row_number().over(window_spec)) \
  .withColumn("rank", rank().over(window_spec)) \
  .withColumn("dense_rank", dense_rank().over(window_spec)) \
  .show()
```

### Common Use Cases:
- Rank employees within each department by salary
- Get previous/next value (`lag` / `lead`)
- Running total / cumulative sum
- Deduplication (keep only the latest record)

**Best practice for deduplication**:
```python
window = Window.partitionBy("emp_id").orderBy(col("updated_at").desc())

df.withColumn("rn", row_number().over(window)) \
  .filter(col("rn") == 1) \
  .drop("rn")
```

---

## 9. Reading & Writing Data (Daily Life Operations)

### Reading
```python
# CSV
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)

# Parquet (recommended for production)
df = spark.read.parquet("path/to/folder/")

# JSON
df = spark.read.json("path/to/file.json")
```

### Writing
```python
df.write.mode("overwrite").parquet("output/path/")
df.write.mode("overwrite").csv("output/csv/", header=True)
df.write.mode("overwrite").saveAsTable("my_table")   # Hive table
```

**Modes**:
- `overwrite` → replace existing
- `append` → add to existing
- `ignore` → do nothing if exists
- `error` → throw error if exists (default)

---

## 10. Caching & Persistence (Performance Basics)

If you use the same DataFrame multiple times, **cache** it.

```python
df.cache()          # or df.persist()
df.count()          # triggers caching
# now later actions will be much faster
```

Unpersist when done:
```python
df.unpersist()
```

---

## 11. Repartition vs Coalesce

| Method          | What it does                          | When to use                     |
|-----------------|---------------------------------------|---------------------------------|
| `repartition(n)`| Increases or decreases partitions (full shuffle) | When you need more parallelism |
| `coalesce(n)`   | Only decreases partitions (no full shuffle) | When reducing partitions before writing |

```python
df.repartition(100)     # expensive but balanced
df.coalesce(10)         # cheap way to reduce files
```

**Pro Tip**: Before writing, reduce number of partitions to avoid creating thousands of tiny files.

---

## 12. Handling Null Values (Very Common)

```python
from pyspark.sql.functions import col, when, isnull, coalesce

# Drop rows with any null
df.na.drop()

# Fill nulls
df.na.fill({"age": 0, "name": "Unknown"})

# Replace null with another column
df.withColumn("final_age", coalesce(col("age"), col("default_age")))
```

---

## 13. Spark SQL (Write SQL instead of DataFrame API)

```python
df.createOrReplaceTempView("employees")

spark.sql("""
    SELECT department, AVG(age) as avg_age
    FROM employees
    GROUP BY department
    ORDER BY avg_age DESC
""").show()
```

You can mix DataFrame API and SQL freely.

---

## 14. Quick Performance Tips (The Sweet 20%)

1. **Prefer DataFrame / Spark SQL** over RDD.
2. **Avoid `collect()`** on large data (brings everything to driver → OOM).
3. Use **broadcast joins** when one side is small.
4. **Cache** DataFrames that are used multiple times.
5. Prefer **Parquet** over CSV.
6. Avoid UDFs if possible (they are slow). Prefer built-in functions.
7. Use `explain()` to understand the physical plan:
   ```python
   df.groupBy("dept").count().explain(True)
   ```
8. Watch out for **data skew** (one key has too many records).

---

## 15. Must-Know Interview Questions (With Perfect Answers)

### Q1. What is the difference between Transformation and Action?
**Answer**: Transformations are lazy and build a plan. Actions trigger the actual computation and return results.

### Q2. What is Lazy Evaluation?
**Answer**: Spark does not execute transformations immediately. It waits until an action is called so it can optimize the entire plan.

### Q3. Narrow vs Wide Transformations?
- **Narrow**: No shuffle (filter, select, map) — data stays in same partition.
- **Wide**: Requires shuffle (groupBy, join, repartition) — data moves across network.

### Q4. Why is DataFrame faster than RDD?
**Answer**: DataFrame has schema → Catalyst Optimizer can rewrite the plan + Tungsten generates efficient code.

### Q5. Difference between `cache()` and `persist()`?
**Answer**: `cache()` is `persist()` with default storage level (MEMORY_AND_DISK). `persist()` lets you choose storage level.

### Q6. How do you handle data skew?
**Answer**: Salting, broadcast joins, or increasing parallelism / AQE (Adaptive Query Execution).

### Q7. What does `collect()` do? Is it safe?
**Answer**: Brings all data to the driver. Dangerous on large datasets. Prefer `take()`, `show()`, or writing to disk.

---

## 16. Mini Practice Project (Do This!)

Create a small employee dataset and practice:

1. Create DataFrame
2. Filter employees older than 28
3. Add a new column `salary_band`
4. Find average age per department
5. Rank employees by age within department
6. Join with a department master table
7. Write the result as Parquet

```python
# Your practice playground starts here
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Practice").getOrCreate()

# Create your data...
# Write your code...
```

---

## Final Words for Students

You don’t need to memorize 100 functions.  
Master these core ideas:

- SparkSession
- DataFrame over RDD
- Lazy Evaluation
- select / filter / withColumn / groupBy / join
- Window functions
- Basic performance (cache, broadcast, coalesce)

Practice these again and again.  
Once these become muscle memory, advanced topics will feel easy.

---

