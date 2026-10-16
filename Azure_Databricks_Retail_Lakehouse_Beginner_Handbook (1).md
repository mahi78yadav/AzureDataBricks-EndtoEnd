# Azure Databricks Retail Lakehouse

## Beginner-Friendly Implementation Handbook  
### Source → Bronze → Silver → Gold

---

**Purpose**  
Build a complete medallion architecture (Bronze → Silver → Gold) from the four supplied CSV files using **Unity Catalog**, **ADLS Gen2**, **Delta Lake**, and **Auto Loader**.

This guide explains every object, shows the exact SQL/Python commands used to inspect it, and provides a single clear implementation path.

**Environment used in this guide**

| Item                    | Value                                      |
|-------------------------|--------------------------------------------|
| Storage Account         | `stdatabricksretailsit`                    |
| Catalog                 | `retailsit`                                |
| External Locations      | `ext_retailsit_bronze`, `ext_retailsit_silver`, `ext_retailsit_gold` |
| Containers              | `source`, `bronze`, `silver`, `gold`       |
| Tables                  | `customers`, `products`, `orders`, `stores`|

> Always verify these names in **your** workspace before running any command.  
> External location names and ADLS paths are separate objects.

---

## 1. The Whole Project in One Picture

```text
ADLS Gen2 source container (CSV files)
   │
   │  Auto Loader discovers new files
   │  and tracks progress in a checkpoint
   ▼
Bronze Delta tables (raw, append-only ingestion)
   │
   │  PySpark cleaning, validation,
   │  deduplication & type conversion
   ▼
Silver Delta tables (clean, consistent records)
   │
   │  Joins, calculations & aggregations
   ▼
Gold Delta tables (business-ready reporting)
```

### Explain it like you’re teaching a high-school student

| Concept              | Simple Explanation |
|----------------------|--------------------|
| Source CSV           | Original Excel sheets delivered to a shop |
| External Location    | Unity Catalog security object that grants Databricks access to a specific cloud path using a storage credential |
| Catalog              | Top-level library |
| Schema               | Shelf / folder inside the library |
| Table                | Named dataset you can query with SQL |
| Delta Table          | Data files + transaction log (`_delta_log`) that makes every change reliable |
| Bronze               | Keep the incoming data with almost no changes |
| Silver               | Clean and validate the data |
| Gold                 | Organize the data so the business can answer questions easily |
| Auto Loader          | Watches a cloud folder for new files and remembers what it has already processed |

---

## 2. External Location vs Schema vs Table

These are **different objects**. Creating one does **not** automatically create the others.

| Object                | Example                              | What it does |
|-----------------------|--------------------------------------|--------------|
| Storage Account       | `stdatabricksretailsit`              | Azure storage resource |
| Container             | `source`, `bronze`, `silver`, `gold` | Top-level ADLS Gen2 container |
| External Location     | `ext_retailsit_bronze`               | Unity Catalog object that combines a cloud path + storage credential |
| Catalog               | `retailsit`                          | Top-level namespace for governed data objects |
| Schema                | `retailsit.bronze`                   | Namespace for tables in the Bronze layer |
| Table                 | `retailsit.bronze.customers`         | Named table registered in Unity Catalog |
| Table Storage Path    | Folder under the Bronze container    | Physical Delta data files + `_delta_log` |

### Crucial Rules

1. An **External Location** only grants governed access to a path. It is **not** a table and does not register files.
2. `CREATE SCHEMA ... MANAGED LOCATION 'abfss://...'` sets the default storage root for **managed tables** in that schema. It uses the path, **not** the external-location name.
3. A managed table created with `saveAsTable("retailsit.bronze.customers")` is stored by Unity Catalog under the schema’s managed location.
4. An external table created with `CREATE TABLE ... LOCATION 'abfss://...'` points to an **existing** Delta folder.
5. **Never** register a raw CSV folder as `USING DELTA`. First read the CSV and write it as Delta (or let Auto Loader do it).

**Recommended approach for this project**  
Use external locations for governed access → set schema managed locations → write **managed** Delta tables into those schemas.

---

## 3. Inspect Unity Catalog Before Changing Anything

Run these commands one at a time in the **Databricks SQL Editor**.

### 3.1 Current Context

```sql
SELECT current_catalog(), current_schema();
```

### 3.2 List Catalogs, Schemas and Tables

```sql
SHOW CATALOGS;
SHOW SCHEMAS IN retailsit;
SHOW TABLES IN retailsit.bronze;
SHOW TABLES IN retailsit.silver;
SHOW TABLES IN retailsit.gold;
```

### 3.3 List External Locations

```sql
SHOW EXTERNAL LOCATIONS;

DESCRIBE EXTERNAL LOCATION ext_retailsit_bronze;
DESCRIBE EXTERNAL LOCATION ext_retailsit_silver;
DESCRIBE EXTERNAL LOCATION ext_retailsit_gold;
```

Check grants if needed:

```sql
SHOW GRANTS ON EXTERNAL LOCATION ext_retailsit_bronze;
SHOW GRANTS ON EXTERNAL LOCATION ext_retailsit_silver;
SHOW GRANTS ON EXTERNAL LOCATION ext_retailsit_gold;
```

Inspect storage credentials:

```sql
SHOW STORAGE CREDENTIALS;
DESCRIBE STORAGE CREDENTIAL cred_adls_retail;   -- replace with the real name in your workspace
```

### 3.4 Expected ADLS Paths

```text
Source:  abfss://source@stdatabricksretailsit.dfs.core.windows.net/
Bronze:  abfss://bronze@stdatabricksretailsit.dfs.core.windows.net/
Silver:  abfss://silver@stdatabricksretailsit.dfs.core.windows.net/
Gold:    abfss://gold@stdatabricksretailsit.dfs.core.windows.net/
```

Always verify the exact URL with `DESCRIBE EXTERNAL LOCATION`.

---

## 4. Create Catalog and Schemas

```sql
CREATE CATALOG IF NOT EXISTS retailsit;
```

Create the three schemas with managed locations (only if they do not already exist):

```sql
CREATE SCHEMA IF NOT EXISTS retailsit.bronze
MANAGED LOCATION 'abfss://bronze@stdatabricksretailsit.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS retailsit.silver
MANAGED LOCATION 'abfss://silver@stdatabricksretailsit.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS retailsit.gold
MANAGED LOCATION 'abfss://gold@stdatabricksretailsit.dfs.core.windows.net/';
```

**Important**  
`CREATE SCHEMA IF NOT EXISTS ... MANAGED LOCATION` does **not** change the location of an existing schema. Always inspect first:

```sql
DESCRIBE SCHEMA EXTENDED retailsit.bronze;
DESCRIBE SCHEMA EXTENDED retailsit.silver;
DESCRIBE SCHEMA EXTENDED retailsit.gold;
```

Look for `Location` / `RootLocation`. If a schema already has the wrong location, do **not** drop it blindly.

---

## 5. Source Files Overview

| File            | Rows | Columns |
|-----------------|------|---------|
| `customers.csv` | 10   | `CustomerID`, `CustomerName`, `Email`, `City`, `State`, `ModifiedDate` |
| `products.csv`  | 10   | `ProductID`, `ProductName`, `Category`, `UnitPrice`, `StockQuantity`, `ModifiedDate` |
| `orders.csv`    | 25   | `OrderID`, `CustomerID`, `ProductID`, `Quantity`, `TotalAmount`, `OrderDate`, `ModifiedDate` |
| `stores.csv`    | 7    | `StoreID`, `StoreName`, `City`, `State`, `ModifiedDate` |

### Recommended Folder Structure (required for Auto Loader)

```text
source/
├── customers/
│   └── customers.csv
├── products/
│   └── products.csv
├── orders/
│   └── orders.csv
└── stores/
    └── stores.csv
```

For incremental testing, simply drop a new file into the matching folder, e.g.:

```text
source/orders/orders_2026-10-12.csv
```

---

## 6. Verify Source Paths (Python Notebook)

```python
storage_account = "stdatabricksretailsit"
source_root = f"abfss://source@{storage_account}.dfs.core.windows.net/"

display(dbutils.fs.ls(source_root))

for entity in ["customers", "products", "orders", "stores"]:
    path = f"{source_root}{entity}/"
    print(f"\n--- {entity} ---")
    display(dbutils.fs.ls(path))
```

---

## 7. Bronze Layer – Auto Loader Ingestion

### What Auto Loader Does

- Discovers new files in cloud storage
- Tracks progress in a **checkpoint**
- Does **not** clean data, deduplicate business keys, or create Silver/Gold tables

### 7.1 Ingest All Four Entities

```python
from pyspark.sql import functions as F

storage_account = "stdatabricksretailsit"
source_root = f"abfss://source@{storage_account}.dfs.core.windows.net/"
bronze_root = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"

def ingest_csv_to_bronze(entity: str):
    source_path     = f"{source_root}{entity}/"
    schema_path     = f"{bronze_root}_autoloader/schema/{entity}/"
    checkpoint_path = f"{bronze_root}_autoloader/checkpoint/{entity}/"

    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("header", "true")
        .option("inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )

    query = (
        stream_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(f"retailsit.bronze.{entity}")
    )

    query.awaitTermination()
    print(f"Finished available files for {entity}")

for entity in ["customers", "products", "orders", "stores"]:
    ingest_csv_to_bronze(entity)
```

### 7.2 Verify Bronze Tables

```sql
SHOW TABLES IN retailsit.bronze;

DESCRIBE TABLE EXTENDED retailsit.bronze.customers;
DESCRIBE TABLE EXTENDED retailsit.bronze.products;
DESCRIBE TABLE EXTENDED retailsit.bronze.orders;
DESCRIBE TABLE EXTENDED retailsit.bronze.stores;

SELECT COUNT(*) FROM retailsit.bronze.customers;   -- expect 10
SELECT COUNT(*) FROM retailsit.bronze.products;    -- expect 10
SELECT COUNT(*) FROM retailsit.bronze.orders;      -- expect 25
SELECT COUNT(*) FROM retailsit.bronze.stores;      -- expect 7
```

### 7.3 Test Incremental Load

1. Leave the existing checkpoint paths unchanged.
2. Add a new file (same schema) to the correct folder, e.g. `source/orders/orders_2026-10-12.csv`.
3. Re-run the same Auto Loader notebook.
4. Verify:

```sql
SELECT COUNT(*) FROM retailsit.bronze.orders;

SELECT _source_file, COUNT(*) AS rows_loaded
FROM retailsit.bronze.orders
GROUP BY _source_file
ORDER BY _source_file;
```

> Auto Loader tracks **files**, not business-key changes.  
> If the same customer appears in a later file, Bronze will contain multiple versions. Silver must keep only the latest version.

---

## 8. Silver Layer – Clean & Deduplicate

Silver always reads from **Bronze Delta tables**.

### 8.1 Customers

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

customers_bronze = spark.table("retailsit.bronze.customers")

w = Window.partitionBy("CustomerID").orderBy(F.col("ModifiedDate").desc_nulls_last())

customers_silver = (
    customers_bronze
    .withColumn("CustomerName", F.trim(F.col("CustomerName")))
    .withColumn("Email", F.lower(F.trim(F.col("Email"))))
    .withColumn("City", F.initcap(F.trim(F.col("City"))))
    .withColumn("State", F.upper(F.trim(F.col("State"))))
    .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    .filter(F.col("CustomerID").isNotNull())
    .withColumn("_row_num", F.row_number().over(w))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

(
    customers_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retailsit.silver.customers")
)
```

### 8.2 Products

```python
products_bronze = spark.table("retailsit.bronze.products")

w = Window.partitionBy("ProductID").orderBy(F.col("ModifiedDate").desc_nulls_last())

products_silver = (
    products_bronze
    .withColumn("ProductName", F.trim(F.col("ProductName")))
    .withColumn("Category", F.initcap(F.trim(F.col("Category"))))
    .withColumn("UnitPrice", F.col("UnitPrice").cast("decimal(12,2)"))
    .withColumn("StockQuantity", F.col("StockQuantity").cast("int"))
    .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    .filter(F.col("ProductID").isNotNull())
    .filter(F.col("UnitPrice").isNotNull() & (F.col("UnitPrice") >= 0))
    .withColumn("_row_num", F.row_number().over(w))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

(
    products_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retailsit.silver.products")
)
```

### 8.3 Orders

```python
orders_bronze = spark.table("retailsit.bronze.orders")

w = Window.partitionBy("OrderID").orderBy(F.col("ModifiedDate").desc_nulls_last())

orders_silver = (
    orders_bronze
    .withColumn("Quantity", F.col("Quantity").cast("int"))
    .withColumn("TotalAmount", F.col("TotalAmount").cast("decimal(14,2)"))
    .withColumn("OrderDate", F.to_timestamp("OrderDate"))
    .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    .filter(F.col("OrderID").isNotNull())
    .filter(F.col("Quantity").isNotNull() & (F.col("Quantity") > 0))
    .filter(F.col("TotalAmount").isNotNull() & (F.col("TotalAmount") >= 0))
    .withColumn("_row_num", F.row_number().over(w))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

(
    orders_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retailsit.silver.orders")
)
```

### 8.4 Stores

```python
stores_bronze = spark.table("retailsit.bronze.stores")

w = Window.partitionBy("StoreID").orderBy(F.col("ModifiedDate").desc_nulls_last())

stores_silver = (
    stores_bronze
    .withColumn("StoreName", F.trim(F.col("StoreName")))
    .withColumn("City", F.initcap(F.trim(F.col("City"))))
    .withColumn("State", F.upper(F.trim(F.col("State"))))
    .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
    .filter(F.col("StoreID").isNotNull())
    .withColumn("_row_num", F.row_number().over(w))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

(
    stores_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retailsit.silver.stores")
)
```

> Why a window instead of `dropDuplicates`?  
> `dropDuplicates` does not guarantee which version is kept. Ordering by `ModifiedDate` keeps the **latest** record for each business key.

---

## 9. Gold Layer – Business-Ready Tables

### 9.1 Sales Detail (Fact)

```python
from pyspark.sql import functions as F

orders    = spark.table("retailsit.silver.orders").alias("o")
customers = spark.table("retailsit.silver.customers").alias("c")
products  = spark.table("retailsit.silver.products").alias("p")

gold_sales = (
    orders
    .join(customers, F.col("o.CustomerID") == F.col("c.CustomerID"), "left")
    .join(products,  F.col("o.ProductID")  == F.col("p.ProductID"),  "left")
    .select(
        F.col("o.OrderID"),
        F.col("o.OrderDate"),
        F.col("o.CustomerID"),
        F.col("c.CustomerName"),
        F.col("o.ProductID"),
        F.col("p.ProductName"),
        F.col("p.Category"),
        F.col("o.Quantity"),
        F.col("o.TotalAmount")
    )
)

(
    gold_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retailsit.gold.gold_sales")
)
```

### 9.2 Daily Sales Summary

```python
gold_daily_sales = (
    spark.table("retailsit.gold.gold_sales")
    .withColumn("OrderDate", F.to_date("OrderDate"))
    .groupBy("OrderDate")
    .agg(
        F.countDistinct("OrderID").alias("OrderCount"),
        F.sum("Quantity").alias("UnitsSold"),
        F.sum("TotalAmount").alias("SalesAmount")
    )
    .orderBy("OrderDate")
)

(
    gold_daily_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retailsit.gold.gold_daily_sales")
)

display(spark.table("retailsit.gold.gold_daily_sales"))
```

### 9.3 Category Sales Summary

```python
gold_category_sales = (
    spark.table("retailsit.gold.gold_sales")
    .groupBy("Category")
    .agg(
        F.countDistinct("OrderID").alias("OrderCount"),
        F.sum("Quantity").alias("UnitsSold"),
        F.sum("TotalAmount").alias("SalesAmount")
    )
)

(
    gold_category_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("retailsit.gold.gold_category_sales")
)
```

---

## 10. Validation SQL Commands

### 10.1 Schema Locations

```sql
DESCRIBE SCHEMA EXTENDED retailsit.bronze;
DESCRIBE SCHEMA EXTENDED retailsit.silver;
DESCRIBE SCHEMA EXTENDED retailsit.gold;
```

### 10.2 Table Definitions

```sql
DESCRIBE TABLE EXTENDED retailsit.bronze.customers;
DESCRIBE TABLE EXTENDED retailsit.silver.customers;
DESCRIBE TABLE EXTENDED retailsit.gold.gold_sales;

DESCRIBE DETAIL retailsit.bronze.customers;
DESCRIBE DETAIL retailsit.silver.customers;
DESCRIBE DETAIL retailsit.gold.gold_sales;
```

### 10.3 Row Counts

```sql
-- Bronze
SELECT COUNT(*) AS customer_count FROM retailsit.bronze.customers;
SELECT COUNT(*) AS product_count  FROM retailsit.bronze.products;
SELECT COUNT(*) AS order_count    FROM retailsit.bronze.orders;
SELECT COUNT(*) AS store_count    FROM retailsit.bronze.stores;

-- Silver
SELECT COUNT(*) AS customer_count FROM retailsit.silver.customers;
SELECT COUNT(*) AS product_count  FROM retailsit.silver.products;
SELECT COUNT(*) AS order_count    FROM retailsit.silver.orders;
SELECT COUNT(*) AS store_count    FROM retailsit.silver.stores;

-- Gold
SELECT COUNT(*) AS sales_rows FROM retailsit.gold.gold_sales;
```

### 10.4 Sample Gold Data

```sql
SELECT * FROM retailsit.gold.gold_sales LIMIT 10;
SELECT * FROM retailsit.gold.gold_daily_sales ORDER BY OrderDate;
SELECT * FROM retailsit.gold.gold_category_sales ORDER BY SalesAmount DESC;
```

### 10.5 Delta History

```sql
DESCRIBE HISTORY retailsit.bronze.customers;
DESCRIBE HISTORY retailsit.silver.customers;
DESCRIBE HISTORY retailsit.gold.gold_sales;
```

### 10.6 Grants

```sql
SHOW GRANTS ON SCHEMA retailsit.bronze;
SHOW GRANTS ON TABLE retailsit.bronze.customers;
SHOW GRANTS ON EXTERNAL LOCATION ext_retailsit_bronze;
```

---

## 11. Common Errors & How to Fix Them

| Error | Meaning | What to do |
|-------|---------|------------|
| `TABLE_OR_VIEW_NOT_FOUND` | Wrong name or table not yet created | Run `SHOW CATALOGS / SCHEMAS / TABLES` |
| `DELTA_READ_TABLE_WITHOUT_COLUMNS` | Table points to invalid / empty Delta location | Test the path directly with `spark.read.format("delta").load(path)` |
| `UNRESOLVED_COLUMN` | Column name does not exist | Run `printSchema()` on the table |
| Access denied | Missing external location grant or Azure RBAC | Check external location + storage credential |
| Auto Loader loads nothing | No new file, wrong path, or checkpoint issue | Confirm new file exists and checkpoint path is unchanged |

---

## 12. Final Validation Checklist

- [ ] External locations exist and point to the correct containers  
- [ ] Catalog `retailsit` and schemas `bronze`, `silver`, `gold` exist  
- [ ] Source files are organized in entity-specific folders  
- [ ] Auto Loader successfully wrote the four Bronze tables  
- [ ] Bronze counts match the source files (10 / 10 / 25 / 7)  
- [ ] Incremental file test works (new rows appear, checkpoint stays intact)  
- [ ] Silver tables contain only the latest version of each business key  
- [ ] Gold sales detail + daily + category tables query correctly  
- [ ] Table locations verified with `DESCRIBE TABLE EXTENDED` / `DESCRIBE DETAIL`  
- [ ] No source files or storage folders were deleted during troubleshooting  

---

## 13. Recommended Teaching Sequence (8 Days)

| Day | Focus |
|-----|-------|
| 1   | Catalog, Schema, Table, External Location, Storage Path |
| 2   | Read a CSV in PySpark and inspect its schema |
| 3   | Write a CSV as a Bronze Delta table |
| 4   | Auto Loader + checkpoints (add a new file) |
| 5   | Clean Customers & Products → Silver |
| 6   | Clean Orders & Stores → Silver |
| 7   | Join Silver tables → Gold sales summaries |
| 8   | Full validation (counts, locations, history, grants) |

---

**Core idea to remember**

> External Location → governed access to cloud storage  
> Schema → organizes tables  
> Delta Table → reliable structured data  
> Auto Loader → incremental file discovery  
> Silver → clean & deduplicated records  
> Gold → business-ready answers

---

*End of handbook*  
*Azure Databricks Retail Lakehouse – Beginner Implementation Guide*