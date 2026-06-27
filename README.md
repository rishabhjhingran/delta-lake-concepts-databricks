# 🔷 Delta Lake Concepts — Deep Dive on Databricks

[![Databricks](https://img.shields.io/badge/Databricks-Community_Edition-FF3621?style=flat&logo=databricks)](https://community.cloud.databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Full_Concept_Coverage-003366?style=flat)](https://delta.io)
[![PySpark](https://img.shields.io/badge/PySpark_%26_SQL-Both-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)

A comprehensive hands-on reference covering **every core Delta Lake concept** — built and verified on Databricks Community Edition with real NYC Taxi data and custom datasets.

---

## 📁 Repository Structure

```
delta-lake-concepts-databricks/
│
├── DeltaLakeBasics/
│   └── Demo1.sql                    # Tables, volumes, delta log exploration
│
├── ACID Properties/
│   ├── ACID Properties.py           # Atomicity, Consistency, Isolation, Durability
│   └── ISOLATION.py                 # Concurrent write isolation demo
│
├── Upsert Operations/
│   └── Upsert Operations.py         # 3 MERGE INTO use cases (update/insert/delete)
│
├── CHANGE DATA FEED/
│   └── ChangeDataFeed.py            # CDF with PySpark streaming integration
│
├── CLONING/
│   ├── DEEP CLONING/
│   │   └── DEEP CLONING.py          # Full copy with independent history
│   └── SHALLOW CLONING/
│       └── CloningConcept_ShallowCloningDemo.py
│
├── DeletionVector/
│   └── DeletionVectors.sql          # Row-level deletes without file rewrite
│
├── Delta Lake Uniform/
│   └── Delta Lake Uniform.py        # UniForm: Iceberg/Hudi compatibility layer
│
├── LIQUID CLUSTERING/
│   └── LiquidClustering.py          # NYC Taxi: clustering vs partitioning vs Z-order
│
├── PARTITIONING/
│   └── Partitioning.py              # Country-partitioned orders, skewness demo
│
├── SCHEMA EVOLUTION AND ENFORCEMENT/
│   └── SchemaEvolutionEnforcement.py # mergeSchema, MERGE WITH SCHEMA EVOLUTION, rename/drop
│
├── Small File Problem(OPTIMIZE)/
│   └── Optimize(Small File Problem).py
│
├── Upsert Operations/
│   └── Upsert Operations.py
│
└── ZOrdering/
    └── ZOrdering.py                 # NYC Taxi: Z-order on trip_distance, file scan reduction
```

---

## 🔑 Concepts Covered

### 1. MERGE INTO — 3 Use Cases (`Upsert Operations.py`)

**Use Case 1: Update + Insert**
```sql
MERGE INTO delta.`...` AS target
USING incoming_batch AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Use Case 2: Update + Insert + Conditional Delete**
```sql
MERGE INTO delta.`...` AS target
USING incoming_flag AS source
ON target.product_id = source.product_id
WHEN MATCHED AND source.is_deleted = true THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Use Case 3: Source-Priority Sync (deletes anything not in source)**
```sql
MERGE INTO delta.`...` AS target
USING snapshot_data AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE  ← source wins
```

> Every MERGE INTO creates a **deletion vector** (marks rows as deleted without rewriting the Parquet file) and then runs an auto-OPTIMIZE to create a clean merged file.

---

### 2. Change Data Feed (`ChangeDataFeed.py`)

Tracks row-level changes between versions with **pre-image and post-image** for every UPDATE:

```python
# Enable CDF
ALTER TABLE orders_managed_cdf SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# SQL: view changes from version 2 onwards
SELECT * FROM table_changes('orders_managed_cdf', 2)
-- Returns: _change_type (insert/update_preimage/update_postimage/delete)

# PySpark: stream changes to a downstream table
spark.readStream.format('delta')
    .option('readChangeFeed', 'true')
    .option('startingVersion', '2')
    .table('orders_managed_cdf')
    .filter(col('_change_type') == 'delete')
    .writeStream
    .option('checkpointLocation', '/Volumes/.../checkpoints/')
    .trigger(availableNow=True)
    .table('orders_deleted_stream')
```

**Key insight demonstrated:** CDF only tracks changes **after** it's enabled. Version 0 data is not tracked — you'll get `[DELTA_MISSING_CHANGE_DATA]` if you query before the enable version.

---

### 3. Liquid Clustering vs Partitioning vs Z-Ordering (`LiquidClustering.py`, `Partitioning.py`, `ZOrdering.py`)

All three tested on **real NYC Taxi data** from `dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`:

| Approach | Setup | Query Performance | Drawbacks |
|---|---|---|---|
| **Partitioning** | `partitionBy('vendor_id')` | Fast for `WHERE vendor_id = X` | Small file problem, skew, can't change column |
| **Z-Ordering** | `OPTIMIZE ... ZORDER BY (trip_distance)` | Good for range filters, still per-partition | Can't cross partition boundaries, overlapping ranges |
| **Liquid Clustering** | `.clusterBy(['vendor_id', 'trip_distance'])` | Best file co-location, no partition boundaries | Requires OPTIMIZE to take effect |
| **Auto Clustering** | `CLUSTER BY AUTO` | Databricks chooses column | Needs OPTIMIZE to see the chosen columns |

**Z-order result demonstrated:**
- Before: reading 89 files for `WHERE trip_distance > 200`
- After Z-order on `trip_distance`: reading **2 files** for same query

**Liquid Clustering result:**
- Less range overlap than Z-order across partition boundaries
- Incremental clustering — only new data gets re-clustered, no full rewrite

---

### 4. Schema Evolution & Enforcement (`SchemaEvolutionEnforcement.py`)

**Schema Enforcement — what fails:**
```sql
-- Fails: STRING 'two' cannot be cast to INT product_qty
INSERT INTO ... VALUES (6, 'sku-5', 'Bad Row', 'Mis', 'two', 99.00)

-- Succeeds: Databricks auto-casts '2' (STRING) to 2 (INT)
INSERT INTO ... VALUES (6, 'sku-5', 'Bad Row', 'Mis', '2', 99.00)
```

**Schema Evolution — adding columns:**
```sql
-- SQL approach: MERGE WITH SCHEMA EVOLUTION
MERGE WITH SCHEMA EVOLUTION INTO target AS t
USING source AS s ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
-- ↑ New columns in source are automatically added to target schema

-- PySpark approach: mergeSchema option
df.write.format('delta').mode('append').option('mergeSchema', 'true').save(volume_path)
```

**Column Rename/Drop — without rewriting Parquet:**
```sql
-- Requires: ALTER TABLE ... SET TBLPROPERTIES('delta.columnMapping.mode' = 'name')
ALTER TABLE ... RENAME COLUMN product_qty TO product_quantity
ALTER TABLE ... DROP COLUMN promocode
```

> **Key insight:** Both rename and drop are **logical operations only** — they add a JSON entry to the Delta transaction log without touching the Parquet file. The physical column name in the Parquet file remains unchanged; the mapping is handled at the metadata level.

---

### 5. Auto Loader (`Aotoloader Demo1.py` + Demo2, Demo3)

```python
orders_df = (spark.readStream
    .format('cloudFiles')                          # Auto Loader trigger
    .option('cloudFiles.format', 'csv')
    .option('cloudFiles.inferSchema', 'true')       # Infers column count
    .option('cloudFiles.schemaLocation', checkpoint_file)  # Schema tracked for evolution
    .option('cloudFiles.inferColumnTypes', 'true')  # Type inference for CSV/JSON/XML
    .load(orders_file)
)

orders_df.writeStream
    .format('delta')
    .option('checkpointLocation', checkpoint_file)
    .option('mergeSchema', 'true')
    .outputMode('append')
    .trigger(availableNow=True)
    .toTable('orders_delta')
```

**Schema evolution behaviour tested:**
- New column in incoming file → `[UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE]` → re-run `readStream` to pick up new schema
- Wrong data type in column → data rescued to `_rescued_data` column automatically (no failure)

**How Auto Loader tracks files:** Uses **RocksDB** (a fast key-value store) in the checkpoint location to record every ingested file path. On restart, it reads from RocksDB to know which files are already processed — giving exactly-once semantics.

---

### 6. Partitioning Deep Dive (`Partitioning.py`)

Builds a country-partitioned orders dataset (India, USA, UK, Germany, Canada) and demonstrates:

```python
df.write.format("delta").mode("overwrite").partitionBy("country").save(volume_path)

# Query with partition pruning (reads only India partition)
SELECT sum(qty) FROM orders WHERE country = 'India'  # reads 1 partition ✅

# Query without partition column (reads all partitions)
SELECT sum(qty) FROM orders WHERE unit_price > 100.00  # reads all partitions ⚠️

# After OPTIMIZE: 10 small files per partition → 1 file per partition
OPTIMIZE delta.`volume_path`
```

**Drawbacks demonstrated:**
- High cardinality on `order_id` → one file per order = extreme small file problem
- Data skew: `vendor_id=1` has far fewer records than `vendor_id=2`
- Cannot change partition column without full table rewrite

---

## 🚀 How to Run

1. Import notebooks to Databricks Community Edition workspace
2. For NYC Taxi demos: data is available at `dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow/`
3. For orders demos: create a Volume and upload the CSV/JSON datasets from the `datasets/` folder
4. Run notebooks in order within each concept folder

---

## 🔗 Related Projects

- [E-Commerce Orders Pipeline (Batch + Databricks Jobs)](https://github.com/rishabhjhingran/ecommerce-orders-pipeline-databricks)
- [Structured Streaming Pipeline](https://github.com/rishabhjhingran/structured-streaming-databricks)

---

*Built by [Rishabh Jhingran](https://github.com/rishabhjhingran) · Databricks Certified Data Engineer Professional*
