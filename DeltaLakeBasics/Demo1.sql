-- Databricks notebook source
CREATE OR REPLACE TABLE orders_managed(
  order_id BIGINT,
  sku STRING,
  product_name STRING,
  product_category STRING,
  qty INT,
  unit_price DECIMAL(10,2)
)USING DELTA

-- COMMAND ----------

USE workspace.default;

INSERT INTO orders_managed VALUES
(1,'A101','Charger','Electronics',2,19.99),
(2,'A102','Cable','Electronics',3,25.99),
(3,'A103','HDMI','Electronics',1,36.99)

-- COMMAND ----------

DESCRIBE FORMATTED orders_managed

-- COMMAND ----------

CREATE CATALOG DeltaLakeCatalog MANA

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC
-- MAGIC DeltaTable.createIfNotExists(spark) \
-- MAGIC     .tableName("order_delta_format") \
-- MAGIC     .addColumn("order_id", "int") \
-- MAGIC     .addColumn("order_date", "string") \
-- MAGIC     .addColumn("order_customer_id", "int") \
-- MAGIC     .addColumn("order_status", "string") \
-- MAGIC     .execute()

-- COMMAND ----------

create volume if not exists workspace.default.delta_volume1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('/Volumes/workspace/default/delta_volume1/ordersdata1')

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC mkdirs '/Volumes/workspace/default/delta_volume1/ordersdata2'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
-- MAGIC data = [
-- MAGIC     ("Rame", 25, "HR", 50000.0, "New York"),
-- MAGIC     ("Rahul", 30, "IT", 65000.0, "San Francisco"),
-- MAGIC    
-- MAGIC ]
-- MAGIC
-- MAGIC # Define schema explicitly for clarity
-- MAGIC schema = StructType([
-- MAGIC     StructField("Name", StringType(), True),
-- MAGIC     StructField("Age", IntegerType(), True),
-- MAGIC     StructField("Department", StringType(), True),
-- MAGIC     StructField("Salary", FloatType(), True),
-- MAGIC     StructField("City", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC df = spark.createDataFrame(data, schema)
-- MAGIC
-- MAGIC # Show DataFrame
-- MAGIC df.show()
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC volume_path='/Volumes/workspace/default/delta_volume1/ordersdata1/'
-- MAGIC df.write.format('delta').save(volume_path)

-- COMMAND ----------

select * from delta.`/Volumes/workspace/default/delta_volume1/ordersdata1/`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format('delta').saveAsTable('orders_data_result')
-- MAGIC

-- COMMAND ----------

SHOW VOLUMES;
DESCRIBE DETAIL DELTA.`/Volumes/workspace/default/delta_volume1/ordersdata1`

-- COMMAND ----------

DESCRIBE EXTENDED DELTA.`/Volumes/workspace/default/delta_volume1/ordersdata1`


-- COMMAND ----------

CREATE OR REPLACE TABLE log_reader
COMMENT 'This will read log json file.'
AS
SELECT * FROM json.`/Volumes/workspace/default/delta_volume1/ordersdata1/_delta_log/00000000000000000000.json`
DELTA

-- COMMAND ----------

SELECT * FROM log_reader

-- COMMAND ----------

CREATE OR REPLACE TABLE orders_read
AS
SELECT * FROM delta.`/Volumes/workspace/default/delta_volume1/ordersdata1`


-- COMMAND ----------

DESCRIBE HISTORY DELTA.`/Volumes/workspace/default/delta_volume1/ordersdata3`


-- COMMAND ----------

create volume if not exists workspace.default.delta_volume2

-- COMMAND ----------

DESCRIBE HISTORY `/Volumes/workspace/default/delta_volume1/ordersdata3`