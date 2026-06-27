# Databricks notebook source
# MAGIC %md
# MAGIC ## NOTES -
# MAGIC
# MAGIC > Delta is a very famous lakehouse file format,
# MAGIC
# MAGIC > Iceberg - Another famous file format used for data engineering 
# MAGIC   > they both have parquet files
# MAGIC   > The difference b/w delta and iceberg is how they store metadata.
# MAGIC
# MAGIC > If the Iceberg client has to read the delta table it has to make a copy of the data in iceberg format
# MAGIC   > It will rewrite the entire parquet file and the metadata.
# MAGIC   > Lets say the file if of 100 GB then it will duplicate 100 GB of data.
# MAGIC   > There will be a huge storage space issue plus the storage cost will also increase 
# MAGIC
# MAGIC > In an ideal case scneario we should not copy the whole data thats where Uniform comes into the picture since they both read the same parquet but store metadata differently we should ideally create twop metadata files which are comparitively less in storage size and cost.
# MAGIC
# MAGIC > So the solution is 1 paarquet file and 2 metadata files 1 for delta format and 1 for iceberg .
# MAGIC
# MAGIC > UNIFORM - multiple formats single copy of the data 
# MAGIC
# MAGIC > Lets try and demonstrate it 

# COMMAND ----------

# MAGIC %md 
# MAGIC > CREATE THE TABLE FIRST

# COMMAND ----------

# MAGIC %sql
# MAGIC ----- create a managed table orders_managed
# MAGIC DROP TABLE IF EXISTS orders_managed;
# MAGIC CREATE OR REPLACE TABLE orders_managed (
# MAGIC   order_id BIGINT,
# MAGIC   sku STRING,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   qty INT,
# MAGIC   unit_price DECIMAL(10,2)
# MAGIC );

# COMMAND ----------

# MAGIC %md 
# MAGIC > To add the option to create meta data in other formats other than delta we need to add it to the TBLPROPERTIES

# COMMAND ----------

# MAGIC %md
# MAGIC > TO create the universal format enabled for Iceberg we first need to disable the deletion vectors and then Purge the table to add the feature of iceberg.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_managed
# MAGIC SET TBLPROPERTIES(
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'false'
# MAGIC );
# MAGIC
# MAGIC -- Purge the table
# MAGIC TRUNCATE TABLE orders_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC > Verify and add extra constraint again to check the deletion vector being disabled 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED orders_managed;
# MAGIC ALTER TABLE orders_managed
# MAGIC SET TBLPROPERTIES(
# MAGIC   'delta.enableChangeDataFeed' = 'true',
# MAGIC   'delta.enableDeletionVectors' = 'false'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC > Add three table properties to enable the UNIFORM Feature 
# MAGIC -   > delta.columnMapping.mode=name
# MAGIC -   > delta.enableIcebergCompatV2=true
# MAGIC -   > delta.universalFormat.enabledFormats=iceberg

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_managed
# MAGIC SET TBLPROPERTIES(
# MAGIC   'delta.columnMapping.mode' = 'name',
# MAGIC   'delta.enableIcebergCompatV2' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats'='iceberg'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FORMATTED orders_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC > In the above results you can find the meta data for both the delta table and the iceberg with its path