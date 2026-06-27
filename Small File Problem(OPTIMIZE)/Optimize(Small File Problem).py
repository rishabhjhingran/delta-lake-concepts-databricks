# Databricks notebook source
# MAGIC %md
# MAGIC ### SMALL FILES PROBLEM
# MAGIC > You have 10k files each file is roughly 1 MB
# MAGIC - select avg(sales) from orders_managed.
# MAGIC - it will have to go to open 10k files to find an average.
# MAGIC - 100 files of 1MB will be easy to read
# MAGIC Now consider if files come very 1 hour there is some incremental update which is coming so that will take a lot of time if we have to read each file of 100MB.
# MAGIC - To solve that problem we will have to use optimize command to create an optimized version

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
# MAGIC )USING DELTA;
# MAGIC --- now we will have to remove the auto optimize option for our demonstration 

# COMMAND ----------

# MAGIC %md
# MAGIC > USE Options 
# MAGIC - delta.autoOptimize.optimizeWrite
# MAGIC - delta.autoOptimize.autoCompact
# MAGIC - set the values to False

# COMMAND ----------

# MAGIC %sql
# MAGIC ------- alter the TBL POPERTIES
# MAGIC ALTER TABLE orders_managed SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = false,
# MAGIC                                              delta.autoOptimize.autoCompact = false);

# COMMAND ----------

from decimal import Decimal
from pyspark.sql.functions import col

# Step 1: Read the existing table schema
target_df = spark.table("orders_managed")
target_schema = target_df.schema

# Step 2: Generate 100 records
records = []
for i in range(1, 101):
    record = (
        i,  # order_id
        f"SKU-{i:05d}",  # sku
        f"Product-{i}",  # product_name
        f"Category-{(i % 5) + 1}",  # product_category
        int((i % 10) + 1),  # qty as INT
        Decimal(f"{(i % 50) + 10}.00")  # unit_price as DECIMAL(10,2)
    )
    records.append(record)

# Step 3: Loop through each record and insert one by one
for rec in records:
    df = spark.createDataFrame([rec], ["order_id","sku","product_name","product_category","qty","unit_price"])
    
    # Cast columns to match target schema
    for field in target_schema:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    
    # Step 4: Append into managed table
    df.write.mode("append").saveAsTable("orders_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT _metadata.file_path AS file_path FROM orders_managed

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now if you see above it is going too read 100 files to get your data which will take a lot of time and multiple reads which will cost us more time and if it we try to find an average it will have to go through all 100 files , letr try that 

# COMMAND ----------

query='select avg(unit_price) as avg_price from orders_managed'
res=spark.sql(query).collect()
print(res)

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if you see the performance it had to read 100 files and took 423ms to find an average which is not an ideal scenario now furthermore if we see the history we will find 100 versions of the data but each data contains just 1 row which is waste of our time and space .

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC > Now Lets try to optimize the results and then see the performance
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE orders_managed;

# COMMAND ----------

# MAGIC %md 
# MAGIC > Now we can see in the metric that it removed 100 files and addd 1 file , now lets check meta data to verify it.
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT DISTINCT _metadata.file_path from orders_managed;

# COMMAND ----------

# MAGIC %md 
# MAGIC > If you see above we just see 1 file now lets run the avg command check the performance

# COMMAND ----------

query='select avg(unit_price) as avg_price from orders_managed'
result=spark.sql(query).collect()
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC > The result will be very fast , now you may not see it because the data was a bit less but it is really fast

# COMMAND ----------

# MAGIC %md
# MAGIC ### THEORY -
# MAGIC > We have an option to update the max file size and minimum number of files we want to use for the optimization but that is not possible in serverless but available in clasic compute .
# MAGIC
# MAGIC > spark.databricks.deltaOptimize.maxFileSize~1GB
# MAGIC
# MAGIC > spark.databricks.delta.autoCompactminNumFiles=(triggering point)
# MAGIC
# MAGIC > Optimize uses [bin packing algorithm]- this is the algorithm uses for compacting the files
# MAGIC -     > Logic - First it will sort in descending order
# MAGIC -     > It will create buckets to optimize 
# MAGIC -       > bin1 - 600Mb,300MB,100MB
# MAGIC -       > bin2 - 300MB,300MB,400MB
# MAGIC     This is how bin packing works by dividing small parquet files into busket of 1GB.

# COMMAND ----------

# MAGIC %md
# MAGIC ### AUTO  OPTIMIZE
# MAGIC > Databricks already has an auto Optiomize feature that automatically optimizes files on the fly.
# MAGIC
# MAGIC > Lets demonstrate by creating a new table and using the similar logic to add 100 small files
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ----- create a managed table orders_managed
# MAGIC DROP TABLE IF EXISTS orders_auto_optimize;
# MAGIC CREATE OR REPLACE TABLE orders_auto_optimize (
# MAGIC   order_id BIGINT,
# MAGIC   sku STRING,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   qty INT,
# MAGIC   unit_price DECIMAL(10,2)
# MAGIC )USING DELTA;

# COMMAND ----------

from decimal import Decimal
from pyspark.sql.functions import col

# Step 1: Read the existing table schema
target_df = spark.table("orders_auto_optimize")
target_schema = target_df.schema

# Step 2: Generate 100 records
records = []
for i in range(1, 101):
    record = (
        i,  # order_id
        f"SKU-{i:05d}",  # sku
        f"Product-{i}",  # product_name
        f"Category-{(i % 5) + 1}",  # product_category
        int((i % 10) + 1),  # qty as INT
        Decimal(f"{(i % 50) + 10}.00")  # unit_price as DECIMAL(10,2)
    )
    records.append(record)

# Step 3: Loop through each record and insert one by one
for rec in records:
    df = spark.createDataFrame([rec], ["order_id","sku","product_name","product_category","qty","unit_price"])
    
    # Cast columns to match target schema
    for field in target_schema:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    
    # Step 4: Append into managed table
    df.write.mode("append").saveAsTable("orders_auto_optimize")

# COMMAND ----------

# MAGIC %md 
# MAGIC > We will be able to see that it will automatically keep on optimizing the small files on the fly so that we dont have to do it manually .

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT DISTINCT _metadata.file_path FROM orders_auto_optimize;

# COMMAND ----------

# MAGIC %md
# MAGIC > If we see above the auto optimizer automatically optimized the files when it was writing it hence we see 7 files being used to display or represent our data.