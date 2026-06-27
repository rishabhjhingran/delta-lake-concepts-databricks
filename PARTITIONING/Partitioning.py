# Databricks notebook source
# MAGIC %md
# MAGIC ### Lets Create partitions now 

# COMMAND ----------

# MAGIC %md 
# MAGIC > DATA SKIPPING - 
# MAGIC - Now wouldnt it be faster if instead of 1000 files we just use 800 files or ordered it to 10 files to fetch our data , for that we use skipping some data that is not required by us.
# MAGIC - This is done by putting layout over data using partioning.
# MAGIC
# MAGIC This can be done using thre concepts -
# MAGIC - Partioning
# MAGIC - Z ordering
# MAGIC - Liquid Clustering
# MAGIC
# MAGIC > Lets Start with Partitioning- 
# MAGIC Lets consider a scenario where we have sales record of multiple countries INDIA,AUSTRALIA ETC. wouldnt it be beneficial we use partition out data with country that ways we can get the results of the country records faster.
# MAGIC
# MAGIC > How does databricks does parttitoning-
# MAGIC - By dividing main folder into partition folders that is subfolders for each category.
# MAGIC - parquet files are created for each partition individually.
# MAGIC - partitioning is always done on low cardinality column that means column with less unique values.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC > Creation of partition Demo

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets try it by creating a volume 

# COMMAND ----------

# MAGIC %sql
# MAGIC create volume if not exists partitioning_demo

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/default/partitioning_demo/orders_managed')

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets add our data to the volume and give the partition key as country 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DecimalType
from pyspark.sql import SparkSession
from decimal import Decimal

# Initialize Spark session (Databricks usually provides spark by default)
spark = SparkSession.builder.getOrCreate()

# Define schema
schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("sku", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("unit_price", DecimalType(10, 2), True)
])
countries = ["India", "USA", "UK", "Germany", "Canada"]

data = []
order_id_start = 1000
for idx, country in enumerate(countries):
    for j in range(10):
        order_id = order_id_start + (idx * 10) + j + 1
        sku = f"SKU-{order_id:03d}"
        qty = (j % 10) + 1
        # IMPORTANT: wrap in Decimal instead of float
        unit_price = Decimal(str(round(((order_id * 3.7) % 250) + 20, 2)))
        data.append((order_id, sku, qty, country, unit_price))



# Create DataFrame
df = spark.createDataFrame(data, schema)
volume_path = "/Volumes/workspace/default/partitioning_demo/orders_managed/"

df.write.format("delta").mode("overwrite").partitionBy("country").save(volume_path)


# COMMAND ----------

# MAGIC %md
# MAGIC > Now we can clearly see there is 1 folder created for each country and it will have its own parquet files.

# COMMAND ----------

# MAGIC %md
# MAGIC > Now since we did not end the auto optimize to be true we see just 1 parquet file that is optiomized.
# MAGIC
# MAGIC > There will be no country dat stored in the partition files since it already has the folder name created with it.
# MAGIC
# MAGIC > Lets create a scenario where we dont have the auto optimize to true 
# MAGIC

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS  delta.`/Volumes/workspace/default/partitioning_demo/orders_managed/`

# COMMAND ----------

# MAGIC %md
# MAGIC > If you still find the files in volume delete it manually
# MAGIC
# MAGIC > Create a new volume table by addign partition and auto optimize to false

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DecimalType
from pyspark.sql import SparkSession
from decimal import Decimal

# Initialize Spark session (Databricks usually provides spark by default)
spark = SparkSession.builder.getOrCreate()

# Define schema
schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("sku", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("unit_price", DecimalType(10, 2), True)
])
countries = ["India", "USA", "UK", "Germany", "Canada"]

data = []
order_id_start = 1000
for idx, country in enumerate(countries):
    for j in range(10):
        order_id = order_id_start + (idx * 10) + j + 1
        sku = f"SKU-{order_id:03d}"
        qty = (j % 10) + 1
        # IMPORTANT: wrap in Decimal instead of float
        unit_price = Decimal(str(round(((order_id * 3.7) % 250) + 20, 2)))
        data.append((order_id, sku, qty, country, unit_price))



# Create DataFrame
df = spark.createDataFrame(data, schema)
volume_path = "/Volumes/workspace/default/partitioning_demo/orders_managed/"

df.write.format("delta") \
  .mode("overwrite") \
  .partitionBy("country") \
  .option("delta.autoOptimize.optimizeWrite", "false") \
  .option("delta.autoOptimize.autoCompact", "false") \
  .save(volume_path)


# COMMAND ----------

df.repartition(2, "country") \
  .write.format("delta") \
  .mode("overwrite") \
  .partitionBy("country") \
  .save(volume_path)


# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO delta.`/Volumes/workspace/default/partitioning_demo/orders_managed/`
# MAGIC VALUES
# MAGIC   (2001, 'SKU-2001', 1, 'India', 199.99),
# MAGIC   (2002, 'SKU-2002', 2, 'India', 49.50),
# MAGIC   (2003, 'SKU-2003', 3, 'India', 9.99),
# MAGIC   (2004, 'SKU-2004', 4, 'India', 25.00),
# MAGIC   (2005, 'SKU-2005', 5, 'India', 75.25),
# MAGIC   (2006, 'SKU-2006', 6, 'India', 120.00),
# MAGIC   (2007, 'SKU-2007', 7, 'India', 33.33),
# MAGIC   (2008, 'SKU-2008', 8, 'India', 88.88),
# MAGIC   (2009, 'SKU-2009', 9, 'India', 150.00),
# MAGIC   (2010, 'SKU-2010', 10, 'India', 60.00),
# MAGIC
# MAGIC   (2011, 'SKU-2011', 1, 'USA', 199.99),
# MAGIC   (2012, 'SKU-2012', 2, 'USA', 49.50),
# MAGIC   (2013, 'SKU-2013', 3, 'USA', 9.99),
# MAGIC   (2014, 'SKU-2014', 4, 'USA', 25.00),
# MAGIC   (2015, 'SKU-2015', 5, 'USA', 75.25),
# MAGIC   (2016, 'SKU-2016', 6, 'USA', 120.00),
# MAGIC   (2017, 'SKU-2017', 7, 'USA', 33.33),
# MAGIC   (2018, 'SKU-2018', 8, 'USA', 88.88),
# MAGIC   (2019, 'SKU-2019', 9, 'USA', 150.00),
# MAGIC   (2020, 'SKU-2020', 10, 'USA', 60.00),
# MAGIC
# MAGIC   (2021, 'SKU-2021', 1, 'UK', 199.99),
# MAGIC   (2022, 'SKU-2022', 2, 'UK', 49.50),
# MAGIC   (2023, 'SKU-2023', 3, 'UK', 9.99),
# MAGIC   (2024, 'SKU-2024', 4, 'UK', 25.00),
# MAGIC   (2025, 'SKU-2025', 5, 'UK', 75.25),
# MAGIC   (2026, 'SKU-2026', 6, 'UK', 120.00),
# MAGIC   (2027, 'SKU-2027', 7, 'UK', 33.33),
# MAGIC   (2028, 'SKU-2028', 8, 'UK', 88.88),
# MAGIC   (2029, 'SKU-2029', 9, 'UK', 150.00),
# MAGIC   (2030, 'SKU-2030', 10, 'UK', 60.00),
# MAGIC
# MAGIC   (2031, 'SKU-2031', 1, 'Germany', 199.99),
# MAGIC   (2032, 'SKU-2032', 2, 'Germany', 49.50),
# MAGIC   (2033, 'SKU-2033', 3, 'Germany', 9.99),
# MAGIC   (2034, 'SKU-2034', 4, 'Germany', 25.00),
# MAGIC   (2035, 'SKU-2035', 5, 'Germany', 75.25),
# MAGIC   (2036, 'SKU-2036', 6, 'Germany', 120.00),
# MAGIC   (2037, 'SKU-2037', 7, 'Germany', 33.33),
# MAGIC   (2038, 'SKU-2038', 8, 'Germany', 88.88),
# MAGIC   (2039, 'SKU-2039', 9, 'Germany', 150.00),
# MAGIC   (2040, 'SKU-2040', 10, 'Germany', 60.00),
# MAGIC
# MAGIC   (2041, 'SKU-2041', 1, 'Canada', 199.99),
# MAGIC   (2042, 'SKU-2042', 2, 'Canada', 49.50),
# MAGIC   (2043, 'SKU-2043', 3, 'Canada', 9.99),
# MAGIC   (2044, 'SKU-2044', 4, 'Canada', 25.00),
# MAGIC   (2045, 'SKU-2045', 5, 'Canada', 75.25),
# MAGIC   (2046, 'SKU-2046', 6, 'Canada', 120.00),
# MAGIC   (2047, 'SKU-2047', 7, 'Canada', 33.33),
# MAGIC   (2048, 'SKU-2048', 8, 'Canada', 88.88),
# MAGIC   (2049, 'SKU-2049', 9, 'Canada', 150.00),
# MAGIC   (2050, 'SKU-2050', 10, 'Canada', 60.00);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL delta.`/Volumes/workspace/default/partitioning_demo/orders_managed/`

# COMMAND ----------

# MAGIC %md
# MAGIC > You can see 10 files being added for the whole table including partitions

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets Check from the sql 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS delta.`/Volumes/workspace/default/partitioning_demo/orders_managed/`

# COMMAND ----------

# MAGIC %md
# MAGIC > Now lets run a query to see the performance

# COMMAND ----------

# DBTITLE 1,y
# MAGIC %sql
# MAGIC Select sum(qty) as total_qty from delta.`/Volumes/workspace/default/partitioning_demo/orders_managed/` WHERE country='India';

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if we check the performance.
# MAGIC - it read only the files that were part of the partition India that improves the performance of the select query.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from delta.`/Volumes/workspace/default/partitioning_demo/orders_managed/`

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if we see the performance of the above query we find it didnot read any files and just read the metadata for the response.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(qty) as total_qty FROM delta.`/Volumes/workspace/default/partitioning_demo/orders_managed/` where unit_price>100.00;

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if we see it had to read 9 files across all partitions to get us this data.
# MAGIC
# MAGIC > When there is no where clause in the partitioned table it will have to read all the files and partitions.

# COMMAND ----------

# MAGIC %md
# MAGIC > Lets optimize the files

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/workspace/default/partitioning_demo/orders_managed/`

# COMMAND ----------

# MAGIC %md
# MAGIC > Now if we see that all the 10 files are removed from the partitions and just 1 parquet file for each partition.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(qty) as total_qty FROM delta.`/Volumes/workspace/default/partitioning_demo/orders_managed/` where unit_price>100.00;

# COMMAND ----------

# MAGIC %md
# MAGIC > We will find that it had to read only 3 files

# COMMAND ----------

# MAGIC %md
# MAGIC ## NOTES -
# MAGIC - > Columns with low cardinality need to be selected.
# MAGIC - > So if we apply on order_id there will be huge number of files.
# MAGIC - > No obvious benefit of data skipping.
# MAGIC - > If usage pattern changes then it will have to create a new table for partioned.
# MAGIC - > Can have smnall file problems.
# MAGIC - > High Chances of data skewness -- one partition might have more values and one partition might have less values ----out of memory error.
# MAGIC - > Always have to define partition columns at the creation level