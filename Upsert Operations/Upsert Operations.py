# Databricks notebook source
# MAGIC %md # SESSION 03/12/2025

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT OPERATIONS
# MAGIC #### UPSERT means update/insert/delte operations together in a sing query
# MAGIC #### Use Case when we insert/update/delete data on the already inserted delta table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a delta table using python dataframe StructField and StructType

# COMMAND ----------

from decimal import Decimal
from pyspark.sql.types import StructType, StructField, StringType, LongType,StringType,IntegerType,DecimalType
data=[
    (1,'sku-1001','yoga','fitness',3,Decimal("109.00")),
    (2,'sku-2001','dumbell','fitness',5,Decimal("509.00")),
    (3,'sku-3001','mat','fitness',1,Decimal("809.00")),
    (4,'sku-4001','back massager','fitness',7,Decimal("459.00")),
    (5,'sku-5001','protein bar','fitness',4,Decimal("809.00"))
]
schema=StructType([
    StructField('product_id',LongType(),False),
    StructField('product_sku',StringType(),True),
    StructField('product_name',StringType(),True),
    StructField('product_category',StringType(),True),
    StructField('product_qty',IntegerType(),True),
    StructField('product_price',DecimalType(10,2),True)
])
df=spark.createDataFrame(data,schema=schema)
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md #### Now Lets create a new volume for our demo 

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Volume if not exists upsert_volume

# COMMAND ----------

# MAGIC %md #### Create a folder inside volume to store the data

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/default/upsert_volume/set1')

# COMMAND ----------

# MAGIC %md #### Now lets write the data into the volume with delta table format

# COMMAND ----------

volume_path='/Volumes/workspace/default/upsert_volume/set1'
df.write.format('delta').mode('overwrite').save(volume_path)

# COMMAND ----------

# MAGIC %md ##### Describe the data with delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/upsert_volume/set1/`

# COMMAND ----------

# MAGIC %md ###### We dont see any creation operation above in the history because we created a volume so 1 parquet file and 1 delta log will be created!

# COMMAND ----------

# MAGIC %md #### Now We will create a temperory view to batch our incoming data 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW incoming_batch AS
# MAGIC SELECT * From VALUES
# MAGIC (2,'sku-8001','yoga','fitness',3,105.00),
# MAGIC     (5,'sku-9001','red','fitness',5,205.15),
# MAGIC     (6,'sku-001','white','fitness',5,208.15)
# MAGIC      as t(product_id,product_sku,product_name,product_category,product_qty,product_price)

# COMMAND ----------

# MAGIC %md #### Now we have stored the temp view in incoming_batch, this will be our source data

# COMMAND ----------

# MAGIC %md #### Now if we want to add above data , we see the product_id is common with out original data so to manage that we will have to perform upsert operation
# MAGIC * First we will update the records found in the target table
# MAGIC * Insert the Records that are not present in the target table

# COMMAND ----------

# MAGIC %md
# MAGIC #### MERGE INTO is the sql command we are going to use

# COMMAND ----------

# MAGIC %md #### First Use Case where we perform update and insert

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/workspace/default/upsert_volume/set1` as target
# MAGIC USING incoming_batch as source
# MAGIC ON target.product_id=source.product_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md ##### Now if we see the volume/set1 we can find the parquet file created and the deletion vector also created since we have done and update operation here and finally we also find that it has optimized our parquet file

# COMMAND ----------

# MAGIC %md #### Lets View the Data we created Now

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/upsert_volume/set1`

# COMMAND ----------

# MAGIC %md #### Lets Check the history to find more conclusive view

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/upsert_volume/set1`

# COMMAND ----------

# MAGIC %md
# MAGIC #### USE CASE 2 - Insert/Update and Delete
# MAGIC ##### if the is_deleted value in our new incoming source view is TRUE then delete the record else update or insert 

# COMMAND ----------

# MAGIC %md #### Create Temporary View
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---- Create the temporary view with flag
# MAGIC CREATE OR REPLACE TEMP VIEW incoming_flag AS
# MAGIC SELECT * FROM VALUES
# MAGIC (2,'sku-2001','dumbell','fitness',5,205.15,FALSE),
# MAGIC (3,'sku-3001','plate','fitness',10,100.00,FALSE),
# MAGIC (4,'sku-8001','plate','fitness',3,190.00,TRUE),
# MAGIC (10,'sku-8001','plate','fitness',3,190.00,TRUE)
# MAGIC
# MAGIC AS T(product_id,product_sku,product_name,product_category,product_qty,product_price,is_deleted)
# MAGIC

# COMMAND ----------

# MAGIC %md #### Perform the Merge Operation and view your data

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/workspace/default/upsert_volume/set1` as target
# MAGIC USING incoming_flag as source
# MAGIC ON target.product_id=source.product_id
# MAGIC WHEN MATCHED AND source.is_deleted=true THEN DELETE
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/upsert_volume/set1`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Now we see the above result the common products in source and target product_id 4 record is delted since we chose is_deleted=TRUE and is_deleted =FALSE will simply update the data like in case of 2 and 3 and if product_id is not matched then insert like in case for product_id 10

# COMMAND ----------

# MAGIC %md #### USE CASE 3 - When we want to make the source as the priority that is if the data is not present in source then we delete the data from target source 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Create a view 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW snapshot_data AS
# MAGIC SELECT * FROM VALUES
# MAGIC (1,'sku-8001','data','fitness',3,190.00),
# MAGIC (2,'sku-8001','killer','fitness',3,190.00),
# MAGIC (4,'sku-8001','parle G','fitness',3,190.00)
# MAGIC As t(product_id,product_sku,product_name,product_category,product_qty,product_price)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### MERGE INTO TO CREATE A DELTA TABLE WITH SOURCE AS THE PRIORITY 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/workspace/default/upsert_volume/set1` as target
# MAGIC USING snapshot_data as source
# MAGIC ON target.product_id=source.product_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE THEN DELETE;--- this is the line which provides the source priority here 
# MAGIC
# MAGIC SELECT * from delta.`/Volumes/workspace/default/upsert_volume/set1`
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### If you see the above result we can see that the records that are present in only the source view are available in our data set.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### LETS VIEW THE HISTORY OF OUR DATA TO VIEW THE CHANGES

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/upsert_volume/set1`

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Now if we see the results we can view that for every update operation we find a delete vector and evry optimize operation is autoperformed to create a new parquet file.

# COMMAND ----------

# MAGIC %md 
# MAGIC Sarthak will remind us when we study pyspark to perform the same operations using pyspark!!!!!!!!!!!

# COMMAND ----------

