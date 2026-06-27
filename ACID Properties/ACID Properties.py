# Databricks notebook source
# MAGIC %md
# MAGIC ## 10-12-2025

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACID PROPERTIES

# COMMAND ----------

# MAGIC %md 
# MAGIC ### A demo for databaricks following databrick properties
# MAGIC #### Atomicity-All complete or all fail.
# MAGIC #### Consistency-All the results stay consistent throughout the process , the value visible will be same between two processes.
# MAGIC #### Isolation-No process intereferes with one another they work in their own isolation.
# MAGIC #### Durability- Data will not be corrupted even if their is any system failure.

# COMMAND ----------

# MAGIC %md
# MAGIC #### * We will create a managed table for this demo 

# COMMAND ----------

# MAGIC %sql
# MAGIC -------- view the catalogs
# MAGIC show catalogs;
# MAGIC use catalog workspace;
# MAGIC -------- show the schemas
# MAGIC show schemas;
# MAGIC use schema default;
# MAGIC -------- view the tables
# MAGIC show tables;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create the managed table orders_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_managed;
# MAGIC CREATE OR REPLACE TABLE orders_managed(
# MAGIC   order_id BIGINT,
# MAGIC   sku STRING,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   qty INT,
# MAGIC   unit_price DECIMAL(10,2)
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC ------- view the table
# MAGIC DESCRIBE EXTENDED orders_managed;
# MAGIC ------- insert the records
# MAGIC INSERT INTO orders_managed VALUES 
# MAGIC (1,'sku-1','dumbel','gym_equipment',5,300.50),
# MAGIC (2,'sku-2','kettleburg','gym_equipment',3,309.50),
# MAGIC (3,'sku-3','yoga_mat','gym_equipment',4,400.50),
# MAGIC (4,'sku-4','rods','gym_equipment',1,380.50),
# MAGIC (5,'sku-5','plates','gym_equipment',8,500.50);
# MAGIC ------- describe the table
# MAGIC DESCRIBE EXTENDED orders_managed;
# MAGIC Select * from orders_managed;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding Constraints to the existing table
# MAGIC * Add Not Null to the order_id column

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_managed
# MAGIC ALTER COLUMN order_id SET NOT NULL
# MAGIC

# COMMAND ----------

# MAGIC %md * Add Constraints

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC ALTER TABLE orders_managed
# MAGIC ADD CONSTRAINT chck_qty_price CHECK (qty>0 AND unit_price>0)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### On the fly it will check if your existing data is compliant to the constraint.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### If we check the results above we find that it created a new transactional log for each of the constraints that we added.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lets now try to add a violation

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_managed
# MAGIC VALUES
# MAGIC (4,'sku7','rods','gym_equipment',0,-10.0)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Now if we check the results above we find a DELTA VIOLATION ERROR for our check constraint

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed;
# MAGIC ------ we dont find any transactional log for cirrupted records

# COMMAND ----------

# MAGIC %md
# MAGIC ### ATOMICITY CHECK USING MERGE INTO 
# MAGIC Now we will check the automicity using the Merge Into where Automocity will be maintained i.e. We will check if all the operations are completed or none of them 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### For this testing I will create a volume and use it as delta table because then only we will be able to fully view the logic

# COMMAND ----------

# MAGIC %sql
# MAGIC Create volume if not exists acid_demo;
# MAGIC
# MAGIC     
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/workspace/default/acid_demo/orders_managed')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create the volume as Delta Table

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
volume_path='/Volumes/workspace/default/acid_demo/orders_managed'
df.write.format('delta').mode('overwrite').save(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Now we have 1 parquet and 1 json file currently
# MAGIC  Now lets add constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/acid_demo/orders_managed`
# MAGIC ADD CONSTRAINT check_qty CHECK (product_qty > 0 AND product_price > 0);
# MAGIC
# MAGIC DESCRIBE HISTORY delta.`/Volumes/workspace/default/acid_demo/orders_managed`;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Now if we see after adding constraint we just have 1 more delta log file but just 1 parquet file 
# MAGIC Now lets create a temporary view to add the values 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW order_temp AS
# MAGIC SELECT * FROM VALUES
# MAGIC (4,'sku-4001','back massager','fitness',7,58.0),
# MAGIC     (5,'sku-5001','protein bar','fitness',0,-10.0)
# MAGIC AS T(product_id,product_sku,product_name,product_category,product_qty,product_price)
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### If we see the above dataset we have one positive scenario and 1 negative scenario , lets use merge into to add it to our delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/Volumes/workspace/default/acid_demo/orders_managed` as target
# MAGIC USING order_temp as source
# MAGIC ON target.product_id = source.product_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### UNDERSTANDING -
# MAGIC If you see there will be no transactional logs created for the update/insert operation since one of the row we were inserting is violating the constraint we created but there will be a parquet file created (in our case a deletion vector is created) but since there is no transactional log created that will be of no use.
# MAGIC Thats how the atomicity is maintained either all operations in our case insert takes place or none of them take place.
# MAGIC Consistency is also maintained as we only see the original records even though the other operation took place but since it failed we dont have any corrupt data with us.Durability is maintained since we are running on the same disk.
# MAGIC
# MAGIC Now lets view our table-

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/acid_demo/orders_managed`
# MAGIC ---- it is having just records we initially updated..

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ISOLATION
# MAGIC Isolation example we will create another notebook 

# COMMAND ----------

# MAGIC %md ##### We created another notebook ISOLATION in that we will use 
# MAGIC %run ./path of this notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(product_qty) FROM delta.`/Volumes/workspace/default/acid_demo/orders_managed`;
# MAGIC UPDATE delta.`/Volumes/workspace/default/acid_demo/orders_managed`
# MAGIC SET product_qty=product_qty+10;
# MAGIC
# MAGIC ------ it should ideallhy give 'concurrent append error'
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ### This example will be difficult to depict since it depends a lot on timing so lets understand it better with example.
# MAGIC Isolation -
# MAGIC
# MAGIC There are two types of concurrency controls -
# MAGIC * Pessimistic Concurrency Control - Row level locking , it will lock the rows work on it & then release the lock so others can use it.
# MAGIC * Optimistic Concurrency control (followed by databricks)- it will create versions of the table and people can work concurrently on those versions.
# MAGIC * Now Lets understand the scenario 2 (OCC) using an example -
# MAGIC   * We have two writers w1 and w2 using the version 2 of the table.
# MAGIC   * W1 starts working at t1 and W2 starts five minute later on the same version V2 of the table
# MAGIC   * W2 completes it works and now the version V3 is created for the table.
# MAGIC   * W1 when it will complete its work will see the table version has changed to V3.
# MAGIC   * Now it will check if the changes it has made are consistent with the version V3 of the table if yes -
# MAGIC     * It will merge its change and then V4 will be created
# MAGIC     * If the changes are not consistent with V3 the changes then it will give a 'concurrent append error'
# MAGIC
# MAGIC This is how the isolation is maintained in Databricks.